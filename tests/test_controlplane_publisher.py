"""Unit tests for ControlPlanePublisher (spec §7, plan §3.5) — fake writer + ctx, no broker."""

import asyncio
import contextlib
from collections.abc import Callable
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import AwareDatetime

from calfkit.controlplane import ControlPlaneConfig, ControlPlaneRecord, ControlPlaneStamp
from calfkit.controlplane.advert import AdvertInfo
from calfkit.controlplane.publisher import ControlPlanePublisher, control_plane_writer_key


class _Rec(ControlPlaneRecord):
    schema_version: int = 1
    content: str


class _CapRec(ControlPlaneRecord):
    schema_version: int = 1
    content_updated_at: AwareDatetime


class _Node:
    """A node stand-in exposing a fixed factory method named ``make_record``."""

    def __init__(self, node_id: str, factory: Callable[[ControlPlaneStamp], ControlPlaneRecord]) -> None:
        self.node_id = node_id
        self._factory = factory

    def make_record(self, identity: ControlPlaneStamp) -> ControlPlaneRecord:
        return self._factory(identity)


def _advert(topic: str, record: type[ControlPlaneRecord] = _Rec) -> AdvertInfo:
    return AdvertInfo(topic=topic, record=record, name="make_record")


def _rec_factory(content: str = "x") -> Callable[[ControlPlaneStamp], ControlPlaneRecord]:
    def factory(identity: ControlPlaneStamp) -> ControlPlaneRecord:
        return _Rec(**identity.model_dump(), content=content)

    return factory


def _cap_factory(content_updated_at: datetime) -> Callable[[ControlPlaneStamp], ControlPlaneRecord]:
    def factory(identity: ControlPlaneStamp) -> ControlPlaneRecord:
        return _CapRec(**identity.model_dump(), content_updated_at=content_updated_at)

    return factory


def _boom_factory() -> Callable[[ControlPlaneStamp], ControlPlaneRecord]:
    def factory(identity: ControlPlaneStamp) -> ControlPlaneRecord:
        raise RuntimeError("factory boom")

    return factory


class _Writer:
    def __init__(self, log: list[tuple[str, str, str]] | None = None) -> None:
        self.sets: list[tuple[str, str, ControlPlaneRecord]] = []
        self.deletes: list[tuple[str, str]] = []
        self._log = log

    async def set(self, group: str, member: str, value: ControlPlaneRecord) -> None:
        self.sets.append((group, member, value))
        if self._log is not None:
            self._log.append(("set", group, member))

    async def delete(self, group: str, member: str) -> None:
        self.deletes.append((group, member))
        if self._log is not None:
            self._log.append(("delete", group, member))


class _Ctx:
    def __init__(self, resources: dict[str, _Writer]) -> None:
        self.resources = resources


def _key(topic: str) -> str:
    return control_plane_writer_key(topic)


# -- startup: one-shot publish + identity ------------------------------------


async def test_start_publishes_each_advert_once_with_identity() -> None:
    writer = _Writer()
    node = _Node("n1", _rec_factory("x"))
    info = _advert("t")
    pub = ControlPlanePublisher(worker_id="wkr", adverts=[(node, info)], config=ControlPlaneConfig(heartbeat_interval=3600.0))
    ctx = _Ctx({_key("t"): writer})
    await pub.start(ctx)
    try:
        assert len(writer.sets) == 1
        group, member, record = writer.sets[0]
        assert (group, member) == ("n1", "wkr")  # identity is the wire key
        assert record.heartbeat_interval == 3600.0  # stamped from config
        assert record.started_at == pub._started_at
    finally:
        await pub.stop(ctx)


async def test_start_is_fail_loud_on_factory_error() -> None:
    node = _Node("n1", _boom_factory())
    pub = ControlPlanePublisher(worker_id="wkr", adverts=[(node, _advert("t"))], config=ControlPlaneConfig())
    ctx = _Ctx({_key("t"): _Writer()})
    with pytest.raises(RuntimeError, match="factory boom"):
        await pub.start(ctx)  # propagates -> aborts boot


async def test_start_is_fail_loud_on_missing_writer() -> None:
    node = _Node("n1", _rec_factory())
    pub = ControlPlanePublisher(worker_id="wkr", adverts=[(node, _advert("t"))], config=ControlPlaneConfig())
    ctx = _Ctx({})  # writer resource missing -> wiring bug
    with pytest.raises(KeyError):
        await pub.start(ctx)


# -- shutdown: tombstone the declared cross-product, after publish -----------


async def test_stop_tombstones_declared_cross_product() -> None:
    w1, w2 = _Writer(), _Writer()
    node = _Node("n1", _rec_factory())
    pub = ControlPlanePublisher(
        worker_id="wkr",
        adverts=[(node, _advert("t1")), (node, _advert("t2"))],
        config=ControlPlaneConfig(heartbeat_interval=3600.0),
    )
    ctx = _Ctx({_key("t1"): w1, _key("t2"): w2})
    await pub.start(ctx)
    await pub.stop(ctx)
    assert w1.deletes == [("n1", "wkr")]
    assert w2.deletes == [("n1", "wkr")]


async def test_stop_deletes_after_publish_and_sets_flag() -> None:
    log: list[tuple[str, str, str]] = []
    writer = _Writer(log=log)
    node = _Node("n1", _rec_factory())
    pub = ControlPlanePublisher(worker_id="wkr", adverts=[(node, _advert("t"))], config=ControlPlaneConfig(heartbeat_interval=3600.0))
    ctx = _Ctx({_key("t"): writer})
    await pub.start(ctx)
    await pub.stop(ctx)
    assert [op[0] for op in log] == ["set", "delete"]  # publish, then tombstone
    assert pub._shutting_down is True
    assert pub._task is None


# -- pull model: liveness advances, content currency does not ----------------


async def test_content_updated_at_fixed_across_ticks() -> None:
    writer = _Writer()
    content_ts = datetime(2026, 1, 1, tzinfo=timezone.utc)  # node-tracked, never moves here
    node = _Node("n1", _cap_factory(content_ts))
    info = _advert("cap", record=_CapRec)
    pub = ControlPlanePublisher(worker_id="wkr", adverts=[(node, info)], config=ControlPlaneConfig(heartbeat_interval=3600.0))
    ctx = _Ctx({_key("cap"): writer})
    await pub.start(ctx)  # publish #1
    later = datetime.now(tz=timezone.utc) + timedelta(seconds=10)
    await pub._publish_one(ctx, node, info, later)  # publish #2 with advanced now
    await pub.stop(ctx)
    r1, r2 = writer.sets[0][2], writer.sets[1][2]
    assert r1.content_updated_at == r2.content_updated_at == content_ts  # content currency fixed
    assert r2.last_heartbeat_at > r1.last_heartbeat_at  # liveness advanced
    assert r1.started_at == r2.started_at == pub._started_at  # boot time fixed across ticks


# -- loop resilience ---------------------------------------------------------


async def test_loop_is_resilient_to_one_bad_advert() -> None:
    good_writer, bad_writer = _Writer(), _Writer()
    good = _Node("good", _rec_factory())
    bad = _Node("bad", _boom_factory())
    pub = ControlPlanePublisher(
        worker_id="wkr",
        adverts=[(bad, _advert("bad")), (good, _advert("good"))],
        config=ControlPlaneConfig(heartbeat_interval=0.01),
    )
    ctx = _Ctx({_key("good"): good_writer, _key("bad"): bad_writer})
    pub._started_at = datetime.now(tz=timezone.utc)  # start() would set this
    task = asyncio.create_task(pub._loop(ctx))
    await asyncio.sleep(0.06)  # ~6 ticks
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    assert len(good_writer.sets) >= 2  # good advert keeps publishing despite the bad one raising
    assert bad_writer.sets == []  # the bad advert never succeeds, but does not kill the loop


async def test_loop_cancellation_during_publish_propagates() -> None:
    """A cancel landing mid-publish propagates cleanly (the loop re-raises CancelledError)."""
    entered = asyncio.Event()
    release = asyncio.Event()  # never set: the writer blocks until cancelled

    class _BlockingWriter:
        async def set(self, group: str, member: str, value: ControlPlaneRecord) -> None:
            entered.set()
            await release.wait()

        async def delete(self, group: str, member: str) -> None: ...

    node = _Node("n1", _rec_factory())
    pub = ControlPlanePublisher(worker_id="wkr", adverts=[(node, _advert("t"))], config=ControlPlaneConfig(heartbeat_interval=0.01))
    ctx = _Ctx({_key("t"): _BlockingWriter()})  # type: ignore[dict-item]
    pub._started_at = datetime.now(tz=timezone.utc)  # start() would set this
    task = asyncio.create_task(pub._loop(ctx))
    await asyncio.wait_for(entered.wait(), timeout=1.0)  # loop is now awaiting inside _publish_one
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_start_partial_failure_publishes_first_and_spawns_no_loop() -> None:
    # advert #1 publishes, advert #2's factory raises -> start() propagates (fail-loud),
    # and NO heartbeat loop is spawned (the publish loop precedes create_task) -> no orphan task.
    w_ok, w_bad = _Writer(), _Writer()
    ok = _Node("ok", _rec_factory())
    bad = _Node("bad", _boom_factory())
    pub = ControlPlanePublisher(worker_id="wkr", adverts=[(ok, _advert("ok")), (bad, _advert("bad"))], config=ControlPlaneConfig())
    ctx = _Ctx({_key("ok"): w_ok, _key("bad"): w_bad})
    with pytest.raises(RuntimeError, match="factory boom"):
        await pub.start(ctx)
    assert len(w_ok.sets) == 1  # first advert published before the failure
    assert pub._task is None  # no loop spawned -> no orphan task leaked


async def test_loop_sleeps_at_the_configured_interval(monkeypatch: pytest.MonkeyPatch) -> None:
    # Deterministic cadence check: the loop sleeps exactly heartbeat_interval each tick.
    recorded: list[float] = []

    async def fake_sleep(delay: float) -> None:
        recorded.append(delay)
        raise asyncio.CancelledError  # stop the loop right after its first interval sleep

    monkeypatch.setattr("calfkit.controlplane.publisher.asyncio.sleep", fake_sleep)
    node = _Node("n1", _rec_factory())
    pub = ControlPlanePublisher(worker_id="wkr", adverts=[(node, _advert("t"))], config=ControlPlaneConfig(heartbeat_interval=42.0))
    ctx = _Ctx({_key("t"): _Writer()})
    pub._started_at = datetime.now(tz=timezone.utc)
    with pytest.raises(asyncio.CancelledError):
        await pub._loop(ctx)
    assert recorded == [42.0]


async def test_started_at_shared_across_nodes() -> None:
    # One boot time, shared by every hosted node's record (the restart-detection basis).
    writer = _Writer()
    n1, n2 = _Node("n1", _rec_factory()), _Node("n2", _rec_factory())
    pub = ControlPlanePublisher(
        worker_id="wkr",
        adverts=[(n1, _advert("t")), (n2, _advert("t"))],
        config=ControlPlaneConfig(heartbeat_interval=3600.0),
    )
    ctx = _Ctx({_key("t"): writer})
    await pub.start(ctx)
    try:
        assert {record.started_at for _, _, record in writer.sets} == {pub._started_at}
    finally:
        await pub.stop(ctx)
