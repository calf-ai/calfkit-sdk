"""Wiring tests for fan-out ktable tuning (spec §11, plan §3.5, ADR-0021).

Covers ``Worker(fanout=...)`` storage, ``KtablesFanoutBatchStore`` forwarding the cadence/
catch-up knobs to its two readers and using ``barrier_timeout``, and the agent resource
threading ``worker._fanout`` into the store. The real store is exercised offline by patching
the ktables ``KafkaTable``/``KafkaTableWriter`` factories.
"""

from __future__ import annotations

import pytest

from calfkit.client import Client
from calfkit.providers.pydantic_ai.model_client import PydanticModelClient
from calfkit.tuning import FanoutConfig, KTableReaderTuning
from calfkit.worker.worker import Worker


class _FakeModel(PydanticModelClient):
    @property
    def model_name(self) -> str:
        return "fake"

    @property
    def system(self) -> str:
        return "fake"

    async def request(self, *args: object, **kwargs: object) -> object:
        raise NotImplementedError


class FakeKafkaTable:
    """Records the kwargs passed to ``KafkaTable.json`` and the barrier timeout used."""

    instances: list[FakeKafkaTable] = []

    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs
        self.status = "ready"
        self.topic = kwargs.get("topic")
        self.barrier_timeouts: list[float | None] = []
        FakeKafkaTable.instances.append(self)

    @classmethod
    def json(cls, *, model: object, **kwargs: object) -> FakeKafkaTable:
        return cls(model=model, **kwargs)

    async def barrier(self, timeout: float | None = None) -> bool:
        self.barrier_timeouts.append(timeout)
        return True

    def get(self, key: str) -> None:
        return None

    async def start(self) -> None: ...

    async def stop(self) -> None: ...


class FakeKafkaTableWriter:
    instances: list[FakeKafkaTableWriter] = []

    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs
        FakeKafkaTableWriter.instances.append(self)

    @classmethod
    def json(cls, *, model: object = None, **kwargs: object) -> FakeKafkaTableWriter:
        return cls(model=model, **kwargs)

    async def start(self) -> None: ...

    async def stop(self) -> None: ...


@pytest.fixture
def fake_ktables(monkeypatch: pytest.MonkeyPatch) -> None:
    FakeKafkaTable.instances = []
    FakeKafkaTableWriter.instances = []
    monkeypatch.setattr("ktables.KafkaTable", FakeKafkaTable)
    monkeypatch.setattr("ktables.KafkaTableWriter", FakeKafkaTableWriter)


def _store(**kwargs: object):
    from calfkit.client._connection import ConnectionProfile
    from calfkit.nodes._fanout_store import KtablesFanoutBatchStore

    return KtablesFanoutBatchStore(
        connection=ConnectionProfile(bootstrap_servers="kafka:9092", security_opts={}, max_message_bytes=5 * 1024 * 1024),
        node_id="agent1",
        **kwargs,
    )


# ── Worker(fanout=) storage ───────────────────────────────────────────────────


def test_worker_stores_fanout_config() -> None:
    cfg = FanoutConfig(barrier_timeout=5.0)
    worker = Worker(Client.connect("kafka:9092"), fanout=cfg)
    assert worker._fanout is cfg


def test_worker_fanout_defaults_when_omitted() -> None:
    worker = Worker(Client.connect("kafka:9092"))
    assert worker._fanout == FanoutConfig()


# ── KtablesFanoutBatchStore construction ──────────────────────────────────────


def test_store_threads_the_profile_connection_to_all_four_clients(fake_ktables: None) -> None:
    # §8.10: the profile-derived ktables connection (guard on writers, floor on readers,
    # security everywhere) must actually reach every client the store builds — a dropped
    # connection here would pass the rest of the offline suite silently.
    _store()
    for client in (*FakeKafkaTable.instances, *FakeKafkaTableWriter.instances):
        conn = client.kwargs["connection"]
        assert conn.bootstrap_servers == "kafka:9092"
        assert conn.producer_opts["max_request_size"] == 5 * 1024 * 1024
        assert conn.consumer_opts["max_partition_fetch_bytes"] == 5 * 1024 * 1024


def test_store_forwards_reader_tuning_to_both_readers(fake_ktables: None) -> None:
    _store(reader_tuning=KTableReaderTuning(poll_timeout_ms=20, fetch_max_wait_ms=10))
    assert len(FakeKafkaTable.instances) == 2  # state + basestate readers
    for reader in FakeKafkaTable.instances:
        assert reader.kwargs["poll_timeout_ms"] == 20
        assert reader.kwargs["fetch_max_wait_ms"] == 10


def test_store_forwards_catchup_timeout_to_both_readers(fake_ktables: None) -> None:
    _store(catchup_timeout=7.0)
    for reader in FakeKafkaTable.instances:
        assert reader.kwargs["catchup_timeout"] == 7.0


def test_store_default_omits_cadence_and_catchup(fake_ktables: None) -> None:
    _store()
    for reader in FakeKafkaTable.instances:
        assert "poll_timeout_ms" not in reader.kwargs
        assert "fetch_max_wait_ms" not in reader.kwargs
        assert "catchup_timeout" not in reader.kwargs


def test_store_forwards_a_single_set_knob(fake_ktables: None) -> None:
    # Partial tuning: exactly the set knob is forwarded; the unset one is omitted.
    _store(reader_tuning=KTableReaderTuning(fetch_max_wait_ms=10))
    for reader in FakeKafkaTable.instances:
        assert reader.kwargs["fetch_max_wait_ms"] == 10
        assert "poll_timeout_ms" not in reader.kwargs


async def test_await_fresh_uses_configured_barrier_timeout(fake_ktables: None) -> None:
    store = _store(barrier_timeout=9.0)
    await store.read_state("X")  # _await_fresh barriers the state reader before reading
    assert store._state_reader.barrier_timeouts == [9.0]


async def test_await_fresh_default_barrier_timeout_is_30(fake_ktables: None) -> None:
    store = _store()
    await store.read_state("X")
    assert store._state_reader.barrier_timeouts == [30.0]  # behavior-preserving (was _BARRIER_TIMEOUT_S)


# ── agent resource threads worker._fanout into the store ──────────────────────


async def test_fanout_resource_passes_worker_fanout(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class SpyStore:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

        async def start(self) -> None: ...

        async def stop(self) -> None: ...

    monkeypatch.setattr("calfkit.nodes.agent.KtablesFanoutBatchStore", SpyStore)

    from calfkit.nodes.agent import StatelessAgent

    agent = StatelessAgent("a", subscribe_topics="a.in", model_client=_FakeModel())
    worker = Worker(
        Client.connect("kafka:9092"),
        nodes=[agent],
        fanout=FanoutConfig(reader_tuning=KTableReaderTuning(poll_timeout_ms=5), catchup_timeout=7.0, barrier_timeout=9.0),
    )
    agent._worker = worker  # the back-reference the resource reads
    gen = agent._fanout_store_resource(None)  # type: ignore[arg-type]
    await anext(gen)
    assert captured["reader_tuning"].poll_timeout_ms == 5  # type: ignore[attr-defined]
    assert captured["catchup_timeout"] == 7.0
    assert captured["barrier_timeout"] == 9.0
    with pytest.raises(StopAsyncIteration):
        await anext(gen)


# ── enable_idempotence: one knob threaded from the client (default: calfkit sets nothing) ─────


def test_store_forwards_enable_idempotence_to_both_writers(fake_ktables: None) -> None:
    _store(enable_idempotence=True)
    writers = FakeKafkaTableWriter.instances
    assert len(writers) == 2  # state + basestate
    for writer in writers:
        assert writer.kwargs["enable_idempotence"] is True


def test_store_forwards_explicit_false_idempotence(fake_ktables: None) -> None:
    _store(enable_idempotence=False)
    for writer in FakeKafkaTableWriter.instances:
        assert writer.kwargs["enable_idempotence"] is False


def test_store_default_omits_enable_idempotence(fake_ktables: None) -> None:
    # Unset (None) -> calfkit passes nothing; the ktables writer default applies.
    _store()
    for writer in FakeKafkaTableWriter.instances:
        assert "enable_idempotence" not in writer.kwargs


async def test_fanout_resource_threads_client_idempotence_opt_in(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class SpyStore:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

        async def start(self) -> None: ...

        async def stop(self) -> None: ...

    monkeypatch.setattr("calfkit.nodes.agent.KtablesFanoutBatchStore", SpyStore)
    from calfkit.nodes.agent import StatelessAgent

    agent = StatelessAgent("a", subscribe_topics="a.in", model_client=_FakeModel())
    worker = Worker(Client.connect("kafka:9092", enable_idempotence=True), nodes=[agent])
    agent._worker = worker
    gen = agent._fanout_store_resource(None)  # type: ignore[arg-type]
    await anext(gen)
    assert captured["enable_idempotence"] is True
    with pytest.raises(StopAsyncIteration):
        await anext(gen)


async def test_fanout_resource_defaults_to_unset_idempotence(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class SpyStore:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

        async def start(self) -> None: ...

        async def stop(self) -> None: ...

    monkeypatch.setattr("calfkit.nodes.agent.KtablesFanoutBatchStore", SpyStore)
    from calfkit.nodes.agent import StatelessAgent

    agent = StatelessAgent("a", subscribe_topics="a.in", model_client=_FakeModel())
    worker = Worker(Client.connect("kafka:9092"), nodes=[agent])
    agent._worker = worker
    gen = agent._fanout_store_resource(None)  # type: ignore[arg-type]
    await anext(gen)
    assert captured["enable_idempotence"] is None
    with pytest.raises(StopAsyncIteration):
        await anext(gen)


async def test_fanout_resource_boots_through_offline_store_mirror() -> None:
    # No SpyStore re-patch here: the autouse `_offline_fanout_store` fixture swaps in the real
    # `OfflineFanoutBatchStore`, so this exercises its signature mirror — it must accept the new
    # kwargs the resource now passes (`reader_tuning`/`catchup_timeout`/`barrier_timeout`), or
    # construction raises `TypeError`. This makes the fake's "loud failure on drift" claim real.
    from calfkit.nodes.agent import StatelessAgent
    from tests._fanout_fakes import OfflineFanoutBatchStore

    agent = StatelessAgent("a", subscribe_topics="a.in", model_client=_FakeModel())
    worker = Worker(
        Client.connect("kafka:9092"),
        nodes=[agent],
        fanout=FanoutConfig(reader_tuning=KTableReaderTuning(poll_timeout_ms=5), catchup_timeout=7.0, barrier_timeout=9.0),
    )
    agent._worker = worker
    gen = agent._fanout_store_resource(None)  # type: ignore[arg-type]
    store = await anext(gen)
    assert isinstance(store, OfflineFanoutBatchStore)
    with pytest.raises(StopAsyncIteration):
        await anext(gen)
