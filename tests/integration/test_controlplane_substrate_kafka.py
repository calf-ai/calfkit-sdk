"""Integration (kafka lane): the control-plane substrate end-to-end on a real broker.

A worker-owned ControlPlanePublisher writes instance-keyed records to a real
GroupedKafkaTable; a ControlPlaneView reads them back collapsed. Covers the
advertise→read round-trip, the clean-shutdown tombstone, the 2-worker same-node
replica regression (ADR-0010 / CRITICAL-1), and degraded-view observability
(CRITICAL-4). Broker supplied by the ``kafka_bootstrap`` fixture.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from types import SimpleNamespace

import pytest
from aiokafka.errors import KafkaConnectionError
from ktables import GroupedKafkaTable, GroupedKafkaTableWriter

from calfkit.controlplane import ControlPlaneConfig, ControlPlaneIdentity, ControlPlaneRecord, ControlPlaneView
from calfkit.controlplane.advert import AdvertInfo, advertises
from calfkit.controlplane.publisher import ControlPlanePublisher
from calfkit.nodes import BaseNodeDef

pytestmark = pytest.mark.kafka


class _Rec(ControlPlaneRecord):
    schema_version: int = 1
    kind: str


def _only_advert(node: BaseNodeDef) -> AdvertInfo:
    return next(iter(type(node)._adverts.values()))


async def _start_publisher(
    bootstrap: str, topic: str, node: BaseNodeDef, worker_id: str, *, ensure_topic: bool
) -> tuple[GroupedKafkaTableWriter[_Rec], ControlPlanePublisher, SimpleNamespace]:
    writer: GroupedKafkaTableWriter[_Rec] = GroupedKafkaTableWriter.json(bootstrap_servers=bootstrap, topic=topic, ensure_topic=ensure_topic)
    await writer.start()
    pub = ControlPlanePublisher(
        worker_id=worker_id,
        adverts=[(node, _only_advert(node))],
        config=ControlPlaneConfig(heartbeat_interval=0.05, bootstrap_servers=bootstrap),
    )
    ctx = SimpleNamespace(resources={ControlPlanePublisher._writer_key(topic): writer})
    await pub.start(ctx)
    return writer, pub, ctx


async def _poll(refresh: Callable[[], Awaitable[object]], predicate: Callable[[], bool], *, tries: int = 300, delay: float = 0.02) -> bool:
    for _ in range(tries):
        await refresh()
        if predicate():
            return True
        await asyncio.sleep(delay)
    return predicate()


async def test_advertise_roundtrip_and_clean_tombstone(kafka_bootstrap: str, topic_namespace: str) -> None:
    topic = f"{topic_namespace}.presence"

    class _Node(BaseNodeDef):
        @advertises(topic=topic, record=_Rec)
        def _record(self, identity: ControlPlaneIdentity) -> _Rec:
            return _Rec(**identity.model_dump(), kind="agent")

    node = _Node(node_id="agent-1", subscribe_topics=["agent-1.in"])
    writer, pub, ctx = await _start_publisher(kafka_bootstrap, topic, node, "w1", ensure_topic=True)
    view: ControlPlaneView[_Rec] = ControlPlaneView.open(bootstrap_servers=kafka_bootstrap, topic=topic, record_type=_Rec, ensure_topic=False)
    await view.start()
    try:
        # round-trip: the advertised node shows up, collapsed, with its content
        assert await _poll(view.barrier, lambda: view.get("agent-1") is not None)
        record = view.get("agent-1")
        assert record is not None and record.kind == "agent"
        assert view.online_nodes() == {"agent-1"}

        # clean shutdown tombstones the instance; the view drops the node
        await pub.stop(ctx)
        assert await _poll(view.barrier, lambda: view.get("agent-1") is None)
        assert view.online_nodes() == set()
    finally:
        if pub._task is not None:
            await pub.stop(ctx)
        await view.stop()
        await writer.stop()


async def test_two_worker_same_node_replica_regression(kafka_bootstrap: str, topic_namespace: str) -> None:
    """ADR-0010 / CRITICAL-1: one replica's clean tombstone must not blank a node
    that is still alive on another replica."""
    topic = f"{topic_namespace}.presence"

    class _Node(BaseNodeDef):
        @advertises(topic=topic, record=_Rec)
        def _record(self, identity: ControlPlaneIdentity) -> _Rec:
            return _Rec(**identity.model_dump(), kind="agent")

    # Two instances of the SAME node_id, hosted by two workers (members w-a, w-b).
    node_a = _Node(node_id="shared", subscribe_topics=["shared.in"])
    node_b = _Node(node_id="shared", subscribe_topics=["shared.in"])
    writer_a, pub_a, ctx_a = await _start_publisher(kafka_bootstrap, topic, node_a, "w-a", ensure_topic=True)
    writer_b, pub_b, ctx_b = await _start_publisher(kafka_bootstrap, topic, node_b, "w-b", ensure_topic=False)

    raw: GroupedKafkaTable[_Rec] = GroupedKafkaTable.json(bootstrap_servers=kafka_bootstrap, topic=topic, model=_Rec, ensure_topic=False)
    view: ControlPlaneView[_Rec] = ControlPlaneView.open(bootstrap_servers=kafka_bootstrap, topic=topic, record_type=_Rec, ensure_topic=False)
    await raw.start()
    await view.start()
    try:
        # both instances present under the one node_id
        assert await _poll(raw.barrier, lambda: {"w-a", "w-b"} <= set(raw.members("shared")))

        # worker A shuts down cleanly: it tombstones ONLY its own member key
        await pub_a.stop(ctx_a)
        assert await _poll(raw.barrier, lambda: "w-a" not in raw.members("shared"))

        # the node is STILL online via the surviving instance (the proof)
        assert "w-b" in raw.members("shared")
        await view.barrier()
        assert view.get("shared") is not None
        assert view.online_nodes() == {"shared"}

        # and when B also leaves, the node finally goes offline
        await pub_b.stop(ctx_b)
        assert await _poll(view.barrier, lambda: view.get("shared") is None)
    finally:
        for pub, ctx in ((pub_a, ctx_a), (pub_b, ctx_b)):
            if pub._task is not None:
                await pub.stop(ctx)
        await raw.stop()
        await view.stop()
        await writer_a.stop()
        await writer_b.stop()


async def test_unreachable_view_fails_loud(topic_namespace: str) -> None:
    """CRITICAL-4 (integration): a view that cannot reach its broker fails LOUDLY at
    ``start()`` rather than silently serving an empty snapshot. (The status/failure
    passthrough for a *connected-but-failing* reader is unit-tested separately —
    ktables raises on an unreachable broker rather than serving degraded.)"""
    view: ControlPlaneView[_Rec] = ControlPlaneView.open(
        bootstrap_servers="127.0.0.1:1",  # nothing listening
        topic=f"{topic_namespace}.presence",
        record_type=_Rec,
        catchup_timeout=3.0,
        ensure_topic=False,
    )
    try:
        with pytest.raises(KafkaConnectionError):
            await view.start()
    finally:
        with contextlib.suppress(Exception):
            await view.stop()
