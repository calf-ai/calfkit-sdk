"""Real-broker (``kafka`` lane) tests for the durable fan-out store (PR-4 + PR-6 4.4).

These exercise :class:`KtablesFanoutBatchStore` against a REAL Redpanda broker — the durability
properties the offline ``FakeFanoutBatchStore`` cannot reproduce: the read-your-own-writes barrier
across folds, tombstone visibility, and the parts/fault carriage surviving a real ktables JSON
encode/decode. Opt-in (``-m kafka`` / ``make test-kafka``); skips cleanly without Docker.
"""

from __future__ import annotations

import pytest

from calfkit.models.error_report import ErrorReport
from calfkit.models.fanout import EnvelopeSnapshot, FanoutOpen, FanoutOutcome, SlotRef
from calfkit.models.payload import TextPart
from calfkit.models.session_context import CallFrame, Stack, WorkflowState
from calfkit.models.state import State
from calfkit.nodes._fanout_store import (
    CloseResume,
    FoldComplete,
    FoldParked,
    KtablesFanoutBatchStore,
    SiblingPending,
    classify_sibling,
    close_batch,
    record_outcome,
)

pytestmark = pytest.mark.kafka


def _snapshot() -> EnvelopeSnapshot:
    own = CallFrame(target_topic="a", callback_topic="caller", frame_id="A")
    return EnvelopeSnapshot(state=State(), stack=WorkflowState(call_stack=Stack([own])), deps={"k": "v"})


def _reg() -> FanoutOpen:
    return FanoutOpen(
        fanout_id="A",
        node_id="n",
        expected=[SlotRef(frame_id="f1", tag="tc1", target_topic="tool.a"), SlotRef(frame_id="f2", tag="tc2", target_topic="tool.b")],
    )


def _resolved(slot: str, tag: str, value: str) -> FanoutOutcome:
    return FanoutOutcome(slot=slot, tag=tag, target_topic=f"tool.{slot}", handled=False, parts=[TextPart(text=value)])


async def test_open_then_read_is_read_your_own_writes(kafka_bootstrap: str, topic_namespace: str) -> None:
    """After OPEN, a barriered read sees the just-written state + basestate (RYOW)."""
    store = KtablesFanoutBatchStore(bootstrap_servers=kafka_bootstrap, node_id=f"{topic_namespace}-n")
    await store.start()
    try:
        await store.open("A", _reg(), _snapshot())
        state = await store.read_state("A")
        assert state is not None
        assert {s.frame_id for s in state.open.expected} == {"f1", "f2"}
        assert state.outcomes == {}
        base = await store.read_basestate("A")
        assert base is not None and base.snapshot.deps == {"k": "v"}
    finally:
        await store.stop()


async def test_record_across_folds_then_close_and_tombstone(kafka_bootstrap: str, topic_namespace: str) -> None:
    """The classify/record/close machine runs over the wire: each record's barrier sees the prior
    fold (RYOW), completion closes, and the tombstone is visible on re-read. The resolved ``parts``
    survive the real ktables JSON round-trip."""
    store = KtablesFanoutBatchStore(bootstrap_servers=kafka_bootstrap, node_id=f"{topic_namespace}-n")
    await store.start()
    try:
        await store.open("A", _reg(), _snapshot())
        assert isinstance(await classify_sibling(store, "A", "f1"), SiblingPending)  # barrier saw the OPEN
        assert isinstance(await record_outcome(store, "A", _resolved("f1", "tc1", "r1")), FoldParked)  # 1 of 2
        assert isinstance(await classify_sibling(store, "A", "f2"), SiblingPending)  # barrier saw fold #1 (RYOW)
        assert isinstance(await record_outcome(store, "A", _resolved("f2", "tc2", "r2")), FoldComplete)  # 2 of 2

        closed = await close_batch(store, "A")
        assert isinstance(closed, CloseResume)
        by_slot = {o.slot: o for o in closed.outcomes}
        assert by_slot["f1"].parts == [TextPart(text="r1")]  # parts survive the real round-trip
        assert by_slot["f2"].parts == [TextPart(text="r2")]
        # Tombstoned at close — a barriered re-read sees the delete (RYOW on the null tombstone).
        assert await store.read_state("A") is None
        assert await store.read_basestate("A") is None
    finally:
        await store.stop()


async def test_fault_outcome_folds_and_survives_real_broker_round_trip(kafka_bootstrap: str, topic_namespace: str) -> None:
    """An unhandled-fault outcome folds and survives the durable ``state`` table round-trip with its
    ``ErrorReport`` TYPED (not a bare dict). This is the durable analog of a tool fault escalating at
    close: the failed slot rides as ``FanoutOutcome.fault`` (the rail carriage), partitioned out at the
    closing fault group. Without this, the durable carriage of the FAILURE path is untested over Kafka.
    """
    store = KtablesFanoutBatchStore(bootstrap_servers=kafka_bootstrap, node_id=f"{topic_namespace}-n")
    await store.start()
    try:
        await store.open("A", _reg(), _snapshot())
        assert isinstance(await record_outcome(store, "A", _resolved("f1", "tc1", "r1")), FoldParked)
        fault = ErrorReport(error_type="callee.boom", message="kaboom")
        fault_outcome = FanoutOutcome(slot="f2", tag="tc2", target_topic="tool.b", handled=False, fault=fault)
        assert isinstance(await record_outcome(store, "A", fault_outcome), FoldComplete)

        closed = await close_batch(store, "A")
        assert isinstance(closed, CloseResume)
        by_slot = {o.slot: o for o in closed.outcomes}
        # The fault is a typed ErrorReport after a REAL ktables round-trip; the resolved sibling kept its parts.
        assert by_slot["f2"].fault is not None and by_slot["f2"].fault.error_type == "callee.boom"
        assert by_slot["f2"].parts is None
        assert by_slot["f1"].parts == [TextPart(text="r1")]
        assert await store.read_state("A") is None  # tombstoned at close
    finally:
        await store.stop()
