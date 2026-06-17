"""Real-broker (``kafka`` lane) tests for the durable fan-out store (PR-4 step 7).

These exercise :class:`KtablesFanoutBatchStore` against a REAL Redpanda broker — the
durability properties the offline ``FakeFanoutBatchStore`` cannot reproduce: the
read-your-own-writes barrier across folds, tombstone visibility, and the pure
fold/close state machine running over the wire. Opt-in (``-m kafka`` / ``make
test-kafka``); skips cleanly without Docker.
"""

from __future__ import annotations

import pytest

from calfkit._vendor.pydantic_ai.messages import ToolReturn
from calfkit.models.fanout import EnvelopeSnapshot, FanoutOpen, FanoutOutcome, SlotRef
from calfkit.models.session_context import CallFrame, Stack, WorkflowState
from calfkit.models.state import FailedToolCall, State
from calfkit.nodes._fanout_store import (
    CloseResume,
    FoldComplete,
    FoldParked,
    KtablesFanoutBatchStore,
    close_batch,
    fold_sibling,
)

pytestmark = pytest.mark.kafka


def _snapshot() -> EnvelopeSnapshot:
    own = CallFrame(target_topic="a", callback_topic="caller", frame_id="A")
    return EnvelopeSnapshot(state=State(), stack=WorkflowState(call_stack=Stack([own])), deps={"k": "v"})


def _reg() -> FanoutOpen:
    return FanoutOpen(fanout_id="A", node_id="n", expected=[SlotRef(frame_id="f1", tag="tc1"), SlotRef(frame_id="f2", tag="tc2")])


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


async def test_fold_across_folds_then_close_and_tombstone(kafka_bootstrap: str, topic_namespace: str) -> None:
    """The pure fold/close machine runs over the wire: each fold's barrier sees the prior
    fold (RYOW), completion closes + materializes, and the tombstone is visible on re-read."""
    store = KtablesFanoutBatchStore(bootstrap_servers=kafka_bootstrap, node_id=f"{topic_namespace}-n")
    await store.start()
    try:
        await store.open("A", _reg(), _snapshot())
        first = await fold_sibling(store, "A", FanoutOutcome(slot="f1", tag="tc1", result=ToolReturn(return_value="r1")))
        assert isinstance(first, FoldParked)  # 1 of 2 — barrier saw the OPEN
        second = await fold_sibling(store, "A", FanoutOutcome(slot="f2", tag="tc2", result=ToolReturn(return_value="r2")))
        assert isinstance(second, FoldComplete)  # 2 of 2 — barrier saw fold #1 (RYOW)

        closed = await close_batch(store, "A")
        assert isinstance(closed, CloseResume)
        assert closed.snapshot.state.get_tool_result("tc1") == ToolReturn(return_value="r1")
        assert closed.snapshot.state.get_tool_result("tc2") == ToolReturn(return_value="r2")
        # Tombstoned at close — a barriered re-read sees the delete (RYOW on the null tombstone).
        assert await store.read_state("A") is None
        assert await store.read_basestate("A") is None
    finally:
        await store.stop()


async def test_failed_tool_call_folds_and_materializes_typed_over_real_broker(kafka_bootstrap: str, topic_namespace: str) -> None:
    """A return-only tool FAILURE folds and materializes as a TYPED marker over a real broker.

    The offline fake cannot prove this: its fold never crosses real ktables JSON encode/decode, so it
    can't show that a ``FailedToolCall`` survives the durable ``state`` table round-trip as a typed
    instance (via the ``CalfToolResult`` discriminator) rather than a bare dict. This is the durable
    analog of today's tool-failure → caller-strand path; PR-4 is return-only, so the failure rides as
    a ``CalfToolResult`` marker, not yet a typed fault (the fault rail re-homes ``result`` to
    parts/fault). Without this, the durable carriage of the core FAILURE path is untested over Kafka.
    """
    store = KtablesFanoutBatchStore(bootstrap_servers=kafka_bootstrap, node_id=f"{topic_namespace}-n")
    await store.start()
    try:
        await store.open("A", _reg(), _snapshot())
        marker = FailedToolCall.build_safe(tool_name="boom", tool_call_id="tc1", exc_type="ValueError", exc_message="kaboom")
        first = await fold_sibling(store, "A", FanoutOutcome(slot="f1", tag="tc1", result=marker))
        assert isinstance(first, FoldParked)
        second = await fold_sibling(store, "A", FanoutOutcome(slot="f2", tag="tc2", result=ToolReturn(return_value="r2")))
        assert isinstance(second, FoldComplete)

        closed = await close_batch(store, "A")
        assert isinstance(closed, CloseResume)
        materialized = closed.snapshot.state.get_tool_result("tc1")
        assert isinstance(materialized, FailedToolCall)  # typed via the discriminator after a REAL round-trip
        assert materialized.exc_type == "ValueError"
        assert materialized.tool_call_id == "tc1"
        assert closed.snapshot.state.get_tool_result("tc2") == ToolReturn(return_value="r2")
        assert await store.read_state("A") is None  # tombstoned at close
    finally:
        await store.stop()
