"""Phase-A unit tests for the §6.8 staged-pipeline stage methods on ``BaseNodeDef``
(return-only — PR-4).

These sealed stage methods (``_classify`` / ``_aggregate`` / ``_execute`` / ``_stray_check``)
are added additively *before* ``handler`` is rewired to call them, so the flat handler stays
live and the whole suite stays green until the Phase-B switch. The fault rail (PR-6) extends
these stages additively (the seam stages 1/3/6 + fault arms are TODO insertion points).
"""

from __future__ import annotations

import logging
from typing import Any

import pytest
from aiokafka.errors import KafkaError  # type: ignore[import-untyped]

from calfkit._protocol import HDR_KIND
from calfkit._vendor.pydantic_ai.messages import ToolReturn
from calfkit.models import ReturnCall
from calfkit.models.envelope import Envelope
from calfkit.models.fanout import EnvelopeSnapshot, FanoutOpen, FanoutOutcome, SlotRef
from calfkit.models.reply import ReturnMessage
from calfkit.models.session_context import CallFrame, SessionRunContext, Stack, WorkflowState
from calfkit.models.state import State
from calfkit.nodes._fanout_store import FANOUT_STORE_KEY
from calfkit.nodes.base import _CONSUMED, _DECLINED, _BatchClosed, _BatchOpen
from calfkit.nodes.node import NodeDef
from tests._fanout_fakes import FakeFanoutBatchStore


def _node() -> NodeDef[Any]:
    return NodeDef(node_id="n", subscribe_topics=["t"])


class _FanoutNode(NodeDef[Any]):
    """A minimal fan-out-capable node (no agent machinery) for unit-testing the stages."""

    @property
    def _is_fanout_capable(self) -> bool:
        return True


class _BodyNode(NodeDef[Any]):
    """A fan-out-capable node whose body returns a terminal action (for the _execute tests)."""

    @property
    def _is_fanout_capable(self) -> bool:
        return True

    async def run(self, ctx: SessionRunContext) -> Any:
        return ReturnCall(state=ctx.state, value="done")


class _CaptureBroker:
    """Records (topic, envelope) per publish — for the re-entry self-publish."""

    def __init__(self) -> None:
        self.published: list[tuple[str, Any]] = []

    async def publish(self, envelope: Any, *, topic: str, correlation_id: str, key: bytes, headers: dict[str, str]) -> None:
        self.published.append((topic, envelope))


class _RaisingBroker:
    """A broker stub whose ``publish`` always raises ``KafkaError`` — exercises the
    re-entry-publish-failure abort path in ``_aggregate``'s ``FoldComplete`` arm."""

    async def publish(self, envelope: Any, *, topic: str, correlation_id: str, key: bytes, headers: dict[str, str]) -> None:
        raise KafkaError("simulated re-entry publish failure")


def _fanout_node() -> _FanoutNode:
    return _FanoutNode(node_id="fan", subscribe_topics=["fan.in"])


def _store_ctx(store: FakeFanoutBatchStore, *, state: State | None = None, deps: dict[str, Any] | None = None) -> SessionRunContext:
    ctx = SessionRunContext(state=state if state is not None else State(), deps=deps or {})
    ctx._resources = {FANOUT_STORE_KEY: store}
    ctx._correlation_id = "corr-1"
    return ctx


def _marked_env(*, in_reply_to: str, tag: str | None = "tc1", state: State | None = None) -> Envelope:
    # A marked fan-out frame (fanout_id == frame_id == "A") on top, with a reply slot.
    frame = CallFrame(target_topic="fan", callback_topic="caller", frame_id="A", fanout_id="A")
    return Envelope(
        context=SessionRunContext(state=state if state is not None else State(), deps={}),
        internal_workflow_state=WorkflowState(call_stack=Stack([frame])),
        reply=ReturnMessage(in_reply_to=in_reply_to, tag=tag, parts=[]),
    )


def _plain_env(*, reply: ReturnMessage | None = None) -> Envelope:
    # An ordinary (non-fan-out) delivery: a single unmarked frame on top.
    frame = CallFrame(target_topic="b", callback_topic="caller", frame_id="A")
    return Envelope(
        context=SessionRunContext(state=State(), deps={}),
        internal_workflow_state=WorkflowState(call_stack=Stack([frame])),
        reply=reply,
    )


async def _open(store: FakeFanoutBatchStore, *, snap_state: State | None = None, deps: dict[str, Any] | None = None) -> None:
    own = CallFrame(target_topic="fan", callback_topic="caller", frame_id="A")  # UNMARKED — the pre-stamp snapshot frame
    snap = EnvelopeSnapshot(state=snap_state if snap_state is not None else State(), stack=WorkflowState(call_stack=Stack([own])), deps=deps or {})
    reg = FanoutOpen(fanout_id="A", node_id="fan", expected=[SlotRef(frame_id="f1", tag="tc1"), SlotRef(frame_id="f2", tag="tc2")])
    await store.open("A", reg, snap)


class TestClassify:
    """``_classify`` reads the inbound ``x-calf-kind`` header into the delivery kind that
    drives stage routing (only ``call``/``return`` exist pre-rail; missing ⇒ ``call``)."""

    def test_missing_kind_header_is_call(self) -> None:
        assert _node()._classify({}) == "call"

    def test_return_kind_header_is_return(self) -> None:
        assert _node()._classify({HDR_KIND: "return"}) == "return"

    def test_call_kind_header_is_call(self) -> None:
        assert _node()._classify({HDR_KIND: "call"}) == "call"

    def test_kind_header_decoded_from_bytes(self) -> None:
        # Over the wire header values arrive as bytes; _classify must decode them.
        assert _node()._classify({HDR_KIND: b"return"}) == "return"


class TestAggregate:
    """``_aggregate`` is the durable fold/close stage (§6.8 stage-2). It returns
    ``_BatchClosed`` (proceed to the body — a completed fan-out close OR a stateless
    single-call continuation) or ``_BatchOpen`` (park — an incomplete fold or a no-op close).
    """

    async def test_stateless_continuation_is_batch_closed(self) -> None:
        # An unmarked frame (a single-call return) is not a fan-out → proceed to the body.
        node = _fanout_node()
        frame = CallFrame(target_topic="fan", callback_topic="caller", frame_id="A")  # unmarked
        env = Envelope(
            context=SessionRunContext(state=State(), deps={}),
            internal_workflow_state=WorkflowState(call_stack=Stack([frame])),
            reply=ReturnMessage(in_reply_to="A", tag="tc1", parts=[]),
        )
        result = await node._aggregate(_store_ctx(FakeFanoutBatchStore()), env, "corr-1", _CaptureBroker())
        assert isinstance(result, _BatchClosed)

    async def test_sibling_fold_incomplete_parks(self) -> None:
        node = _fanout_node()
        store = FakeFanoutBatchStore()
        await _open(store)
        st = State()
        st.add_tool_result("tc1", ToolReturn(return_value="r1"))
        result = await node._aggregate(_store_ctx(store, state=st), _marked_env(in_reply_to="f1", tag="tc1", state=st), "corr-1", _CaptureBroker())
        assert isinstance(result, _BatchOpen)  # 1 of 2 → parked
        state = await store.read_state("A")
        assert state is not None and set(state.outcomes) == {"f1"}

    async def test_sibling_fold_complete_publishes_reentry_and_parks(self) -> None:
        node = _fanout_node()
        store = FakeFanoutBatchStore()
        await _open(store)
        broker = _CaptureBroker()
        st1 = State()
        st1.add_tool_result("tc1", ToolReturn(return_value="r1"))
        await node._aggregate(_store_ctx(store, state=st1), _marked_env(in_reply_to="f1", tag="tc1", state=st1), "corr-1", broker)
        st2 = State()
        st2.add_tool_result("tc2", ToolReturn(return_value="r2"))
        result = await node._aggregate(_store_ctx(store, state=st2), _marked_env(in_reply_to="f2", tag="tc2", state=st2), "corr-1", broker)
        assert isinstance(result, _BatchOpen)  # still parked — the re-entry is a fresh delivery
        assert [t for t, _ in broker.published] == ["fan.private.return"]  # closure re-entry self-published
        reentry_env = broker.published[0][1]
        assert reentry_env.reply is not None and reentry_env.reply.in_reply_to == "A"

    async def test_reentry_close_restores_context_and_is_batch_closed(self) -> None:
        node = _fanout_node()
        store = FakeFanoutBatchStore()
        await _open(store, snap_state=State(), deps={"k": "v"})  # snapshot carries deps + a pre-tool state
        await store.fold("A", FanoutOutcome(slot="f1", tag="tc1", result=ToolReturn(return_value="r1")))
        await store.fold("A", FanoutOutcome(slot="f2", tag="tc2", result=ToolReturn(return_value="r2")))
        # The re-entry envelope arrives with cleared context (as _publish_reentry builds it).
        ctx = _store_ctx(store)
        env = _marked_env(in_reply_to="A", tag=None)  # in_reply_to == frame_id "A" → re-entry close
        result = await node._aggregate(ctx, env, "corr-1", _CaptureBroker())
        assert isinstance(result, _BatchClosed)
        # ctx restored from the snapshot, with both outcomes materialized into State.
        assert ctx.state.get_tool_result("tc1") == ToolReturn(return_value="r1")
        assert ctx.state.get_tool_result("tc2") == ToolReturn(return_value="r2")
        assert ctx.deps == {"k": "v"}
        # The envelope stack is restored to the UNMARKED snapshot frame so the resumed body's
        # ReturnCall unwinds the original fan-out frame back to its caller.
        assert env.internal_workflow_state.current_frame.frame_id == "A"
        assert env.internal_workflow_state.current_frame.fanout_id is None
        assert env.context.deps == {"k": "v"}
        assert await store.read_state("A") is None  # tombstoned at close

    async def test_reentry_spurious_incomplete_parks(self) -> None:
        node = _fanout_node()
        store = FakeFanoutBatchStore()
        await _open(store)  # opened, no folds → incomplete
        result = await node._aggregate(_store_ctx(store), _marked_env(in_reply_to="A", tag=None), "corr-1", _CaptureBroker())
        assert isinstance(result, _BatchOpen)  # spurious early re-entry → no-op park
        assert await store.read_state("A") is not None  # batch left untouched

    async def test_completing_fold_reentry_publish_failure_aborts_and_tombstones(self) -> None:
        # (#1) The completing fold tries to self-publish the re-entry; if that publish raises
        # KafkaError, _aggregate must NOT propagate — it aborts (tombstones both records) and
        # still parks (_BatchOpen), so the caller strands rather than a complete-but-unclosed batch.
        node = _fanout_node()
        store = FakeFanoutBatchStore()
        await _open(store)
        st1 = State()
        st1.add_tool_result("tc1", ToolReturn(return_value="r1"))
        await node._aggregate(_store_ctx(store, state=st1), _marked_env(in_reply_to="f1", tag="tc1", state=st1), "corr-1", _CaptureBroker())
        # The SECOND fold completes the batch; its re-entry publish raises.
        st2 = State()
        st2.add_tool_result("tc2", ToolReturn(return_value="r2"))
        result = await node._aggregate(_store_ctx(store, state=st2), _marked_env(in_reply_to="f2", tag="tc2", state=st2), "corr-1", _RaisingBroker())
        assert isinstance(result, _BatchOpen)  # did not propagate; parked
        assert await store.read_state("A") is None  # aborted: both records tombstoned
        assert await store.read_basestate("A") is None

    async def test_sibling_reply_without_in_reply_to_does_not_fold(self, caplog: pytest.LogCaptureFixture) -> None:
        # (#6) A marked sibling reply that carries no in_reply_to is malformed: _aggregate must
        # short-circuit to park (_BatchOpen), NOT fold (the store's outcomes are unchanged), and
        # log a distinct "malformed sibling reply" error (proving the guard fired, not the
        # foreign-stray fallthrough).
        node = _fanout_node()
        store = FakeFanoutBatchStore()
        await _open(store)
        env = _marked_env(in_reply_to="f1", tag="tc1")
        env.reply.in_reply_to = None  # malformed: marked sibling but no slot
        with caplog.at_level(logging.ERROR, logger="calfkit.nodes.base"):
            result = await node._aggregate(_store_ctx(store), env, "corr-1", _CaptureBroker())
        assert isinstance(result, _BatchOpen)
        state = await store.read_state("A")
        assert state is not None and state.outcomes == {}  # nothing folded
        assert any("malformed sibling reply" in r.getMessage() for r in caplog.records)

    async def test_sibling_foreign_slot_parks_store_unchanged(self) -> None:
        # (coverage c) A reply to a slot not in the batch is a foreign stray → park, store untouched.
        node = _fanout_node()
        store = FakeFanoutBatchStore()
        await _open(store)
        result = await node._aggregate(_store_ctx(store), _marked_env(in_reply_to="f99", tag="tcX"), "corr-1", _CaptureBroker())
        assert isinstance(result, _BatchOpen)
        state = await store.read_state("A")
        assert state is not None and state.outcomes == {}

    async def test_sibling_duplicate_slot_parks_store_unchanged(self) -> None:
        # (coverage c) A second reply for an already-folded slot is a duplicate stray → park.
        node = _fanout_node()
        store = FakeFanoutBatchStore()
        await _open(store)
        st = State()
        st.add_tool_result("tc1", ToolReturn(return_value="r1"))
        await node._aggregate(_store_ctx(store, state=st), _marked_env(in_reply_to="f1", tag="tc1", state=st), "corr-1", _CaptureBroker())
        result = await node._aggregate(_store_ctx(store, state=st), _marked_env(in_reply_to="f1", tag="tc1", state=st), "corr-1", _CaptureBroker())
        assert isinstance(result, _BatchOpen)
        state = await store.read_state("A")
        assert state is not None and set(state.outcomes) == {"f1"}  # still just the one fold

    async def test_sibling_fold_unavailable_store_parks_abort_path(self) -> None:
        # (coverage c) A store that died mid-fold aborts (FoldAbort) but still parks (_BatchOpen).
        node = _fanout_node()
        store = FakeFanoutBatchStore()
        await _open(store)
        store.make_unavailable()
        result = await node._aggregate(_store_ctx(store), _marked_env(in_reply_to="f1", tag="tc1"), "corr-1", _CaptureBroker())
        assert isinstance(result, _BatchOpen)

    async def test_reentry_over_tombstoned_batch_abandons_parks(self) -> None:
        # (coverage c) A re-entry whose batch is already closed/tombstoned → abandon → park.
        node = _fanout_node()
        store = FakeFanoutBatchStore()  # never opened "A"
        result = await node._aggregate(_store_ctx(store), _marked_env(in_reply_to="A", tag=None), "corr-1", _CaptureBroker())
        assert isinstance(result, _BatchOpen)

    async def test_reentry_basestate_missing_aborts_parks(self) -> None:
        # (coverage c) A complete batch whose basestate is gone aborts at close → park.
        node = _fanout_node()
        store = FakeFanoutBatchStore()
        await _open(store)
        await store.fold("A", FanoutOutcome(slot="f1", tag="tc1", result=ToolReturn(return_value="r1")))
        await store.fold("A", FanoutOutcome(slot="f2", tag="tc2", result=ToolReturn(return_value="r2")))
        store._basestate.pop("A")  # white-box: simulate the impossible-by-ordering miss
        result = await node._aggregate(_store_ctx(store), _marked_env(in_reply_to="A", tag=None), "corr-1", _CaptureBroker())
        assert isinstance(result, _BatchOpen)


class TestExecute:
    """``_execute`` orders the return-only stages: stage-2 ``_aggregate`` (on ``return`` kind)
    then stage-4 the body. ``_BatchOpen`` → ``_CONSUMED`` (park, body skipped); ``_BatchClosed``
    → run the body; an all-declined body → ``_DECLINED``. ``call`` kind skips ``_aggregate``."""

    async def test_call_kind_runs_body(self) -> None:
        node = _BodyNode(node_id="b", subscribe_topics=["b.in"])
        ctx = SessionRunContext(state=State(), deps={})
        ctx._correlation_id = "corr-1"
        env = _plain_env()
        seam = node._build_seam_context(ctx, env, {}, "call")
        result = await node._execute(ctx, seam, "call", env, None, None, awaiting_reply=False, correlation_id="corr-1", broker=_CaptureBroker())
        assert isinstance(result, ReturnCall) and result.value == "done"

    async def test_return_stateless_continuation_runs_body(self) -> None:
        node = _BodyNode(node_id="b", subscribe_topics=["b.in"])
        ctx = SessionRunContext(state=State(), deps={})
        ctx._correlation_id = "corr-1"
        env = _plain_env(reply=ReturnMessage(in_reply_to="A", tag="tc1", parts=[]))  # unmarked → _BatchClosed
        seam = node._build_seam_context(ctx, env, {}, "return")
        result = await node._execute(ctx, seam, "return", env, None, None, awaiting_reply=False, correlation_id="corr-1", broker=_CaptureBroker())
        assert isinstance(result, ReturnCall)

    async def test_return_parked_fold_is_consumed_without_running_body(self) -> None:
        node = _BodyNode(node_id="fan", subscribe_topics=["fan.in"])
        store = FakeFanoutBatchStore()
        await _open(store)
        st = State()
        st.add_tool_result("tc1", ToolReturn(return_value="r1"))
        ctx = _store_ctx(store, state=st)
        env = _marked_env(in_reply_to="f1", tag="tc1", state=st)
        seam = node._build_seam_context(ctx, env, {}, "return")
        result = await node._execute(ctx, seam, "return", env, None, None, awaiting_reply=False, correlation_id="corr-1", broker=_CaptureBroker())
        assert result is _CONSUMED  # parked fold — the body never runs

    async def test_all_declined_body_is_declined(self) -> None:
        node = _fanout_node()  # base run() declines (returns Next)
        ctx = SessionRunContext(state=State(), deps={})
        ctx._correlation_id = "corr-1"
        env = _plain_env()
        seam = node._build_seam_context(ctx, env, {}, "call")
        result = await node._execute(ctx, seam, "call", env, None, None, awaiting_reply=False, correlation_id="corr-1", broker=_CaptureBroker())
        assert result is _DECLINED
