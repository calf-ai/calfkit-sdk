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
from calfkit.models import ReturnCall
from calfkit.models.envelope import Envelope
from calfkit.models.error_report import ErrorReport, FaultTypes
from calfkit.models.fanout import EnvelopeSnapshot, FanoutOpen, FanoutOutcome, SlotRef
from calfkit.models.payload import TextPart
from calfkit.models.reply import FaultMessage, ReturnMessage
from calfkit.models.session_context import CallFrame, SessionRunContext, Stack, WorkflowState
from calfkit.models.state import State
from calfkit.nodes._fanout_store import FANOUT_STORE_KEY, record_outcome
from calfkit.nodes.base import _CONSUMED, _BatchClosed, _BatchFaulted, _BatchOpen, _Declined
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


def _marked_env(
    *, in_reply_to: str, tag: str | None = "tc1", parts: list[Any] | None = None, fault: ErrorReport | None = None, state: State | None = None
) -> Envelope:
    # A marked fan-out frame (fanout_id == frame_id == "A") on top, with a reply slot. The carriage
    # switch carries a sibling's result in reply.parts (a return) or reply.error (a fault).
    frame = CallFrame(target_topic="fan", callback_topic="caller", frame_id="A", fanout_id="A")
    reply: ReturnMessage | FaultMessage
    if fault is not None:
        reply = FaultMessage(in_reply_to=in_reply_to, tag=tag, error=fault)
    else:
        reply = ReturnMessage(in_reply_to=in_reply_to, tag=tag, parts=parts if parts is not None else [])
    return Envelope(
        context=SessionRunContext(state=state if state is not None else State(), deps={}),
        internal_workflow_state=WorkflowState(call_stack=Stack([frame])),
        reply=reply,
    )


def _plain_env(*, reply: ReturnMessage | FaultMessage | None = None) -> Envelope:
    # An ordinary (non-fan-out) delivery: a single unmarked frame on top.
    frame = CallFrame(target_topic="b", callback_topic="caller", frame_id="A")
    return Envelope(
        context=SessionRunContext(state=State(), deps={}),
        internal_workflow_state=WorkflowState(call_stack=Stack([frame])),
        reply=reply,
    )


def _seam(node: NodeDef[Any], run_ctx: SessionRunContext, env: Envelope, kind: str) -> Any:
    """Build the seam context the staged pipeline threads (sharing run_ctx.state)."""
    return node._build_seam_context(run_ctx, env, {}, kind)  # type: ignore[arg-type]


async def _open(store: FakeFanoutBatchStore, *, snap_state: State | None = None, deps: dict[str, Any] | None = None) -> None:
    own = CallFrame(target_topic="fan", callback_topic="caller", frame_id="A")  # UNMARKED — the pre-stamp snapshot frame
    snap = EnvelopeSnapshot(state=snap_state if snap_state is not None else State(), stack=WorkflowState(call_stack=Stack([own])), deps=deps or {})
    reg = FanoutOpen(
        fanout_id="A",
        node_id="fan",
        expected=[SlotRef(frame_id="f1", tag="tc1", target_topic="tool.a"), SlotRef(frame_id="f2", tag="tc2", target_topic="tool.b")],
    )
    await store.open("A", reg, snap)


def _resolved_outcome(slot: str, tag: str, value: str) -> FanoutOutcome:
    return FanoutOutcome(slot=slot, tag=tag, target_topic=f"tool.{slot}", handled=False, parts=[TextPart(text=value)])


def _failed_outcome(slot: str, tag: str, error_type: str = "callee.boom") -> FanoutOutcome:
    return FanoutOutcome(slot=slot, tag=tag, target_topic=f"tool.{slot}", handled=False, fault=ErrorReport(error_type=error_type))


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

    def test_fault_kind_header_is_fault(self) -> None:
        assert _node()._classify({HDR_KIND: "fault"}) == "fault"

    def test_unknown_kind_header_is_ignored(self) -> None:
        # An unrecognized value is not classified as work — None signals ignore (§4.1 rule 2):
        # a node must not execute a delivery it cannot classify.
        assert _node()._classify({HDR_KIND: "bogus"}) is None


class TestStrayCheck:
    """``_stray_check`` gates the kind↔reply-slot SHAPE agreement (§4.1 rule 3): ``call``↔no reply,
    ``return``↔``ReturnMessage``, ``fault``↔``FaultMessage``. Agreement → ``None`` (proceed); a
    disagreement → a ``_Stray`` (floored, never run as work). A pure body-aware gate, separate
    from header classification."""

    def test_agreeing_shapes_are_not_stray(self) -> None:
        node = _node()
        assert node._stray_check("call", _plain_env(reply=None)) is None
        assert node._stray_check("return", _plain_env(reply=ReturnMessage(in_reply_to="A", tag="t", parts=[]))) is None
        assert node._stray_check("fault", _plain_env(reply=FaultMessage(in_reply_to="A", tag="t", error=ErrorReport(error_type="x")))) is None

    def test_fault_kind_with_no_reply_is_stray(self) -> None:
        # scenario 29: kind=fault but reply=None — a disagreement (never the node-own-failure path).
        assert _node()._stray_check("fault", _plain_env(reply=None)) is not None

    def test_return_kind_with_fault_reply_is_stray(self) -> None:
        env = _plain_env(reply=FaultMessage(in_reply_to="A", tag="t", error=ErrorReport(error_type="x")))
        assert _node()._stray_check("return", env) is not None

    def test_call_kind_with_a_reply_is_stray(self) -> None:
        # A call carries no reply slot; one present is a disagreement.
        env = _plain_env(reply=ReturnMessage(in_reply_to="A", tag="t", parts=[]))
        assert _node()._stray_check("call", env) is not None


async def _agg(
    node: _FanoutNode, store: FakeFanoutBatchStore, env: Envelope, kind: str, broker: Any = None, *, state: State | None = None, deps: Any = None
) -> Any:
    """Drive ``_aggregate`` with a freshly built run_ctx + seam_ctx (sharing state); return all three."""
    run_ctx = _store_ctx(store, state=state, deps=deps)
    seam = _seam(node, run_ctx, env, kind)
    result = await node._aggregate(run_ctx, seam, kind, env, "corr-1", broker or _CaptureBroker())
    return run_ctx, seam, result


class TestAggregate:
    """``_aggregate`` is the durable fold/close + stage-1 stage (§6.8 stage-2). It resolves the callee
    slot uniformly (return materializes; fault runs on_callee_error) and returns ``_BatchClosed``
    (proceed to body), ``_BatchOpen`` (park), or ``_BatchFaulted`` (escalate — single-call unhandled
    fault, or a closing batch's fault group)."""

    # ── single-call continuation (None arm) ──────────────────────────────────────
    async def test_single_call_return_resolves_slot_and_closes(self) -> None:
        # An unmarked frame (a single-call return) resolves the one slot, then proceeds to the body.
        node = _fanout_node()
        env = _plain_env(reply=ReturnMessage(in_reply_to="cf1", tag="tc1", parts=[TextPart(text="r")]))
        _run_ctx, seam, result = await _agg(node, FakeFanoutBatchStore(), env, "return")
        assert isinstance(result, _BatchClosed)
        assert len(seam.callee_results) == 1 and seam.callee_results[0].tag == "tc1"

    async def test_single_call_fault_unhandled_escalates(self) -> None:
        # A single-call fault with no on_callee_error escalates (§8) — _BatchFaulted, the body skipped.
        node = _fanout_node()
        env = _plain_env(reply=FaultMessage(in_reply_to="cf1", tag="tc1", error=ErrorReport(error_type="callee.boom")))
        _run_ctx, _seam, result = await _agg(node, FakeFanoutBatchStore(), env, "fault")
        assert isinstance(result, _BatchFaulted) and result.report.error_type == "callee.boom"

    async def test_single_call_fault_handled_resolves_and_closes(self) -> None:
        # on_callee_error returns a substitute → the slot resolves (handled) and the body runs.
        node = _fanout_node()
        node.on_callee_error(lambda ctx, fault: "recovered")
        env = _plain_env(reply=FaultMessage(in_reply_to="cf1", tag="tc1", error=ErrorReport(error_type="callee.boom")))
        _run_ctx, seam, result = await _agg(node, FakeFanoutBatchStore(), env, "fault")
        assert isinstance(result, _BatchClosed)
        assert seam.callee_results[0].handled is True and seam.callee_results[0].parts == [TextPart(text="recovered")]

    # ── sibling fold ─────────────────────────────────────────────────────────────
    async def test_sibling_fold_incomplete_parks(self) -> None:
        node = _fanout_node()
        store = FakeFanoutBatchStore()
        await _open(store)
        _run_ctx, _seam, result = await _agg(node, store, _marked_env(in_reply_to="f1", tag="tc1", parts=[TextPart(text="r1")]), "return")
        assert isinstance(result, _BatchOpen)  # 1 of 2 → parked
        state = await store.read_state("A")
        assert state is not None and set(state.outcomes) == {"f1"}
        assert state.outcomes["f1"].parts == [TextPart(text="r1")]  # the reply's parts were folded

    async def test_sibling_fold_complete_publishes_reentry_and_parks(self) -> None:
        node = _fanout_node()
        store = FakeFanoutBatchStore()
        await _open(store)
        broker = _CaptureBroker()
        await _agg(node, store, _marked_env(in_reply_to="f1", tag="tc1", parts=[TextPart(text="r1")]), "return", broker)
        _run_ctx, _seam, result = await _agg(node, store, _marked_env(in_reply_to="f2", tag="tc2", parts=[TextPart(text="r2")]), "return", broker)
        assert isinstance(result, _BatchOpen)  # still parked — the re-entry is a fresh delivery
        assert [t for t, _ in broker.published] == ["fan.private.return"]  # closure re-entry self-published
        reentry_env = broker.published[0][1]
        assert reentry_env.reply is not None and reentry_env.reply.in_reply_to == "A"

    async def test_sibling_fault_unhandled_folds_as_a_failed_outcome(self) -> None:
        # A sibling FAULT with no on_callee_error folds as a failed outcome (escalates at closure).
        node = _fanout_node()
        store = FakeFanoutBatchStore()
        await _open(store)
        env = _marked_env(in_reply_to="f1", tag="tc1", fault=ErrorReport(error_type="callee.boom"))
        _run_ctx, _seam, result = await _agg(node, store, env, "fault")
        assert isinstance(result, _BatchOpen)
        state = await store.read_state("A")
        assert state is not None and state.outcomes["f1"].fault is not None and state.outcomes["f1"].fault.error_type == "callee.boom"

    # ── re-entry close (three-way) ───────────────────────────────────────────────
    async def test_reentry_all_resolved_restores_materializes_and_closes(self) -> None:
        node = _fanout_node()
        store = FakeFanoutBatchStore()
        await _open(store, snap_state=State(), deps={"k": "v"})  # snapshot carries deps + a pre-tool state
        await record_outcome(store, "A", _resolved_outcome("f1", "tc1", "r1"))
        await record_outcome(store, "A", _resolved_outcome("f2", "tc2", "r2"))
        env = _marked_env(in_reply_to="A", tag=None)  # in_reply_to == frame_id "A" → re-entry close
        run_ctx, seam, result = await _agg(node, store, env, "return")
        assert isinstance(result, _BatchClosed)
        # Each resolved slot was driven through _resolve_slot (base records a CalleeResult per outcome).
        assert {cr.tag for cr in seam.callee_results} == {"tc1", "tc2"}
        # ctx restored from the snapshot (deps + the UNMARKED frame so the body's ReturnCall unwinds it).
        assert run_ctx.deps == {"k": "v"}
        assert env.internal_workflow_state.current_frame.frame_id == "A"
        assert env.internal_workflow_state.current_frame.fanout_id is None
        assert env.context.deps == {"k": "v"}
        assert await store.read_state("A") is None  # tombstoned at close

    async def test_reentry_with_one_unhandled_fault_escalates_flattened(self) -> None:
        # ANY unhandled fault fails the batch (§7.3); a SINGLE one flattens to the bare child fault
        # (identity preserved, §4.4), the per-slot topology copied onto its details.
        node = _fanout_node()
        store = FakeFanoutBatchStore()
        await _open(store)
        await record_outcome(store, "A", _resolved_outcome("f1", "tc1", "r1"))
        await record_outcome(store, "A", _failed_outcome("f2", "tc2", "callee.boom"))
        _run_ctx, _seam, result = await _agg(node, store, _marked_env(in_reply_to="A", tag=None), "return")
        assert isinstance(result, _BatchFaulted)
        assert result.report.error_type == "callee.boom"  # flattened to the bare child, not a group
        assert result.report.details[FaultTypes.FANOUT_TOPOLOGY]["failed"] == 1
        assert await store.read_state("A") is None  # tombstoned

    async def test_reentry_with_two_unhandled_faults_is_a_fault_group(self) -> None:
        node = _fanout_node()
        store = FakeFanoutBatchStore()
        await _open(store)
        await record_outcome(store, "A", _failed_outcome("f1", "tc1", "boom.a"))
        await record_outcome(store, "A", _failed_outcome("f2", "tc2", "boom.b"))
        _run_ctx, _seam, result = await _agg(node, store, _marked_env(in_reply_to="A", tag=None), "return")
        assert isinstance(result, _BatchFaulted)
        assert result.report.error_type == FaultTypes.FAULT_GROUP
        assert {c.error_type for c in result.report.causes} == {"boom.a", "boom.b"}
        assert result.report.details[FaultTypes.FANOUT_TOPOLOGY]["failed"] == 2

    async def test_reentry_spurious_incomplete_parks(self) -> None:
        node = _fanout_node()
        store = FakeFanoutBatchStore()
        await _open(store)  # opened, no folds → incomplete
        _run_ctx, _seam, result = await _agg(node, store, _marked_env(in_reply_to="A", tag=None), "return")
        assert isinstance(result, _BatchOpen)  # spurious early re-entry → no-op park
        assert await store.read_state("A") is not None  # batch left untouched

    async def test_completing_fold_reentry_publish_failure_aborts_and_escalates(self) -> None:
        # The completing fold tries to self-publish the re-entry; if that publish raises KafkaError,
        # _aggregate must NOT propagate — it tombstones both AND escalates a fault to the caller (4.6).
        node = _fanout_node()
        store = FakeFanoutBatchStore()
        await _open(store)
        await _agg(node, store, _marked_env(in_reply_to="f1", tag="tc1", parts=[TextPart(text="r1")]), "return")
        # The SECOND fold completes the batch; its re-entry publish raises.
        env2 = _marked_env(in_reply_to="f2", tag="tc2", parts=[TextPart(text="r2")])
        _run_ctx, _seam, result = await _agg(node, store, env2, "return", _RaisingBroker())
        assert isinstance(result, _BatchFaulted)  # escalated, not parked
        assert result.report.error_type == FaultTypes.FANOUT_ABORTED
        assert result.report.details[FaultTypes.REASON] == FaultTypes.REASON_REENTRY_FAILED
        assert await store.read_state("A") is None  # aborted: both records tombstoned
        assert await store.read_basestate("A") is None

    # ── strays + edges (stray-check BEFORE the seams, decision 10) ────────────────
    async def test_sibling_reply_without_in_reply_to_does_not_fold(self, caplog: pytest.LogCaptureFixture) -> None:
        node = _fanout_node()
        store = FakeFanoutBatchStore()
        await _open(store)
        env = _marked_env(in_reply_to="f1", tag="tc1", parts=[TextPart(text="r1")])
        assert env.reply is not None
        env.reply.in_reply_to = None  # malformed: marked sibling but no slot
        with caplog.at_level(logging.ERROR, logger="calfkit.nodes.base"):
            _run_ctx, _seam, result = await _agg(node, store, env, "return")
        assert isinstance(result, _BatchOpen)
        state = await store.read_state("A")
        assert state is not None and state.outcomes == {}  # nothing folded
        assert any("malformed sibling reply" in r.getMessage() for r in caplog.records)

    async def test_sibling_foreign_slot_parks_store_unchanged(self) -> None:
        node = _fanout_node()
        store = FakeFanoutBatchStore()
        await _open(store)
        _run_ctx, _seam, result = await _agg(node, store, _marked_env(in_reply_to="f99", tag="tcX", parts=[TextPart(text="x")]), "return")
        assert isinstance(result, _BatchOpen)
        state = await store.read_state("A")
        assert state is not None and state.outcomes == {}

    async def test_sibling_duplicate_slot_parks_store_unchanged(self) -> None:
        node = _fanout_node()
        store = FakeFanoutBatchStore()
        await _open(store)
        await _agg(node, store, _marked_env(in_reply_to="f1", tag="tc1", parts=[TextPart(text="r1")]), "return")
        _run_ctx, _seam, result = await _agg(node, store, _marked_env(in_reply_to="f1", tag="tc1", parts=[TextPart(text="r1b")]), "return")
        assert isinstance(result, _BatchOpen)
        state = await store.read_state("A")
        assert state is not None and set(state.outcomes) == {"f1"}  # still just the one fold

    async def test_sibling_unavailable_store_aborts_and_escalates(self) -> None:
        # A store that died during the classify/fold tombstones (best-effort) AND escalates a fault.
        node = _fanout_node()
        store = FakeFanoutBatchStore()
        await _open(store)
        store.make_unavailable()
        _run_ctx, _seam, result = await _agg(node, store, _marked_env(in_reply_to="f1", tag="tc1", parts=[TextPart(text="r1")]), "return")
        assert isinstance(result, _BatchFaulted)
        assert result.report.error_type == FaultTypes.FANOUT_ABORTED
        assert result.report.details[FaultTypes.REASON] == "store_unavailable"

    async def test_reentry_over_tombstoned_batch_abandons_parks(self) -> None:
        node = _fanout_node()
        store = FakeFanoutBatchStore()  # never opened "A"
        _run_ctx, _seam, result = await _agg(node, store, _marked_env(in_reply_to="A", tag=None), "return")
        assert isinstance(result, _BatchOpen)

    async def test_reentry_basestate_missing_aborts_and_escalates(self) -> None:
        node = _fanout_node()
        store = FakeFanoutBatchStore()
        await _open(store)
        await record_outcome(store, "A", _resolved_outcome("f1", "tc1", "r1"))
        await record_outcome(store, "A", _resolved_outcome("f2", "tc2", "r2"))
        store._basestate.pop("A")  # white-box: simulate the impossible-by-ordering miss
        _run_ctx, _seam, result = await _agg(node, store, _marked_env(in_reply_to="A", tag=None), "return")
        assert isinstance(result, _BatchFaulted)
        assert result.report.error_type == FaultTypes.FANOUT_ABORTED
        assert result.report.details[FaultTypes.REASON] == "basestate_missing"
        assert await store.read_state("A") is None  # tombstoned on abort

    async def test_reentry_store_dead_at_close_aborts_and_escalates(self) -> None:
        node = _fanout_node()
        store = FakeFanoutBatchStore()
        await _open(store)
        await record_outcome(store, "A", _resolved_outcome("f1", "tc1", "r1"))
        await record_outcome(store, "A", _resolved_outcome("f2", "tc2", "r2"))
        store.make_unavailable()  # the store dies before the close reads it
        _run_ctx, _seam, result = await _agg(node, store, _marked_env(in_reply_to="A", tag=None), "return")
        assert isinstance(result, _BatchFaulted)
        assert result.report.error_type == FaultTypes.FANOUT_ABORTED
        assert result.report.details[FaultTypes.REASON] == "store_unavailable"


class TestMidBatchAbort:
    """A node-own raise / publish failure MID-BATCH (the marked fan-out frame on top, the batch still
    OPEN) must abort — tombstone + escalate ONCE — NOT ``_publish_fault``/``on_node_error``, which would
    double-reply while siblings are still pending (spec §6.8 / R4)."""

    async def test_in_batch_work_reads_the_fanout_marker(self) -> None:
        node = _fanout_node()
        marked = node._build_seam_context(_store_ctx(FakeFanoutBatchStore()), _marked_env(in_reply_to="f1"), {}, "return")
        unmarked = node._build_seam_context(_store_ctx(FakeFanoutBatchStore()), _plain_env(), {}, "call")
        assert node._in_batch_work(_marked_env(in_reply_to="f1")) is True  # fanout_id set on the frame
        assert node._in_batch_work(_plain_env()) is False
        # the seam contexts exist only to keep the build helper exercised
        assert marked is not None and unmarked is not None

    async def test_node_own_raise_during_sibling_fold_aborts_once(self) -> None:
        # An UNEXPECTED node-own raise while folding a sibling (marked frame, batch open) routes to the
        # abort (tombstone + escalate the exception), not on_node_error → _publish_fault.
        class _RaisingFoldNode(_FanoutNode):
            async def _resolve_callee(self, *a: Any, **k: Any) -> Any:
                raise RuntimeError("boom mid-fold")

        node = _RaisingFoldNode(node_id="fan", subscribe_topics=["fan.in"])
        store = FakeFanoutBatchStore()
        node.resources[FANOUT_STORE_KEY] = store
        await _open(store)
        env = _marked_env(in_reply_to="f1", tag="tc1", parts=[TextPart(text="r1")])
        resp = await node.handler(env, correlation_id="corr-1", headers={HDR_KIND: "return"}, broker=_CaptureBroker())  # type: ignore[arg-type]
        assert isinstance(resp.body.reply, FaultMessage)  # escalated the node's own exception
        assert resp.body.reply.error.error_type == FaultTypes.UNHANDLED
        assert await store.read_state("A") is None  # the open batch was tombstoned (abort, not _publish_fault)


class TestExecute:
    """``_execute`` orders the return-only stages: stage-2 ``_aggregate`` (on ``return`` kind)
    then stage-4 the body. ``_BatchOpen`` → ``_CONSUMED`` (park, body skipped); ``_BatchClosed``
    → run the body; an all-declined body → ``_Declined(reason)``. ``call`` kind skips ``_aggregate``."""

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
        ctx = _store_ctx(store)
        env = _marked_env(in_reply_to="f1", tag="tc1", parts=[TextPart(text="r1")])  # 1 of 2 → parks
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
        assert isinstance(result, _Declined) and result.reason == "all_declined"


class TestClosureSeams:
    """§6.4 / scenario 38: ``before_node`` fires exactly ONCE at fan-out closure (NOT on mid-batch
    sibling folds), and there it observes the RESTORED snapshot state — the load-bearing
    ``_resync_seam_context`` wiring (``_restore_from_snapshot`` swaps in a fresh snapshot ``state``
    object, so without the re-sync the closure seams would see the cleared re-entry state)."""

    async def test_closure_fires_before_node_once_on_restored_state(self) -> None:
        node = _BodyNode(node_id="fan", subscribe_topics=["fan.in"])  # fan-out-capable + a body
        before_states: list[Any] = []
        after_fired: list[int] = []

        @node.before_node
        def record(ctx: Any) -> None:
            before_states.append(ctx.state.metadata)  # a marker baked into the OPEN-time snapshot state
            return None

        @node.after_node
        def count(ctx: Any, output: Any) -> None:
            after_fired.append(1)
            return None

        store = FakeFanoutBatchStore()
        await _open(store, snap_state=State(metadata={"marker": "restored"}), deps={"k": "v"})
        await record_outcome(store, "A", _resolved_outcome("f1", "tc1", "r1"))
        await record_outcome(store, "A", _resolved_outcome("f2", "tc2", "r2"))
        env = _marked_env(in_reply_to="A", tag=None)  # in_reply_to == frame_id "A" → the re-entry close
        ctx = _store_ctx(store)
        seam = node._build_seam_context(ctx, env, {}, "return")

        await node._execute(ctx, seam, "return", env, None, None, awaiting_reply=False, correlation_id="corr-1", broker=_CaptureBroker())

        assert before_states == [{"marker": "restored"}]  # fired ONCE, observing the RESTORED snapshot state
        assert after_fired == [1]  # after_node also fired exactly once at closure

    async def test_midbatch_sibling_fold_does_not_fire_before_node(self) -> None:
        node = _BodyNode(node_id="fan", subscribe_topics=["fan.in"])
        fired: list[int] = []

        @node.before_node
        def record(ctx: Any) -> None:
            fired.append(1)
            return None

        store = FakeFanoutBatchStore()
        await _open(store)  # expects f1 + f2
        env = _marked_env(in_reply_to="f1", tag="tc1", parts=[TextPart(text="r1")])  # 1 of 2 → parks
        ctx = _store_ctx(store)
        seam = node._build_seam_context(ctx, env, {}, "return")

        result = await node._execute(ctx, seam, "return", env, None, None, awaiting_reply=False, correlation_id="corr-1", broker=_CaptureBroker())

        assert result is _CONSUMED  # the fold parked (incomplete batch)
        assert fired == []  # before_node did NOT fire on a mid-batch sibling (§6.4)
