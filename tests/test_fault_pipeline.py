"""PR-6 step 3 — the staged fault pipeline (fault-rail spec §6.8).

Tests for the pipeline rewrite: _build_seam_context (the SeamContext the seams get,
sharing the body's run_ctx.state), the fault boundary, _publish_fault/_publish_abort,
the §6.8 stages, _classify widen, stray handling, and the auto-fault. See
notes/pr6-fault-rail-implementation-plan.md §3 step 3 / §4 layer 7.
"""

from __future__ import annotations

import logging
from typing import Any

import pytest
from aiokafka.errors import KafkaError, MessageSizeTooLargeError  # type: ignore[import-untyped]
from pydantic import BaseModel

from calfkit._protocol import HDR_ERROR_TYPE, HDR_KIND
from calfkit._registry import handler
from calfkit.exceptions import NodeFaultError
from calfkit.models import CallFrame, CallFrameStack, Envelope, ReturnCall, SessionRunContext, State, TailCall, TextPart, WorkflowState
from calfkit.models.error_report import ErrorReport
from calfkit.models.reply import FaultMessage, ReturnMessage
from calfkit.models.seam_context import SeamContext
from calfkit.models.state import OverridesState
from calfkit.nodes.base import BaseNodeDef


def _node(**kwargs: object) -> BaseNodeDef:
    return BaseNodeDef(node_id="orchestrator", subscribe_topics=["in"], **kwargs)


class _CaptureBroker:
    """Records (topic, envelope, headers) per publish."""

    def __init__(self) -> None:
        self.published: list[tuple[str, Any, dict[str, str]]] = []

    async def publish(self, envelope: Any, *, topic: str, correlation_id: str, key: bytes, headers: dict[str, str]) -> None:
        self.published.append((topic, envelope, headers))


class _FailThenCaptureBroker:
    """Raises on the FIRST publish (a size-class failure), captures the rest — exercises the §4.3
    strip-and-retry: the minimal fault must reach the caller on the retry."""

    def __init__(self) -> None:
        self.published: list[tuple[str, Any, dict[str, str]]] = []
        self._calls = 0

    async def publish(self, envelope: Any, *, topic: str, correlation_id: str, key: bytes, headers: dict[str, str]) -> None:
        self._calls += 1
        if self._calls == 1:
            raise MessageSizeTooLargeError("simulated message-too-large on the full fault")
        self.published.append((topic, envelope, headers))


def _framed_envelope(*, payload: object = None, callback_topic: str | None = "cb") -> Envelope:
    stack = CallFrameStack()
    stack.push(CallFrame(target_topic="orchestrator.in", callback_topic=callback_topic, payload=payload, tag="t1"))
    return Envelope(
        internal_workflow_state=WorkflowState(call_stack=stack),
        context=SessionRunContext(state=State(), deps={"k": "v"}),
    )


class TestBuildSeamContext:
    async def test_builds_capability_view_sharing_run_ctx_state(self) -> None:
        node = _node()
        envelope = _framed_envelope(payload={"args": 1}, callback_topic="cb")
        run_ctx = await node.prepare_context(envelope, emitter_node_id="upstream", correlation_id="cid-123")

        seam_ctx = node._build_seam_context(run_ctx, envelope, {}, "return")

        assert isinstance(seam_ctx, SeamContext)
        assert seam_ctx.state is run_ctx.state  # SHARED — before_node's input-transform channel
        assert seam_ctx.deps is run_ctx.deps
        assert seam_ctx.node_id == "orchestrator"
        assert seam_ctx.correlation_id == "cid-123"
        assert seam_ctx.emitter_node_id == "upstream"
        assert seam_ctx.payload == {"args": 1}
        assert seam_ctx.delivery_kind == "return"
        assert seam_ctx.awaiting_reply is True  # frame has a callback_topic
        # stage-scoped fields start empty
        assert seam_ctx.callee_results == []
        assert seam_ctx.failing_call is None
        assert seam_ctx.exception is None

    async def test_route_is_exposed_only_on_call_kind(self) -> None:
        # route is ingress-only (§6.3): a return/fault delivery carries no route key.
        node = _node()
        env = _framed_envelope()
        run_ctx = await node.prepare_context(env, correlation_id="c")
        headers = {"x-calf-route": "do.thing"}
        assert node._build_seam_context(run_ctx, env, headers, "call").route == "do.thing"
        assert node._build_seam_context(run_ctx, env, headers, "return").route is None

    async def test_no_frame_yields_no_payload_and_not_awaiting_reply(self) -> None:
        node = _node()
        env = Envelope(internal_workflow_state=WorkflowState(call_stack=CallFrameStack()), context=SessionRunContext(state=State(), deps={}))
        run_ctx = await node.prepare_context(env, correlation_id="c")
        seam_ctx = node._build_seam_context(run_ctx, env, {}, "call")
        assert seam_ctx.payload is None
        assert seam_ctx.awaiting_reply is False


class TestPublishFault:
    async def test_publishes_fault_to_caller_callback_with_echoed_correlation(self) -> None:
        # A fault rides the same callback rail a ReturnCall would (P2): popped frame →
        # FaultMessage(in_reply_to, tag) to the caller, x-calf-kind=fault + x-calf-error-type.
        node = _node()
        inbound = _framed_envelope(callback_topic="caller.return")
        frame_id = inbound.internal_workflow_state.current_frame.frame_id
        snapshot = node._stack_snapshot(inbound)
        broker = _CaptureBroker()

        mirror, kind = await node._publish_fault(ErrorReport(error_type="calf.unhandled", message="boom"), snapshot, inbound, "cid", broker)

        assert kind == "fault"
        assert len(broker.published) == 1
        topic, env, headers = broker.published[0]
        assert topic == "caller.return"
        assert isinstance(env.reply, FaultMessage)
        assert env.reply.error.error_type == "calf.unhandled"
        assert env.reply.in_reply_to == frame_id  # answers the caller's frame
        assert env.reply.tag == "t1"  # echoed transport token
        assert headers[HDR_KIND] == "fault"
        assert headers[HDR_ERROR_TYPE] == "calf.unhandled"
        # the popped stack travels for the next escalation hop
        assert env.internal_workflow_state.call_stack.is_empty()
        assert mirror.reply is env.reply  # same envelope returned for the broadcast mirror

    async def test_floors_when_no_callback_topic(self, caplog: pytest.LogCaptureFixture) -> None:
        # A fire-and-forget terminal (no callback) has no caller to answer → floor (ERROR +
        # full report), no point-to-point publish, but the broadcast mirror still fires (§13).
        node = _node()
        inbound = _framed_envelope(callback_topic=None)
        broker = _CaptureBroker()
        with caplog.at_level(logging.ERROR):
            mirror, kind = await node._publish_fault(ErrorReport(error_type="calf.unhandled"), node._stack_snapshot(inbound), inbound, "cid", broker)
        assert kind == "fault"
        assert broker.published == []  # nothing delivered point-to-point
        assert isinstance(mirror.reply, FaultMessage)  # still returned for the publish_topic mirror
        assert "floored" in caplog.text

    async def test_escalation_re_addresses_but_never_wraps_the_report(self) -> None:
        # §4.4: the same report (stable report_id) is re-addressed each hop, never wrapped.
        node = _node()
        report = ErrorReport(error_type="calf.model.context_window_exceeded")
        inbound = _framed_envelope(callback_topic="ancestor.return")
        broker = _CaptureBroker()
        await node._publish_fault(report, node._stack_snapshot(inbound), inbound, "cid", broker)
        published_report = broker.published[0][1].reply.error
        assert published_report.report_id == report.report_id
        assert published_report.error_type == report.error_type
        assert published_report.causes == []  # not wrapped

    async def test_oversized_fault_strips_to_minimal_and_retries(self) -> None:
        # §4.3: a fault publish that fails (commonly the report exceeding the carriage budget) must NOT
        # strand the caller — strip to the minimal report (identity only) and retry ONCE before flooring.
        # Pre-fix _publish_fault log-floored the failure and the caller received nothing.
        node = _node()
        report = ErrorReport(
            error_type="calf.unhandled",
            message="boom",
            origin_node_id="orchestrator",
            details={"big": "x" * 100},  # non-empty ⇒ to_minimal() strips it
            causes=[ErrorReport(error_type="calf.inner", message="inner")],
        )
        inbound = _framed_envelope(callback_topic="caller.return")
        broker = _FailThenCaptureBroker()

        mirror, kind = await node._publish_fault(report, node._stack_snapshot(inbound), inbound, "cid", broker)

        assert kind == "fault"
        assert len(broker.published) == 1  # the retry delivered (the first, full publish raised)
        _topic, env, headers = broker.published[0]
        assert isinstance(env.reply, FaultMessage)
        assert env.reply.error.error_type == "calf.unhandled"  # identity preserved
        assert env.reply.error.causes == [] and env.reply.error.details == {}  # stripped to minimal
        assert headers[HDR_ERROR_TYPE] == "calf.unhandled"
        assert mirror.reply.error.causes == []  # the returned broadcast mirror is the minimal one too

    async def test_fault_response_mirror_carries_error_type_header(self) -> None:
        # §4.2/§13: the broadcast mirror (the handler Response) must carry x-calf-error-type so faults
        # are broker-filterable on the broadcast rail too, matching the point-to-point publish.
        node = _node()
        report = ErrorReport(error_type="calf.model.context_window_exceeded")
        inbound = _framed_envelope(callback_topic="caller.return")
        resp = await node._fault_response(report, node._stack_snapshot(inbound), inbound, "cid", _CaptureBroker())
        assert resp.headers[HDR_KIND] == "fault"
        assert resp.headers[HDR_ERROR_TYPE] == "calf.model.context_window_exceeded"


class _RaisingNode(BaseNodeDef):
    """A node whose body raises a generic exception (the silent-drop motivating bug)."""

    async def run(self, ctx: SessionRunContext) -> Any:
        raise RuntimeError("body boom")


class TestFaultBoundary:
    async def test_body_raise_becomes_a_fault_to_the_caller(self) -> None:
        # P1: a previously-dropped uncaught body exception now travels the success rail
        # as a typed calf.unhandled fault (no propagation out of the handler).
        node = _RaisingNode(node_id="n", subscribe_topics=["in"])
        inbound = _framed_envelope(callback_topic="caller.return")
        broker = _CaptureBroker()

        resp = await node.handler(inbound, "cid", {}, broker)

        assert len(broker.published) == 1
        topic, env, headers = broker.published[0]
        assert topic == "caller.return"
        assert isinstance(env.reply, FaultMessage)
        assert env.reply.error.error_type == "calf.unhandled"
        assert env.reply.error.details["calf.exception_type"] == "RuntimeError"
        assert env.reply.error.origin_node_id == "n"
        assert headers[HDR_KIND] == "fault"
        assert resp.body.reply is env.reply  # the broadcast mirror carries the same fault

    async def test_node_fault_error_mints_verbatim_and_bypasses_on_node_error(self) -> None:
        # §6.5 mint rule: a deliberate NodeFaultError converts verbatim and does NOT consult
        # on_node_error (an unrelated recovery must not convert a deliberate fault into success).
        node = _MintingNode(node_id="n", subscribe_topics=["in"])
        consulted: list[bool] = []

        @node.on_node_error
        def recover(ctx: object, fault: ErrorReport) -> str:
            consulted.append(True)
            return "would-recover-if-consulted"

        broker = _CaptureBroker()
        await node.handler(_framed_envelope(callback_topic="caller.return"), "cid", {}, broker)

        assert consulted == []  # bypassed
        env = broker.published[0][1]
        assert isinstance(env.reply, FaultMessage)
        assert env.reply.error.error_type == "billing.quota_exceeded"

    async def test_on_node_error_recovery_publishes_the_recovered_output(self) -> None:
        node = _RaisingNode(node_id="n", subscribe_topics=["in"])

        @node.on_node_error
        def recover(ctx: object, fault: ErrorReport) -> str:
            return "recovered-value"

        broker = _CaptureBroker()
        await node.handler(_framed_envelope(callback_topic="caller.return"), "cid", {}, broker)

        env = broker.published[0][1]  # a normal return, not a fault
        assert isinstance(env.reply, ReturnMessage)
        assert env.reply.parts == [TextPart(text="recovered-value")]

    async def test_on_node_error_decline_escalates_the_original_fault(self) -> None:
        node = _RaisingNode(node_id="n", subscribe_topics=["in"])

        @node.on_node_error
        def decline(ctx: object, fault: ErrorReport) -> None:
            return None

        broker = _CaptureBroker()
        await node.handler(_framed_envelope(callback_topic="caller.return"), "cid", {}, broker)

        env = broker.published[0][1]
        assert isinstance(env.reply, FaultMessage)
        assert env.reply.error.error_type == "calf.unhandled"


class _MintingNode(BaseNodeDef):
    """A node body that deliberately mints a typed fault."""

    async def run(self, ctx: SessionRunContext) -> Any:
        raise NodeFaultError("billing.quota_exceeded", message="no funds")


class _ReturningNode(BaseNodeDef):
    async def run(self, ctx: SessionRunContext) -> Any:
        return ReturnCall(state=ctx.state, value="ok")


class _BodyNode(BaseNodeDef):
    """A node whose body returns a terminal value (so a before_node short-circuit is visible)."""

    async def run(self, ctx: SessionRunContext) -> Any:
        return ReturnCall(state=ctx.state, value="from-body")


class TestBeforeNode:
    async def test_before_node_value_short_circuits_the_body(self) -> None:
        # §6.2/§6.4: a before_node returning a value becomes the node's output; the body never runs.
        node = _BodyNode(node_id="n", subscribe_topics=["in"])

        @node.before_node
        def short(ctx: SeamContext[State]) -> str:
            return "from-before-node"

        broker = _CaptureBroker()
        await node.handler(_framed_envelope(callback_topic="caller"), "cid", {}, broker)

        env = broker.published[0][1]
        assert isinstance(env.reply, ReturnMessage)
        assert env.reply.parts == [TextPart(text="from-before-node")]  # not "from-body"

    async def test_before_node_decline_runs_the_body(self) -> None:
        node = _BodyNode(node_id="n", subscribe_topics=["in"])

        @node.before_node
        def declines(ctx: SeamContext[State]) -> None:
            return None

        broker = _CaptureBroker()
        await node.handler(_framed_envelope(callback_topic="caller"), "cid", {}, broker)

        assert broker.published[0][1].reply.parts == [TextPart(text="from-body")]  # body ran


class TestAfterNodeStage:
    async def test_after_node_replaces_the_body_output(self) -> None:
        node = _BodyNode(node_id="n", subscribe_topics=["in"])

        @node.after_node
        def replace(ctx: SeamContext[State], output: object) -> str:
            return "replaced-by-after"

        broker = _CaptureBroker()
        await node.handler(_framed_envelope(callback_topic="caller"), "cid", {}, broker)

        assert broker.published[0][1].reply.parts == [TextPart(text="replaced-by-after")]

    async def test_after_node_sees_the_body_output_view_and_can_keep_it(self) -> None:
        node = _BodyNode(node_id="n", subscribe_topics=["in"])
        seen: list[object] = []

        @node.after_node
        def observe(ctx: SeamContext[State], output: object) -> None:
            seen.append(output)
            return None  # decline → keep the body's output

        broker = _CaptureBroker()
        await node.handler(_framed_envelope(callback_topic="caller"), "cid", {}, broker)

        assert seen == ["from-body"]  # the projected OutputT view of the body's ReturnCall
        assert broker.published[0][1].reply.parts == [TextPart(text="from-body")]


class _FailFirstBroker:
    """Fails the first publish (the terminal ReturnCall), captures the rest (the fault)."""

    def __init__(self) -> None:
        self.published: list[tuple[str, Any, dict[str, str]]] = []
        self.calls = 0

    async def publish(self, envelope: Any, *, topic: str, correlation_id: str, key: bytes, headers: dict[str, str]) -> None:
        self.calls += 1
        if self.calls == 1:
            raise KafkaError("transient publish failure")
        self.published.append((topic, envelope, headers))


class TestPublishGuard:
    async def test_failed_terminal_publish_becomes_a_fault_to_the_caller(self) -> None:
        # Scenario 42: a broker failure on the terminal ReturnCall publish synthesizes a fault
        # addressed to the caller via the pre-mutation snapshot (the publish guard never
        # re-enters on_node_error). _publish_action no longer swallows the failure internally.
        node = _ReturningNode(node_id="n", subscribe_topics=["in"])
        broker = _FailFirstBroker()

        resp = await node.handler(_framed_envelope(callback_topic="caller.return"), "cid", {}, broker)

        assert broker.calls == 2  # the failed return publish, then the fault publish
        topic, env, headers = broker.published[0]
        assert topic == "caller.return"
        assert isinstance(env.reply, FaultMessage)
        assert headers[HDR_KIND] == "fault"
        assert isinstance(resp.body.reply, FaultMessage)


class _Stage0RaisingNode(BaseNodeDef):
    """Context construction raises — a stage-0 failure BELOW the seams (no ctx exists yet)."""

    async def prepare_context(self, *args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("stage-0 boom")


class TestStage0Guard:
    async def test_call_kind_stage0_failure_faults_the_caller(self) -> None:
        # §4.1/§6.8: a stage-0 raise on a call-kind ingress is caught below the seams and
        # faults the caller where the stack is readable — no exception escapes the handler,
        # and the awaiting caller never hangs.
        node = _Stage0RaisingNode(node_id="n", subscribe_topics=["in"])
        inbound = _framed_envelope(callback_topic="caller.return")  # no x-calf-kind ⇒ call
        broker = _CaptureBroker()

        resp = await node.handler(inbound, "cid", {}, broker)

        assert len(broker.published) == 1
        topic, env, headers = broker.published[0]
        assert topic == "caller.return"
        assert isinstance(env.reply, FaultMessage)
        assert env.reply.error.error_type == "calf.unhandled"
        assert headers[HDR_KIND] == "fault"
        assert isinstance(resp.body.reply, FaultMessage)  # the broadcast mirror carries the fault

    async def test_fault_kind_stage0_failure_floors_only(self, caplog: pytest.LogCaptureFixture) -> None:
        # §4.1: a stage-0 failure on a return/fault delivery must NEVER fault the node's own
        # live invocation (junk on the return inbox) — floor only (the readable inbound report
        # logged in full), returning the cleared no-reply mirror.
        node = _Stage0RaisingNode(node_id="n", subscribe_topics=["in"])
        inbound = _framed_envelope(callback_topic="caller.return")
        inbound.reply = FaultMessage(in_reply_to="f", tag="t", error=ErrorReport(error_type="upstream.boom"))
        broker = _CaptureBroker()

        with caplog.at_level(logging.ERROR):
            resp = await node.handler(inbound, "cid", {HDR_KIND: "fault"}, broker)

        assert broker.published == []  # the live invocation is NOT faulted
        assert resp.body.reply is None  # cleared no-reply mirror
        assert resp.headers[HDR_KIND] == "call"
        assert "upstream.boom" in caplog.text  # the readable inbound report floored in full


class TestTailCallFrameIdentity:
    async def test_tailcall_preserves_frame_identity_and_retargets(self) -> None:
        # §4.2/§15: a TailCall is the SAME pending call retargeted — its replacement frame preserves
        # frame_id/tag/overrides/callback_topic (a fresh frame_id would orphan the caller's slot, so
        # the eventual reply's in_reply_to no longer matches), clears payload (TailCall carries no
        # body; the traveling State is its input) + fanout_id (a TailCall is never marked).
        node = _node()
        overrides = OverridesState()
        stack = CallFrameStack()
        stack.push(
            CallFrame(
                target_topic="agent.in",
                callback_topic="caller.return",
                frame_id="F1",
                tag="t1",
                overrides=overrides,
                payload="old-body",
                fanout_id="X",
            )
        )
        envelope = Envelope(internal_workflow_state=WorkflowState(call_stack=stack), context=SessionRunContext(state=State(), deps={}))
        broker = _CaptureBroker()

        await node._publish_action(TailCall(target_topic="next.topic", state=State()), envelope, "cid", broker)

        topic, env, headers = broker.published[0]
        assert topic == "next.topic"  # retargeted
        frame = env.internal_workflow_state.current_frame
        assert frame.frame_id == "F1"  # PRESERVED — not minted fresh (escalation-correctness)
        assert frame.tag == "t1"  # preserved
        assert frame.overrides is overrides  # preserved
        assert frame.callback_topic == "caller.return"  # preserved (the tailcallee inherits it)
        assert frame.target_topic == "next.topic"  # retargeted
        assert frame.payload is None  # cleared — TailCall carries no body
        assert frame.fanout_id is None  # cleared — a TailCall is never fan-out-marked
        assert headers[HDR_KIND] == "call"


class TestUnknownKind:
    async def test_unknown_kind_delivery_is_ignored_not_run_as_work(self, caplog: pytest.LogCaptureFixture) -> None:
        # §4.1 rule 2: an unrecognized x-calf-kind is ERROR-logged + ignored — the body never
        # runs (no publish), and the cleared no-reply mirror is returned. A readable inbound
        # FaultMessage is floored in full before ignoring.
        node = _ReturningNode(node_id="n", subscribe_topics=["in"])  # body WOULD publish a ReturnCall
        inbound = _framed_envelope(callback_topic="caller.return")
        inbound.reply = FaultMessage(in_reply_to="f", tag="t", error=ErrorReport(error_type="upstream.boom"))
        broker = _CaptureBroker()

        with caplog.at_level(logging.ERROR):
            resp = await node.handler(inbound, "cid", {HDR_KIND: "bogus"}, broker)

        assert broker.published == []  # the body did NOT run — no work on an unclassifiable delivery
        assert resp.body.reply is None  # cleared no-reply mirror
        assert resp.headers[HDR_KIND] == "call"
        assert "bogus" in caplog.text  # the unrecognized value is logged
        assert "upstream.boom" in caplog.text  # the readable inbound report floored in full


class TestStrayCheck:
    async def test_fault_kind_with_no_reply_is_a_stray_not_escalated(self, caplog: pytest.LogCaptureFixture) -> None:
        # Scenario 29: kind=fault but reply=None (kind/slot disagreement) → stray, WARNING + ignore.
        # NEVER the node-own-failure path: junk on the return inbox must not fault a live
        # invocation. The body never runs (no publish), cleared no-reply mirror returned.
        node = _ReturningNode(node_id="n", subscribe_topics=["in"])  # body WOULD publish a ReturnCall
        inbound = _framed_envelope(callback_topic="caller.return")  # reply is None
        broker = _CaptureBroker()

        with caplog.at_level(logging.WARNING):
            resp = await node.handler(inbound, "cid", {HDR_KIND: "fault"}, broker)

        assert broker.published == []  # not escalated, body did not run
        assert resp.body.reply is None  # cleared no-reply mirror
        assert resp.headers[HDR_KIND] == "call"
        assert "stray" in caplog.text.lower()

    async def test_stray_readable_fault_floors_and_broadcasts(self, caplog: pytest.LogCaptureFixture) -> None:
        # §6.7: a readable FaultMessage under a disagreeing header (kind=return) → ERROR + the
        # report floored in full + broadcast mirror (kind=fault headers) — never a bare warning,
        # and never a point-to-point publish (a stray has no caller relationship).
        node = _ReturningNode(node_id="n", subscribe_topics=["in"])
        inbound = _framed_envelope(callback_topic="caller.return")
        inbound.reply = FaultMessage(in_reply_to="f", tag="t", error=ErrorReport(error_type="upstream.boom"))
        broker = _CaptureBroker()

        with caplog.at_level(logging.ERROR):
            resp = await node.handler(inbound, "cid", {HDR_KIND: "return"}, broker)

        assert broker.published == []  # no point-to-point publish
        assert isinstance(resp.body.reply, FaultMessage)  # broadcast mirror carries the fault
        assert resp.headers[HDR_KIND] == "fault"
        assert resp.headers[HDR_ERROR_TYPE] == "upstream.boom"
        assert "upstream.boom" in caplog.text  # floored in full


class TestReceivedFaultEscalates:
    async def test_received_fault_escalates_to_the_caller_by_default(self) -> None:
        # §8 default (minimal stage-1): a node with no on_callee_error escalates a received callee
        # fault up its own rail — the SAME report (stable report_id) re-addressed to the node's
        # caller, never wrapped (§4.4). The body never runs.
        node = _node()
        report = ErrorReport(error_type="callee.boom", message="downstream failed")
        inbound = _framed_envelope(callback_topic="caller.return")
        inbound.reply = FaultMessage(in_reply_to="callee-frame", tag="tc", error=report)
        broker = _CaptureBroker()

        resp = await node.handler(inbound, "cid", {HDR_KIND: "fault"}, broker)

        assert len(broker.published) == 1
        topic, env, headers = broker.published[0]
        assert topic == "caller.return"
        assert isinstance(env.reply, FaultMessage)
        assert env.reply.error.report_id == report.report_id  # re-addressed, never wrapped
        assert env.reply.error.error_type == "callee.boom"
        assert headers[HDR_KIND] == "fault"
        assert isinstance(resp.body.reply, FaultMessage)  # broadcast mirror carries the fault

    async def test_escalating_callee_fault_does_not_trip_on_node_error(self) -> None:
        # R5 / §6.8: _BatchFaulted is RETURNED, never raised — escalating a callee fault must not
        # trip the node's OWN on_node_error (the swallow trap that would convert it to a success).
        node = _node()
        consulted: list[bool] = []

        @node.on_node_error
        def recover(ctx: object, fault: ErrorReport) -> str:
            consulted.append(True)
            return "should-never-recover-a-callee-fault"

        inbound = _framed_envelope(callback_topic="caller.return")
        inbound.reply = FaultMessage(in_reply_to="cf", tag="tc", error=ErrorReport(error_type="callee.boom"))
        broker = _CaptureBroker()

        await node.handler(inbound, "cid", {HDR_KIND: "fault"}, broker)

        assert consulted == []  # on_node_error NOT consulted for a received callee fault
        assert broker.published[0][1].reply.error.error_type == "callee.boom"  # escalated as-is


class _NeedsName(BaseModel):
    name: str


class _SchemaNode(BaseNodeDef):
    """A node whose only handler validates a schema — an ill-shaped payload is rejected."""

    @handler("*", schema=_NeedsName)
    async def run(self, ctx: SessionRunContext, payload: _NeedsName) -> Any:
        return ReturnCall(state=ctx.state, value=payload.name)


class TestDeclineAutoFault:
    async def test_reply_owing_all_declined_auto_faults(self) -> None:
        # §10 / scenario 15 (reply-owing half): a reply-owing delivery whose body declines auto-faults
        # the caller (calf.delivery.rejected, reason=all_declined) — #201 closed by construction, the
        # caller never hangs.
        node = _node()  # base run() declines (Next)
        inbound = _framed_envelope(callback_topic="caller.return")  # reply-owing (callback set)
        broker = _CaptureBroker()

        resp = await node.handler(inbound, "cid", {}, broker)  # kind=call

        assert len(broker.published) == 1
        topic, env, headers = broker.published[0]
        assert topic == "caller.return"
        assert isinstance(env.reply, FaultMessage)
        assert env.reply.error.error_type == "calf.delivery.rejected"
        assert env.reply.error.details["reason"] == "all_declined"
        assert headers[HDR_KIND] == "fault"
        assert isinstance(resp.body.reply, FaultMessage)  # broadcast mirror

    async def test_reply_owing_schema_rejection_auto_faults(self) -> None:
        # Scenario 39: a reply-owing delivery whose only matching handler rejects the body schema
        # auto-faults with details.reason="schema_rejected" (the dispatcher reports WHY it declined).
        node = _SchemaNode(node_id="n", subscribe_topics=["in"])
        inbound = _framed_envelope(payload={"wrong": "shape"}, callback_topic="caller.return")
        broker = _CaptureBroker()

        await node.handler(inbound, "cid", {}, broker)

        env = broker.published[0][1]
        assert isinstance(env.reply, FaultMessage)
        assert env.reply.error.error_type == "calf.delivery.rejected"
        assert env.reply.error.details["reason"] == "schema_rejected"

    async def test_fire_and_forget_all_declined_is_a_noop(self) -> None:
        # §10 / scenario 15 (f-a-f half): an all-declined fire-and-forget delivery (no callback owed)
        # stays a no-op — NOT auto-faulted (the stream-filter case; the auto-fault is gated on a
        # reply being owed).
        node = _node()  # declines
        inbound = _framed_envelope(callback_topic=None)  # fire-and-forget: not reply-owing
        broker = _CaptureBroker()

        resp = await node.handler(inbound, "cid", {}, broker)

        assert broker.published == []  # no fault — no reply owed
        assert resp.body.reply is None  # cleared no-reply mirror
