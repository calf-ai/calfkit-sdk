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
from aiokafka.errors import KafkaError  # type: ignore[import-untyped]

from calfkit._protocol import HDR_ERROR_TYPE, HDR_KIND
from calfkit.exceptions import NodeFaultError
from calfkit.models import CallFrame, CallFrameStack, Envelope, ReturnCall, SessionRunContext, State, TextPart, WorkflowState
from calfkit.models.error_report import ErrorReport
from calfkit.models.reply import FaultMessage, ReturnMessage
from calfkit.models.seam_context import SeamContext
from calfkit.nodes.base import BaseNodeDef


def _node(**kwargs: object) -> BaseNodeDef:
    return BaseNodeDef(node_id="orchestrator", subscribe_topics=["in"], **kwargs)


class _CaptureBroker:
    """Records (topic, envelope, headers) per publish."""

    def __init__(self) -> None:
        self.published: list[tuple[str, Any, dict[str, str]]] = []

    async def publish(self, envelope: Any, *, topic: str, correlation_id: str, key: bytes, headers: dict[str, str]) -> None:
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
