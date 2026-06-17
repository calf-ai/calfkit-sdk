"""PR-6 step 3 — the staged fault pipeline (fault-rail spec §6.8).

Tests for the pipeline rewrite: _build_seam_context (the SeamContext the seams get,
sharing the body's run_ctx.state), the fault boundary, _publish_fault/_publish_abort,
the §6.8 stages, _classify widen, stray handling, and the auto-fault. See
notes/pr6-fault-rail-implementation-plan.md §3 step 3 / §4 layer 7.
"""

from __future__ import annotations

from calfkit.models import CallFrame, CallFrameStack, Envelope, SessionRunContext, State, WorkflowState
from calfkit.models.seam_context import SeamContext
from calfkit.nodes.base import BaseNodeDef


def _node(**kwargs: object) -> BaseNodeDef:
    return BaseNodeDef(node_id="orchestrator", subscribe_topics=["in"], **kwargs)


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
