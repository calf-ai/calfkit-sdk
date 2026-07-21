"""``x-calf-task`` forwarding on every node-plane publish path (prep spec §2-B / §5).

The task substrate's carriage half: the handler-injected ``task_id`` (the middleware's
scoped value) must ride ``_headers()`` onto EVERY outbound publish and broadcast-mirror
``Response`` — the four ``_publish_action`` branches, the fan-out sibling build, the
re-entry self-publish, the fault rail (point-to-point AND mirror, including the stray
floor), both no-reply mirrors, and the observer mirror. Forwarded UNCHANGED: these
direct drives pass a distinct value and assert it verbatim — a re-mint or a drop at any
site would fail here (the missed-arm trap the plan's Phase-1.7 tests exist for).

Step messages are deliberately absent: their header stamp is PR-2's (spec §3 ownership
split); the step KEY flip is Phase 2's.
"""

from typing import Any

from calfkit._protocol import HDR_ERROR_TYPE, HDR_KIND, HDR_TASK
from calfkit._registry import handler
from calfkit._vendor.pydantic_ai.messages import ModelResponse, TextPart
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit.models import Call, CallFrame, CallFrameStack, Envelope, ReturnCall, SessionRunContext, TailCall, WorkflowState
from calfkit.models.error_report import ErrorReport
from calfkit.models.reply import FaultMessage
from calfkit.models.session_context import Stack
from calfkit.models.state import State
from calfkit.nodes import Agent
from calfkit.nodes._fanout_store import FANOUT_STORE_KEY
from calfkit.nodes.consumer import ConsumerNode
from calfkit.nodes.node import NodeDef
from tests._broker_fakes import CaptureBroker
from tests._fanout_fakes import FakeFanoutBatchStore

_CORR = "corr-fwd-1"
_TASK = "task-under-test"


def _envelope(callback_topic: str | None = "reply.topic", reply: Any = None) -> Envelope:
    stack = CallFrameStack()
    stack.push(CallFrame(target_topic="t", callback_topic=callback_topic))
    return Envelope(
        internal_workflow_state=WorkflowState(call_stack=stack),
        context=SessionRunContext(state=State(), deps={}),
        reply=reply,
    )


async def _drive(node: NodeDef, *, callback_topic: str | None = "reply.topic", headers: dict | None = None) -> tuple[CaptureBroker, Any]:
    spy = CaptureBroker()
    resp = await node.handler(_envelope(callback_topic), correlation_id=_CORR, task_id=_TASK, headers=headers or {}, broker=spy)
    return spy, resp


def _assert_all_publishes_carry_task(spy: CaptureBroker) -> None:
    assert spy.published, "the driven path emitted no publish — the test drove nothing"
    for call in spy.published:
        assert call.headers.get(HDR_TASK) == _TASK, f"publish to topic={call.topic!r} lost/re-minted x-calf-task: got {call.headers.get(HDR_TASK)!r}"


# ── the four _publish_action branches ──────────────────────────────────────────


async def test_single_call_dispatch_forwards_the_task_header() -> None:
    class N(NodeDef):
        async def run(self, ctx: SessionRunContext) -> Any:
            return Call("downstream.topic", ctx.state)

    spy, _ = await _drive(N(node_id="n-fwd-call", subscribe_topics=["t"]))
    _assert_all_publishes_carry_task(spy)


async def test_parallel_call_dispatch_forwards_on_every_sibling() -> None:
    class N(NodeDef):
        async def run(self, ctx: SessionRunContext) -> Any:
            return [Call("down.a", ctx.state), Call("down.b", ctx.state)]

    spy, _ = await _drive(N(node_id="n-fwd-par", subscribe_topics=["t"]))
    assert len(spy.published) == 2
    _assert_all_publishes_carry_task(spy)


async def test_returncall_reply_forwards_the_task_header() -> None:
    class N(NodeDef):
        async def run(self, ctx: SessionRunContext) -> Any:
            return ReturnCall(state=ctx.state, value="done")

    spy, _ = await _drive(N(node_id="n-fwd-ret", subscribe_topics=["t"]))
    _assert_all_publishes_carry_task(spy)


async def test_tailcall_self_retry_forwards_the_task_header() -> None:
    class N(NodeDef):
        async def run(self, ctx: SessionRunContext) -> Any:
            return TailCall("t", ctx.state)

    spy, _ = await _drive(N(node_id="n-fwd-tail", subscribe_topics=["t"]))
    _assert_all_publishes_carry_task(spy)


# ── the fault rail (point-to-point + broadcast mirror + the stray floor) ───────


async def test_auto_fault_forwards_the_task_header() -> None:
    class N(NodeDef):
        @handler("known.route")
        async def on_known(self, ctx: SessionRunContext) -> Any:
            return ReturnCall(state=ctx.state, value="handled")

    spy, resp = await _drive(N(node_id="n-fwd-af", subscribe_topics=["t"]), headers={"x-calf-route": "unmatched.route"})
    _assert_all_publishes_carry_task(spy)
    assert spy.published[0].headers.get(HDR_ERROR_TYPE)  # we are on the fault rail
    assert resp.headers.get(HDR_TASK) == _TASK  # the fault's broadcast mirror carries it too


async def test_body_raise_fault_forwards_the_task_header() -> None:
    class N(NodeDef):
        async def run(self, ctx: SessionRunContext) -> Any:
            raise RuntimeError("boom")

    spy, resp = await _drive(N(node_id="n-fwd-boom", subscribe_topics=["t"]))
    _assert_all_publishes_carry_task(spy)
    assert resp.headers.get(HDR_TASK) == _TASK


async def test_stray_fault_floor_response_carries_the_task_header() -> None:
    # kind=return over a FaultMessage slot → the stray fault floor's broadcast Response
    # (no point-to-point publish; the fault-headered mirror is the observable).
    class N(NodeDef):
        async def run(self, ctx: SessionRunContext) -> Any:
            return ReturnCall(state=ctx.state, value="never runs")

    env = _envelope(reply=FaultMessage(in_reply_to="x", tag=None, error=ErrorReport(error_type="calf.exception")))
    spy = CaptureBroker()
    resp = await N(node_id="n-fwd-stray", subscribe_topics=["t"]).handler(
        env, correlation_id=_CORR, task_id=_TASK, headers={HDR_KIND: "return"}, broker=spy
    )
    assert resp.headers.get(HDR_KIND) == "fault"
    assert resp.headers.get(HDR_TASK) == _TASK


# ── the no-reply and broadcast mirrors ─────────────────────────────────────────


async def test_broadcast_mirror_response_carries_the_task_header() -> None:
    class N(NodeDef):
        async def run(self, ctx: SessionRunContext) -> Any:
            return ReturnCall(state=ctx.state, value="done")

    _, resp = await _drive(N(node_id="n-fwd-mirror", subscribe_topics=["t"]))
    assert resp.headers.get(HDR_TASK) == _TASK


async def test_fire_and_forget_declined_mirror_carries_the_task_header() -> None:
    # No matching route + no callback → the declined no-op arm's no-reply mirror.
    class N(NodeDef):
        @handler("known.route")
        async def on_known(self, ctx: SessionRunContext) -> Any:
            return ReturnCall(state=ctx.state, value="handled")

    spy, resp = await _drive(N(node_id="n-fwd-noop", subscribe_topics=["t"]), callback_topic=None, headers={"x-calf-route": "unmatched.route"})
    assert not spy.published  # nothing owed, nothing published
    assert resp.headers.get(HDR_TASK) == _TASK


async def test_consumer_observer_mirror_carries_the_task_header() -> None:
    async def _consume(cctx: Any) -> None: ...

    node = ConsumerNode(name="c-fwd", subscribe_topics=["t"], consume_fn=_consume)
    spy = CaptureBroker()
    resp = await node.handler(_envelope(), correlation_id=_CORR, task_id=_TASK, headers={}, broker=spy)
    assert resp.headers.get(HDR_TASK) == _TASK


# ── fan-out: the sibling build + the re-entry self-publish ─────────────────────


def _fanout_agent() -> Agent[str]:
    def _model(_messages: object, _info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[TextPart("ok")])

    return Agent(name="a", subscribe_topics=["a.in"], model_client=FunctionModel(_model))


async def test_fanout_sibling_publishes_forward_the_task_header() -> None:
    agent = _fanout_agent()
    ctx = SessionRunContext(state=State(), deps={})
    ctx._resources = {FANOUT_STORE_KEY: FakeFanoutBatchStore()}
    ctx._correlation_id = _CORR
    own = CallFrame(target_topic="a", callback_topic="caller", frame_id="A")
    env = Envelope(
        context=SessionRunContext(state=State(), deps={}),
        internal_workflow_state=WorkflowState(call_stack=Stack([own])),
    )
    calls = [Call("tool.a", State(), tag="tc1"), Call("tool.b", State(), tag="tc2")]
    spy = CaptureBroker()

    await agent._handle_fanout_open(ctx, calls, env, _CORR, _TASK, spy)

    assert len(spy.published) == 2
    _assert_all_publishes_carry_task(spy)


async def test_reentry_self_publish_forwards_the_task_header() -> None:
    # The re-entry is a LIVE forward path in prep (its durable-side deletion is PR-2's):
    # threaded down the fold chain, it must carry the task header like any publish.
    node = NodeDef(node_id="n-fwd-re", subscribe_topics=["t"])
    frame = CallFrame(target_topic="n-fwd-re", callback_topic="caller.return", frame_id="A", fanout_id="A")
    env = Envelope(
        context=SessionRunContext(state=State(), deps={}),
        internal_workflow_state=WorkflowState(call_stack=Stack([frame])),
    )
    spy = CaptureBroker()

    await node._publish_reentry(env, _CORR, _TASK, spy)

    _assert_all_publishes_carry_task(spy)
