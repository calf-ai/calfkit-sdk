"""PR-A Layer-4 — the publish chokepoint stamps ``x-calf-kind`` + the reply slot per
branch, and the broadcast mirror (the handler's ``Response``) carries the hop's kind.
No-reply hops clear ``reply`` so a stale inbound reply can't ride out (the I3
production-side guard).
"""

from __future__ import annotations

from typing import Any, cast

from calfkit._protocol import HDR_KIND, HDR_ROUTE
from calfkit._registry import handler
from calfkit.models import (
    Call,
    CallFrame,
    CallFrameStack,
    Envelope,
    Next,
    ReturnCall,
    SessionRunContext,
    State,
    TailCall,
    TextPart,
    WorkflowState,
)
from calfkit.models.reply import ReturnMessage
from calfkit.nodes.node import NodeDef
from tests.fakes import CaptureBroker

_CORR = "corr-1234"


def _envelope(
    *,
    callback_topic: str | None = "reply.topic",
    frame_id: str = "F1",
    tag: str | None = None,
    reply: ReturnMessage | None = None,
) -> Envelope:
    stack = CallFrameStack()
    stack.push(CallFrame(target_topic="t", callback_topic=callback_topic, frame_id=frame_id, tag=tag))
    return Envelope(
        internal_workflow_state=WorkflowState(call_stack=stack),
        context=SessionRunContext(state=State(), deps={}),
        reply=reply,
    )


class _RetNode(NodeDef[Any]):
    @handler("go")
    async def go(self, ctx: SessionRunContext) -> Any:
        return ReturnCall(state=ctx.state, value="hello")


class _CallNode(NodeDef[Any]):
    @handler("go")
    async def go(self, ctx: SessionRunContext) -> Any:
        return Call("downstream", ctx.state)


class _FanNode(NodeDef[Any]):
    @handler("go")
    async def go(self, ctx: SessionRunContext) -> Any:
        return [Call("a", ctx.state), Call("b", ctx.state)]


class _TailNode(NodeDef[Any]):
    @handler("go")
    async def go(self, ctx: SessionRunContext) -> Any:
        return TailCall("downstream", ctx.state)


async def _run(node: NodeDef[Any], env: Envelope, broker: CaptureBroker) -> Any:
    return await node.handler(env, correlation_id=_CORR, headers={HDR_ROUTE: "go"}, broker=cast(Any, broker))


class TestReturnCallStamping:
    async def test_callback_publish_is_kind_return_with_reply_slot(self) -> None:
        broker = CaptureBroker()
        node = _RetNode(node_id="n", subscribe_topics=["t"])
        await _run(node, _envelope(frame_id="F1", tag="call-7"), broker)

        assert len(broker.published) == 1
        c = broker.published[0]
        assert c.topic == "reply.topic"
        assert c.headers[HDR_KIND] == "return"
        assert isinstance(c.message.reply, ReturnMessage)
        assert c.message.reply.in_reply_to == "F1"  # echoes the popped frame_id
        assert c.message.reply.tag == "call-7"  # echoes the popped frame tag
        assert c.message.reply.parts == [TextPart(text="hello")]  # value coerced to parts

    async def test_mirror_response_carries_kind_return_and_reply(self) -> None:
        broker = CaptureBroker()
        node = _RetNode(node_id="n", subscribe_topics=["t"])
        resp = await _run(node, _envelope(), broker)
        assert resp.headers[HDR_KIND] == "return"
        assert isinstance(resp.body.reply, ReturnMessage)


class TestCallStamping:
    async def test_call_publish_is_kind_call_with_no_reply(self) -> None:
        broker = CaptureBroker()
        await _run(_CallNode(node_id="n", subscribe_topics=["t"]), _envelope(), broker)
        c = broker.published[0]
        assert c.headers[HDR_KIND] == "call"
        assert c.message.reply is None

    async def test_mirror_response_is_kind_call(self) -> None:
        broker = CaptureBroker()
        resp = await _run(_CallNode(node_id="n", subscribe_topics=["t"]), _envelope(), broker)
        assert resp.headers[HDR_KIND] == "call"


class TestFanOutStamping:
    async def test_each_sibling_is_kind_call_with_no_reply(self) -> None:
        broker = CaptureBroker()
        await _run(_FanNode(node_id="n", subscribe_topics=["t"]), _envelope(), broker)
        assert {c.topic for c in broker.published} == {"a", "b"}
        for c in broker.published:
            assert c.headers[HDR_KIND] == "call"
            assert c.message.reply is None


class TestTailCallStamping:
    async def test_tailcall_publish_is_kind_call_with_no_reply(self) -> None:
        broker = CaptureBroker()
        await _run(_TailNode(node_id="n", subscribe_topics=["t"]), _envelope(), broker)
        c = broker.published[0]
        assert c.headers[HDR_KIND] == "call"
        assert c.message.reply is None


class TestCorrelationKeying:
    """Every point-to-point publish is partition-keyed by ``correlation_id``.

    This is the single-writer affinity the durable fan-out fold and tool-return ordering
    rest on (one batch's siblings/returns all land on one partition, one owner, serial).
    A safety-net pin for the staged-handler restructure: a publish that drops or mis-sets
    the key would fork a batch across partitions and was previously unguarded.
    """

    async def test_call_publish_is_correlation_keyed(self) -> None:
        broker = CaptureBroker()
        await _run(_CallNode(node_id="n", subscribe_topics=["t"]), _envelope(), broker)
        assert broker.keys == [_CORR.encode()]

    async def test_returncall_callback_publish_is_correlation_keyed(self) -> None:
        broker = CaptureBroker()
        await _run(_RetNode(node_id="n", subscribe_topics=["t"]), _envelope(), broker)
        assert broker.keys == [_CORR.encode()]

    async def test_tailcall_publish_is_correlation_keyed(self) -> None:
        broker = CaptureBroker()
        await _run(_TailNode(node_id="n", subscribe_topics=["t"]), _envelope(), broker)
        assert broker.keys == [_CORR.encode()]

    async def test_fanout_siblings_are_all_correlation_keyed(self) -> None:
        broker = CaptureBroker()
        await _run(_FanNode(node_id="n", subscribe_topics=["t"]), _envelope(), broker)
        assert broker.keys == [_CORR.encode(), _CORR.encode()]


class TestNoReplyMirrorClearsInboundReply:
    """I3 production-side guard: a node processing an inbound *return* that then
    fans out / goes silent must NOT re-broadcast the inbound reply on its mirror."""

    async def test_fanout_mirror_clears_inbound_reply(self) -> None:
        broker = CaptureBroker()
        leak = ReturnMessage(in_reply_to="prev", tag="t", parts=[TextPart(text="LEAK")])
        resp = await _run(_FanNode(node_id="n", subscribe_topics=["t"]), _envelope(reply=leak), broker)
        assert resp.body.reply is None

    async def test_no_result_mirror_clears_inbound_reply(self) -> None:
        # The handler-level no-result/all-declined path mirrors kind=call, reply cleared.
        class _DeclineNode(NodeDef[Any]):
            async def run(self, ctx: SessionRunContext) -> Any:
                return Next()  # decline → no handler produced a result

        node = _DeclineNode(node_id="n", subscribe_topics=["t"])
        leak = ReturnMessage(in_reply_to="prev", tag="t", parts=[TextPart(text="LEAK")])
        resp = await _run(node, _envelope(reply=leak), CaptureBroker())
        assert resp.body.reply is None
        assert resp.headers[HDR_KIND] == "call"


class TestReplyStamping:
    """prepare_context stamps ctx._reply from envelope.reply (after the deep-copy,
    unconditionally), and ctx.output_parts is the permanent reply-sourced accessor."""

    async def test_stamps_reply_and_exposes_output_parts(self) -> None:
        node = _CallNode(node_id="n", subscribe_topics=["t"])
        reply = ReturnMessage(in_reply_to="F1", tag=None, parts=[TextPart(text="out")])
        ctx = await node.prepare_context(_envelope(reply=reply), correlation_id=_CORR)
        assert ctx._reply == reply
        assert ctx.output_parts == [TextPart(text="out")]

    async def test_call_kind_reply_none_yields_empty_output_parts(self) -> None:
        # Unconditional stamp: a call-kind delivery (reply=None) clears any stale value.
        node = _CallNode(node_id="n", subscribe_topics=["t"])
        ctx = await node.prepare_context(_envelope(reply=None), correlation_id=_CORR)
        assert ctx._reply is None
        assert ctx.output_parts == []

    async def test_stamp_clears_stale_inbound_reply_on_call_kind(self) -> None:
        # THE pin for the unconditional stamp: a stale _reply on the inbound context
        # (in-process reuse) survives model_copy(deep=True), so prepare_context MUST
        # overwrite it with envelope.reply (None here) — the guarded form would leak it.
        node = _CallNode(node_id="n", subscribe_topics=["t"])
        env = _envelope(reply=None)  # call-kind delivery
        env.context._reply = ReturnMessage(in_reply_to="stale", tag=None, parts=[TextPart(text="STALE")])
        ctx = await node.prepare_context(env, correlation_id=_CORR)
        assert ctx._reply is None
        assert ctx.output_parts == []

    def test_unstamped_context_output_parts_empty_not_error(self) -> None:
        # A context built outside a handler must yield [] (loud-by-emptiness), never raise.
        ctx = SessionRunContext(state=State(), deps={})
        assert ctx.output_parts == []
