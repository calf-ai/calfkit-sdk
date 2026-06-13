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
    ReturnCall,
    SessionRunContext,
    Silent,
    State,
    TailCall,
    TextPart,
    WorkflowState,
)
from calfkit.models.reply import ReturnMessage
from calfkit.nodes.node import NodeDef

_CORR = "corr-1234"


class _CaptureBroker:
    """Node-side broker stub: records (topic, headers, envelope) per publish."""

    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, str], Any]] = []

    async def publish(self, envelope: Any, *, topic: str, correlation_id: str, key: bytes, headers: dict[str, str]) -> None:
        self.published.append((topic, headers, envelope))


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

    async def run(self, ctx: SessionRunContext) -> Any:
        return Silent()


class _CallNode(NodeDef[Any]):
    @handler("go")
    async def go(self, ctx: SessionRunContext) -> Any:
        return Call("downstream", ctx.state)

    async def run(self, ctx: SessionRunContext) -> Any:
        return Silent()


class _FanNode(NodeDef[Any]):
    @handler("go")
    async def go(self, ctx: SessionRunContext) -> Any:
        return [Call("a", ctx.state), Call("b", ctx.state)]

    async def run(self, ctx: SessionRunContext) -> Any:
        return Silent()


class _TailNode(NodeDef[Any]):
    @handler("go")
    async def go(self, ctx: SessionRunContext) -> Any:
        return TailCall("downstream", ctx.state)

    async def run(self, ctx: SessionRunContext) -> Any:
        return Silent()


async def _run(node: NodeDef[Any], env: Envelope, broker: _CaptureBroker) -> Any:
    return await node.handler(env, correlation_id=_CORR, headers={HDR_ROUTE: "go"}, broker=cast(Any, broker))


class TestReturnCallStamping:
    async def test_callback_publish_is_kind_return_with_reply_slot(self) -> None:
        broker = _CaptureBroker()
        node = _RetNode(node_id="n", subscribe_topics=["t"])
        await _run(node, _envelope(frame_id="F1", tag="call-7"), broker)

        assert len(broker.published) == 1
        topic, headers, env = broker.published[0]
        assert topic == "reply.topic"
        assert headers[HDR_KIND] == "return"
        assert isinstance(env.reply, ReturnMessage)
        assert env.reply.in_reply_to == "F1"  # echoes the popped frame_id
        assert env.reply.tag == "call-7"  # echoes the popped frame tag
        assert env.reply.parts == [TextPart(text="hello")]  # value coerced to parts

    async def test_mirror_response_carries_kind_return_and_reply(self) -> None:
        broker = _CaptureBroker()
        node = _RetNode(node_id="n", subscribe_topics=["t"])
        resp = await _run(node, _envelope(), broker)
        assert resp.headers[HDR_KIND] == "return"
        assert isinstance(resp.body.reply, ReturnMessage)


class TestCallStamping:
    async def test_call_publish_is_kind_call_with_no_reply(self) -> None:
        broker = _CaptureBroker()
        await _run(_CallNode(node_id="n", subscribe_topics=["t"]), _envelope(), broker)
        _topic, headers, env = broker.published[0]
        assert headers[HDR_KIND] == "call"
        assert env.reply is None

    async def test_mirror_response_is_kind_call(self) -> None:
        broker = _CaptureBroker()
        resp = await _run(_CallNode(node_id="n", subscribe_topics=["t"]), _envelope(), broker)
        assert resp.headers[HDR_KIND] == "call"


class TestFanOutStamping:
    async def test_each_sibling_is_kind_call_with_no_reply(self) -> None:
        broker = _CaptureBroker()
        await _run(_FanNode(node_id="n", subscribe_topics=["t"]), _envelope(), broker)
        assert {t for t, _, _ in broker.published} == {"a", "b"}
        for _topic, headers, env in broker.published:
            assert headers[HDR_KIND] == "call"
            assert env.reply is None


class TestTailCallStamping:
    async def test_tailcall_publish_is_kind_call_with_no_reply(self) -> None:
        broker = _CaptureBroker()
        await _run(_TailNode(node_id="n", subscribe_topics=["t"]), _envelope(), broker)
        _topic, headers, env = broker.published[0]
        assert headers[HDR_KIND] == "call"
        assert env.reply is None


class TestNoReplyMirrorClearsInboundReply:
    """I3 production-side guard: a node processing an inbound *return* that then
    fans out / goes silent must NOT re-broadcast the inbound reply on its mirror."""

    async def test_fanout_mirror_clears_inbound_reply(self) -> None:
        broker = _CaptureBroker()
        leak = ReturnMessage(in_reply_to="prev", tag="t", parts=[TextPart(text="LEAK")])
        resp = await _run(_FanNode(node_id="n", subscribe_topics=["t"]), _envelope(reply=leak), broker)
        assert resp.body.reply is None

    async def test_silent_mirror_clears_inbound_reply(self) -> None:
        class _SilentNode(NodeDef[Any]):
            @handler("go")
            async def go(self, ctx: SessionRunContext) -> Any:
                return Silent()

            async def run(self, ctx: SessionRunContext) -> Any:
                return Silent()

        broker = _CaptureBroker()
        leak = ReturnMessage(in_reply_to="prev", tag="t", parts=[TextPart(text="LEAK")])
        resp = await _run(_SilentNode(node_id="n", subscribe_topics=["t"]), _envelope(reply=leak), broker)
        assert resp.body.reply is None
