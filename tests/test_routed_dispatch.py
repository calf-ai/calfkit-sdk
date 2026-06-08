"""§8.2 — header route dispatch: the Next sentinel, wire-model carriers, and the
Chain-of-Responsibility dispatch in BaseNodeDef.handler()."""

from typing import Any

import pytest
from pydantic import BaseModel

from calfkit._protocol import HDR_ROUTE
from calfkit._registry import handler
from calfkit.exceptions import RegistryConfigError
from calfkit.models import Call, CallFrame, CallFrameStack, Next, ReturnCall, SessionRunContext, Silent, State, TailCall, WorkflowState
from calfkit.nodes.node import NodeDef

_CORR = "corr1234"


def _ctx() -> SessionRunContext:
    return SessionRunContext(state=State(), deps={})


async def _dispatch(node: Any, route: str, payload: Any = None) -> Any:
    return await node._dispatch_routed(_ctx(), route, payload, None, awaiting_reply=False, correlation_id=_CORR)


def test_next_is_a_distinct_sentinel_from_silent() -> None:
    assert isinstance(Next(), Next)
    assert not isinstance(Next(), Silent)
    assert not isinstance(Silent(), Next)


def test_hdr_route_header_name() -> None:
    assert HDR_ROUTE == "x-calf-route"


def test_callframe_carries_optional_payload_defaulting_none() -> None:
    assert CallFrame(target_topic="t", callback_topic=None).payload is None
    framed = CallFrame(target_topic="t", callback_topic=None, payload={"x": 1})
    assert framed.payload == {"x": 1}


def test_invoke_frame_threads_explicit_payload_onto_the_frame() -> None:
    ws = WorkflowState(call_stack=CallFrameStack())
    call = Call("target", object(), route="order.created", body={"k": 1})
    ws.invoke_frame(call, "callback.topic", payload=call.body)
    assert ws.current_frame.payload == {"k": 1}
    assert ws.current_frame.target_topic == "target"


def test_call_carries_optional_route_and_body() -> None:
    call = Call("topic", object(), route="order.created", body={"x": 1})
    assert call.route == "order.created"
    assert call.body == {"x": 1}
    plain = Call("topic", object())
    assert plain.route is None and plain.body is None


def test_tailcall_does_not_accept_route_or_body() -> None:
    with pytest.raises(TypeError):
        TailCall("topic", object(), route="x")  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# Chain-of-Responsibility dispatch (_dispatch_routed)
# ---------------------------------------------------------------------------


async def test_specific_handler_short_circuits() -> None:
    class N(NodeDef[Any]):
        @handler("order.created")
        async def on_created(self, ctx: SessionRunContext) -> Any:
            return Call("specific", ctx.state)

        @handler("order.*")
        async def on_any(self, ctx: SessionRunContext) -> Any:
            return Call("general", ctx.state)

        async def run(self, ctx: SessionRunContext) -> Any:
            return Silent()

    out = await _dispatch(N(node_id="n", subscribe_topics=["t"]), "order.created")
    assert isinstance(out, Call) and out.target_topic == "specific"


async def test_next_advances_to_more_general_handler() -> None:
    class N(NodeDef[Any]):
        @handler("order.created")
        async def on_created(self, ctx: SessionRunContext) -> Any:
            return Next()

        @handler("order.*")
        async def on_any(self, ctx: SessionRunContext) -> Any:
            return Call("general", ctx.state)

        async def run(self, ctx: SessionRunContext) -> Any:
            return Silent()

    out = await _dispatch(N(node_id="n", subscribe_topics=["t"]), "order.created")
    assert isinstance(out, Call) and out.target_topic == "general"


async def test_falls_through_to_run_when_all_handlers_decline() -> None:
    class N(NodeDef[Any]):
        @handler("order.created")
        async def on_created(self, ctx: SessionRunContext) -> Any:
            return Next()

        async def run(self, ctx: SessionRunContext) -> Any:
            return ReturnCall(state=ctx.state)

    out = await _dispatch(N(node_id="n", subscribe_topics=["t"]), "order.created")
    assert isinstance(out, ReturnCall)


async def test_no_match_and_no_run_fallback_returns_none() -> None:
    class N(NodeDef[Any]):
        @handler("order.created")
        async def on_created(self, ctx: SessionRunContext) -> Any:
            return Call("x", ctx.state)

    out = await _dispatch(N(node_id="n", subscribe_topics=["t"]), "payment.created")
    assert out is None


async def test_valid_payload_is_validated_and_injected() -> None:
    class Body(BaseModel):
        amount: int

    captured: dict[str, Any] = {}

    class N(NodeDef[Any]):
        @handler("order.created", schema=Body)
        async def on_created(self, ctx: SessionRunContext, payload: Body) -> Any:
            captured["payload"] = payload
            return Call("ok", ctx.state)

        async def run(self, ctx: SessionRunContext) -> Any:
            return Silent()

    out = await _dispatch(N(node_id="n", subscribe_topics=["t"]), "order.created", payload={"amount": 5})
    assert isinstance(out, Call) and out.target_topic == "ok"
    assert captured["payload"] == Body(amount=5)


async def test_invalid_payload_skips_to_next_handler() -> None:
    class Body(BaseModel):
        amount: int

    class N(NodeDef[Any]):
        @handler("order.created", schema=Body)
        async def on_created(self, ctx: SessionRunContext, payload: Body) -> Any:
            return Call("specific", ctx.state)

        @handler("order.*")
        async def on_any(self, ctx: SessionRunContext) -> Any:
            return Call("general", ctx.state)

        async def run(self, ctx: SessionRunContext) -> Any:
            return Silent()

    out = await _dispatch(N(node_id="n", subscribe_topics=["t"]), "order.created", payload={"nope": True})
    assert isinstance(out, Call) and out.target_topic == "general"


# ---------------------------------------------------------------------------
# Class-definition validation (§5.1 pairing rule + run/@handler("*") ambiguity)
# ---------------------------------------------------------------------------


def test_payload_param_without_schema_raises_at_class_definition() -> None:
    with pytest.raises(RegistryConfigError):

        class N(NodeDef[Any]):
            @handler("order.created")
            async def on_created(self, ctx: SessionRunContext, payload: Any) -> Any: ...

            async def run(self, ctx: SessionRunContext) -> Any:
                return Silent()


def test_schema_without_payload_param_raises_at_class_definition() -> None:
    class Body(BaseModel):
        x: int

    with pytest.raises(RegistryConfigError):

        class N(NodeDef[Any]):
            @handler("order.created", schema=Body)
            async def on_created(self, ctx: SessionRunContext) -> Any: ...

            async def run(self, ctx: SessionRunContext) -> Any:
                return Silent()


def test_explicit_star_handler_with_overridden_run_raises() -> None:
    with pytest.raises(RegistryConfigError):

        class N(NodeDef[Any]):
            @handler("*")
            async def catch_all(self, ctx: SessionRunContext) -> Any:
                return Silent()

            async def run(self, ctx: SessionRunContext) -> Any:
                return Silent()
