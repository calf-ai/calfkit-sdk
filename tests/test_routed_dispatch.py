"""§8.2 — header route dispatch: the Next sentinel, wire-model carriers, and the
Chain-of-Responsibility dispatch in BaseNodeDef.handler()."""

from typing import Any, cast

import pytest
from pydantic import BaseModel

from calfkit._protocol import HDR_ROUTE
from calfkit._registry import handler
from calfkit.exceptions import RegistryConfigError
from calfkit.models import Call, CallFrame, CallFrameStack, Envelope, Next, ReturnCall, SessionRunContext, Silent, State, TailCall, WorkflowState
from calfkit.nodes.node import NodeDef

_CORR = "corr1234"


def _ctx() -> SessionRunContext:
    return SessionRunContext(state=State(), deps={})


async def _dispatch(node: Any, route: str, payload: Any = None) -> Any:
    return await node._dispatch_routed(_ctx(), route, payload, None, awaiting_reply=False, correlation_id=_CORR)


def _envelope(*, callback_topic: str | None = "reply.topic", payload: Any = None) -> Envelope:
    stack = CallFrameStack()
    stack.push(CallFrame(target_topic="t", callback_topic=callback_topic, payload=payload))
    return Envelope(internal_workflow_state=WorkflowState(call_stack=stack), context=SessionRunContext(state=State(), deps={}))


async def _handle(node: Any, headers: dict[str, Any], envelope: Envelope | None = None) -> Any:
    return await node.handler(envelope or _envelope(), correlation_id=_CORR, headers=headers, broker=cast(Any, None))


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


def test_call_eq_and_repr_include_route_and_body() -> None:
    a = Call("t", 1, route="r1", body={"x": 1})
    b = Call("t", 1, route="r2", body={"x": 2})
    c = Call("t", 1, route="r1", body={"x": 1})
    assert a != b  # route/body distinguish otherwise-identical Calls
    assert a == c
    assert "r1" in repr(a)  # repr surfaces the route


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


async def test_none_return_advances_chain_like_next() -> None:
    # A handler that returns None (e.g. forgot to return) declines and advances,
    # same as Next() — it does not terminate the chain.
    class N(NodeDef[Any]):
        @handler("order.created")
        async def on_created(self, ctx: SessionRunContext) -> Any:
            return None  # forgot to return / explicit decline

        @handler("order.*")
        async def on_any(self, ctx: SessionRunContext) -> Any:
            return Call("general", ctx.state)

        async def run(self, ctx: SessionRunContext) -> Any:
            return Silent()

    out = await _dispatch(N(node_id="n", subscribe_topics=["t"]), "order.created")
    assert isinstance(out, Call) and out.target_topic == "general"


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


async def test_malformed_inbound_route_does_not_partial_match_and_falls_to_fallback() -> None:
    # A malformed inbound key (trailing dot) must NOT partial-match `order.*`;
    # it routes to the run() fallback only (mature "normalize-or-404" behavior).
    seen: list[str] = []

    class N(NodeDef[Any]):
        @handler("order.*")
        async def on_any(self, ctx: SessionRunContext) -> Any:
            seen.append("on_any")
            return Silent()

        async def run(self, ctx: SessionRunContext) -> Any:
            seen.append("run")
            return Silent()

    await _dispatch(N(node_id="n", subscribe_topics=["t"]), "order.")
    assert seen == ["run"]


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


# ---------------------------------------------------------------------------
# handler() integration: route header drives dispatch; no header -> legacy run()
# ---------------------------------------------------------------------------


async def test_handler_dispatches_by_route_header() -> None:
    seen: list[str] = []

    class N(NodeDef[Any]):
        @handler("order.created")
        async def on_created(self, ctx: SessionRunContext) -> Any:
            seen.append("created")
            return Silent()

        @handler("order.*")
        async def on_any(self, ctx: SessionRunContext) -> Any:
            seen.append("any")
            return Silent()

        async def run(self, ctx: SessionRunContext) -> Any:
            seen.append("run")
            return Silent()

    await _handle(N(node_id="n", subscribe_topics=["t"]), {HDR_ROUTE: "order.created"})
    assert seen == ["created"]  # specific handler only (short-circuit), not run()


async def test_handler_without_route_header_runs_legacy_run() -> None:
    seen: list[str] = []

    class N(NodeDef[Any]):
        @handler("order.created")
        async def on_created(self, ctx: SessionRunContext) -> Any:
            seen.append("created")
            return Silent()

        async def run(self, ctx: SessionRunContext) -> Any:
            seen.append("run")
            return Silent()

    await _handle(N(node_id="n", subscribe_topics=["t"]), {})
    assert seen == ["run"]


async def test_handler_no_match_runs_nothing_and_returns_envelope_unchanged() -> None:
    seen: list[str] = []

    class N(NodeDef[Any]):
        @handler("order.created")
        async def on_created(self, ctx: SessionRunContext) -> Any:
            seen.append("created")
            return Silent()

    env = _envelope(callback_topic=None)
    resp = await _handle(N(node_id="n", subscribe_topics=["t"]), {HDR_ROUTE: "payment.x"}, env)
    assert seen == []
    assert resp.body is env  # envelope returned unchanged, nothing published


async def test_call_with_route_stamps_header_and_frame_payload() -> None:
    published: list[tuple[str, dict[str, str], Any]] = []

    class StubBroker:
        async def publish(self, envelope: Any, *, topic: str, correlation_id: str, key: bytes, headers: dict[str, str]) -> None:
            published.append((topic, headers, envelope))

    class N(NodeDef[Any]):
        @handler("trigger")
        async def go(self, ctx: SessionRunContext) -> Any:
            return Call("downstream", ctx.state, route="order.created", body={"amount": 7})

        async def run(self, ctx: SessionRunContext) -> Any:
            return Silent()

    node = N(node_id="n", subscribe_topics=["t"])
    await node.handler(_envelope(), correlation_id=_CORR, headers={HDR_ROUTE: "trigger"}, broker=cast(Any, StubBroker()))

    assert len(published) == 1
    topic, headers, env = published[0]
    assert topic == "downstream"
    assert headers[HDR_ROUTE] == "order.created"
    assert env.internal_workflow_state.current_frame.payload == {"amount": 7}


# ---------------------------------------------------------------------------
# Client ingress: route/body stamped at _publish_call; wildcard route rejected
# ---------------------------------------------------------------------------


class _StubConn:
    _connection = True  # truthy -> skip start()

    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, str], Any]] = []

    async def publish(self, envelope: Any, *, topic: str, correlation_id: str, headers: dict[str, str]) -> None:
        self.published.append((topic, headers, envelope))


def _client(conn: Any) -> Any:
    from calfkit.client.base import BaseClient
    from calfkit.client.reply_dispatcher import _ReplyDispatcher

    return BaseClient(cast(Any, conn), "reply.topic", _ReplyDispatcher(reply_ttl=None), emitter_id="client.test")


async def test_client_publish_call_stamps_route_and_body() -> None:
    conn = _StubConn()
    await _client(conn)._publish_call(
        topic="orders",
        correlation_id=_CORR,
        callback_topic="reply.topic",
        state=State(),
        overrides=None,
        run_args=None,
        deps=None,
        route="order.created",
        body={"amount": 9},
    )
    assert len(conn.published) == 1
    topic, headers, env = conn.published[0]
    assert topic == "orders"
    assert headers[HDR_ROUTE] == "order.created"
    assert env.internal_workflow_state.current_frame.payload == {"amount": 9}


async def test_client_rejects_body_without_route() -> None:
    conn = _StubConn()
    with pytest.raises(ValueError):
        await _client(conn)._publish_call(
            topic="orders",
            correlation_id=_CORR,
            callback_topic=None,
            state=State(),
            overrides=None,
            run_args=None,
            deps=None,
            route=None,
            body={"x": 1},
        )


@pytest.mark.parametrize("bad_route", ["order.*", "order.", "a..b", ".order"])
async def test_client_rejects_non_concrete_producer_route(bad_route: str) -> None:
    conn = _StubConn()
    with pytest.raises(ValueError):
        await _client(conn)._publish_call(
            topic="orders",
            correlation_id=_CORR,
            callback_topic=None,
            state=State(),
            overrides=None,
            run_args=None,
            deps=None,
            route=bad_route,
            body=None,
        )


async def test_emit_to_node_threads_route_and_body_to_the_wire() -> None:
    from calfkit.client.client import Client
    from calfkit.client.reply_dispatcher import _ReplyDispatcher

    conn = _StubConn()
    client = Client(cast(Any, conn), "reply.topic", _ReplyDispatcher(reply_ttl=None), emitter_id="client.test")
    await client.emit_to_node("hello", "orders", route="order.created", body={"amount": 3})

    assert len(conn.published) == 1
    _topic, headers, env = conn.published[0]
    assert headers[HDR_ROUTE] == "order.created"
    assert env.internal_workflow_state.current_frame.payload == {"amount": 3}
