"""§8.2 — header route dispatch: the Next sentinel, wire-model carriers, and the
Chain-of-Responsibility dispatch in BaseNodeDef.handler()."""

import logging
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


def test_call_rejects_malformed_route_at_construction() -> None:
    # Peer Call validation is symmetric with the client path.
    with pytest.raises(ValueError):
        Call("downstream", object(), route="order.")
    with pytest.raises(ValueError):
        Call("downstream", object(), route="order.*")


def test_call_rejects_body_without_route_at_construction() -> None:
    with pytest.raises(ValueError):
        Call("downstream", object(), body={"x": 1})


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


# ---------------------------------------------------------------------------
# Deep-review coverage additions (round 1)
# ---------------------------------------------------------------------------


class _CaptureBroker:
    """Node-side broker stub: records (topic, headers, envelope) per publish."""

    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, str], Any]] = []

    async def publish(self, envelope: Any, *, topic: str, correlation_id: str, key: bytes, headers: dict[str, str]) -> None:
        self.published.append((topic, headers, envelope))


async def test_explicit_star_handler_on_agent_raises() -> None:
    # An Agent's run() IS the LLM loop = the implicit "*" fallback, so an explicit
    # @handler("*") on an Agent subclass is the ambiguous-catch-all error.
    from calfkit.nodes.agent import BaseAgentNodeDef

    with pytest.raises(RegistryConfigError):

        class MyAgent(BaseAgentNodeDef):  # type: ignore[misc]
            @handler("*")
            async def catch_all(self, ctx: SessionRunContext) -> Any:
                return Silent()


async def test_silent_from_handler_is_terminal_and_does_not_advance() -> None:
    seen: list[str] = []

    class N(NodeDef[Any]):
        @handler("order.created")
        async def on_created(self, ctx: SessionRunContext) -> Any:
            seen.append("created")
            return Silent()

        @handler("order.*")
        async def on_any(self, ctx: SessionRunContext) -> Any:
            seen.append("any")
            return Call("nope", ctx.state)

        async def run(self, ctx: SessionRunContext) -> Any:
            return Silent()

    out = await _dispatch(N(node_id="n", subscribe_topics=["t"]), "order.created")
    assert isinstance(out, Silent)
    assert seen == ["created"]  # Silent short-circuited; on_any never ran


async def test_handler_exception_propagates_and_aborts_chain() -> None:
    seen: list[str] = []

    class Boom(Exception): ...

    class N(NodeDef[Any]):
        @handler("order.created")
        async def on_created(self, ctx: SessionRunContext) -> Any:
            raise Boom()

        @handler("order.*")
        async def on_any(self, ctx: SessionRunContext) -> Any:
            seen.append("any")
            return Silent()

        async def run(self, ctx: SessionRunContext) -> Any:
            return Silent()

    with pytest.raises(Boom):
        await _dispatch(N(node_id="n", subscribe_topics=["t"]), "order.created")
    assert seen == []  # chain aborted; more-general handler never ran


async def test_subclass_specific_route_intercepts_before_inherited_general() -> None:
    order: list[str] = []

    class Base(NodeDef[Any]):
        @handler("order.*")
        async def on_any(self, ctx: SessionRunContext) -> Any:
            order.append("base.general")
            return Silent()

        async def run(self, ctx: SessionRunContext) -> Any:
            return Silent()

    class Child(Base):
        @handler("order.created")
        async def on_created(self, ctx: SessionRunContext) -> Any:
            order.append("child.specific")
            return Next()  # decline → fall to inherited general

    await _dispatch(Child(node_id="n", subscribe_topics=["t"]), "order.created")
    assert order == ["child.specific", "base.general"]


async def test_schema_handler_with_no_body_skips_to_next() -> None:
    seen: list[str] = []

    class Body(BaseModel):
        amount: int

    class N(NodeDef[Any]):
        @handler("order.created", schema=Body)
        async def on_created(self, ctx: SessionRunContext, payload: Body) -> Any:
            seen.append("schema")
            return Silent()

        @handler("order.*")
        async def on_any(self, ctx: SessionRunContext) -> Any:
            seen.append("general")
            return Silent()

        async def run(self, ctx: SessionRunContext) -> Any:
            return Silent()

    await _dispatch(N(node_id="n", subscribe_topics=["t"]), "order.created", payload=None)
    assert seen == ["general"]  # None body fails schema → skip to general


def test_valid_schema_pairings_do_not_raise() -> None:
    class Body(BaseModel):
        x: int

    class N(NodeDef[Any]):
        @handler("order.created", schema=Body)
        async def typed(self, ctx: SessionRunContext, payload: Body) -> Any:
            return Silent()

        @handler("order.*")
        async def plain(self, ctx: SessionRunContext) -> Any:
            return Silent()

        async def run(self, ctx: SessionRunContext) -> Any:
            return Silent()

    assert set(N.routes()) == {"order.created", "order.*"}


async def test_parallel_fanout_stamps_per_call_route_and_body() -> None:
    class N(NodeDef[Any]):
        @handler("trigger")
        async def go(self, ctx: SessionRunContext) -> Any:
            return [
                Call("a", ctx.state, route="order.created", body={"i": 0}),
                Call("b", ctx.state),  # no route
            ]

        async def run(self, ctx: SessionRunContext) -> Any:
            return Silent()

    broker = _CaptureBroker()
    await N(node_id="n", subscribe_topics=["t"]).handler(_envelope(), correlation_id=_CORR, headers={HDR_ROUTE: "trigger"}, broker=cast(Any, broker))

    by_topic = {topic: (headers, env) for topic, headers, env in broker.published}
    assert by_topic["a"][0][HDR_ROUTE] == "order.created"
    assert by_topic["a"][1].internal_workflow_state.current_frame.payload == {"i": 0}
    assert HDR_ROUTE not in by_topic["b"][0]  # plain Call carries no route


async def test_tailcall_publish_carries_no_route_header() -> None:
    class N(NodeDef[Any]):
        @handler("trigger")
        async def go(self, ctx: SessionRunContext) -> Any:
            return TailCall("downstream", ctx.state)

        async def run(self, ctx: SessionRunContext) -> Any:
            return Silent()

    broker = _CaptureBroker()
    await N(node_id="n", subscribe_topics=["t"]).handler(_envelope(), correlation_id=_CORR, headers={HDR_ROUTE: "trigger"}, broker=cast(Any, broker))
    assert len(broker.published) == 1
    _topic, headers, _env = broker.published[0]
    assert HDR_ROUTE not in headers


async def test_invoke_node_threads_route_and_body_to_the_wire() -> None:
    from calfkit.client.client import Client
    from calfkit.client.reply_dispatcher import _ReplyDispatcher

    conn = _StubConn()
    client = Client(cast(Any, conn), "reply.topic", _ReplyDispatcher(reply_ttl=None), emitter_id="client.test")
    await client.invoke_node("hello", "orders", route="order.created", body={"n": 1})

    assert len(conn.published) == 1
    _topic, headers, env = conn.published[0]
    assert headers[HDR_ROUTE] == "order.created"
    assert env.internal_workflow_state.current_frame.payload == {"n": 1}


@pytest.mark.parametrize("callback_topic,expected", [("reply.topic", logging.WARNING), (None, logging.DEBUG)])
async def test_no_match_log_level_keys_on_callback_presence(callback_topic: str | None, expected: int, caplog: pytest.LogCaptureFixture) -> None:
    class N(NodeDef[Any]):
        @handler("order.created")
        async def on_created(self, ctx: SessionRunContext) -> Any:
            return Silent()

    env = _envelope(callback_topic=callback_topic)
    with caplog.at_level(logging.DEBUG, logger="calfkit.nodes.base"):
        await N(node_id="n", subscribe_topics=["t"]).handler(env, correlation_id=_CORR, headers={HDR_ROUTE: "payment.x"}, broker=cast(Any, None))
    matched = [r for r in caplog.records if "no handler matched" in r.message]
    assert matched and matched[0].levelno == expected


# ---------------------------------------------------------------------------
# Round-2 coverage: pin the precise round-1-fix interactions
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad_key", ["order.", ""])
async def test_malformed_key_still_reaches_lone_star_handler(bad_key: str) -> None:
    class N(NodeDef[Any]):
        @handler("*")
        async def star(self, ctx: SessionRunContext) -> Any:
            return Call("star", ctx.state)

    out = await _dispatch(N(node_id="n", subscribe_topics=["t"]), bad_key)
    assert isinstance(out, Call) and out.target_topic == "star"


async def test_all_matched_handlers_decline_with_no_run_returns_none() -> None:
    class N(NodeDef[Any]):
        @handler("order.*")
        async def on_any(self, ctx: SessionRunContext) -> Any:
            return None  # decline, and there is no run() fallback

    out = await _dispatch(N(node_id="n", subscribe_topics=["t"]), "order.created")
    assert out is None


@pytest.mark.parametrize("awaiting,expected", [(True, logging.WARNING), (False, logging.DEBUG)])
async def test_schema_skip_log_level_keys_on_awaiting_reply(awaiting: bool, expected: int, caplog: pytest.LogCaptureFixture) -> None:
    class Body(BaseModel):
        amount: int

    class N(NodeDef[Any]):
        @handler("order.created", schema=Body)
        async def typed(self, ctx: SessionRunContext, payload: Body) -> Any:
            return Silent()

        @handler("order.*")
        async def general(self, ctx: SessionRunContext) -> Any:
            return Silent()

        async def run(self, ctx: SessionRunContext) -> Any:
            return Silent()

    node = N(node_id="n", subscribe_topics=["t"])
    with caplog.at_level(logging.DEBUG, logger="calfkit.nodes.base"):
        await node._dispatch_routed(_ctx(), "order.created", {"bad": True}, None, awaiting_reply=awaiting, correlation_id=_CORR)
    recs = [r for r in caplog.records if "validation" in r.message]
    assert recs and recs[0].levelno == expected


@pytest.mark.parametrize("awaiting,expected", [(True, logging.WARNING), (False, logging.DEBUG)])
async def test_malformed_route_log_level_keys_on_awaiting_reply(awaiting: bool, expected: int, caplog: pytest.LogCaptureFixture) -> None:
    class N(NodeDef[Any]):
        @handler("order.*")
        async def on_any(self, ctx: SessionRunContext) -> Any:
            return Silent()

        async def run(self, ctx: SessionRunContext) -> Any:
            return Silent()

    node = N(node_id="n", subscribe_topics=["t"])
    with caplog.at_level(logging.DEBUG, logger="calfkit.nodes.base"):
        await node._dispatch_routed(_ctx(), "order.", None, None, awaiting_reply=awaiting, correlation_id=_CORR)
    recs = [r for r in caplog.records if "malformed inbound route" in r.message]
    assert recs and recs[0].levelno == expected


async def test_execute_node_forwards_route_and_body(monkeypatch: pytest.MonkeyPatch) -> None:
    from calfkit.client.client import Client
    from calfkit.client.reply_dispatcher import _ReplyDispatcher

    client = Client(cast(Any, _StubConn()), "reply.topic", _ReplyDispatcher(reply_ttl=None), emitter_id="client.test")
    captured: dict[str, Any] = {}

    class _FakeHandle:
        async def result(self, timeout: float | None = None) -> str:
            return "ok"

    async def _fake_invoke(*args: Any, **kwargs: Any) -> Any:
        captured.update(kwargs)
        return _FakeHandle()

    monkeypatch.setattr(client, "invoke_node", _fake_invoke)
    await client.execute_node("hello", "orders", route="order.created", body={"n": 1})
    assert captured.get("route") == "order.created"
    assert captured.get("body") == {"n": 1}
