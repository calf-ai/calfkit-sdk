"""Stage 1 of run/handler unification.

``BaseNodeDef.run`` becomes a *declining* ``@handler('*')`` (the registry's
lowest-precedence catch-all), so a node's ``run`` is dispatched through the same
Chain-of-Responsibility path as any ``@handler`` route. Tool dispatch moves from
the positional ``input_args`` channel to a schema-validated ``ToolCallRef``
payload (carried as a routeless ``Call(body=...)``), and the producer-side
"body requires route" guard is relaxed because the ``'*'`` handler can read a
routeless body.
"""

import logging
from typing import Any, cast

import pytest
from pydantic import BaseModel, ValidationError

from calfkit._protocol import HDR_ROUTE
from calfkit._registry import handler
from calfkit._routing import is_concrete_route_key, match_chain, route_matches
from calfkit.exceptions import RegistryConfigError
from calfkit.models import Call, CallFrame, CallFrameStack, Envelope, Silent, State, ToolContext, WorkflowState
from calfkit.models.session_context import SessionRunContext
from calfkit.models.tool_dispatch import ToolCallRef
from calfkit.nodes.node import NodeDef
from calfkit.nodes.tool import ToolNodeDef

_CORR = "corr-run-unif-0"


def _envelope(*, callback_topic: str | None = None, payload: Any = None) -> Envelope:
    stack = CallFrameStack()
    stack.push(CallFrame(target_topic="t", callback_topic=callback_topic, payload=payload))
    return Envelope(internal_workflow_state=WorkflowState(call_stack=stack), context=SessionRunContext(state=State(), deps={}))


async def _handle(node: Any, headers: dict[str, Any], env: Envelope | None = None) -> Any:
    return await node.handler(env or _envelope(), correlation_id=_CORR, headers=headers, broker=cast(Any, None))


# ---------------------------------------------------------------------------
# ToolCallRef payload model
# ---------------------------------------------------------------------------


def test_tool_call_ref_carries_id_and_forbids_extra() -> None:
    assert ToolCallRef(tool_call_id="tc-1").tool_call_id == "tc-1"
    with pytest.raises(ValidationError):
        ToolCallRef(tool_call_id="x", surprise="nope")  # type: ignore[call-arg]
    with pytest.raises(ValidationError):
        ToolCallRef()  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# Base run() as a declining @handler('*')
# ---------------------------------------------------------------------------


def test_run_may_be_redecorated_handler_star_with_schema() -> None:
    # Previously a RegistryConfigError (run + @handler('*') conflict). Now run()
    # IS the '*' handler, so re-decorating it to attach a schema is allowed.
    class Body(BaseModel):
        val: str

    class N(NodeDef[Any]):
        @handler("*", schema=Body)
        async def run(self, ctx: SessionRunContext, payload: Body) -> Any:  # type: ignore[override]
            return Silent()

    assert "*" in N.routes()


async def test_run_star_handler_receives_routeless_payload() -> None:
    class Body(BaseModel):
        val: str

    seen: dict[str, Any] = {}

    class N(NodeDef[Any]):
        @handler("*", schema=Body)
        async def run(self, ctx: SessionRunContext, payload: Body) -> Any:  # type: ignore[override]
            seen["val"] = payload.val
            return Silent()

    await _handle(N(node_id="n", subscribe_topics=["t"]), {}, _envelope(payload={"val": "hi"}))
    assert seen["val"] == "hi"


async def test_plain_run_override_serves_routeless_message_as_star() -> None:
    seen: list[str] = []

    class N(NodeDef[Any]):
        async def run(self, ctx: SessionRunContext) -> Any:
            seen.append("run")
            return Silent()

    await _handle(N(node_id="n", subscribe_topics=["t"]), {})
    assert seen == ["run"]


async def test_routes_only_node_skips_unmatched_via_declining_base_run() -> None:
    seen: list[str] = []

    class N(NodeDef[Any]):
        @handler("order.created")
        async def on_created(self, ctx: SessionRunContext) -> Any:
            seen.append("created")
            return Silent()

    env = _envelope(callback_topic=None)
    resp = await _handle(N(node_id="n", subscribe_topics=["t"]), {HDR_ROUTE: "payment.x"}, env)
    assert seen == []  # neither on_created nor a fallback ran
    assert resp.body is env  # base run declined -> nothing published


# ---------------------------------------------------------------------------
# The pairing rule now also covers run()
# ---------------------------------------------------------------------------


def test_extra_positional_run_without_schema_raises_at_class_def() -> None:
    with pytest.raises(RegistryConfigError):
        # The class name is never used — defining the class is what raises (its
        # __init_subclass__ runs _validate_routes). The leading underscore marks the
        # name intentionally-unused (CodeQL) while staying CapWords-valid (ruff N801).
        class _N(NodeDef[Any]):
            async def run(self, ctx: SessionRunContext, extra: Any) -> Any:  # type: ignore[override]
                return Silent()


# ---------------------------------------------------------------------------
# F1b: a routeless body is allowed (read by the '*' handler)
# ---------------------------------------------------------------------------


def test_call_allows_body_without_route() -> None:
    c = Call("t", State(), body={"x": 1})
    assert c.body == {"x": 1}
    assert c.route is None


def test_routing_none_key_matches_only_star() -> None:
    # Load-bearing: `route_matches` checks `pattern == "*"` BEFORE touching the key,
    # so a header-less (None) route matches ONLY '*'. A reorder would silently break
    # every header-less + tool-dispatch message — pin it here.
    assert route_matches("*", None) is True
    assert route_matches("order.*", None) is False
    assert route_matches("order.created", None) is False
    assert is_concrete_route_key(None) is False
    assert match_chain(None, {"order.*": "x", "*": "run"}) == ["*"]


async def test_malformed_toolcallref_body_to_tool_declines_and_logs_loud(caplog: pytest.LogCaptureFixture) -> None:
    # Known limitation (ADR risk register): a tool dispatch whose body fails ToolCallRef
    # validation is declined at the dispatcher — run() is never entered, so NO ReturnCall
    # is published and an awaiting agent relies on its reply-TTL. Not reachable via the
    # agent (which always sends a valid ToolCallRef); pinned here to assert the decline is
    # LOUD (callback-aware WARNING), not silent, so a hung workflow is diagnosable.
    def _ok(ctx: ToolContext) -> str:
        return "ok"  # never invoked — validation fails first

    tool = ToolNodeDef.create_tool_node(func=_ok, subscribe_topics="t.in", publish_topic="t.out")
    published: list[Any] = []

    class _Broker:
        async def publish(self, *a: Any, **k: Any) -> None:
            published.append((a, k))

    env = _envelope(callback_topic="agent.return", payload={"surprise": "nope"})  # not a valid ToolCallRef
    with caplog.at_level(logging.DEBUG, logger="calfkit.nodes.base"):
        resp = await tool.handler(env, correlation_id=_CORR, headers={}, broker=cast(Any, _Broker()))

    assert published == []  # no ReturnCall — the documented gap
    assert resp.body is env
    validation_logs = [r for r in caplog.records if "validation" in r.message]
    assert validation_logs and validation_logs[0].levelno == logging.WARNING


async def test_unconsumed_routeless_body_is_logged_callback_aware(caplog: pytest.LogCaptureFixture) -> None:
    # F1b residual: a routeless body reaching a node whose '*'/run has no schema is
    # dropped (base run declines) and surfaced at WARNING when a caller awaits a reply.
    class N(NodeDef[Any]):
        @handler("order.created")
        async def on_created(self, ctx: SessionRunContext) -> Any:
            return Silent()

    env = _envelope(callback_topic="reply.topic", payload={"unread": 1})
    with caplog.at_level(logging.DEBUG, logger="calfkit.nodes.base"):
        resp = await _handle(N(node_id="n", subscribe_topics=["t"]), {}, env)

    assert resp.body is env  # nothing published; body dropped
    dropped = [r for r in caplog.records if "body was not consumed" in r.message]
    assert dropped and dropped[0].levelno == logging.WARNING
