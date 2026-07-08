"""Unit tests for the agent tool-error reception surface (``calfkit/nodes/_tool_error.py``).

Covers the pure, in-process pieces: the **level-A** fault renderer (Phase 1), the budget-free
``surface_to_model()`` prebuilt (Phase 2), and the identity substrate — ``AgentSeamContext.tool_call``
+ ``resolve_tool_call``/``resolve_failing_tool_call`` (Phase 3c). The adapter, agent surface, and
end-to-end wiring are tested in their own suites (Phase 4/5).
"""

from __future__ import annotations

from typing import Any

import pytest

from calfkit._vendor.pydantic_ai.messages import ToolCallPart
from calfkit._vendor.pydantic_ai.models.test import TestModel
from calfkit.exceptions import RegistryConfigError
from calfkit.models.error_report import ErrorReport, ExceptionInfo, FrameRef
from calfkit.models.payload import TextPart, is_retry, retry_text_part
from calfkit.models.seam_context import CalleeResult
from calfkit.models.state import State
from calfkit.nodes import Agent
from calfkit.nodes._seams import ON_CALLEE_ERROR, run_chain
from calfkit.nodes._tool_error import (
    AgentSeamContext,
    _adapt_tool_error,
    render_fault_for_model,
    resolve_failing_tool_call,
    resolve_tool_call,
    surface_to_model,
)
from calfkit.nodes.base import BaseNodeDef


def _ctx_with_tool_call(tool_name: str = "web_search", tag: str = "c1") -> AgentSeamContext[State]:
    """An AgentSeamContext whose failing_call resolves (via state) to a registered tool call."""
    state = State()
    state.add_tool_call(ToolCallPart(tool_name, {"q": "x"}, tool_call_id=tag))
    return _agent_ctx(state=state, failing_call=CalleeResult(frame_id="f", tag=tag))


def _agent_ctx(*, failing_call: CalleeResult | None = None, state: State | None = None) -> AgentSeamContext[State]:
    """An ``AgentSeamContext`` with a single failing slot (or none, off the error stage)."""
    return AgentSeamContext(
        state=state if state is not None else State(),
        deps={},
        resources={},
        payload=None,
        node_id="n",
        correlation_id="c",
        emitter_node_id=None,
        route=None,
        delivery_kind="fault",
        awaiting_reply=True,
        failing_call=failing_call,
    )


class TestRenderFaultForModel:
    """Level A (spec D8): the top exception line only — ``exception.type: message`` when a harvested
    exception is present, else ``message`` alone. No ``__cause__`` walk; no internal fields leak."""

    def test_exception_present_renders_type_and_message(self) -> None:
        report = ErrorReport(
            error_type="calf.exception",
            message="rate limit exceeded, retry after 30s",
            exception=ExceptionInfo(type="RateLimitError"),
        )
        assert render_fault_for_model(report) == "RateLimitError: rate limit exceeded, retry after 30s"

    def test_exception_none_renders_message_alone(self) -> None:
        # A framework/transport fault (timeout, materialization failure) has no harvested exception.
        report = ErrorReport(error_type="calf.timeout", message="callee timed out after 30s")
        assert render_fault_for_model(report) == "callee timed out after 30s"

    def test_fault_group_renders_message_alone(self) -> None:
        # A calf.fault_group (reachable via a message_agent peer that fan-outs) has exception=None.
        report = ErrorReport(error_type="calf.fault_group", message="2 of 3 tool calls faulted")
        assert render_fault_for_model(report) == "2 of 3 tool calls faulted"

    def test_exception_present_empty_message_renders_type_only(self) -> None:
        # Degenerate: an exception with no message renders the type alone — no trailing ": ".
        report = ErrorReport(error_type="calf.exception", message="", exception=ExceptionInfo(type="ValueError"))
        assert render_fault_for_model(report) == "ValueError"

    def test_exception_none_empty_message_renders_empty(self) -> None:
        # D8-literal: exception is None → message alone; if the message is empty, so is the render.
        report = ErrorReport(error_type="calf.exception", message="")
        assert render_fault_for_model(report) == ""

    def test_no_cause_walk_level_a(self) -> None:
        # Level A renders ONLY the top line — a nested __cause__ chain (report.causes) never appears.
        inner = ErrorReport(error_type="calf.inner", message="INNER_CAUSE_MUST_NOT_APPEAR", exception=ExceptionInfo(type="KeyError"))
        report = ErrorReport(
            error_type="calf.exception",
            message="outer failed",
            exception=ExceptionInfo(type="RuntimeError"),
            causes=[inner],
        )
        rendered = render_fault_for_model(report)
        assert rendered == "RuntimeError: outer failed"
        assert "INNER_CAUSE_MUST_NOT_APPEAR" not in rendered
        assert "KeyError" not in rendered

    def test_excludes_all_internal_fields(self) -> None:
        # Only exception.type + message reach the model — every calfkit-internal field is withheld (D8).
        report = ErrorReport(
            error_type="calf.exception",
            message="boom",
            retryable=True,
            origin_node_id="SECRET_NODE",
            origin_frame_id="SECRET_FRAME",
            frame_chain=[FrameRef(frame_id="SECRET_FRAME_ID", target_topic="SECRET_TOPIC")],
            details={"secret": "SECRET_DETAIL"},
            causes=[ErrorReport(error_type="calf.inner", message="SECRET_CAUSE")],
            exception=ExceptionInfo(type="RuntimeError", module="SECRET_MODULE", attrs={"secret_attr": "SECRET_ATTR"}),
        )
        rendered = render_fault_for_model(report)
        assert rendered == "RuntimeError: boom"  # exact match already proves nothing else leaked
        for leaked in (
            "calf.exception",  # error_type
            "SECRET_NODE",  # origin_node_id
            "SECRET_FRAME",  # origin_frame_id / frame_chain
            "SECRET_TOPIC",  # frame_chain.target_topic
            "SECRET_DETAIL",  # details
            "SECRET_CAUSE",  # causes
            "SECRET_MODULE",  # exception.module
            "SECRET_ATTR",  # exception.attrs
            "retryable",
            "report_id",
        ):
            assert leaked not in rendered


class TestSurfaceToModel:
    """The budget-free ``surface_to_model()`` prebuilt (spec D9): a zero-arg ``on_tool_error`` handler
    that renders the fault (level A) and returns it ``is_error=True`` via the ``calf.retry`` marker. It
    renders from the ``report`` and ignores ``tool_call``/``ctx``."""

    def test_zero_arg_returns_a_callable_handler(self) -> None:
        assert callable(surface_to_model())

    def test_handler_returns_the_level_a_render_as_a_marked_text_part(self) -> None:
        report = ErrorReport(error_type="calf.exception", message="rate limited", exception=ExceptionInfo(type="RateLimitError"))
        result = surface_to_model()(None, None, report)  # tool_call/ctx unused — renders from report (D9)
        assert isinstance(result, TextPart)
        assert result.text == "RateLimitError: rate limited"
        assert result.text == render_fault_for_model(report)  # exactly the level-A renderer's output

    def test_handler_marks_is_error_via_calf_retry(self) -> None:
        # The calf.retry marker on the returned part is what materializes Anthropic is_error=True.
        result = surface_to_model()(None, None, ErrorReport(error_type="calf.timeout", message="timed out"))
        assert is_retry([result])

    def test_budget_free_converts_every_fault(self) -> None:
        # Budget-free (D9): the same handler converts EVERY fault — no per-tool counting or exhaustion.
        handler = surface_to_model()
        r1 = handler(None, None, ErrorReport(error_type="calf.exception", message="one", exception=ExceptionInfo(type="E")))
        r2 = handler(None, None, ErrorReport(error_type="calf.exception", message="two", exception=ExceptionInfo(type="E")))
        assert is_retry([r1]) and is_retry([r2])
        assert r1.text == "E: one" and r2.text == "E: two"


class TestResolveToolCall:
    """The shared ``tag → ToolCall`` lookup (spec D3), used by both ``ctx.tool_call`` and the agent's
    ``_resolve_slot``: **carriage-first** for the ``message_agent``/``isolate_state`` arm, else **state**."""

    def test_state_arm_returns_the_full_call_with_args(self) -> None:
        state = State()
        state.add_tool_call(ToolCallPart("web_search", {"q": "kafka"}, tool_call_id="c1"))
        call = resolve_tool_call(state, tag="c1", carried_tool_name=None)
        assert call is not None
        assert call.tool_name == "web_search"
        assert call.args == {"q": "kafka"}  # the FULL call — args intact (normal tool / fan-out sibling)

    def test_carriage_arm_reconstructs_with_args_none(self) -> None:
        # A carried tool_name (the message_agent arm) reconstructs a minimal call, args unknown.
        call = resolve_tool_call(State(), tag="m1", carried_tool_name="message_agent")
        assert call is not None
        assert call.tool_name == "message_agent"
        assert call.args is None  # the peer arm can't recover args (documented limitation, D3/D5)
        assert call.tool_call_id == "m1"

    def test_carriage_arm_never_reads_the_foreign_state_collision_guard(self) -> None:
        # THE collision guard (Phase-0 finding): the folded state is the PEER's — it may contain a
        # tool-call under the SAME tag. Carriage-first must ignore it and return the message_agent
        # reconstruction, never the peer's foreign call.
        peer_state = State()
        peer_state.add_tool_call(ToolCallPart("PEER_TOOL", {"a": 1}, tool_call_id="m1"))
        call = resolve_tool_call(peer_state, tag="m1", carried_tool_name="message_agent")
        assert call is not None
        assert call.tool_name == "message_agent"  # NOT "PEER_TOOL"
        assert call.args is None  # NOT the peer's {"a": 1}

    def test_missing_tag_returns_none(self) -> None:
        state = State()
        state.add_tool_call(ToolCallPart("web_search", {"q": "x"}, tool_call_id="c1"))
        assert resolve_tool_call(state, tag="absent", carried_tool_name=None) is None  # tag not registered
        assert resolve_tool_call(state, tag=None, carried_tool_name=None) is None  # no tag
        assert resolve_tool_call(state, tag="", carried_tool_name=None) is None  # empty tag


class TestResolveFailingToolCall:
    """``resolve_failing_tool_call(ctx)`` — the ``ctx.failing_call``-based wrapper backing
    ``ctx.tool_call``. ``None`` off the ``on_callee_error`` stage."""

    def test_off_error_stage_returns_none(self) -> None:
        # No failing_call (e.g. before_node, or a non-error firing) → not tool-attributable → None.
        assert resolve_failing_tool_call(_agent_ctx(failing_call=None)) is None

    def test_normal_tool_fold_resolves_from_state_with_args(self) -> None:
        state = State()
        state.add_tool_call(ToolCallPart("web_search", {"q": "kafka"}, tool_call_id="c1"))
        ctx = _agent_ctx(state=state, failing_call=CalleeResult(frame_id="f", tag="c1", tool_name=None))
        call = resolve_failing_tool_call(ctx)
        assert call is not None and call.tool_name == "web_search" and call.args == {"q": "kafka"}

    def test_message_agent_fold_resolves_from_carriage(self) -> None:
        # The failing_call carries tool_name (message_agent arm); the state is the PEER's (foreign).
        peer_state = State()
        peer_state.add_tool_call(ToolCallPart("PEER_TOOL", {"a": 1}, tool_call_id="bx"))
        ctx = _agent_ctx(
            state=peer_state,
            failing_call=CalleeResult(frame_id="f", tag="m1", tool_name="message_agent", target_topic="agent.billing.private.input"),
        )
        call = resolve_failing_tool_call(ctx)
        assert call is not None and call.tool_name == "message_agent" and call.args is None


class TestAgentSeamContextToolCall:
    """``AgentSeamContext.tool_call`` is a lazy property delegating to ``resolve_failing_tool_call``."""

    def test_tool_call_property_resolves_the_failing_tool(self) -> None:
        state = State()
        state.add_tool_call(ToolCallPart("web_search", {"q": "x"}, tool_call_id="c1"))
        ctx = _agent_ctx(state=state, failing_call=CalleeResult(frame_id="f", tag="c1"))
        assert ctx.tool_call is not None and ctx.tool_call.tool_name == "web_search"

    def test_tool_call_is_none_off_error_stage(self) -> None:
        assert _agent_ctx(failing_call=None).tool_call is None

    def test_agent_seam_context_is_a_seam_context(self) -> None:
        # LSP: it IS a SeamContext (covariant), so it carries every base seam field.
        from calfkit.models.seam_context import SeamContext

        ctx: Any = _agent_ctx(failing_call=None)
        assert isinstance(ctx, SeamContext)
        assert ctx.delivery_kind == "fault" and ctx.callee_results == []


class TestAdaptToolError:
    """The thin agent-owned adapter (spec D5/D6): hoist ``ctx.tool_call`` to the flat ``tool_call``
    param, decline when the fault is not tool-attributable, and let the return flow through untouched."""

    def test_hoists_tool_call_to_the_flat_param(self) -> None:
        seen: dict[str, Any] = {}

        def handler(tool_call: Any, ctx: Any, report: Any) -> str:
            seen["tool_call"] = tool_call
            return "handled"

        result = _adapt_tool_error(handler)(_ctx_with_tool_call("web_search"), ErrorReport(error_type="calf.exception", message="boom"))
        assert result == "handled"
        assert seen["tool_call"].tool_name == "web_search"  # ctx.tool_call hoisted to the flat param

    def test_declines_when_not_tool_attributable(self) -> None:
        called: list[bool] = []

        def handler(tool_call: Any, ctx: Any, report: Any) -> str:
            called.append(True)
            return "should-not-run"

        # ctx.tool_call is None (no failing_call) → the adapter declines (D6b) WITHOUT calling the handler.
        assert _adapt_tool_error(handler)(_agent_ctx(failing_call=None), ErrorReport(error_type="x")) is None
        assert called == []

    def test_return_flows_through_untouched(self) -> None:
        # D6f: whatever the handler returns is passed through unchanged (no adapter-level coercion).
        part = retry_text_part("boom")

        def handler(tool_call: Any, ctx: Any, report: Any) -> Any:
            return part

        assert _adapt_tool_error(handler)(_ctx_with_tool_call(), ErrorReport(error_type="x")) is part

    def test_wraps_names_the_handler_and_registers_at_arity_two(self) -> None:
        def my_search_handler(tool_call: Any, ctx: Any, report: Any) -> None:
            return None

        wrapped = _adapt_tool_error(my_search_handler)
        assert wrapped.__name__ == "my_search_handler"  # §13 INFO names the developer's handler (needs 3a)
        node = BaseNodeDef(node_id="n", subscribe_topics=["in"])
        node.on_callee_error(wrapped)  # registers as an arity-2 on_callee_error entry (proves the 3a fix)
        assert wrapped in node._chains[ON_CALLEE_ERROR]

    async def test_async_handler_is_awaited_through_run_chain(self) -> None:
        async def handler(tool_call: Any, ctx: Any, report: Any) -> str:
            return "async-handled"

        wrapped = _adapt_tool_error(handler)
        result = await run_chain([wrapped], _ctx_with_tool_call(), ErrorReport(error_type="x"), seam_name=ON_CALLEE_ERROR)
        assert result == "async-handled"  # the sync wrapper returns the coroutine; run_chain awaits it


class TestOnToolErrorSurface:
    """The agent surface (spec D5/D6): ``Agent(on_tool_error=)`` + ``@agent.on_tool_error``, arity-3
    validated on both paths, merged onto the base ``on_callee_error`` chain (``on_tool_error`` first)."""

    def _agent(self, **kwargs: Any) -> Agent[str]:
        return Agent("a", subscribe_topics="a.in", model_client=TestModel(), **kwargs)

    def test_ctor_registers_wrapped_on_the_callee_error_chain(self) -> None:
        def handler(tool_call: Any, ctx: Any, report: Any) -> None:
            return None

        agent = self._agent(on_tool_error=handler)
        assert len(agent._chains[ON_CALLEE_ERROR]) == 1  # wrapped and registered

    def test_ctor_accepts_a_list(self) -> None:
        def a(tool_call: Any, ctx: Any, report: Any) -> None:
            return None

        def b(tool_call: Any, ctx: Any, report: Any) -> None:
            return None

        agent = self._agent(on_tool_error=[a, b])
        assert len(agent._chains[ON_CALLEE_ERROR]) == 2

    def test_ctor_arity_is_validated(self) -> None:
        def bad(ctx: Any, report: Any) -> None:  # arity 2 — wrong for on_tool_error (needs tool_call, ctx, report)
            return None

        with pytest.raises(RegistryConfigError):
            self._agent(on_tool_error=bad)

    def test_decorator_registers_and_returns_fn_unchanged(self) -> None:
        agent = self._agent()

        @agent.on_tool_error
        def handler(tool_call: Any, ctx: Any, report: Any) -> None:
            return None

        assert handler.__name__ == "handler" and callable(handler)  # returns fn unchanged (directly testable)
        assert len(agent._chains[ON_CALLEE_ERROR]) == 1

    def test_decorator_arity_is_validated(self) -> None:
        agent = self._agent()
        with pytest.raises(RegistryConfigError):

            @agent.on_tool_error
            def bad(ctx: Any, report: Any) -> None:
                return None

    def test_merge_order_on_tool_error_before_raw_on_callee_error(self) -> None:
        # D6e: ctor on_tool_error handlers register BEFORE raw on_callee_error, so the promoted surface
        # wins the first-non-None chain; the inherited on_callee_error param is NOT clobbered.
        def tool_err(tool_call: Any, ctx: Any, report: Any) -> None:
            return None

        def raw(ctx: Any, report: Any) -> None:
            return None

        agent = self._agent(on_tool_error=tool_err, on_callee_error=raw)
        chain = agent._chains[ON_CALLEE_ERROR]
        assert len(chain) == 2
        assert chain[0].__name__ == "tool_err"  # the wrapped on_tool_error, first (wraps names it)
        assert chain[1] is raw  # the raw on_callee_error, second, un-clobbered

    def test_decorator_appends_after_ctor_entries(self) -> None:
        # A decorator-registered handler always appends after ctor entries (base §6.1 chain-order).
        def raw(ctx: Any, report: Any) -> None:
            return None

        agent = self._agent(on_callee_error=raw)

        @agent.on_tool_error
        def late(tool_call: Any, ctx: Any, report: Any) -> None:
            return None

        chain = agent._chains[ON_CALLEE_ERROR]
        assert chain[0] is raw and chain[1].__name__ == "late"
