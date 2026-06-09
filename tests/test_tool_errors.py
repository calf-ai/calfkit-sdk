"""Unit-scope contracts for the ``FailedToolCall`` / ``ToolExecutionError``
flow. Tests bypass ``TestKafkaBroker`` and ``Client`` and call ``run()``
directly so the marker/exception contracts are isolated from the messaging
layer.
"""

from __future__ import annotations

import asyncio
import logging
import pickle  # nosec B403 - used in test for regression coverage of ToolExecutionError picklability
from typing import Annotated

import pytest
from pydantic import BeforeValidator, ValidationError

from calfkit._vendor.pydantic_ai.exceptions import ModelRetry
from calfkit._vendor.pydantic_ai.messages import (
    ModelMessage,
    ModelResponse,
    RetryPromptPart,
    ToolCallPart,
    ToolReturn,
)
from calfkit._vendor.pydantic_ai.messages import (
    TextPart as ModelTextPart,
)
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit._vendor.pydantic_ai.models.test import TestModel
from calfkit.exceptions import ToolExecutionError
from calfkit.models import SessionRunContext, ToolCallRef, ToolContext
from calfkit.models.actions import Call, ReturnCall, Silent, TailCall
from calfkit.models.node_schema import BaseToolNodeSchema
from calfkit.models.state import (
    FailedToolCall,
    OverridesState,
    PendingToolBatch,
    State,
    _calf_tool_result_discriminator,
)
from calfkit.nodes import Agent, ToolNodeDef

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ctx(
    state: State,
    correlation_id: str = "cid-tool-errors-00000000",
    frame_id: str | None = None,
) -> SessionRunContext:
    ctx = SessionRunContext(state=state, deps={})
    # ``_correlation_id`` / ``_frame_id`` are ``PrivateAttr``s populated by
    # ``BaseNodeDef.prepare_context`` in the live path; tests that drive
    # ``agent.run`` directly must set them here. ``_frame_id`` matters for
    # parallel-mode aggregation (which keys ``_pending_batches`` on it).
    ctx._correlation_id = correlation_id
    if frame_id is not None:
        ctx._frame_id = frame_id
    return ctx


def _register_tool_call(state: State, *, tool_name: str, tool_call_id: str, args: dict | None = None) -> ToolCallPart:
    part = ToolCallPart(tool_name=tool_name, args=args or {}, tool_call_id=tool_call_id)
    state.add_tool_call(part)
    # message_history is needed for state.latest_tool_calls() to resolve.
    state.message_history.append(ModelResponse(parts=[part]))
    return part


def _final_text_model() -> FunctionModel:
    def _fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[ModelTextPart("done")])

    return FunctionModel(_fn)


def _model_emits_tool_calls(tool_calls: list[ToolCallPart]) -> FunctionModel:
    """FunctionModel that always responds with the given tool calls.

    Used to deterministically drive the agent dispatch loop for arg-validation
    tests — the LLM round is replaced by a fixed ``ModelResponse(parts=...)``.
    """

    def _fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=list(tool_calls))

    return FunctionModel(_fn)


# ---------------------------------------------------------------------------
# Worker-side: ToolNodeDef.run captures exceptions into typed results
# ---------------------------------------------------------------------------


async def test_tool_raises_arbitrary_exception_stores_error_marker():
    def boom(ctx: ToolContext) -> str:
        raise ValueError("bad")

    tool_node = ToolNodeDef.create_tool_node(
        func=boom,
        subscribe_topics="tool.boom.input",
        publish_topic="tool.boom.output",
    )

    state = State()
    tool_call_id = "tc-arb-001"
    _register_tool_call(state, tool_name="boom", tool_call_id=tool_call_id)
    ctx = _make_ctx(state)

    result = await tool_node.run(ctx, ToolCallRef(tool_call_id=tool_call_id))

    # The reply path must still publish so the agent gets unblocked.
    assert isinstance(result, ReturnCall), f"expected ReturnCall, got {type(result).__name__}"

    stored = ctx.state.tool_results.get(tool_call_id)
    assert isinstance(stored, FailedToolCall), f"expected FailedToolCall in tool_results, got {type(stored).__name__}: {stored!r}"
    assert stored.exc_type == "ValueError"
    assert stored.exc_message == "bad"
    assert stored.tool_name == "boom"
    assert stored.tool_call_id == tool_call_id
    assert stored.marker_kind == "calfkit-tool-error"
    # Pin that ReturnCall.state IS the same state holding the marker — a
    # regression that returned ReturnCall(state=State()) would otherwise pass.
    assert result.state.tool_results[tool_call_id] is stored


async def test_tool_raises_model_retry_stores_retry_prompt():
    def please_retry(ctx: ToolContext) -> str:
        raise ModelRetry("please slow down")

    tool_node = ToolNodeDef.create_tool_node(
        func=please_retry,
        subscribe_topics="tool.please_retry.input",
        publish_topic="tool.please_retry.output",
    )

    state = State()
    tool_call_id = "tc-retry-001"
    _register_tool_call(state, tool_name="please_retry", tool_call_id=tool_call_id)
    ctx = _make_ctx(state)

    result = await tool_node.run(ctx, ToolCallRef(tool_call_id=tool_call_id))

    assert isinstance(result, ReturnCall), f"expected ReturnCall, got {type(result).__name__}"

    stored = ctx.state.tool_results.get(tool_call_id)
    assert isinstance(stored, RetryPromptPart), f"expected RetryPromptPart, got {type(stored).__name__}: {stored!r}"
    # The marker must NOT be used for ModelRetry — that would short-circuit
    # the LLM-visible retry behavior we explicitly preserve.
    assert not isinstance(stored, FailedToolCall)
    assert stored.content == "please slow down"
    assert stored.tool_name == "please_retry"
    assert stored.tool_call_id == tool_call_id
    assert result.state.tool_results[tool_call_id] is stored


async def test_tool_success_unchanged():
    def happy(ctx: ToolContext) -> str:
        return "ok"

    tool_node = ToolNodeDef.create_tool_node(
        func=happy,
        subscribe_topics="tool.happy.input",
        publish_topic="tool.happy.output",
    )

    state = State()
    tool_call_id = "tc-happy-001"
    _register_tool_call(state, tool_name="happy", tool_call_id=tool_call_id)
    ctx = _make_ctx(state)

    result = await tool_node.run(ctx, ToolCallRef(tool_call_id=tool_call_id))

    assert isinstance(result, ReturnCall), f"expected ReturnCall, got {type(result).__name__}"

    stored = ctx.state.tool_results.get(tool_call_id)
    assert isinstance(stored, ToolReturn), f"expected ToolReturn, got {type(stored).__name__}: {stored!r}"
    assert not isinstance(stored, FailedToolCall)
    assert not isinstance(stored, RetryPromptPart)
    assert stored.return_value == "ok"
    assert result.state.tool_results[tool_call_id] is stored


# ---------------------------------------------------------------------------
# Agent-side: BaseAgentNodeDef.run raises on observed error marker
# ---------------------------------------------------------------------------


async def test_agent_detects_error_marker_and_raises_tool_execution_error():
    agent = Agent(
        "agent_under_test",
        system_prompt="x",
        subscribe_topics="agent_under_test.input",
        publish_topic="agent_under_test.output",
        model_client=TestModel(),
    )

    state = State()
    tool_call_id = "id1"
    tool_name = "t"
    _register_tool_call(state, tool_name=tool_name, tool_call_id=tool_call_id)
    state.add_tool_result(
        tool_call_id,
        FailedToolCall(
            tool_name=tool_name,
            tool_call_id=tool_call_id,
            exc_type="ValueError",
            exc_message="boom",
        ),
    )
    ctx = _make_ctx(state)

    with pytest.raises(ToolExecutionError) as exc_info:
        await agent.run(ctx)

    err = exc_info.value
    assert err.tool_name == tool_name
    assert err.tool_call_id == tool_call_id
    assert err.exc_type == "ValueError"
    assert err.exc_message == "boom"


async def test_agent_success_path_unchanged():
    agent = Agent(
        "agent_success",
        system_prompt="x",
        subscribe_topics="agent_success.input",
        publish_topic="agent_success.output",
        model_client=_final_text_model(),
    )

    state = State()
    tool_call_id = "tc-success-001"
    tool_name = "happy_tool"
    _register_tool_call(state, tool_name=tool_name, tool_call_id=tool_call_id)
    state.add_tool_result(
        tool_call_id,
        ToolReturn(return_value="ok", metadata={"tool_call_id": tool_call_id}),
    )
    ctx = _make_ctx(state)

    result = await agent.run(ctx)

    # Pin the happy-path shape: a successful tool result followed by a terminal
    # model response should end the agent run with a ReturnCall, not Silent,
    # TailCall, or an exception. Locks the regression gate against accidental
    # short-circuits.
    assert isinstance(result, ReturnCall), f"expected ReturnCall, got {type(result).__name__}"


# ---------------------------------------------------------------------------
# Wire-compatibility: marker survives JSON round-trip
# ---------------------------------------------------------------------------


def test_marker_survives_json_round_trip_in_state():
    # Regression guard: without _calf_tool_result_discriminator the marker
    # arrives as a plain dict and the agent's isinstance check silently fails.
    state = State()
    tool_call_id = "tc-roundtrip-001"
    tool_name = "buggy_tool"
    _register_tool_call(state, tool_name=tool_name, tool_call_id=tool_call_id)
    state.add_tool_result(
        tool_call_id,
        FailedToolCall(
            tool_name=tool_name,
            tool_call_id=tool_call_id,
            exc_type="ValueError",
            exc_message="bad",
        ),
    )

    restored = State.model_validate_json(state.model_dump_json())

    result = restored.tool_results[tool_call_id]
    assert isinstance(result, FailedToolCall)
    assert result.tool_name == tool_name
    assert result.tool_call_id == tool_call_id
    assert result.exc_type == "ValueError"
    assert result.exc_message == "bad"
    assert result.marker_kind == "calfkit-tool-error"


def test_existing_tool_result_types_survive_json_round_trip():
    # Regression for the union flatten: pydantic-ai's tagged types must still
    # round-trip. ModelRetry isn't checked here because it never reaches state
    # — when a tool raises it, the worker stores a RetryPromptPart instead.
    state = State()

    success_id = "tc-rt-success"
    _register_tool_call(state, tool_name="happy", tool_call_id=success_id)
    state.add_tool_result(success_id, ToolReturn(return_value="ok", metadata={"tool_call_id": success_id}))

    retry_id = "tc-rt-retry"
    _register_tool_call(state, tool_name="retryable", tool_call_id=retry_id)
    state.add_tool_result(
        retry_id,
        RetryPromptPart(content="please retry", tool_name="retryable", tool_call_id=retry_id),
    )

    restored = State.model_validate_json(state.model_dump_json())

    assert isinstance(restored.tool_results[success_id], ToolReturn)
    assert restored.tool_results[success_id].return_value == "ok"

    assert isinstance(restored.tool_results[retry_id], RetryPromptPart)
    assert restored.tool_results[retry_id].content == "please retry"


async def test_agent_raises_on_marker_after_json_round_trip():
    # End-to-end wire-compat: marker survives JSON, agent still raises.
    agent = Agent(
        "agent_roundtrip",
        system_prompt="x",
        subscribe_topics="agent_roundtrip.input",
        publish_topic="agent_roundtrip.output",
        model_client=TestModel(),
    )

    state = State()
    tool_call_id = "tc-rt-error-001"
    tool_name = "buggy_tool"
    _register_tool_call(state, tool_name=tool_name, tool_call_id=tool_call_id)
    state.add_tool_result(
        tool_call_id,
        FailedToolCall(
            tool_name=tool_name,
            tool_call_id=tool_call_id,
            exc_type="ValueError",
            exc_message="boom",
        ),
    )

    wired_state = State.model_validate_json(state.model_dump_json())
    ctx = _make_ctx(wired_state)

    with pytest.raises(ToolExecutionError) as exc_info:
        await agent.run(ctx)

    err = exc_info.value
    assert err.tool_name == tool_name
    assert err.tool_call_id == tool_call_id
    assert err.exc_type == "ValueError"
    assert err.exc_message == "boom"


async def test_agent_ignores_stale_marker_not_in_latest_tool_calls():
    # The detection loop is scoped to ``latest_tool_calls()`` so a marker for
    # a tool_call_id that belongs to a previous turn (still lingering in
    # ``tool_results``) does NOT re-fire. Without this scope, a higher layer
    # that catches ``ToolExecutionError`` and retries would loop forever on
    # the stale entry.
    agent = Agent(
        "agent_stale",
        system_prompt="x",
        subscribe_topics="agent_stale.input",
        publish_topic="agent_stale.output",
        model_client=_final_text_model(),
    )

    state = State()
    current_id = "tc-current-ok"
    _register_tool_call(state, tool_name="happy_tool", tool_call_id=current_id)
    state.add_tool_result(
        current_id,
        ToolReturn(return_value="ok", metadata={"tool_call_id": current_id}),
    )

    # Inject a stale FailedToolCall for a tool_call_id that is NOT in the
    # current ModelResponse, i.e. not returned by latest_tool_calls().
    stale_id = "tc-stale-fail"
    state.tool_results[stale_id] = FailedToolCall(
        tool_name="old_tool",
        tool_call_id=stale_id,
        exc_type="ValueError",
        exc_message="from-a-previous-turn",
    )

    ctx = _make_ctx(state)

    result = await agent.run(ctx)
    assert isinstance(result, ReturnCall), f"expected ReturnCall, got {type(result).__name__}"


# ---------------------------------------------------------------------------
# Parallel fanout: success + failure in the same batch
# ---------------------------------------------------------------------------


async def test_agent_parallel_mode_marker_in_batch_triggers_error_after_aggregation():
    # Parallel mode: the agent waits for both tool replies via
    # _parallel_state_aggregation, then the error check fires once the batch
    # is complete.
    agent = Agent(
        "agent_parallel",
        system_prompt="x",
        subscribe_topics="agent_parallel.input",
        publish_topic="agent_parallel.output",
        model_client=TestModel(),
    )

    correlation_id = "cid-parallel-mixed"
    frame_id = "frame-parallel-mixed"
    success_id = "tc-parallel-ok"
    fail_id = "tc-parallel-fail"

    base_state = State()
    _register_tool_call(base_state, tool_name="happy_tool", tool_call_id=success_id)
    _register_tool_call(base_state, tool_name="buggy_tool", tool_call_id=fail_id)

    agent._pending_batches[frame_id] = PendingToolBatch(
        expected_tool_call_ids=frozenset({success_id, fail_id}),
        base_state=base_state,
    )

    inflight_state = State()
    _register_tool_call(inflight_state, tool_name="happy_tool", tool_call_id=success_id)
    _register_tool_call(inflight_state, tool_name="buggy_tool", tool_call_id=fail_id)
    inflight_state.add_tool_result(
        success_id,
        ToolReturn(return_value="ok", metadata={"tool_call_id": success_id}),
    )
    inflight_state.add_tool_result(
        fail_id,
        FailedToolCall(
            tool_name="buggy_tool",
            tool_call_id=fail_id,
            exc_type="ValueError",
            exc_message="boom",
        ),
    )

    ctx = _make_ctx(inflight_state, correlation_id=correlation_id, frame_id=frame_id)

    with pytest.raises(ToolExecutionError) as exc_info:
        await agent.run(ctx)

    err = exc_info.value
    assert err.tool_call_id == fail_id
    assert err.tool_name == "buggy_tool"
    assert err.exc_type == "ValueError"
    assert err.exc_message == "boom"


async def test_agent_parallel_mode_waits_for_incomplete_batch():
    # Companion check: if only one of two tools has replied, the agent must
    # return Silent() without raising — even when the one that did reply is
    # a FailedToolCall. The error check is gated on batch completion.
    agent = Agent(
        "agent_parallel_partial",
        system_prompt="x",
        subscribe_topics="agent_parallel_partial.input",
        publish_topic="agent_parallel_partial.output",
        model_client=TestModel(),
    )

    correlation_id = "cid-parallel-partial"
    frame_id = "frame-parallel-partial"
    pending_id = "tc-parallel-pending"
    fail_id = "tc-parallel-partial-fail"

    base_state = State()
    _register_tool_call(base_state, tool_name="slow_tool", tool_call_id=pending_id)
    _register_tool_call(base_state, tool_name="buggy_tool", tool_call_id=fail_id)

    agent._pending_batches[frame_id] = PendingToolBatch(
        expected_tool_call_ids=frozenset({pending_id, fail_id}),
        base_state=base_state,
    )

    inflight_state = State()
    _register_tool_call(inflight_state, tool_name="buggy_tool", tool_call_id=fail_id)
    inflight_state.add_tool_result(
        fail_id,
        FailedToolCall(
            tool_name="buggy_tool",
            tool_call_id=fail_id,
            exc_type="ValueError",
            exc_message="early",
        ),
    )

    ctx = _make_ctx(inflight_state, correlation_id=correlation_id, frame_id=frame_id)

    result = await agent.run(ctx)
    assert isinstance(result, Silent), f"expected Silent while batch incomplete, got {type(result).__name__}"


# ---------------------------------------------------------------------------
# BaseException carve-out: shutdown / cancellation signals must propagate
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "exc_factory",
    [
        pytest.param(lambda: KeyboardInterrupt(), id="keyboard-interrupt"),
        pytest.param(lambda: SystemExit(1), id="system-exit"),
        pytest.param(lambda: asyncio.CancelledError(), id="cancelled-error"),
    ],
)
async def test_tool_base_exceptions_propagate(exc_factory):
    # The worker catches Exception, NOT BaseException. KeyboardInterrupt,
    # SystemExit, and asyncio.CancelledError must propagate so operators can
    # terminate workers and FastStream can perform graceful shutdown.
    raised = exc_factory()

    def boom(ctx: ToolContext) -> str:
        raise raised

    tool_node = ToolNodeDef.create_tool_node(
        func=boom,
        subscribe_topics="tool.boom_base.input",
        publish_topic="tool.boom_base.output",
    )

    state = State()
    tool_call_id = "tc-base-001"
    _register_tool_call(state, tool_name="boom", tool_call_id=tool_call_id)
    ctx = _make_ctx(state)

    with pytest.raises(type(raised)):
        await tool_node.run(ctx, ToolCallRef(tool_call_id=tool_call_id))

    # No marker should be stored — the exception must not be silently captured.
    assert tool_call_id not in ctx.state.tool_results


# ---------------------------------------------------------------------------
# Agent-side arg validation: malformed LLM tool args become RetryPromptParts
# before any Kafka dispatch, preserving pydantic-ai's fix-and-retry semantics.
# ---------------------------------------------------------------------------


async def test_agent_validates_args_and_adds_retry_prompt_on_bad_args():
    # Pins the core contract: when the LLM produces args that fail the tool's
    # pydantic validator, the agent must not dispatch — instead it stores a
    # RetryPromptPart so the LLM sees the validation error on the next turn.
    def typed_tool(ctx: ToolContext, x: int) -> str:
        return f"got {x}"

    tool_node = ToolNodeDef.create_tool_node(
        func=typed_tool,
        subscribe_topics="tool.typed_tool.input",
        publish_topic="tool.typed_tool.output",
    )

    tool_call_id = "tc-bad"
    bad_call = ToolCallPart(tool_name="typed_tool", args={"x": "not-a-number"}, tool_call_id=tool_call_id)

    agent = Agent(
        "agent_validate_bad",
        system_prompt="x",
        subscribe_topics="agent_validate_bad.input",
        publish_topic="agent_validate_bad.output",
        model_client=_model_emits_tool_calls([bad_call]),
        tools=[tool_node],
    )

    ctx = _make_ctx(State())
    result = await agent.run(ctx)

    # All (one) tool calls invalid → the agent TailCalls itself to give the LLM
    # another turn with the retry prompt visible in tool_results.
    assert isinstance(result, TailCall), f"expected TailCall, got {type(result).__name__}"

    stored = ctx.state.tool_results.get(tool_call_id)
    assert isinstance(stored, RetryPromptPart), f"expected RetryPromptPart, got {type(stored).__name__}: {stored!r}"
    assert stored.tool_name == "typed_tool"
    assert stored.tool_call_id == tool_call_id
    # Content is the pydantic-ai-style error list (list of dicts) — must be
    # non-empty so the LLM has something to act on.
    assert stored.content, f"expected non-empty validation error content, got {stored.content!r}"


async def test_agent_dispatches_valid_args_unchanged():
    # Regression: valid args must continue to dispatch unchanged — the
    # validation branch must not add false positives or interfere with the
    # normal Kafka hop. No tool_results entry is added (still pending reply).
    def typed_tool(ctx: ToolContext, x: int) -> str:
        return f"got {x}"

    tool_node = ToolNodeDef.create_tool_node(
        func=typed_tool,
        subscribe_topics="tool.typed_tool.input",
        publish_topic="tool.typed_tool.output",
    )

    tool_call_id = "tc-good"
    good_call = ToolCallPart(tool_name="typed_tool", args={"x": 5}, tool_call_id=tool_call_id)

    agent = Agent(
        "agent_validate_good",
        system_prompt="x",
        subscribe_topics="agent_validate_good.input",
        publish_topic="agent_validate_good.output",
        model_client=_model_emits_tool_calls([good_call]),
        tools=[tool_node],
    )

    ctx = _make_ctx(State())
    result = await agent.run(ctx)

    # Single valid call → sequential dispatch via Call (no parallel batch).
    assert isinstance(result, Call), f"expected Call, got {type(result).__name__}"
    assert tool_call_id not in ctx.state.tool_results


async def test_agent_partial_validation_failure_dispatches_valid_calls():
    # Partial-failure: in a mixed batch, the invalid call lands as a
    # RetryPromptPart and the valid call dispatches normally. Without this
    # behavior a single bad arg from the LLM would block the whole batch.
    def tool_a(ctx: ToolContext, x: int) -> str:
        return f"a={x}"

    def tool_b(ctx: ToolContext, y: str) -> str:
        return f"b={y}"

    tool_a_node = ToolNodeDef.create_tool_node(
        func=tool_a,
        subscribe_topics="tool.tool_a.input",
        publish_topic="tool.tool_a.output",
    )
    tool_b_node = ToolNodeDef.create_tool_node(
        func=tool_b,
        subscribe_topics="tool.tool_b.input",
        publish_topic="tool.tool_b.output",
    )

    valid_id = "tc-valid"
    invalid_id = "tc-invalid"
    valid_call = ToolCallPart(tool_name="tool_a", args={"x": 5}, tool_call_id=valid_id)
    # y is typed str but the LLM emitted an int → pydantic rejects.
    invalid_call = ToolCallPart(tool_name="tool_b", args={"y": 42}, tool_call_id=invalid_id)

    agent = Agent(
        "agent_partial",
        system_prompt="x",
        subscribe_topics="agent_partial.input",
        publish_topic="agent_partial.output",
        model_client=_model_emits_tool_calls([valid_call, invalid_call]),
        tools=[tool_a_node, tool_b_node],
    )

    ctx = _make_ctx(State())
    result = await agent.run(ctx)

    # The invalid call must surface as a RetryPromptPart in tool_results so
    # the LLM sees the typed feedback once the valid call completes.
    stored_invalid = ctx.state.tool_results.get(invalid_id)
    assert isinstance(stored_invalid, RetryPromptPart), (
        f"expected RetryPromptPart for invalid call, got {type(stored_invalid).__name__}: {stored_invalid!r}"
    )
    assert stored_invalid.tool_name == "tool_b"
    assert stored_invalid.tool_call_id == invalid_id

    # The valid call must not be pre-populated in tool_results — it's pending
    # the worker reply.
    assert valid_id not in ctx.state.tool_results

    # Only one valid pending call remains, so the agent takes the sequential
    # dispatch branch (len(pending_tool_calls) == 1 → single Call, not list).
    if isinstance(result, list):
        target_ids = [call.body.tool_call_id for call in result if isinstance(call, Call)]
        assert valid_id in target_ids, f"expected Call targeting {valid_id}, got bodies {target_ids}"
    else:
        assert isinstance(result, Call), f"expected Call, got {type(result).__name__}"
        assert isinstance(result.body, ToolCallRef) and result.body.tool_call_id == valid_id, (
            f"expected Call targeting {valid_id}, got body {result.body!r}"
        )


async def test_agent_skips_validation_for_schema_only_override_tools():
    # Override carve-out: BaseToolNodeSchema (wire-only, no validator) skips
    # the validation branch entirely. Without this carve-out, an override
    # toolset would crash on every dispatch because there is no validator to
    # call. Pins the documented limitation so a future refactor that adds
    # validation for overrides updates this test deliberately.
    def typed_tool(ctx: ToolContext, x: int) -> str:
        return f"got {x}"

    full_tool_node = ToolNodeDef.create_tool_node(
        func=typed_tool,
        subscribe_topics="tool.typed_tool.input",
        publish_topic="tool.typed_tool.output",
    )

    # Construct a wire-only schema mirroring the real tool but lacking the
    # _tool/validator attribute. It must take the override path in agent.run.
    schema_only = BaseToolNodeSchema(
        node_id="tool_typed_tool",
        subscribe_topics=list(full_tool_node.subscribe_topics),
        publish_topic=full_tool_node.publish_topic,
        tool_schema=full_tool_node.tool_schema,
    )

    tool_call_id = "tc-override"
    # Args that would fail validation if the validator ran.
    bad_call = ToolCallPart(tool_name="typed_tool", args={"x": "not-a-number"}, tool_call_id=tool_call_id)

    agent = Agent(
        "agent_override_skip",
        system_prompt="x",
        subscribe_topics="agent_override_skip.input",
        publish_topic="agent_override_skip.output",
        model_client=_model_emits_tool_calls([bad_call]),
        tools=[full_tool_node],
    )

    state = State()
    state.overrides = OverridesState(override_agent_tools=[schema_only])
    ctx = _make_ctx(state)
    result = await agent.run(ctx)

    # No RetryPromptPart — validation was skipped, so the call dispatches.
    assert tool_call_id not in ctx.state.tool_results
    assert isinstance(result, Call), f"expected Call, got {type(result).__name__}"
    assert isinstance(result.body, ToolCallRef) and result.body.tool_call_id == tool_call_id


async def test_agent_handles_malformed_json_args_as_retry_prompt():
    # Regression for the widened dispatch-validation catch: when the LLM emits
    # args as a malformed JSON string, args_as_dict() raises ValueError. The
    # agent must store a RetryPromptPart rather than crashing the run.
    def typed_tool(ctx: ToolContext, x: int) -> str:
        return f"got {x}"

    tool_node = ToolNodeDef.create_tool_node(
        func=typed_tool,
        subscribe_topics="tool.typed.input",
        publish_topic="tool.typed.output",
    )

    bad_call = ToolCallPart(
        tool_name="typed_tool",
        args="not-valid-json",
        tool_call_id="tc-malformed",
    )

    agent = Agent(
        "agent_malformed",
        system_prompt="x",
        subscribe_topics="agent_malformed.input",
        publish_topic="agent_malformed.output",
        model_client=_model_emits_tool_calls([bad_call]),
        tools=[tool_node],
    )

    ctx = _make_ctx(State())
    result = await agent.run(ctx)

    assert isinstance(result, TailCall), f"expected TailCall (all calls invalid), got {type(result).__name__}"

    stored = ctx.state.tool_results.get("tc-malformed")
    assert isinstance(stored, RetryPromptPart), f"expected RetryPromptPart, got {type(stored).__name__}"
    assert "Malformed tool arguments" in str(stored.content), f"content should mention malformed args, got {stored.content!r}"


async def test_agent_handles_non_dict_json_args_as_retry_prompt():
    # Companion to the malformed-JSON case: a JSON array (not a dict) trips
    # args_as_dict's `assert isinstance(args, dict)`, raising AssertionError.
    # The same widened catch must convert it into a RetryPromptPart.
    def typed_tool(ctx: ToolContext, x: int) -> str:
        return f"got {x}"

    tool_node = ToolNodeDef.create_tool_node(
        func=typed_tool,
        subscribe_topics="tool.typed.input",
        publish_topic="tool.typed.output",
    )

    bad_call = ToolCallPart(
        tool_name="typed_tool",
        args="[1,2,3]",
        tool_call_id="tc-array",
    )

    agent = Agent(
        "agent_nondict",
        system_prompt="x",
        subscribe_topics="agent_nondict.input",
        publish_topic="agent_nondict.output",
        model_client=_model_emits_tool_calls([bad_call]),
        tools=[tool_node],
    )

    ctx = _make_ctx(State())
    result = await agent.run(ctx)

    assert isinstance(result, TailCall)
    stored = ctx.state.tool_results.get("tc-array")
    assert isinstance(stored, RetryPromptPart)
    assert "Malformed tool arguments" in str(stored.content)


async def test_tool_long_exception_message_is_clamped_not_rejected():
    # Regression: previously a >4096-char exc_message caused FailedToolCall
    # construction to raise ValidationError inside the worker's except block,
    # hanging the run. The clamping validator must silently truncate instead.
    long_msg = "x" * 5000

    def boom_with_long_msg(ctx: ToolContext) -> str:
        raise ValueError(long_msg)

    tool_node = ToolNodeDef.create_tool_node(
        func=boom_with_long_msg,
        subscribe_topics="tool.boom_long.input",
        publish_topic="tool.boom_long.output",
    )

    state = State()
    tool_call_id = "tc-long-msg-001"
    _register_tool_call(state, tool_name="boom_with_long_msg", tool_call_id=tool_call_id)
    ctx = _make_ctx(state)

    result = await tool_node.run(ctx, ToolCallRef(tool_call_id=tool_call_id))

    assert isinstance(result, ReturnCall), f"expected ReturnCall (no hang), got {type(result).__name__}"

    stored = ctx.state.tool_results.get(tool_call_id)
    assert isinstance(stored, FailedToolCall)
    assert stored.exc_type == "ValueError"
    # exc_message must be clamped to 4096, not the full 5000.
    assert len(stored.exc_message) == 4096, f"expected clamped to 4096 chars, got {len(stored.exc_message)}"
    assert stored.exc_message == "x" * 4096


# ---------------------------------------------------------------------------
# Discriminator: direct unit coverage of _calf_tool_result_discriminator
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "payload,expected",
    [
        ({"marker_kind": "calfkit-tool-error"}, "calfkit-tool-error"),
        ({"kind": "tool-return"}, "tool-return"),
        ({"kind": "model-retry"}, "model-retry"),
        ({"part_kind": "retry-prompt"}, "retry-prompt"),
        # marker_kind takes precedence over kind / part_kind
        ({"marker_kind": "calfkit-tool-error", "kind": "tool-return"}, "calfkit-tool-error"),
        # Non-string tag values are ignored (returns None, falls through to Any arm)
        ({"kind": 42}, None),
        ({"kind": None}, None),
        ({"marker_kind": ["x"]}, None),
        # Unknown shapes return None
        ({}, None),
        ({"unknown": "shape"}, None),
        # Non-dict, non-object inputs return None
        ("hello", None),
        (None, None),
        (42, None),
    ],
)
def test_calf_tool_result_discriminator_tags_dict_payloads(payload, expected):
    assert _calf_tool_result_discriminator(payload) == expected


def test_calf_tool_result_discriminator_reads_object_attributes():
    # Object inputs (already-constructed model instances) must resolve via
    # attribute lookup, not just dict key lookup.
    marker = FailedToolCall(
        tool_name="t",
        tool_call_id="id1",
        exc_type="V",
        exc_message="m",
    )
    assert _calf_tool_result_discriminator(marker) == "calfkit-tool-error"


# ---------------------------------------------------------------------------
# BaseToolNodeDef.validate_call_args: direct coverage
# ---------------------------------------------------------------------------


def test_validate_call_args_passes_valid_args():
    def typed_tool(ctx: ToolContext, x: int) -> str:
        return f"got {x}"

    tool_node = ToolNodeDef.create_tool_node(
        func=typed_tool,
        subscribe_topics="tool.typed.input",
        publish_topic="tool.typed.output",
    )

    result = tool_node.validate_call_args({"x": 5})
    # Validator may return the coerced/validated args; we just confirm no raise.
    assert result is not None


def test_validate_call_args_raises_on_wrong_type():
    def typed_tool(ctx: ToolContext, x: int) -> str:
        return f"got {x}"

    tool_node = ToolNodeDef.create_tool_node(
        func=typed_tool,
        subscribe_topics="tool.typed.input",
        publish_topic="tool.typed.output",
    )

    with pytest.raises(ValidationError):
        tool_node.validate_call_args({"x": "not-an-int"})


def test_validate_call_args_raises_on_missing_required_arg():
    def typed_tool(ctx: ToolContext, x: int, y: str) -> str:
        return f"{x}-{y}"

    tool_node = ToolNodeDef.create_tool_node(
        func=typed_tool,
        subscribe_topics="tool.typed.input",
        publish_topic="tool.typed.output",
    )

    with pytest.raises(ValidationError):
        tool_node.validate_call_args({"x": 5})  # missing y


# ---------------------------------------------------------------------------
# Adversarial worker-side regressions: broken __str__, logger.exception
# ---------------------------------------------------------------------------


async def test_tool_exception_with_broken_str_still_produces_failed_tool_call():
    # Regression: a bare str(e) on the worker can itself raise if the
    # exception's __str__ is broken. That would propagate out of except Exception
    # and re-introduce the silent-hang failure mode the feature prevents.
    class BadStrError(Exception):
        def __str__(self) -> str:
            raise RuntimeError("cannot stringify")

    def boom(ctx: ToolContext) -> str:
        raise BadStrError()

    tool_node = ToolNodeDef.create_tool_node(
        func=boom,
        subscribe_topics="tool.bad_str.input",
        publish_topic="tool.bad_str.output",
    )

    state = State()
    tool_call_id = "tc-bad-str-001"
    _register_tool_call(state, tool_name="boom", tool_call_id=tool_call_id)
    ctx = _make_ctx(state)

    result = await tool_node.run(ctx, ToolCallRef(tool_call_id=tool_call_id))

    assert isinstance(result, ReturnCall), f"expected ReturnCall, got {type(result).__name__}"

    stored = ctx.state.tool_results.get(tool_call_id)
    assert isinstance(stored, FailedToolCall), f"expected FailedToolCall, got {type(stored).__name__}"
    assert stored.exc_type == "BadStrError"
    # exc_message should be a non-empty string (the safe fallback content)
    assert stored.exc_message, f"expected non-empty exc_message, got {stored.exc_message!r}"


async def test_tool_worker_logs_exception_with_traceback(caplog):
    # The PR explicitly trades client-side observability for worker-side log
    # diagnostics. Pin that logger.exception (not logger.error) fires so the
    # traceback is captured — a silent demote to logger.debug would lose the
    # only forensic surface across the Kafka boundary.
    def boom(ctx: ToolContext) -> str:
        raise ValueError("bad-for-logs")

    tool_node = ToolNodeDef.create_tool_node(
        func=boom,
        subscribe_topics="tool.boom_log.input",
        publish_topic="tool.boom_log.output",
    )

    state = State()
    tool_call_id = "tc-log-001"
    _register_tool_call(state, tool_name="boom", tool_call_id=tool_call_id)
    ctx = _make_ctx(state)

    with caplog.at_level(logging.ERROR, logger="calfkit.nodes.tool"):
        await tool_node.run(ctx, ToolCallRef(tool_call_id=tool_call_id))

    # Find the worker's exception log record.
    err_records = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert err_records, "expected at least one ERROR-level log from the worker"
    # exc_info must be present (logger.exception sets this) so the traceback
    # is captured across the Kafka boundary.
    matching = [r for r in err_records if r.exc_info is not None]
    assert matching, "expected logger.exception with exc_info, got logger.error only"
    # tool_call_id should be in the log message for correlation.
    assert any(tool_call_id in r.getMessage() for r in matching), "tool_call_id missing from worker log"


# ---------------------------------------------------------------------------
# Multi-failure parallel batch: every failure must be logged before raising
# ---------------------------------------------------------------------------


async def test_agent_parallel_mode_logs_all_failures_before_raising(caplog):
    # Regression: a parallel batch with multiple failures must log every one
    # so operators see all failures, not just the first to be raised.
    agent = Agent(
        "agent_multi_fail",
        system_prompt="x",
        subscribe_topics="agent_multi_fail.input",
        publish_topic="agent_multi_fail.output",
        model_client=TestModel(),
    )

    correlation_id = "cid-multi-fail"
    frame_id = "frame-multi-fail"
    first_id = "tc-first-fail"
    second_id = "tc-second-fail"

    base_state = State()
    _register_tool_call(base_state, tool_name="buggy_a", tool_call_id=first_id)
    _register_tool_call(base_state, tool_name="buggy_b", tool_call_id=second_id)

    agent._pending_batches[frame_id] = PendingToolBatch(
        expected_tool_call_ids=frozenset({first_id, second_id}),
        base_state=base_state,
    )

    inflight_state = State()
    _register_tool_call(inflight_state, tool_name="buggy_a", tool_call_id=first_id)
    _register_tool_call(inflight_state, tool_name="buggy_b", tool_call_id=second_id)
    inflight_state.add_tool_result(
        first_id,
        FailedToolCall(
            tool_name="buggy_a",
            tool_call_id=first_id,
            exc_type="ValueError",
            exc_message="boom-a",
        ),
    )
    inflight_state.add_tool_result(
        second_id,
        FailedToolCall(
            tool_name="buggy_b",
            tool_call_id=second_id,
            exc_type="KeyError",
            exc_message="boom-b",
        ),
    )

    ctx = _make_ctx(inflight_state, correlation_id=correlation_id, frame_id=frame_id)

    with caplog.at_level(logging.ERROR, logger="calfkit.nodes.agent"):
        with pytest.raises(ToolExecutionError):
            await agent.run(ctx)

    # Both failures must appear in the logs.
    log_text = caplog.text
    assert "buggy_a" in log_text and first_id in log_text, "first failure missing from logs"
    assert "buggy_b" in log_text and second_id in log_text, "second failure missing from logs"


# ---------------------------------------------------------------------------
# ToolExecutionError picklability and FailedToolCall frozen/validation
# ---------------------------------------------------------------------------


def test_tool_execution_error_is_picklable():
    # Regression: keyword-only __init__ broke pickle. Override __reduce__/__setstate__
    # to restore picklability so the exception can cross worker boundaries (process
    # pools, multiprocessing, etc.).
    err = ToolExecutionError(
        tool_name="t",
        tool_call_id="id-1",
        exc_type="ValueError",
        exc_message="something",
    )

    restored = pickle.loads(pickle.dumps(err))
    assert isinstance(restored, ToolExecutionError)
    assert restored.tool_name == "t"
    assert restored.tool_call_id == "id-1"
    assert restored.exc_type == "ValueError"
    assert restored.exc_message == "something"
    assert str(restored) == str(err)


def test_failed_tool_call_is_frozen():
    f = FailedToolCall(
        tool_name="t",
        tool_call_id="id-1",
        exc_type="V",
        exc_message="m",
    )
    with pytest.raises(Exception):
        f.tool_name = "mutated"  # frozen=True should reject


def test_failed_tool_call_rejects_empty_tool_call_id():
    # tool_call_id is the correlation key; empty values are invalid and must be
    # rejected at construction.
    with pytest.raises(ValidationError):
        FailedToolCall(
            tool_name="t",
            tool_call_id="",
            exc_type="V",
            exc_message="m",
        )


# ---------------------------------------------------------------------------
# Override (schema-only) dispatch path: malformed JSON args become RetryPromptPart
# ---------------------------------------------------------------------------


async def test_agent_override_path_malformed_args_become_retry_prompt():
    # Regression: previously the dispatch loop only ran args_as_dict() inside
    # the BaseToolNodeDef branch, so override (schema-only) tools dispatched
    # raw malformed JSON to the worker, where it surfaced as a hard
    # FailedToolCall instead of an LLM-retryable RetryPromptPart. The
    # refactored loop parses args on all dispatch paths.
    def typed_tool(ctx: ToolContext, x: int) -> str:
        return f"got {x}"

    full_tool_node = ToolNodeDef.create_tool_node(
        func=typed_tool,
        subscribe_topics="tool.typed.input",
        publish_topic="tool.typed.output",
    )

    schema_only_override = BaseToolNodeSchema(
        node_id="override_typed_tool",
        tool_schema=full_tool_node.tool_schema,
        subscribe_topics=list(full_tool_node.subscribe_topics),
        publish_topic=full_tool_node.publish_topic,
    )

    bad_call = ToolCallPart(
        tool_name="typed_tool",
        args="not-valid-json",
        tool_call_id="tc-override-malformed",
    )

    agent = Agent(
        "agent_override_malformed",
        system_prompt="x",
        subscribe_topics="agent_override_malformed.input",
        publish_topic="agent_override_malformed.output",
        model_client=_model_emits_tool_calls([bad_call]),
        tools=[full_tool_node],
    )

    state = State(overrides=OverridesState(override_agent_tools=[schema_only_override]))
    ctx = _make_ctx(state)
    result = await agent.run(ctx)

    assert isinstance(result, TailCall), f"expected TailCall (all calls invalid), got {type(result).__name__}"

    stored = ctx.state.tool_results.get("tc-override-malformed")
    assert isinstance(stored, RetryPromptPart)
    assert "Malformed tool arguments" in str(stored.content)


# ---------------------------------------------------------------------------
# Defensive construction: the worker's failure path must not itself raise
# ---------------------------------------------------------------------------


async def test_tool_failed_marker_construction_falls_back_to_sentinel():
    # Regression: if the primary FailedToolCall construction raises (e.g.
    # min_length=1 rejects an empty tool_call_id), the worker must fall back
    # to a hardcoded sentinel marker and still publish a reply. Without this,
    # the ValidationError would escape the ``except Exception`` block and
    # re-introduce the silent-hang failure mode the feature exists to prevent.
    def boom(ctx: ToolContext) -> str:
        raise ValueError("original failure")

    tool_node = ToolNodeDef.create_tool_node(
        func=boom,
        subscribe_topics="tool.fallback.input",
        publish_topic="tool.fallback.output",
    )

    state = State()
    # Empty tool_call_id triggers FailedToolCall's min_length=1 rejection
    # during primary construction.
    part = ToolCallPart(tool_name="boom", args={}, tool_call_id="")
    state.add_tool_call(part)
    state.message_history.append(ModelResponse(parts=[part]))
    ctx = _make_ctx(state)

    result = await tool_node.run(ctx, ToolCallRef(tool_call_id=""))

    # Reply still publishes — no silent hang.
    assert isinstance(result, ReturnCall), f"expected ReturnCall, got {type(result).__name__}"

    # Marker stored under the original tool_call_id key so the agent can find
    # it via state.tool_results.get(tool_call_id).
    stored = ctx.state.tool_results.get("")
    assert isinstance(stored, FailedToolCall), f"expected FailedToolCall, got {type(stored).__name__}"
    # Fallback preserves real ``tool_name`` (operators need correlation), only
    # substitutes ``<missing>`` for the empty ``tool_call_id`` that caused
    # primary construction to fail. ``exc_type`` is the construction-failure
    # sentinel.
    assert stored.tool_call_id == "<missing>"
    assert stored.tool_name == "boom"
    assert stored.exc_type == "FailedToolCallConstructionError"
    assert "Failed to construct primary marker" in stored.exc_message


# ---------------------------------------------------------------------------
# Validator raising non-ValidationError must surface as RetryPromptPart
# ---------------------------------------------------------------------------


def _angry_validator(v: int) -> int:
    raise RuntimeError("angry validator")


def _tool_with_bad_validator(ctx: ToolContext, x: Annotated[int, BeforeValidator(_angry_validator)]) -> str:
    return f"x={x}"


async def test_agent_validator_raising_runtime_error_becomes_retry_prompt():
    # Regression: a Pydantic BeforeValidator (or field_validator) in a tool's
    # arg schema can raise anything (RuntimeError, TypeError, custom). The
    # agent's narrow ``except ValidationError`` catch must be widened so these
    # do not escape ``run()`` and silently hang the caller.

    tool_node = ToolNodeDef.create_tool_node(
        func=_tool_with_bad_validator,
        subscribe_topics="tool.angry.input",
        publish_topic="tool.angry.output",
    )

    bad_call = ToolCallPart(
        tool_name="_tool_with_bad_validator",
        args={"x": 5},
        tool_call_id="tc-angry-001",
    )

    agent = Agent(
        "agent_angry_validator",
        system_prompt="x",
        subscribe_topics="agent_angry_validator.input",
        publish_topic="agent_angry_validator.output",
        model_client=_model_emits_tool_calls([bad_call]),
        tools=[tool_node],
    )

    ctx = _make_ctx(State())
    result = await agent.run(ctx)

    # All (one) calls invalid → TailCall to give the LLM another turn;
    # critically, no exception escapes the dispatch loop.
    assert isinstance(result, TailCall), f"expected TailCall, got {type(result).__name__}"

    stored = ctx.state.tool_results.get("tc-angry-001")
    assert isinstance(stored, RetryPromptPart), f"expected RetryPromptPart, got {type(stored).__name__}"
    assert "RuntimeError" in str(stored.content)


async def test_agent_validator_failure_branch_continues_loop():
    # Regression for the ``continue`` in the validation-failure branch: a
    # batch with one invalid and one valid call must produce a RetryPromptPart
    # for the invalid one AND dispatch the valid one. A missing ``continue``
    # would let any future code addition below the branch silently execute
    # on validation-failed iterations.
    def typed_tool(ctx: ToolContext, x: int) -> str:
        return f"got {x}"

    tool_node = ToolNodeDef.create_tool_node(
        func=typed_tool,
        subscribe_topics="tool.typed.input",
        publish_topic="tool.typed.output",
    )

    bad_call = ToolCallPart(tool_name="typed_tool", args={"x": "nope"}, tool_call_id="tc-bad-002")
    good_call = ToolCallPart(tool_name="typed_tool", args={"x": 7}, tool_call_id="tc-good-002")

    agent = Agent(
        "agent_continue",
        system_prompt="x",
        subscribe_topics="agent_continue.input",
        publish_topic="agent_continue.output",
        model_client=_model_emits_tool_calls([bad_call, good_call]),
        tools=[tool_node],
    )

    ctx = _make_ctx(State())
    result = await agent.run(ctx)

    # Bad call lands as a RetryPromptPart; good call is dispatched (only one
    # valid pending call remains, so the agent takes the sequential Call branch).
    bad_stored = ctx.state.tool_results.get("tc-bad-002")
    assert isinstance(bad_stored, RetryPromptPart)
    assert "tc-good-002" not in ctx.state.tool_results, "good call should still be pending dispatch"
    assert isinstance(result, Call), f"expected Call (good call dispatched), got {type(result).__name__}"


# ---------------------------------------------------------------------------
# args_as_dict() raises TypeError on non-string/non-dict args; must not escape
# ---------------------------------------------------------------------------


async def test_agent_handles_typeerror_args_as_retry_prompt():
    # Regression: when the LLM emits a ``ToolCallPart.args`` that is neither a
    # JSON string nor a dict (e.g. an int / list from an off-spec provider),
    # ``args_as_dict()`` raises ``TypeError`` from ``pydantic_core.from_json``.
    # The dispatch catch must widen beyond (ValueError, AssertionError) so the
    # exception does not escape ``run()`` and hang the caller. Surfaces as a
    # ``RetryPromptPart`` for LLM-visible retry.
    def typed_tool(ctx: ToolContext, x: int) -> str:
        return f"got {x}"

    tool_node = ToolNodeDef.create_tool_node(
        func=typed_tool,
        subscribe_topics="tool.typeerr.input",
        publish_topic="tool.typeerr.output",
    )

    # args is an int, not a dict or JSON string — args_as_dict() raises TypeError.
    bad_call = ToolCallPart(tool_name="typed_tool", args=123, tool_call_id="tc-typeerr")

    agent = Agent(
        "agent_typeerr",
        system_prompt="x",
        subscribe_topics="agent_typeerr.input",
        publish_topic="agent_typeerr.output",
        model_client=_model_emits_tool_calls([bad_call]),
        tools=[tool_node],
    )

    ctx = _make_ctx(State())
    result = await agent.run(ctx)

    assert isinstance(result, TailCall), f"expected TailCall, got {type(result).__name__}"
    stored = ctx.state.tool_results.get("tc-typeerr")
    assert isinstance(stored, RetryPromptPart)
    assert "TypeError" in str(stored.content)
    assert "Malformed tool arguments" in str(stored.content)


# ---------------------------------------------------------------------------
# Sentinel fallback preserves real tool identity when valid
# ---------------------------------------------------------------------------


async def test_fallback_marker_preserves_real_tool_name_and_id_when_valid(monkeypatch):
    # Regression: when primary FailedToolCall construction fails for any reason
    # OTHER than empty tool_call_id/tool_name (the previously-documented trigger),
    # the fallback must still preserve real ``tool_name`` / ``tool_call_id`` so
    # operators don't lose the correlation key in the raised ToolExecutionError.
    import calfkit.nodes.tool as tool_module

    _original_marker_cls = tool_module.FailedToolCall
    _call_count = {"n": 0}

    def _make_failing_then_real(*args, **kwargs):
        _call_count["n"] += 1
        if _call_count["n"] == 1:
            # Simulate ANY construction failure unrelated to the input fields
            # (e.g., a future schema-evolution constraint).
            raise RuntimeError("simulated primary construction failure")
        return _original_marker_cls(*args, **kwargs)

    monkeypatch.setattr(tool_module, "FailedToolCall", _make_failing_then_real)

    def boom(ctx: ToolContext) -> str:
        raise ValueError("original tool failure")

    tool_node = ToolNodeDef.create_tool_node(
        func=boom,
        subscribe_topics="tool.preserve.input",
        publish_topic="tool.preserve.output",
    )

    state = State()
    tool_call_id = "real-correlation-id-abc123"
    _register_tool_call(state, tool_name="boom", tool_call_id=tool_call_id)
    ctx = _make_ctx(state)

    result = await tool_node.run(ctx, ToolCallRef(tool_call_id=tool_call_id))
    assert isinstance(result, ReturnCall)

    stored = ctx.state.tool_results.get(tool_call_id)
    assert isinstance(stored, FailedToolCall)
    # Real values preserved — operators can still grep the log for the call id.
    assert stored.tool_call_id == tool_call_id, f"expected real id preserved, got {stored.tool_call_id!r}"
    assert stored.tool_name == "boom", f"expected real tool_name preserved, got {stored.tool_name!r}"
    # exc_type marks this as a fallback marker.
    assert stored.exc_type == "FailedToolCallConstructionError"
    assert "Failed to construct primary marker" in stored.exc_message


# ---------------------------------------------------------------------------
# Unserializable tool return values must not silently hang the worker
# ---------------------------------------------------------------------------


async def test_tool_unserializable_return_value_becomes_failed_tool_call():
    # Regression: a tool returning a non-JSON-serializable value (a user class,
    # an unmapped stdlib type, etc.) would pass through ``ToolReturn(__init__)``
    # but raise ``PydanticSerializationError`` at FastStream's envelope publish
    # boundary — killing the worker handler before any reply published, which
    # is the silent-hang failure mode this module exists to prevent. The worker
    # must eagerly verify wire-safety and surface a FailedToolCall instead.

    class _NotJsonSerializable:
        pass

    def returns_unserializable(ctx: ToolContext) -> object:
        return _NotJsonSerializable()

    tool_node = ToolNodeDef.create_tool_node(
        func=returns_unserializable,
        subscribe_topics="tool.unserializable.input",
        publish_topic="tool.unserializable.output",
    )

    state = State()
    tool_call_id = "tc-unserializable-001"
    _register_tool_call(state, tool_name="returns_unserializable", tool_call_id=tool_call_id)
    ctx = _make_ctx(state)

    result = await tool_node.run(ctx, ToolCallRef(tool_call_id=tool_call_id))
    assert isinstance(result, ReturnCall), f"expected ReturnCall (no hang), got {type(result).__name__}"

    stored = ctx.state.tool_results.get(tool_call_id)
    assert isinstance(stored, FailedToolCall), f"expected FailedToolCall, got {type(stored).__name__}"
    assert stored.exc_type == "PydanticSerializationError"
    assert "Unable to serialize" in stored.exc_message

    # And critically: the state must now JSON-round-trip cleanly so the actual
    # Kafka publish wouldn't itself raise.
    state.model_dump_json()


# ---------------------------------------------------------------------------
# Corrupt FailedToolCall marker dict (schema drift / version skew) must raise
# ---------------------------------------------------------------------------


async def test_agent_detects_corrupt_marker_dict_and_raises():
    # Regression: an entry in ``tool_results`` that carries the calfkit marker
    # tag but fails FailedToolCall validation (e.g. a required field added in
    # a newer schema; a stale message replayed; a tampered payload) round-trips
    # through ``CalfToolResult | Any`` as a plain dict. The agent's isinstance
    # check would silently miss it without this defense.
    agent = Agent(
        "agent_corrupt_marker",
        system_prompt="x",
        subscribe_topics="agent_corrupt_marker.input",
        publish_topic="agent_corrupt_marker.output",
        model_client=TestModel(),
    )

    state = State()
    tool_call_id = "tc-corrupt-001"
    _register_tool_call(state, tool_name="buggy", tool_call_id=tool_call_id)
    # Insert a raw dict carrying the marker tag but missing required fields;
    # bypass model validation by writing directly to the dict.
    state.tool_results[tool_call_id] = {
        "marker_kind": "calfkit-tool-error",
        "tool_name": "buggy",
        "tool_call_id": tool_call_id,
        # missing exc_type, exc_message
    }
    ctx = _make_ctx(state)

    with pytest.raises(ToolExecutionError) as exc_info:
        await agent.run(ctx)

    err = exc_info.value
    assert err.exc_type == "CorruptFailedToolCallMarker"
    assert err.tool_call_id == tool_call_id
    # Real tool_name from the dict is preserved when valid.
    assert err.tool_name == "buggy"
    # Diagnostic message names the corruption shape.
    assert "schema drift" in err.exc_message or "raw_keys" in err.exc_message


async def test_agent_corrupt_marker_with_missing_tool_name_uses_sentinel():
    # Defense-in-depth: a corrupt marker dict missing even ``tool_name`` must
    # still produce a typed raise rather than crashing the agent. Sentinel
    # value substituted.
    agent = Agent(
        "agent_corrupt_no_name",
        system_prompt="x",
        subscribe_topics="agent_corrupt_no_name.input",
        publish_topic="agent_corrupt_no_name.output",
        model_client=TestModel(),
    )

    state = State()
    tool_call_id = "tc-corrupt-noname"
    _register_tool_call(state, tool_name="some_tool", tool_call_id=tool_call_id)
    state.tool_results[tool_call_id] = {
        "marker_kind": "calfkit-tool-error",
        # missing tool_name, exc_type, exc_message
    }
    ctx = _make_ctx(state)

    with pytest.raises(ToolExecutionError) as exc_info:
        await agent.run(ctx)

    err = exc_info.value
    assert err.exc_type == "CorruptFailedToolCallMarker"
    assert err.tool_name == "<unknown>"
    assert err.tool_call_id == tool_call_id


# ---------------------------------------------------------------------------
# Parallel-same-agent invocations: ``_pending_batches`` is keyed by ``frame_id``,
# not ``correlation_id``. A supervisor that fans out two ``Call``s to the same
# agent shares one correlation_id across both invocations — keying on
# correlation_id used to silently collide the batches and wedge one of them.
# ---------------------------------------------------------------------------


async def test_pending_batches_keyed_by_frame_id_not_correlation_id():
    # Direct unit-level regression for the collision bug. Two parallel
    # invocations of the same agent (same correlation_id, different frame_ids)
    # must each track their own PendingToolBatch independently, and a reply
    # for invocation A's tool_call_ids must aggregate into A's batch only.
    agent = Agent(
        "agent_parallel_same",
        system_prompt="x",
        subscribe_topics="agent_parallel_same.input",
        publish_topic="agent_parallel_same.output",
        model_client=TestModel(),
    )

    correlation_id = "cid-shared-by-both"

    # Invocation A
    frame_a = "frame-A"
    a_tool_id = "tc-A-1"
    base_state_a = State()
    _register_tool_call(base_state_a, tool_name="tool_a", tool_call_id=a_tool_id)
    agent._pending_batches[frame_a] = PendingToolBatch(
        expected_tool_call_ids=frozenset({a_tool_id}),
        base_state=base_state_a,
    )

    # Invocation B (same correlation_id, different frame_id, disjoint tool_call_ids)
    frame_b = "frame-B"
    b_tool_id = "tc-B-1"
    base_state_b = State()
    _register_tool_call(base_state_b, tool_name="tool_b", tool_call_id=b_tool_id)
    agent._pending_batches[frame_b] = PendingToolBatch(
        expected_tool_call_ids=frozenset({b_tool_id}),
        base_state=base_state_b,
    )

    # Both batches coexist — keying on correlation_id would have overwritten one.
    assert set(agent._pending_batches.keys()) == {frame_a, frame_b}, (
        f"both per-invocation batches must coexist; got keys={list(agent._pending_batches.keys())}"
    )
    assert agent._pending_batches[frame_a].expected_tool_call_ids == frozenset({a_tool_id})
    assert agent._pending_batches[frame_b].expected_tool_call_ids == frozenset({b_tool_id})

    # Reply for invocation A arrives. ctx carries A's frame_id.
    a_reply_state = State()
    _register_tool_call(a_reply_state, tool_name="tool_a", tool_call_id=a_tool_id)
    a_reply_state.add_tool_result(
        a_tool_id,
        ToolReturn(return_value="a-done", metadata={"tool_call_id": a_tool_id}),
    )
    ctx_a = _make_ctx(a_reply_state, correlation_id=correlation_id, frame_id=frame_a)

    agent._parallel_state_aggregation(ctx_a)

    # A's batch completed and was popped; B's must be untouched.
    assert frame_a not in agent._pending_batches, "A's batch must be popped after completion"
    assert frame_b in agent._pending_batches, "B's batch must NOT be affected by A's reply"
    assert agent._pending_batches[frame_b].collected_results == {}, "B's batch must not have collected any of A's results"

    # Now B's reply arrives.
    b_reply_state = State()
    _register_tool_call(b_reply_state, tool_name="tool_b", tool_call_id=b_tool_id)
    b_reply_state.add_tool_result(
        b_tool_id,
        ToolReturn(return_value="b-done", metadata={"tool_call_id": b_tool_id}),
    )
    ctx_b = _make_ctx(b_reply_state, correlation_id=correlation_id, frame_id=frame_b)

    agent._parallel_state_aggregation(ctx_b)

    assert frame_b not in agent._pending_batches, "B's batch must be popped after completion"


async def test_parallel_replies_with_wrong_frame_id_do_not_aggregate():
    # Defense-in-depth: if a tool reply arrives carrying an unrelated frame_id
    # (e.g. a stale message, a routing error, a reply destined for some other
    # agent invocation), the aggregator must not absorb its tool_results into
    # the wrong batch. The batch under the correct frame_id must remain
    # untouched.
    agent = Agent(
        "agent_wrong_frame",
        system_prompt="x",
        subscribe_topics="agent_wrong_frame.input",
        publish_topic="agent_wrong_frame.output",
        model_client=TestModel(),
    )

    real_frame = "frame-real"
    real_tool_id = "tc-real-1"
    base_state = State()
    _register_tool_call(base_state, tool_name="tool_real", tool_call_id=real_tool_id)
    agent._pending_batches[real_frame] = PendingToolBatch(
        expected_tool_call_ids=frozenset({real_tool_id}),
        base_state=base_state,
    )

    # A reply arrives carrying tool_results for ``real_tool_id`` but a
    # different frame_id — must NOT aggregate into the real batch.
    wrong_frame = "frame-unrelated"
    inflight_state = State()
    _register_tool_call(inflight_state, tool_name="tool_real", tool_call_id=real_tool_id)
    inflight_state.add_tool_result(
        real_tool_id,
        ToolReturn(return_value="leaked", metadata={"tool_call_id": real_tool_id}),
    )
    ctx = _make_ctx(inflight_state, frame_id=wrong_frame)

    agent._parallel_state_aggregation(ctx)

    # Real batch is untouched.
    assert real_frame in agent._pending_batches
    assert agent._pending_batches[real_frame].collected_results == {}


def test_prepare_context_populates_frame_id_from_envelope():
    # Plumbing regression: ``prepare_context`` must read
    # ``current_frame.frame_id`` into ``ctx._frame_id`` so that
    # ``_pending_batches`` lookups in ``agent.run`` see the right key. Without
    # this, ``ctx.frame_id`` returns ``None`` and every parallel batch lives
    # under a single ``None`` key, re-introducing the collision bug at runtime.
    import asyncio

    from calfkit.models.envelope import Envelope
    from calfkit.models.session_context import CallFrame, CallFrameStack, WorkflowState

    agent = Agent(
        "agent_prep_ctx",
        system_prompt="x",
        subscribe_topics="agent_prep_ctx.input",
        publish_topic="agent_prep_ctx.output",
        model_client=TestModel(),
    )

    frame = CallFrame(
        target_topic="agent_prep_ctx.input",
        callback_topic="caller.return",
    )
    wf = WorkflowState(call_stack=CallFrameStack(_internal_list=[frame]))
    envelope = Envelope(
        context=SessionRunContext(state=State(), deps={}),
        internal_workflow_state=wf,
    )

    ctx = asyncio.run(agent.prepare_context(envelope))
    assert ctx.frame_id == frame.frame_id, f"prepare_context must mirror current_frame.frame_id onto ctx; got {ctx.frame_id!r} vs {frame.frame_id!r}"


def test_prepare_context_stamps_correlation_id_from_transport():
    # ``correlation_id`` is transport-sourced: ``prepare_context`` must stamp the
    # value the handler received via FastStream ``Context()`` onto ``ctx`` so
    # ``ctx.correlation_id`` is readable (it is NOT carried on the envelope body).
    import asyncio

    from calfkit.models.envelope import Envelope
    from calfkit.models.session_context import CallFrame, CallFrameStack, WorkflowState

    agent = Agent(
        "agent_prep_cid",
        system_prompt="x",
        subscribe_topics="agent_prep_cid.input",
        publish_topic="agent_prep_cid.output",
        model_client=TestModel(),
    )
    wf = WorkflowState(call_stack=CallFrameStack(_internal_list=[CallFrame(target_topic="agent_prep_cid.input", callback_topic="caller.return")]))
    envelope = Envelope(context=SessionRunContext(state=State(), deps={}), internal_workflow_state=wf)

    ctx = asyncio.run(agent.prepare_context(envelope, correlation_id="cid-stamp-42"))
    assert ctx.correlation_id == "cid-stamp-42"


def test_frame_id_survives_envelope_json_round_trip():
    # The CallFrame's frame_id must survive Envelope JSON serialization
    # verbatim — otherwise per-invocation aggregation keys would diverge across
    # the Kafka boundary and the collision-bug fix would be ineffective in
    # production (only in-process tests would see correct behavior).
    from calfkit.models.envelope import Envelope
    from calfkit.models.session_context import CallFrame, CallFrameStack, WorkflowState

    frame = CallFrame(
        target_topic="some.topic",
        callback_topic="caller.return",
    )
    wf = WorkflowState(call_stack=CallFrameStack(_internal_list=[frame]))
    envelope = Envelope(
        context=SessionRunContext(state=State(), deps={}),
        internal_workflow_state=wf,
    )

    restored = Envelope.model_validate_json(envelope.model_dump_json())
    assert restored.internal_workflow_state.current_frame.frame_id == frame.frame_id
