"""Unit-scope contracts for the tool body + the agent's in-process tool-call handling.

Tests bypass ``TestKafkaBroker`` and ``Client`` and call ``run()`` directly so the carriage
(tool result on the reply slot) and the agent's arg-validation / dispatch contracts are isolated
from the messaging layer. The ``FailedToolCall``/``ToolExecutionError`` blob-carriage retired with
the fault rail's carriage switch.
"""

from __future__ import annotations

import asyncio
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
from calfkit.models import SessionRunContext, ToolCallRef, ToolContext
from calfkit.models.actions import Call, ReturnCall, TailCall
from calfkit.models.payload import TextPart, is_retry
from calfkit.models.state import OverridesState, State
from calfkit.models.tool_dispatch import ToolBinding
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
    # ``agent.run`` directly must set them here. ``_frame_id`` surfaces the
    # per-invocation frame id (used in the parallel-mode incomplete-batch
    # diagnostic ``RuntimeError`` raised by ``agent.run``).
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
# Worker-side: ToolNodeDef.run returns its result on the reply slot (4.4 carriage)
# ---------------------------------------------------------------------------
# The carriage switch (fault-rail §4.5/§6.9): the tool body stops blob-writing into
# ``state.tool_results`` and instead returns its result as ``ReturnCall.value``, which the
# chokepoint coerces onto ``reply.parts``. The agent materializes it from the reply slot at
# the callee slot (``_resolve_slot``), keyed by the echoed ``tag``. A generic tool exception
# is no longer captured into a ``FailedToolCall`` — it ESCAPES to the chokepoint, where
# ``on_node_error`` gets its edge chance and the fault rail carries an ``ErrorReport``.


async def test_tool_executes_from_payload_and_returns_value():
    # The ToolCallRef payload is the authoritative invocation source: name, args, and
    # tool_call_id all come from the ref, with NO lookup of the ToolCallPart in ctx.state.
    def echo(ctx: ToolContext, x: int) -> str:
        return f"got {x}"

    tool_node = ToolNodeDef.create_tool_node(func=echo, subscribe_topics="tool.echo.input", publish_topic="tool.echo.output")

    ctx = _make_ctx(State())  # deliberately no registered tool call
    result = await tool_node.run(ctx, ToolCallRef(tool_call_id="tc-payload-001", args={"x": 7}, name="echo"))

    assert isinstance(result, ReturnCall), f"expected ReturnCall, got {type(result).__name__}"
    assert result.value == "got 7"  # rides the reply slot, not a tool_results blob-write
    assert ctx.state.tool_results == {}  # the blob-write protocol is gone


async def test_tool_raises_arbitrary_exception_escapes():
    # The generic-exception catch is deleted: an uncaught tool exception ESCAPES to the
    # chokepoint (on_node_error → fault rail), not captured into a FailedToolCall blob.
    def boom(ctx: ToolContext) -> str:
        raise ValueError("bad")

    tool_node = ToolNodeDef.create_tool_node(func=boom, subscribe_topics="tool.boom.input", publish_topic="tool.boom.output")

    ctx = _make_ctx(State())
    with pytest.raises(ValueError, match="bad"):
        await tool_node.run(ctx, ToolCallRef(tool_call_id="tc-arb-001", args={}, name="boom"))
    assert ctx.state.tool_results == {}  # nothing captured


async def test_tool_raises_model_retry_returns_marked_text():
    # ModelRetry stays a model-visible recoverable (P3), but its carriage migrated (§4.5): the
    # tool renders it AT ORIGIN to a calf.retry-marked TextPart on the reply slot — the RAW message
    # (option 1; the agent hydrates the RetryPromptPart, the provider renders the suffix once). NOT
    # a tool_results blob-write, NOT the fault rail.
    def please_retry(ctx: ToolContext) -> str:
        raise ModelRetry("please slow down")

    tool_node = ToolNodeDef.create_tool_node(func=please_retry, subscribe_topics="tool.please_retry.input", publish_topic="tool.please_retry.output")

    ctx = _make_ctx(State())
    result = await tool_node.run(ctx, ToolCallRef(tool_call_id="tc-retry-001", args={}, name="please_retry"))

    assert isinstance(result, ReturnCall), f"expected ReturnCall, got {type(result).__name__}"
    assert isinstance(result.value, list) and len(result.value) == 1
    part = result.value[0]
    assert isinstance(part, TextPart) and part.text == "please slow down"  # the raw message, not rendered+suffixed
    assert is_retry([part])  # carries the calf.retry marker the agent honors
    assert ctx.state.tool_results == {}


async def test_tool_success_returns_value():
    def happy(ctx: ToolContext) -> str:
        return "ok"

    tool_node = ToolNodeDef.create_tool_node(func=happy, subscribe_topics="tool.happy.input", publish_topic="tool.happy.output")

    ctx = _make_ctx(State())
    result = await tool_node.run(ctx, ToolCallRef(tool_call_id="tc-happy-001", args={}, name="happy"))

    assert isinstance(result, ReturnCall), f"expected ReturnCall, got {type(result).__name__}"
    assert result.value == "ok"
    assert ctx.state.tool_results == {}


# ---------------------------------------------------------------------------
# Agent-side: the happy path (a materialized tool result → the run completes)
# ---------------------------------------------------------------------------
# The old FailedToolCall-scan → ToolExecutionError is deleted (4.4): a faulting tool now escalates
# via the rail (handler stage-1 / closing fault group), so a FailedToolCall never reaches the agent's
# tool_results and run() never scans for one. The agent-side scan regressions retire with it.


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
# Wire-compatibility: CalfToolResult types survive JSON round-trip
# ---------------------------------------------------------------------------


def test_existing_tool_result_types_survive_json_round_trip():
    # Regression for the union flatten: pydantic-ai's tagged types (the agent's PRIVATE
    # tool_results bookkeeping post-carriage-switch) must still round-trip through State JSON.
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


# NOTE: the agent-side FailedToolCall scan (a marker in tool_results → ToolExecutionError, incl. the
# JSON-round-trip, stale-marker-scoping, corrupt-marker, and completed-fan-out-batch variants) retired
# with the carriage switch (4.4). A faulting tool escalates via the rail, so a FailedToolCall never
# reaches the agent's tool_results — the typed fault escalation is covered by the staged-pipeline +
# durable-fan-out e2e tests.


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
    part = _register_tool_call(state, tool_name="boom", tool_call_id=tool_call_id)
    ctx = _make_ctx(state)

    with pytest.raises(type(raised)):
        await tool_node.run(ctx, ToolCallRef.from_tool_call_part(part))

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
    # Override carve-out: a validator-less ToolBinding (the wire form — the
    # validator never serializes) skips the validation branch entirely.
    # Without this carve-out, an override toolset would crash on every
    # dispatch because there is no validator to call. Pins the documented
    # limitation so a future refactor that adds validation for overrides
    # updates this test deliberately.
    def typed_tool(ctx: ToolContext, x: int) -> str:
        return f"got {x}"

    full_tool_node = ToolNodeDef.create_tool_node(
        func=typed_tool,
        subscribe_topics="tool.typed_tool.input",
        publish_topic="tool.typed_tool.output",
    )

    # Construct a wire-form binding mirroring the real tool but lacking the
    # validator. It must take the override path in agent.run.
    schema_only = ToolBinding(
        tool_def=full_tool_node.tool_schema,
        dispatch_topic=full_tool_node.subscribe_topics[0],
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

    schema_only_override = ToolBinding(
        tool_def=full_tool_node.tool_schema,
        dispatch_topic=full_tool_node.subscribe_topics[0],
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
# Unserializable tool return: the B1 wire-safety check escapes to the chokepoint
# ---------------------------------------------------------------------------


async def test_tool_unserializable_return_value_escapes_via_wire_safety_check():
    # The B1 eager wire-safety check (``pydantic_core.to_json(result)``) runs in the tool body so a
    # non-JSON-serializable return raises HERE and ESCAPES to the chokepoint (on_node_error → the
    # fault rail), instead of killing the envelope serialization mid-publish (the silent-hang mode).
    # With the generic-except deleted, it is no longer captured into a FailedToolCall.
    import pydantic_core

    class _NotJsonSerializable:
        pass

    def returns_unserializable(ctx: ToolContext) -> object:
        return _NotJsonSerializable()

    tool_node = ToolNodeDef.create_tool_node(
        func=returns_unserializable,
        subscribe_topics="tool.unserializable.input",
        publish_topic="tool.unserializable.output",
    )

    ctx = _make_ctx(State())
    with pytest.raises(pydantic_core.PydanticSerializationError):
        await tool_node.run(ctx, ToolCallRef(tool_call_id="tc-unserializable-001", args={}, name="returns_unserializable"))
    assert ctx.state.tool_results == {}  # nothing captured


# NOTE: the white-box `_parallel_state_aggregation` regressions (per-frame-id batch keying;
# wrong-frame replies don't aggregate) were removed with the in-process aggregation. The durable
# fold keys batches by `fanout_id` (the node's own frame_id) in the store, and a foreign-slot reply
# is a stray — covered by tests/test_fanout_fold.py (test_fold_foreign_slot_is_stray_*) and the
# fanout_id-keyed records in tests/test_staged_pipeline.py / tests/test_fanout_handler.py.


def test_prepare_context_populates_frame_id_from_envelope():
    # Plumbing regression: ``prepare_context`` must read
    # ``current_frame.frame_id`` into ``ctx._frame_id`` so ``ctx.frame_id``
    # reports the frame this delivery runs under. Without this, ``ctx.frame_id``
    # returns ``None``. (Durable fan-out is keyed by ``fanout_id`` in the store,
    # not by this field, but the per-invocation frame id is still surfaced.)
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
