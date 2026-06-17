"""Schema-only tool dispatch via the ``Agent(tools=[...])`` kwarg path.

Pins two load-bearing properties of the agent loop for tools registered as
validator-less ``ToolBinding`` instances passed through the
``Agent(tools=[...])`` kwarg:

  1. The agent dispatches the tool call without attempting client-side
     argument validation. The ``binding.validator is not None`` gate in
     ``calfkit/nodes/agent.py`` is False for a validator-less binding, so the
     validation block is skipped entirely.

  2. Malformed JSON args from the LLM still become a ``RetryPromptPart`` (an
     LLM-visible recoverable, not an escalating fault), via the ``args_as_dict()``
     try/except which runs on *all* dispatch paths regardless of the validation gate.

The override-mode tests in ``test_tool_errors.py`` cover the same two
properties via the ``state.overrides.override_agent_tools`` path; this test
exercises the ``Agent(tools=[...])`` kwarg branch instead.
"""

from __future__ import annotations

import pytest

from calfkit._vendor.pydantic_ai.messages import RetryPromptPart, ToolCallPart
from calfkit._vendor.pydantic_ai.tools import ToolDefinition
from calfkit.models.actions import Call, TailCall
from calfkit.models.state import State
from calfkit.models.tool_dispatch import ToolBinding, ToolCallRef
from calfkit.nodes import Agent

# Reuse the proven helpers from the override-mode tests rather than
# re-implementing. If these helpers move or rename, the import is a
# fail-fast signal to update both tests together.
from tests.test_tool_errors import _make_ctx, _model_emits_tool_calls


def _make_schema_only_tool(
    *,
    tool_name: str = "search",
    topic_base: str = "tools.search",
) -> ToolBinding:
    """Construct a validator-less ``ToolBinding`` (a schema-only tool).

    Carries a real ``ToolDefinition`` (so the LLM sees a valid schema) but
    ``validator=None`` — which is what makes it "schema-only": the agent's
    ``binding.validator is not None`` gate is False, skipping validation.
    """
    return ToolBinding(
        dispatch_topic=f"{topic_base}.input",
        tool_def=ToolDefinition(
            name=tool_name,
            description="Synthetic tool with one required string arg.",
            parameters_json_schema={
                "type": "object",
                "properties": {"q": {"type": "string"}},
                "required": ["q"],
                "additionalProperties": False,
            },
        ),
    )


async def test_schema_only_tool_via_tools_kwarg_dispatches_without_validation() -> None:
    """Property 1: ``Agent(tools=[ToolBinding(...)])`` dispatches the call.

    No validator runs (the binding carries none), and the result is a ``Call``
    to the binding's dispatch topic.
    """
    schema_only = _make_schema_only_tool()

    tool_call_id = "tc-schema-only-01"
    call = ToolCallPart(tool_name="search", args={"q": "hello"}, tool_call_id=tool_call_id)

    agent = Agent(
        "agent_schema_only",
        system_prompt="x",
        subscribe_topics="agent_schema_only.input",
        publish_topic="agent_schema_only.output",
        model_client=_model_emits_tool_calls([call]),
        tools=[schema_only],  # NB: tools= kwarg path, not OverridesState
    )

    ctx = _make_ctx(State())
    result = await agent.run(ctx)

    # Dispatch happened — no RetryPromptPart stored, result is a Call to
    # the tool's subscribe topic with the tool_call_id carried as a ToolCallRef body.
    assert tool_call_id not in ctx.state.tool_results, f"unexpected tool_result stored: {ctx.state.tool_results.get(tool_call_id)!r}"
    assert isinstance(result, Call), f"expected Call, got {type(result).__name__}"
    assert isinstance(result.body, ToolCallRef) and result.body.tool_call_id == tool_call_id
    assert result.target_topic == "tools.search.input"


async def test_schema_only_tool_malformed_args_become_retry_prompt() -> None:
    """Property 2: malformed JSON args still become ``RetryPromptPart`` on the
    schema-only path.

    This is the safety net that lets a schema-only tool skip client-side
    validation without crashing the agent on every off-spec model emission.
    Mirrors ``test_tool_errors.py`` (the override-mode equivalent) but routes
    through the ``tools=`` kwarg instead of ``OverridesState``.
    """
    schema_only = _make_schema_only_tool()

    bad_call = ToolCallPart(
        tool_name="search",
        args="not-valid-json",  # str, not dict — args_as_dict() will raise
        tool_call_id="tc-schema-only-malformed",
    )

    agent = Agent(
        "agent_schema_only_malformed",
        system_prompt="x",
        subscribe_topics="agent_schema_only_malformed.input",
        publish_topic="agent_schema_only_malformed.output",
        model_client=_model_emits_tool_calls([bad_call]),
        tools=[schema_only],
    )

    ctx = _make_ctx(State())
    result = await agent.run(ctx)

    # All (one) calls were invalid → the agent TailCalls itself to give
    # the LLM another turn with the retry prompt visible in tool_results.
    assert isinstance(result, TailCall), f"expected TailCall, got {type(result).__name__}"

    stored = ctx.state.tool_results.get("tc-schema-only-malformed")
    assert isinstance(stored, RetryPromptPart), f"expected RetryPromptPart, got {type(stored).__name__}: {stored!r}"
    assert "Malformed tool arguments" in str(stored.content)


@pytest.mark.parametrize(
    "args",
    [
        "[1,2,3]",  # JSON array — trips args_as_dict's expected-dict assertion
        "{not json",  # malformed JSON — ValueError
        '"just a string"',  # JSON string — AssertionError (not a dict)
    ],
)
async def test_schema_only_tool_handles_various_bad_arg_shapes(args: str) -> None:
    """Defense-in-depth across off-spec ``args`` payloads. Each shape must
    end as a ``RetryPromptPart`` regardless of which exception class
    ``args_as_dict()`` raises (ValueError, AssertionError, TypeError).

    Note: ``args=""`` is NOT a malformed-args case — pydantic-ai's
    ``args_as_dict()`` parses an empty string as ``{}`` (a valid empty
    dict), which then dispatches normally rather than raising.
    """
    schema_only = _make_schema_only_tool()

    tcid = f"tc-schema-only-bad-{abs(hash(args))}"
    bad_call = ToolCallPart(tool_name="search", args=args, tool_call_id=tcid)

    agent = Agent(
        "agent_schema_only_param",
        system_prompt="x",
        subscribe_topics="agent_schema_only_param.input",
        publish_topic="agent_schema_only_param.output",
        model_client=_model_emits_tool_calls([bad_call]),
        tools=[schema_only],
    )

    ctx = _make_ctx(State())
    await agent.run(ctx)

    stored = ctx.state.tool_results.get(tcid)
    assert isinstance(stored, RetryPromptPart), f"args={args!r}: expected RetryPromptPart, got {type(stored).__name__}"
