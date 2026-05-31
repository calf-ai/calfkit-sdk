"""Baseline test for the MCP adaptor's load-bearing claim.

The MCP adaptor relies on the agent loop already supporting schema-only
tools registered as ``BaseToolNodeSchema`` instances via the
``Agent(tools=[...])`` path, with two specific properties:

  1. The agent dispatches the tool call without attempting client-side
     argument validation. The ``isinstance(tool_node, BaseToolNodeDef)``
     gate at ``calfkit/nodes/agent.py:374`` — a bare ``BaseToolNodeSchema``
     fails the check, so the validation block is skipped entirely.

  2. Malformed JSON args from the LLM still become a ``RetryPromptPart``
     (not a hard ``FailedToolCall``), via the ``args_as_dict()`` try/except
     at ``calfkit/nodes/agent.py:350-369`` which runs on *all* dispatch
     paths regardless of the validation gate.

The existing override-mode tests at ``test_tool_errors.py:687-723`` and
``:1118-1157`` cover the same two properties but only via the
``state.overrides.override_agent_tools`` path (``agent.py:165`` branch).
The MCP adaptor uses the ``Agent(tools=[...])`` path instead (``agent.py:167``
branch), so this test exercises that exact branch — if it fails, the v1
plan's "zero agent code changes" claim does not hold and Phase 1 must
not start.
"""

from __future__ import annotations

import pytest

from calfkit._vendor.pydantic_ai.messages import RetryPromptPart, ToolCallPart
from calfkit._vendor.pydantic_ai.tools import ToolDefinition
from calfkit.models.actions import Call, TailCall
from calfkit.models.node_schema import BaseToolNodeSchema
from calfkit.models.state import State
from calfkit.nodes import Agent

# Reuse the proven helpers from the override-mode tests rather than
# re-implementing. If these helpers move or rename, the import is a
# fail-fast signal to update both tests together.
from tests.test_tool_errors import _make_ctx, _model_emits_tool_calls


def _make_schema_only_tool(
    *,
    tool_name: str = "mcp_search",
    topic_base: str = "mcp.everything.search",
) -> BaseToolNodeSchema:
    """Construct a ``BaseToolNodeSchema`` mirroring what ``McpToolDef`` will
    yield in Phase 1. Carries a real ``ToolDefinition`` (so the LLM sees a
    valid schema) but no ``_tool``/validator attribute (which is what makes
    it "schema-only" — the agent's ``isinstance(tool_node, BaseToolNodeDef)``
    gate fails, skipping validation).
    """
    return BaseToolNodeSchema(
        node_id=f"mcp_{tool_name}",
        subscribe_topics=[f"{topic_base}.input"],
        publish_topic=f"{topic_base}.output",
        tool_schema=ToolDefinition(
            name=tool_name,
            description="Synthetic MCP-style tool with one required string arg.",
            parameters_json_schema={
                "type": "object",
                "properties": {"q": {"type": "string"}},
                "required": ["q"],
                "additionalProperties": False,
            },
        ),
    )


async def test_schema_only_tool_via_tools_kwarg_dispatches_without_validation() -> None:
    """Property 1: ``Agent(tools=[BaseToolNodeSchema(...)])`` dispatches the call.

    No ``BaseToolNodeDef`` validator runs (there is none on a bare
    ``BaseToolNodeSchema``), and the result is a ``Call`` to the tool's
    subscribe topic. This is the path Phase 1's ``McpToolDef``-derived
    ``BaseToolNodeSchema`` instances will rely on.
    """
    schema_only = _make_schema_only_tool()

    tool_call_id = "tc-mcp-baseline-01"
    call = ToolCallPart(tool_name="mcp_search", args={"q": "hello"}, tool_call_id=tool_call_id)

    agent = Agent(
        "agent_mcp_baseline",
        system_prompt="x",
        subscribe_topics="agent_mcp_baseline.input",
        publish_topic="agent_mcp_baseline.output",
        model_client=_model_emits_tool_calls([call]),
        tools=[schema_only],  # NB: tools= kwarg path, not OverridesState
    )

    ctx = _make_ctx(State())
    result = await agent.run(ctx)

    # Dispatch happened — no RetryPromptPart stored, result is a Call to
    # the tool's subscribe topic with the tool_call_id as the input arg.
    assert tool_call_id not in ctx.state.tool_results, f"unexpected tool_result stored: {ctx.state.tool_results.get(tool_call_id)!r}"
    assert isinstance(result, Call), f"expected Call, got {type(result).__name__}"
    assert result.input_args is not None and result.input_args[0] == tool_call_id
    assert result.target_topic == "mcp.everything.search.input"


async def test_schema_only_tool_malformed_args_become_retry_prompt() -> None:
    """Property 2: malformed JSON args still become ``RetryPromptPart`` on the
    schema-only path.

    This is the safety net that lets the MCP adaptor skip client-side
    validation without crashing the agent on every off-spec model emission.
    Mirrors ``test_tool_errors.py:1118-1157`` (the override-mode equivalent)
    but routes through the ``tools=`` kwarg instead of ``OverridesState``.
    """
    schema_only = _make_schema_only_tool()

    bad_call = ToolCallPart(
        tool_name="mcp_search",
        args="not-valid-json",  # str, not dict — args_as_dict() will raise
        tool_call_id="tc-mcp-baseline-malformed",
    )

    agent = Agent(
        "agent_mcp_baseline_malformed",
        system_prompt="x",
        subscribe_topics="agent_mcp_baseline_malformed.input",
        publish_topic="agent_mcp_baseline_malformed.output",
        model_client=_model_emits_tool_calls([bad_call]),
        tools=[schema_only],
    )

    ctx = _make_ctx(State())
    result = await agent.run(ctx)

    # All (one) calls were invalid → the agent TailCalls itself to give
    # the LLM another turn with the retry prompt visible in tool_results.
    assert isinstance(result, TailCall), f"expected TailCall, got {type(result).__name__}"

    stored = ctx.state.tool_results.get("tc-mcp-baseline-malformed")
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

    tcid = f"tc-mcp-baseline-bad-{abs(hash(args))}"
    bad_call = ToolCallPart(tool_name="mcp_search", args=args, tool_call_id=tcid)

    agent = Agent(
        "agent_mcp_baseline_param",
        system_prompt="x",
        subscribe_topics="agent_mcp_baseline_param.input",
        publish_topic="agent_mcp_baseline_param.output",
        model_client=_model_emits_tool_calls([bad_call]),
        tools=[schema_only],
    )

    ctx = _make_ctx(State())
    await agent.run(ctx)

    stored = ctx.state.tool_results.get(tcid)
    assert isinstance(stored, RetryPromptPart), f"args={args!r}: expected RetryPromptPart, got {type(stored).__name__}"
