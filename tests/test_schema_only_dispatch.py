"""Schema-only tool dispatch via the ``StatelessAgent(tools=[...])`` kwarg path.

Pins the load-bearing properties of the agent loop for tools registered as
validator-less ``ToolBinding`` instances passed through the
``StatelessAgent(tools=[...])`` kwarg:

  1. A call whose args CONFORM to the binding's advertised
     ``parameters_json_schema`` dispatches. The binding carries no process-local
     validator, so the agent builds a schema fallback validator from the advertised
     schema and checks the args against it before dispatch.

  2. A call whose args VIOLATE the advertised schema is rejected pre-dispatch as a
     ``RetryPromptPart`` — the same in-band recoverable a local tool produces (no
     more crossing the wire to fault at the callee).

  3. Malformed JSON args from the LLM still become a ``RetryPromptPart`` via the
     ``args_as_dict()`` try/except which runs on *all* dispatch paths before the
     validation step.

``test_tool_errors.py`` covers the same properties on further wire-form binding
shapes; this suite exercises the schema-only ``StatelessAgent(tools=[...])`` branch.
"""

from __future__ import annotations

from typing import Any

import pytest

from calfkit._vendor.pydantic_ai.messages import RetryPromptPart, ToolCallPart
from calfkit._vendor.pydantic_ai.tools import ToolDefinition
from calfkit.models.actions import Call, TailCall
from calfkit.models.state import State
from calfkit.models.tool_dispatch import ToolBinding, ToolCallRef
from calfkit.nodes import StatelessAgent

# Reuse the proven helpers from the wire-form tool tests rather than
# re-implementing. If these helpers move or rename, the import is a
# fail-fast signal to update both tests together.
from tests.test_tool_errors import _make_ctx, _model_emits_tool_calls, _unwrap

# The default schema-only tool: one required string arg, no extra keys.
_DEFAULT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"q": {"type": "string"}},
    "required": ["q"],
    "additionalProperties": False,
}


def _make_schema_only_tool(
    *,
    tool_name: str = "search",
    topic_base: str = "tools.search",
    parameters_json_schema: dict[str, Any] | None = None,
) -> ToolBinding:
    """Construct a validator-less ``ToolBinding`` (a schema-only tool).

    Carries a real ``ToolDefinition`` (so the LLM sees a valid schema) but
    ``validator=None`` — the wire form. At dispatch the agent builds a schema fallback
    validator from ``parameters_json_schema`` (overridable, e.g. to add a typed field
    for the strictness pin).
    """
    return ToolBinding(
        dispatch_topic=f"{topic_base}.input",
        tool_def=ToolDefinition(
            name=tool_name,
            description="Synthetic tool.",
            parameters_json_schema=parameters_json_schema or _DEFAULT_SCHEMA,
        ),
    )


async def test_schema_only_tool_via_tools_kwarg_dispatches_conforming_args() -> None:
    """Property 1: ``StatelessAgent(tools=[ToolBinding(...)])`` dispatches a schema-CONFORMING call.

    The schema fallback validator passes, and the result is a ``Call`` to the binding's
    dispatch topic.
    """
    schema_only = _make_schema_only_tool()

    tool_call_id = "tc-schema-only-01"
    call = ToolCallPart(tool_name="search", args={"q": "hello"}, tool_call_id=tool_call_id)

    agent = StatelessAgent(
        "agent_schema_only",
        system_prompt="x",
        subscribe_topics="agent_schema_only.input",
        publish_topic="agent_schema_only.output",
        model_client=_model_emits_tool_calls([call]),
        tools=[schema_only],  # NB: the tools= kwarg path
    )

    ctx = _make_ctx(State())
    result = _unwrap(await agent.run(ctx))

    # Dispatch happened — no RetryPromptPart stored, result is a Call to
    # the tool's subscribe topic with the tool_call_id carried as a ToolCallRef body.
    assert tool_call_id not in ctx.state.tool_results, f"unexpected tool_result stored: {ctx.state.tool_results.get(tool_call_id)!r}"
    assert isinstance(result, Call), f"expected Call, got {type(result).__name__}"
    assert isinstance(result.body, ToolCallRef) and result.body.tool_call_id == tool_call_id
    assert result.target_topic == "tools.search.input"


@pytest.mark.parametrize(
    ("args", "why"),
    [
        ({}, "missing required 'q'"),
        ({"q": "hi", "extra": 1}, "additionalProperties:false forbids 'extra'"),
        ({"q": 123}, "'q' must be a string"),
    ],
    ids=["missing-required", "extra-key", "wrong-type"],
)
async def test_schema_only_tool_schema_violating_args_become_retry_prompt(args, why) -> None:
    """Property 2: args that VIOLATE the advertised schema are rejected pre-dispatch.

    Each shape trips the schema fallback validator and lands a ``RetryPromptPart`` instead of
    dispatching — the fix's whole point (no fault-at-the-callee for a model mistake).
    """
    schema_only = _make_schema_only_tool()

    tcid = "tc-schema-violation"
    bad_call = ToolCallPart(tool_name="search", args=args, tool_call_id=tcid)

    agent = StatelessAgent(
        "agent_schema_only_violation",
        system_prompt="x",
        subscribe_topics="agent_schema_only_violation.input",
        publish_topic="agent_schema_only_violation.output",
        model_client=_model_emits_tool_calls([bad_call]),
        tools=[schema_only],
    )

    ctx = _make_ctx(State())
    result = _unwrap(await agent.run(ctx))

    assert isinstance(result, TailCall), f"{why}: expected TailCall, got {type(result).__name__}"
    stored = ctx.state.tool_results.get(tcid)
    assert isinstance(stored, RetryPromptPart), f"{why}: expected RetryPromptPart, got {type(stored).__name__}"
    assert stored.content, f"{why}: expected non-empty violation content"


async def test_schema_only_tool_rejects_coercible_but_off_spec_args() -> None:
    """The strictness delta (spec D6.2): a discovered tool's schema is enforced AS WRITTEN. An
    integer field emitted as the string ``"3"`` — which a lax callee would coerce — is rejected at
    the caller and the model retries. This is the deliberate, chosen behavior, not an accident.
    """
    schema = {
        "type": "object",
        "properties": {"n": {"type": "integer"}},
        "required": ["n"],
        "additionalProperties": False,
    }
    schema_only = _make_schema_only_tool(tool_name="counter", topic_base="tools.counter", parameters_json_schema=schema)

    tcid = "tc-coercible-off-spec"
    call = ToolCallPart(tool_name="counter", args={"n": "3"}, tool_call_id=tcid)

    agent = StatelessAgent(
        "agent_schema_only_strict",
        system_prompt="x",
        subscribe_topics="agent_schema_only_strict.input",
        publish_topic="agent_schema_only_strict.output",
        model_client=_model_emits_tool_calls([call]),
        tools=[schema_only],
    )

    ctx = _make_ctx(State())
    result = _unwrap(await agent.run(ctx))

    assert isinstance(result, TailCall), f"expected TailCall (non-coercing reject), got {type(result).__name__}"
    stored = ctx.state.tool_results.get(tcid)
    assert isinstance(stored, RetryPromptPart), f"expected RetryPromptPart, got {type(stored).__name__}"


async def test_schema_only_tool_malformed_args_become_retry_prompt() -> None:
    """Property 2: malformed JSON args still become ``RetryPromptPart`` on the
    schema-only path.

    This is the safety net that lets a schema-only tool skip client-side
    validation without crashing the agent on every off-spec model emission.
    Mirrors ``test_tool_errors.py`` but routes through the ``tools=`` kwarg.
    """
    schema_only = _make_schema_only_tool()

    bad_call = ToolCallPart(
        tool_name="search",
        args="not-valid-json",  # str, not dict — args_as_dict() will raise
        tool_call_id="tc-schema-only-malformed",
    )

    agent = StatelessAgent(
        "agent_schema_only_malformed",
        system_prompt="x",
        subscribe_topics="agent_schema_only_malformed.input",
        publish_topic="agent_schema_only_malformed.output",
        model_client=_model_emits_tool_calls([bad_call]),
        tools=[schema_only],
    )

    ctx = _make_ctx(State())
    result = _unwrap(await agent.run(ctx))

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

    agent = StatelessAgent(
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
