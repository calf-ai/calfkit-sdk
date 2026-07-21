"""Unit 3 — agent-loop integration tests (docs/designs/agent-pov-projection.md §6, §7, §14).

These drive the agent's ``run()`` / loop with the deterministic vendored
``FunctionModel`` (no API key required) so the projection + final-output wiring
is exercised end-to-end without a live provider.

Covered:
  * §14.6 — tool-mode structured run with a ``TextPart`` preamble surfaces BOTH
    the preamble and the structured value into ``final_output_parts``. MUST be
    tool mode (the default): a ``str``/native fixture would pass even with the
    old ``new_messages()[-1]`` bug because there's no trailing ``final_result``
    ModelRequest to mis-read.
  * §6.2 — a deferred-tool re-entry keeps the viewer's own in-flight
    ``ToolCallPart`` ids verbatim in the projected input (no ``UserError``).
"""

from __future__ import annotations

from pydantic import BaseModel

from calfkit._vendor.pydantic_ai import models
from calfkit._vendor.pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    TextPart,
    ToolCallPart,
)
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit._vendor.pydantic_ai.tools import ToolReturn
from calfkit.models import DataPart
from calfkit.models import TextPart as PayloadTextPart
from calfkit.models.session_context import SessionRunContext
from calfkit.models.state import State
from calfkit.nodes import StatelessAgent, agent_tool

models.ALLOW_MODEL_REQUESTS = True


def _ctx(state: State) -> SessionRunContext:
    """Build a SessionRunContext with the transport identity stamped.

    Driving ``StatelessAgent.run()`` directly (no Kafka handler) skips
    ``prepare_context``, so ``correlation_id`` / ``frame_id`` would be unset and
    the agent's logging/aggregation paths would raise. Stamp deterministic
    values the way the framework handler would.
    """
    ctx: SessionRunContext = SessionRunContext(state=state, deps={})
    ctx._correlation_id = "test-correlation-id"
    ctx._frame_id = "test-frame-id"
    return ctx


class FlightPlan(BaseModel):
    flights: int


# --------------------------------------------------------------------------- #
# §14.6 — tool-mode structured final output: preamble + structure both surfaced #
# --------------------------------------------------------------------------- #


def _tool_mode_structured_with_preamble(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
    """Emit a tool-mode final response: a TextPart preamble AND a final_result call.

    In tool mode (the default for calfkit providers) pydantic-ai validates the
    output tool call, sets ``result.output`` to the structured value, and appends
    a trailing ``final_result`` ToolReturn ModelRequest AFTER this response — so
    ``new_messages()[-1]`` is that ModelRequest, NOT this response. The §7 fix
    must reverse-scan for the last ModelResponse to read the preamble.
    """
    output_tool_name = info.output_tools[0].name
    return ModelResponse(
        parts=[
            TextPart(content="On it."),
            ToolCallPart(tool_name=output_tool_name, args={"flights": 3}),
        ]
    )


async def test_final_output_parts_tool_mode_structured_surfaces_preamble_and_structure():
    """§14.6: tool-mode structured output with a preamble → [TextPart(preamble), DataPart(structured)]."""
    model = FunctionModel(_tool_mode_structured_with_preamble)
    agent: StatelessAgent[FlightPlan] = StatelessAgent[FlightPlan](
        "preamble_agent",
        system_prompt="You are a helpful AI assistant.",
        subscribe_topics="preamble_agent.input",
        model_client=model,  # pyright: ignore[reportArgumentType]
        final_output_type=FlightPlan,
    )

    state = State(message_history=[ModelRequest.user_text_prompt("book flights")])
    ctx = _ctx(state)

    result = await agent.run(ctx)

    parts = result.value  # output now rides ReturnCall.value -> reply.parts (§4.5)
    # BOTH the preamble and the structured value are surfaced, in order.
    assert len(parts) == 2, parts
    assert isinstance(parts[0], PayloadTextPart)
    assert parts[0].text == "On it."
    assert isinstance(parts[1], DataPart)
    assert parts[1].data == FlightPlan(flights=3)


def _tool_mode_structured_no_preamble(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
    """Tool-mode final response with no text preamble → only the structured value."""
    output_tool_name = info.output_tools[0].name
    return ModelResponse(parts=[ToolCallPart(tool_name=output_tool_name, args={"flights": 7})])


async def test_final_output_parts_tool_mode_structured_no_preamble_is_data_only():
    """§7: a structured output with no preamble → [DataPart] only (no empty TextPart)."""
    model = FunctionModel(_tool_mode_structured_no_preamble)
    agent: StatelessAgent[FlightPlan] = StatelessAgent[FlightPlan](
        "no_preamble_agent",
        system_prompt="You are a helpful AI assistant.",
        subscribe_topics="no_preamble_agent.input",
        model_client=model,  # pyright: ignore[reportArgumentType]
        final_output_type=FlightPlan,
    )

    state = State(message_history=[ModelRequest.user_text_prompt("book flights")])
    ctx = _ctx(state)

    result = await agent.run(ctx)

    parts = result.value  # output now rides ReturnCall.value -> reply.parts (§4.5)
    assert len(parts) == 1, parts
    assert isinstance(parts[0], DataPart)
    assert parts[0].data == FlightPlan(flights=7)


def _prompted_mode_structured(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
    """Prompted/native-mode final response: the structured answer IS the response text
    (JSON), with NO ``final_result`` tool call (``info.output_tools`` is empty)."""
    return ModelResponse(parts=[TextPart(content='{"flights": 5}')])


async def test_final_output_parts_prompted_mode_is_data_only_no_duplication():
    """§7: when the structured answer comes back as the response TEXT (native/prompted
    mode, no ``final_result`` call), ``final_output_parts`` is ``[DataPart]`` only — the
    JSON text must NOT be duplicated as a ``TextPart`` preamble alongside it.
    """
    from calfkit._vendor.pydantic_ai import PromptedOutput

    model = FunctionModel(_prompted_mode_structured)
    agent: StatelessAgent[FlightPlan] = StatelessAgent[FlightPlan](
        "prompted_agent",
        system_prompt="You are a helpful AI assistant.",
        subscribe_topics="prompted_agent.input",
        model_client=model,  # pyright: ignore[reportArgumentType]
        final_output_type=PromptedOutput(FlightPlan),  # type: ignore[arg-type]
    )

    state = State(message_history=[ModelRequest.user_text_prompt("book flights")])
    ctx = _ctx(state)

    result = await agent.run(ctx)

    parts = result.value  # output now rides ReturnCall.value -> reply.parts (§4.5)
    assert len(parts) == 1, parts  # only the structured value — no duplicated preamble
    assert isinstance(parts[0], DataPart)
    assert parts[0].data == FlightPlan(flights=5)


def _str_output(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
    return ModelResponse(parts=[TextPart(content="plain string answer")])


async def test_final_output_parts_str_output_unchanged():
    """A plain str output stays a single TextPart (regression guard for the str branch)."""
    model = FunctionModel(_str_output)
    agent: StatelessAgent[str] = StatelessAgent[str](
        "str_agent",
        system_prompt="You are a helpful AI assistant.",
        subscribe_topics="str_agent.input",
        model_client=model,  # pyright: ignore[reportArgumentType]
    )

    state = State(message_history=[ModelRequest.user_text_prompt("say something")])
    ctx = _ctx(state)

    result = await agent.run(ctx)

    parts = result.value  # output now rides ReturnCall.value -> reply.parts (§4.5)
    assert len(parts) == 1, parts
    assert isinstance(parts[0], PayloadTextPart)
    assert parts[0].text == "plain string answer"


# --------------------------------------------------------------------------- #
# §6.1 — the model runs on the projected history; canonical stays untouched     #
# --------------------------------------------------------------------------- #


async def test_model_input_is_projected_pov():
    """§6.1: the FunctionModel sees the OTHER agents as attributed surface requests."""
    seen: dict[str, list[ModelMessage]] = {}

    def _capture(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        seen["history"] = list(messages)
        return ModelResponse(parts=[TextPart(content="viewer reply")])

    model = FunctionModel(_capture)
    agent: StatelessAgent[str] = StatelessAgent[str](
        "viewer",
        system_prompt="You are a helpful AI assistant.",
        subscribe_topics="viewer.input",
        model_client=model,  # pyright: ignore[reportArgumentType]
    )

    history: list[ModelMessage] = [
        ModelRequest.user_text_prompt("kick off"),
        ModelResponse(parts=[TextPart(content="alpha says hi")], name="alpha"),
        ModelResponse(parts=[TextPart(content="beta says hi")], name="beta"),
    ]
    state = State(message_history=history)
    ctx = _ctx(state)

    await agent.run(ctx)

    projected = seen["history"]
    # Other agents are re-roled to attributed user requests; no raw ModelResponse
    # with name "alpha"/"beta" leaks into the model input.
    assert not any(isinstance(m, ModelResponse) and m.name in {"alpha", "beta"} for m in projected)
    user_texts = [
        p.content
        for m in projected
        if isinstance(m, ModelRequest)
        for p in m.parts
        if getattr(p, "part_kind", None) == "user-prompt" and isinstance(p.content, str)
    ]
    assert any("<alpha>" in t and "alpha says hi" in t for t in user_texts)
    assert any("<beta>" in t and "beta says hi" in t for t in user_texts)

    # Canonical history is untouched as input (still author-tagged, prefix-free)
    # and the viewer's own new response was appended, name-stamped.
    assert state.message_history[1].name == "alpha"
    assert state.message_history[2].name == "beta"
    new_responses = [m for m in state.message_history if isinstance(m, ModelResponse) and m.name == "viewer"]
    assert len(new_responses) == 1
    assert new_responses[0].parts[0].content == "viewer reply"
    # canonical content is prefix-free (no projection artifact stored)
    assert all("<alpha>" not in (getattr(p, "content", "") or "") for m in state.message_history if isinstance(m, ModelRequest) for p in m.parts)


# --------------------------------------------------------------------------- #
# §6.2 — deferred-tool re-entry keeps the viewer's own ToolCallPart ids          #
# --------------------------------------------------------------------------- #


def _tool(location: str) -> str:
    """A weather tool."""
    return "ok"


async def test_deferred_tool_reentry_keeps_self_tool_call_ids_no_user_error():
    """§6.2: re-entering mid-tool-round-trip in an ENGAGED projection must not raise UserError.

    The viewer's in-flight ToolCallPart (id + name) must survive projection
    verbatim so ``_handle_deferred_tool_results`` matches the deferred result to
    the call by ``tool_call_id``. We seed a multi-participant history (so
    projection is engaged — alpha + viewer = 2 agent names) whose tail is the
    viewer's own tool-call response, and re-enter with a completed tool result
    staged in ``State``.
    """
    captured: dict[str, list[ModelMessage]] = {}

    def _after_tool(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        captured["history"] = list(messages)
        return ModelResponse(parts=[TextPart(content="done with tool")])

    tool_node = agent_tool(_tool)
    model = FunctionModel(_after_tool)
    agent: StatelessAgent[str] = StatelessAgent[str](
        "viewer",
        system_prompt="You are a helpful AI assistant.",
        subscribe_topics="viewer.input",
        model_client=model,  # pyright: ignore[reportArgumentType]
        tools=[tool_node],
    )

    # Multi-participant canonical history with the viewer's own in-flight tool call last.
    call_id = "tc-self-1"
    history: list[ModelMessage] = [
        ModelRequest.user_text_prompt("kick off"),
        ModelResponse(parts=[TextPart(content="alpha here")], name="alpha"),
        ModelResponse(
            parts=[ToolCallPart(tool_name="_tool", args={"location": "NYC"}, tool_call_id=call_id)],
            name="viewer",
        ),
    ]
    state = State(message_history=history)
    # Stage the completed tool result so the agent re-enters via deferred_tool_results.
    state.add_tool_call(ToolCallPart(tool_name="_tool", args={"location": "NYC"}, tool_call_id=call_id))
    state.add_tool_result(call_id, ToolReturn(return_value="sunny"))

    ctx = _ctx(state)

    # If projection dropped the viewer's tool-call response, this would raise
    # UserError ("Cannot find tool call ..." / unmatched deferred result).
    await agent.run(ctx)

    # The projected input the model saw must still contain the viewer's own
    # ToolCallPart id (full-fidelity self view).
    projected = captured["history"]
    self_tool_call_ids = {p.tool_call_id for m in projected if isinstance(m, ModelResponse) for p in m.parts if isinstance(p, ToolCallPart)}
    assert call_id in self_tool_call_ids
    # and the model produced a final response on the projected re-entry
    assert any(isinstance(m, ModelResponse) for m in state.message_history if m.parts and getattr(m, "name", None) == "viewer")


# --------------------------------------------------------------------------- #
# §14.8 — group chat: two agents on one shared, accumulating canonical history  #
# --------------------------------------------------------------------------- #


def _make_chat_agent(node_id: str, reply: str) -> tuple[StatelessAgent, dict[str, list[ModelMessage]]]:
    """An agent whose FunctionModel records the POV history it was run on."""
    pov: dict[str, list[ModelMessage]] = {}

    def _capture(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        pov["history"] = list(messages)
        return ModelResponse(parts=[TextPart(content=reply)])

    agent: StatelessAgent[str] = StatelessAgent[str](
        node_id,
        system_prompt="You are a helpful AI assistant.",
        subscribe_topics="channel.discord.123",
        model_client=FunctionModel(_capture),  # pyright: ignore[reportArgumentType]
    )
    return agent, pov


def _user_prompt_texts(messages: list[ModelMessage]) -> list[str]:
    return [
        p.content
        for m in messages
        if isinstance(m, ModelRequest)
        for p in m.parts
        if getattr(p, "part_kind", None) == "user-prompt" and isinstance(p.content, str)
    ]


async def test_group_chat_two_agents_one_channel_pov_and_canonical_invariants():
    """§14.8: two agents share one channel; each sees the other surface-only and itself full-fidelity.

    The canonical ``message_history`` is the channel transcript (§3.3). Each agent
    that acts projects → runs → appends its stamped turn to the SAME canonical
    list, which the next agent then reads. Per §5.1 detection counts the distinct
    agent names ALREADY in the history, so projection engages only once >=2 agents
    have spoken (turns 1-2 are single-agent transparent; turns 3-4 are engaged).
    We assert:
      * once engaged, each agent's MODEL INPUT is its own POV (the other attributed
        surface-only; itself full-fidelity, name stripped);
      * the canonical wire history stays prefix-free and author-tagged throughout;
      * ``latest_tool_calls()`` never picks up another agent's calls.
    """
    agent_alpha, pov_alpha = _make_chat_agent("alpha", "alpha reply")
    agent_beta, pov_beta = _make_chat_agent("beta", "beta reply")

    # One shared canonical history — the channel. A human posts first.
    state = State(message_history=[ModelRequest.user_text_prompt("hey team, status?")])

    # Turns 1 & 2: alpha then beta speak. Each is single-agent transparent at the
    # time it runs (only <=1 prior agent name in the history), so we don't assert
    # POV here — only that the canonical history accumulates author-tagged turns.
    await agent_alpha.run(_ctx(state))
    assert state.message_history[-1].name == "alpha"
    await agent_beta.run(_ctx(state))
    assert state.message_history[-1].name == "beta"

    # The channel now has BOTH alpha and beta → projection is ENGAGED for the
    # next turn of either agent.

    # Turn 3: alpha re-enters in engaged projection. It sees ITSELF full-fidelity
    # (its prior "alpha reply" stays a name-stripped ModelResponse) and beta
    # surface-only (an attributed <beta> user request).
    await agent_alpha.run(_ctx(state))
    alpha_input = pov_alpha["history"]
    self_responses = [m for m in alpha_input if isinstance(m, ModelResponse)]
    assert any(r.name is None and r.parts and getattr(r.parts[0], "content", None) == "alpha reply" for r in self_responses)
    assert not any(isinstance(m, ModelResponse) and m.name == "beta" for m in alpha_input)
    assert any("<beta>" in t and "beta reply" in t for t in _user_prompt_texts(alpha_input))
    assert any("<user>" in t and "status" in t for t in _user_prompt_texts(alpha_input))

    # Turn 4: beta re-enters in engaged projection. It sees ITSELF full-fidelity
    # and alpha surface-only.
    await agent_beta.run(_ctx(state))
    beta_input = pov_beta["history"]
    beta_self = [m for m in beta_input if isinstance(m, ModelResponse)]
    assert any(r.name is None and r.parts and getattr(r.parts[0], "content", None) == "beta reply" for r in beta_self)
    assert not any(isinstance(m, ModelResponse) and m.name == "alpha" for m in beta_input)
    assert any("<alpha>" in t and "alpha reply" in t for t in _user_prompt_texts(beta_input))

    # Canonical wire history is prefix-free and author-tagged throughout.
    for m in state.message_history:
        if isinstance(m, ModelResponse):
            assert m.name in {"alpha", "beta"}  # author-tagged
        if isinstance(m, ModelRequest):
            for p in m.parts:
                content = getattr(p, "content", "") or ""
                if isinstance(content, str):
                    assert "<alpha>" not in content and "<beta>" not in content and "<user>" not in content

    # latest_tool_calls() on the canonical history never surfaces another agent's
    # calls — there are no pending tool calls after completed text turns.
    assert state.latest_tool_calls() == []
