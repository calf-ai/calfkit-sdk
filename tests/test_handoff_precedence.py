"""Spec §8 precedence-hierarchy pins (impl-plan S2c): answer vs handoff co-emitted.

Calfkit's DECLARED PERMANENT policy is pydantic-ai's arbitration hierarchy —
**validated output-tool call > other tool calls > text** — the winner is decided by signal
strength, not output mode. Today the vendor graph enforces ranks 1 and 3 and calfkit
enforces rank 2; these characterization pins make a future vendor bump that flips any rank
fail loudly (and at #230 they become the acceptance tests for `arbitrate_handoff`'s
re-implementation of the same rule).
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from calfkit._vendor.pydantic_ai import PromptedOutput
from calfkit._vendor.pydantic_ai.messages import ModelResponse, ToolCallPart, ToolReturnPart
from calfkit._vendor.pydantic_ai.messages import TextPart as ModelTextPart
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit.models.actions import ReturnCall, TailCall
from calfkit.models.agents import derive_input_topic
from calfkit.models.payload import DataPart
from calfkit.nodes import StatelessAgent
from calfkit.nodes._steps import Observed
from calfkit.peers import Handoff
from tests._peer_fakes import agents_view as _view
from tests._peer_fakes import ctx_with_view as _ctx_with_view
from tests._peer_fakes import handoff_part
from tests.test_tool_errors import _unwrap


class _Answer(BaseModel):
    text: str


def _handoff_part(call_id: str = "h1") -> ToolCallPart:
    return handoff_part("billing", "take over", call_id=call_id)


async def test_rank1_structured_answer_beats_handoff_in_tool_mode() -> None:
    """§8 row 1: a validated `final_result` output-tool call outranks the handoff call —
    the vendor processes output tools first, the run ends with the answer, and the handoff
    is stubbed by the VENDOR before calfkit ever sees a DeferredToolRequests."""

    def _model(messages: list[Any], info: AgentInfo) -> ModelResponse:
        final = next(t for t in info.output_tools if t.name.startswith("final_result"))
        return ModelResponse(
            parts=[
                ToolCallPart(tool_name=final.name, args={"text": "the answer"}, tool_call_id="f1"),
                _handoff_part(),
            ]
        )

    agent = StatelessAgent(
        "triage",
        subscribe_topics="triage.in",
        model_client=FunctionModel(_model),
        final_output_type=_Answer,
        peers=[Handoff("billing")],
    )
    ctx = _ctx_with_view(_view({"billing": None}))
    result = await agent.run(ctx)

    assert isinstance(result, ReturnCall)  # the ANSWER wins — no TailCall, no transfer
    assert any(isinstance(p, DataPart) and p.data == _Answer(text="the answer") for p in result.value)
    # The vendor closed the losing handoff call with ITS stub — transcript stays valid.
    returns = [p for m in ctx.state.message_history for p in getattr(m, "parts", []) if isinstance(p, ToolReturnPart)]
    assert any(p.tool_call_id == "h1" and "not executed" in str(p.content) for p in returns)


async def test_rank2_handoff_beats_prompted_mode_json_text_answer() -> None:
    """§8 row 2 (native/prompted, pinned via PromptedOutput per plan S2c): the answer is
    JSON *text* — rank 3 — so the co-emitted handoff call (rank 2) wins the turn."""

    def _model(messages: list[Any], info: AgentInfo) -> ModelResponse:
        assert not info.output_tools  # prompted mode: no output tool exists
        return ModelResponse(parts=[ModelTextPart('{"text": "the answer"}'), _handoff_part()])

    agent = StatelessAgent(
        "triage",
        subscribe_topics="triage.in",
        model_client=FunctionModel(_model),
        final_output_type=PromptedOutput(_Answer),  # type: ignore[arg-type]
        peers=[Handoff("billing")],
    )
    ctx = _ctx_with_view(_view({"billing": None}))
    observed = await agent.run(ctx)

    assert isinstance(observed, Observed)
    result = observed.action
    assert isinstance(result, TailCall)  # the HANDOFF wins
    assert result.target_topic == derive_input_topic("billing")
    # The losing JSON-text answer surfaces as the hop's PREAMBLE fact (the step_preamble
    # docstring's deliberate §8 corner — review round 1): text lost the turn, so "the text
    # this hop emitted" is exactly what the Said fact carries.
    assert [type(f).__name__ for f in observed.facts] == ["Said", "HandedOff"]
    assert observed.facts[0].parts[0].text == '{"text": "the answer"}'


async def test_rank2_handoff_beats_str_agent_text_answer() -> None:
    """§8 row 3: a `str` agent's text answer is rank 3 — the handoff call wins and the
    text becomes the hop's preamble, not a final answer."""

    def _model(messages: list[Any], info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[ModelTextPart("You should contact billing."), _handoff_part()])

    agent = StatelessAgent("triage", subscribe_topics="triage.in", model_client=FunctionModel(_model), peers=[Handoff("billing")])
    ctx = _ctx_with_view(_view({"billing": None}))
    result = _unwrap(await agent.run(ctx))

    assert isinstance(result, TailCall)
    assert result.target_topic == derive_input_topic("billing")
