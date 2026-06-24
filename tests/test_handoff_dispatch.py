"""PR-C Item 4: the final-output-branch handoff dispatch (§5.3/§5.4).

When the model produces a ``HandoffRequest`` as its turn output, the final-output branch (which has already
persisted A's output via ``extend_with_responses``) routes by ``isinstance``:
- LIVE target -> null ``state.overrides`` + ``TailCall(derive_input_topic(name), clear_overrides=True)`` — the
  frame retargets preserving frame_id/tag/callback_topic + caller_node_id, so the peer inherits A's ORIGINAL
  caller + full conversation and A drops out; both override channels are cleared (C2).
- STALE (target gone between render and dispatch) -> append a FEEDBACK TURN + ``TailCall`` to SELF (no
  clear_overrides). The feedback turn makes the history tail a ``ModelRequest`` so the re-entered model is
  forced to re-decide; without it, pydantic-ai's ``UserPromptNode`` no-model-call shortcut would return the
  stale handoff output verbatim in native/prompted mode (the CRITICAL regression).

Invalid/self/hallucinated names never reach here — the per-turn ``Literal`` + pydantic-ai auto-retry handle
them (covered in tests/test_handoff_request.py).
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from pydantic import BaseModel

from calfkit._vendor.pydantic_ai.messages import ModelRequest, ModelResponse, ToolCallPart, UserPromptPart
from calfkit._vendor.pydantic_ai.messages import TextPart as ModelTextPart
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit._vendor.pydantic_ai.models.test import TestModel
from calfkit.models.actions import TailCall
from calfkit.models.agents import AGENTS_VIEW_RESOURCE_KEY, derive_input_topic
from calfkit.models.state import OverridesState, State
from calfkit.nodes import Agent
from calfkit.peers import Handoff
from calfkit.peers.handoff import HandoffRequest
from tests.test_tool_errors import _make_ctx


class _MutableView:
    """A duck-typed agents view whose snapshot can change between reads — to simulate the render->dispatch
    staleness race (and a peer flapping back online for the self-retry re-entry)."""

    def __init__(self, cards: dict[str, str | None]) -> None:
        self._cards = cards

    def snapshot(self) -> dict[str, Any]:
        return {n: SimpleNamespace(description=d) for n, d in self._cards.items()}

    def set(self, cards: dict[str, str | None]) -> None:
        self._cards = cards


def _view(cards: dict[str, str | None]) -> _MutableView:
    return _MutableView(cards)


def _ctx_with_view(view: object) -> Any:
    ctx = _make_ctx(State())
    ctx._resources = {AGENTS_VIEW_RESOURCE_KEY: view}
    ctx._ancestor_callers = frozenset()
    return ctx


def _agent(model: Any, **kw: Any) -> Agent[Any]:
    return Agent("triage", subscribe_topics="triage.in", model_client=model, **kw)


def _emit_handoff(name: str, message: str) -> Any:
    """A FunctionModel function that emits the HandoffRequest output-tool call (finding its name from the
    offered output_tools — ``final_result`` for a str agent, ``final_result_HandoffRequest`` for a structured
    one), so pydantic-ai parses ``result.output`` into a HandoffRequest instance."""

    def _fn(messages: list[Any], info: AgentInfo) -> ModelResponse:
        tool = next(t for t in info.output_tools if t.name == "final_result" or "HandoffRequest" in t.name)
        return ModelResponse(parts=[ToolCallPart(tool_name=tool.name, args={"name": name, "message": message}, tool_call_id="h1")])

    return _fn


# ── unit: _dispatch_handoff routing (live / stale) ──


def test_dispatch_handoff_live_target_tailcalls_with_clear_overrides() -> None:
    agent = _agent(TestModel(), peers=[Handoff("billing")])
    ctx = _ctx_with_view(_view({"billing": "Billing."}))
    ctx.state.overrides = OverridesState()  # the caller's per-run overrides are present
    result = agent._dispatch_handoff(HandoffRequest(name="billing", message="take over"), ctx)
    assert isinstance(result, TailCall)
    assert result.target_topic == derive_input_topic("billing")  # relinquish to the peer's input topic
    assert result.clear_overrides is True  # genuine handoff -> clear the frame channel (C2)
    assert ctx.state.overrides is None  # ...and the state channel (both nulled)
    assert result.state is ctx.state  # carries the CURRENT full conversation


def test_dispatch_handoff_stale_target_self_retries_with_feedback_turn() -> None:
    agent = _agent(TestModel(), peers=[Handoff("billing")])
    ctx = _ctx_with_view(_view({}))  # billing gone (stale at dispatch)
    result = agent._dispatch_handoff(HandoffRequest(name="billing", message="x"), ctx)
    assert isinstance(result, TailCall)
    assert result.target_topic == agent._return_topic  # self-retry, NOT a relinquish
    assert result.clear_overrides is False  # keep A's own surface (self-retry to self)
    last = ctx.state.message_history[-1]  # a feedback turn is appended so the model re-decides on re-entry
    assert isinstance(last, ModelRequest)
    assert any(isinstance(p, UserPromptPart) for p in last.parts)


async def test_staleness_self_retry_re_invokes_the_model() -> None:
    # CRITICAL: the feedback turn makes the history tail a ModelRequest, defeating pydantic-ai's
    # UserPromptNode no-model-call shortcut — so the re-entered run CALLS the model (a fresh decision)
    # instead of returning the stale handoff output verbatim (the native/prompted str-fallthrough bug).
    calls = {"n": 0}

    def _model(messages: list[Any], info: AgentInfo) -> ModelResponse:
        calls["n"] += 1
        return ModelResponse(parts=[ModelTextPart("answering the user directly")])

    agent = _agent(FunctionModel(_model), peers=[Handoff("billing")])
    view = _view({})  # billing offline -> _dispatch_handoff sees it stale
    ctx = _ctx_with_view(view)
    # A produced a handoff (native/prompted = a JSON TextPart), already persisted by the final-output branch:
    ctx.state.extend_with_responses([ModelResponse(parts=[ModelTextPart('{"name":"billing","message":"x"}')])], agent.name)
    agent._dispatch_handoff(HandoffRequest(name="billing", message="x"), ctx)  # stale -> appends the feedback turn
    view.set({"billing": "Billing."})  # billing flaps back online -> re-entry has EMPTY instructions (no note)
    await agent.run(ctx)  # re-entry: the ModelRequest tail forces the model call
    assert calls["n"] == 1  # the model WAS re-invoked (not shortcut to returning the stale handoff JSON)


# ── end-to-end: the model produces a HandoffRequest output (str + structured agents) ──


async def test_str_agent_produces_handoff_and_dispatches_to_peer() -> None:
    calls = {"n": 0}

    def _model(messages: list[Any], info: AgentInfo) -> ModelResponse:
        calls["n"] += 1
        return _emit_handoff("billing", "please take over")(messages, info)

    agent = _agent(FunctionModel(_model), peers=[Handoff("billing")])
    ctx = _ctx_with_view(_view({"billing": "Billing."}))
    result = await agent.run(ctx)
    assert isinstance(result, TailCall)  # discriminated by isinstance, dispatched as a relinquish
    assert result.target_topic == derive_input_topic("billing")
    assert result.clear_overrides is True
    assert ctx.state.overrides is None
    assert calls["n"] == 1  # terminal — producing the handoff ends the run (no further model turn)


async def test_structured_agent_produces_handoff_and_dispatches_identically() -> None:
    # The branch keys on isinstance(result.output, HandoffRequest), NOT the output mode — a structured
    # final_output_type (handoff member renamed final_result_HandoffRequest) dispatches the same way.
    class _Answer(BaseModel):
        text: str

    agent = Agent(
        "triage",
        subscribe_topics="triage.in",
        model_client=FunctionModel(_emit_handoff("billing", "ctx")),
        final_output_type=_Answer,
        peers=[Handoff("billing")],
    )
    ctx = _ctx_with_view(_view({"billing": None}))
    result = await agent.run(ctx)
    assert isinstance(result, TailCall)
    assert result.target_topic == derive_input_topic("billing")
    assert result.clear_overrides is True
