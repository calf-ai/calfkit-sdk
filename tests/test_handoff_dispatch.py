"""The handoff TOOL transport's turn dispatch (handoff-tool-transport-spec §3-§5/§7, impl-plan S2b).

The model requests a transfer by calling ``handoff_to_agent``; calfkit arbitrates the whole
response (§3.0-gated): the first VALID handoff wins the turn — siblings are closed by stub
``ToolReturnPart``s in ONE closing ``ModelRequest`` (§4), ``state.tool_calls``/``tool_results``
stay untouched, the winner emits ``HandoffStep`` only, and ``_dispatch_handoff`` (a pure
TailCall builder) relinquishes to the peer. Invalid handoffs are standard pre-dispatch
rejections (``RetryPromptPart`` + error step pair) — a stale target no longer emits a
``HandoffStep`` (documented contract change, ADR-0035).
"""

from __future__ import annotations

import logging
from typing import Any

import pytest

from calfkit._vendor.pydantic_ai.messages import (
    ModelRequest,
    ModelResponse,
    RetryPromptPart,
    ToolCallPart,
    ToolReturnPart,
)
from calfkit._vendor.pydantic_ai.messages import TextPart as ModelTextPart
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit._vendor.pydantic_ai.models.test import TestModel
from calfkit.models.actions import Call, ReturnCall, TailCall
from calfkit.models.agents import derive_input_topic
from calfkit.models.marker import ToolCallMarker
from calfkit.models.state import OverridesState, State
from calfkit.models.step import AgentMessageStep, HandoffStep, ToolCallStep, ToolResultStep
from calfkit.nodes import Agent
from calfkit.peers import Handoff, Messaging
from calfkit.peers.handoff import (
    _REJECT_NONE_ONLINE,
    _REJECT_UNREACHABLE,
    _STUB_HANDOFF_NOT_EXECUTED,
    _STUB_TOOL_NOT_EXECUTED,
    HANDOFF_TOOL,
)
from tests._peer_fakes import agents_view as _view
from tests._peer_fakes import ctx_with_view as _ctx_with_view
from tests._peer_fakes import handoff_part as _handoff_part
from tests._peer_fakes import triage_agent as _agent
from tests._peer_fakes import user_tool as _user_tool


def _emit_once_then_text(*parts: Any) -> FunctionModel:
    """Emit the given parts on the FIRST model call; plain text on any later call (re-entry)."""
    calls = {"n": 0}

    def _fn(messages: list[Any], info: AgentInfo) -> ModelResponse:
        calls["n"] += 1
        if calls["n"] == 1:
            return ModelResponse(parts=list(parts))
        return ModelResponse(parts=[ModelTextPart("FRESH")])

    model = FunctionModel(_fn)
    model._test_calls = calls  # type: ignore[attr-defined]
    return model


def _closing_request(state: State) -> ModelRequest:
    last = state.message_history[-1]
    assert isinstance(last, ModelRequest), f"history must end with the closing ModelRequest, got {type(last).__name__}"
    return last


# --------------------------------------------------------------------------- #
# _dispatch_handoff — a pure TailCall builder (spec §5)                        #
# --------------------------------------------------------------------------- #


def test_dispatch_handoff_builds_the_relinquish_tailcall() -> None:
    agent = _agent(TestModel(), peers=[Handoff("billing")])
    ctx = _ctx_with_view(_view({"billing": "Billing."}))
    ctx.state.overrides = OverridesState()  # the caller's per-run overrides are present
    result = agent._dispatch_handoff("billing", ctx)
    assert isinstance(result, TailCall)
    assert result.target_topic == derive_input_topic("billing")
    assert result.clear_overrides is True  # frame channel cleared (C2)
    assert ctx.state.overrides is None  # ...and the state channel (both nulled, single home §5)
    assert result.state is ctx.state  # carries the CURRENT full conversation


# --------------------------------------------------------------------------- #
# Winner path (plan tests 1-3)                                                 #
# --------------------------------------------------------------------------- #


async def test_winner_alone_relinquishes_with_closing_request(caplog: pytest.LogCaptureFixture) -> None:
    """Plan test 1: TailCall to the peer; ONE closing ModelRequest with the winner's
    'Transferred' return; tool_calls/tool_results untouched; HandoffStep only; DEBUGs."""
    model = _emit_once_then_text(_handoff_part("billing", "customer needs a refund"))
    agent = _agent(model, peers=[Handoff("billing")])
    ctx = _ctx_with_view(_view({"billing": "Billing."}))
    with caplog.at_level(logging.DEBUG, logger="calfkit.nodes.agent"):
        result = await agent.run(ctx)

    assert isinstance(result, TailCall)
    assert result.target_topic == derive_input_topic("billing")
    assert result.clear_overrides is True
    assert ctx.state.overrides is None
    assert model._test_calls["n"] == 1  # terminal — the winning handoff ends the turn

    closing = _closing_request(ctx.state)
    (ret,) = closing.parts
    assert isinstance(ret, ToolReturnPart)
    assert ret.tool_call_id == "h1" and ret.tool_name == HANDOFF_TOOL
    assert ret.content == "Transferred to billing."

    assert ctx.state.tool_calls == {}  # §4: no registration on a winning turn
    assert ctx.state.tool_results == {}

    draft = ctx._step_draft or []
    assert [type(e).__name__ for e in draft] == ["HandoffStep"]  # winner emits HandoffStep ONLY
    assert isinstance(draft[0], HandoffStep) and draft[0].target == "billing" and draft[0].reason == "customer needs a refund"

    assert any(r.levelno == logging.DEBUG and "relinquish" in r.message for r in caplog.records)


async def test_winner_with_preamble_drafts_message_then_handoff() -> None:
    """Plan test 1 (preamble variant): AgentMessageStep then HandoffStep — today's stream shape."""
    model = _emit_once_then_text(ModelTextPart("Let me transfer you to billing."), _handoff_part("billing", "take over"))
    agent = _agent(model, peers=[Handoff("billing")])
    ctx = _ctx_with_view(_view({"billing": "Billing."}))
    await agent.run(ctx)
    draft = ctx._step_draft or []
    assert [type(e).__name__ for e in draft] == ["AgentMessageStep", "HandoffStep"]
    assert isinstance(draft[0], AgentMessageStep) and draft[0].parts[0].text == "Let me transfer you to billing."


async def test_winner_stubs_every_parallel_sibling(caplog: pytest.LogCaptureFixture) -> None:
    """Plan test 2: winner + tool + message_agent siblings → no Call/batch; every sibling
    closed in the closing request with the §4 stubs; sibling step pairs + HandoffStep."""
    model = _emit_once_then_text(
        ToolCallPart(tool_name="search", args={}, tool_call_id="t1"),
        ToolCallPart(tool_name="message_agent", args={"name": "support", "message": "hi"}, tool_call_id="m1"),
        _handoff_part("billing", "take over"),
    )
    agent = _agent(model, tools=[_user_tool("search")], peers=[Messaging("support"), Handoff("billing")])
    ctx = _ctx_with_view(_view({"billing": None, "support": None}))
    with caplog.at_level(logging.DEBUG, logger="calfkit.nodes.agent"):
        result = await agent.run(ctx)

    assert isinstance(result, TailCall)  # NOT a Call / list[Call] — nothing dispatches
    assert result.target_topic == derive_input_topic("billing")

    closing = _closing_request(ctx.state)
    by_id = {p.tool_call_id: p for p in closing.parts}
    assert set(by_id) == {"h1", "t1", "m1"}
    assert by_id["h1"].content == "Transferred to billing."
    assert by_id["t1"].content == _STUB_TOOL_NOT_EXECUTED
    assert by_id["m1"].content == _STUB_TOOL_NOT_EXECUTED
    assert ctx.state.tool_calls == {} and ctx.state.tool_results == {}

    draft = ctx._step_draft or []
    call_steps = [e for e in draft if isinstance(e, ToolCallStep)]
    result_steps = [e for e in draft if isinstance(e, ToolResultStep)]
    assert [c.tool_call_id for c in call_steps] == ["t1", "m1"]  # NO ToolCallStep for the winner
    # Stub siblings are refusals-to-dispatch: ``denied``, never ``success`` (spec §7a).
    assert {r.tool_call_id: r.outcome for r in result_steps} == {"t1": "denied", "m1": "denied"}
    assert all(r.parts[0].text == _STUB_TOOL_NOT_EXECUTED for r in result_steps)
    assert isinstance(draft[-1], HandoffStep)

    assert any(r.levelno == logging.DEBUG and "stub" in r.message for r in caplog.records)


async def test_first_valid_handoff_wins_rejected_and_later_stubbed() -> None:
    """Plan test 3 (§3.1): [invalid, valid, valid] — first gets a denied step pair (precise
    reason) + a generic closing stub; second wins; third gets a denied stub pair + closing stub."""
    model = _emit_once_then_text(
        _handoff_part("ghost", "hi", call_id="h1"),  # invalid: not reachable
        _handoff_part("billing", "take over", call_id="h2"),
        _handoff_part("support", "or you", call_id="h3"),
    )
    agent = _agent(model, peers=[Handoff("billing", "support", "ghost")])
    ctx = _ctx_with_view(_view({"billing": None, "support": None}))  # ghost NOT live
    result = await agent.run(ctx)

    assert isinstance(result, TailCall) and result.target_topic == derive_input_topic("billing")

    closing = _closing_request(ctx.state)
    by_id = {p.tool_call_id: p.content for p in closing.parts}
    assert by_id == {
        "h2": "Transferred to billing.",
        "h1": _STUB_HANDOFF_NOT_EXECUTED,  # rejected-before-winner: generic transcript stub (§3.1)
        "h3": _STUB_HANDOFF_NOT_EXECUTED,  # post-winner handoff
    }
    assert ctx.state.tool_results == {}  # rejection did NOT ride tool_results on a winning turn

    draft = ctx._step_draft or []
    results = {e.tool_call_id: e for e in draft if isinstance(e, ToolResultStep)}
    assert results["h1"].outcome == "denied"  # rejected handoff (§7b); precise reason in the step stream
    assert _REJECT_UNREACHABLE.format(name="ghost") in results["h1"].parts[0].text
    assert results["h3"].outcome == "denied"  # post-winner stub (§7a)
    assert sum(isinstance(e, HandoffStep) for e in draft) == 1


# --------------------------------------------------------------------------- #
# No-winner paths (plan tests 4, 5, 5b, 5c, 5d, 7)                             #
# --------------------------------------------------------------------------- #


async def test_stale_target_is_a_standard_rejection(caplog: pytest.LogCaptureFixture) -> None:
    """Plan test 4: the render→arbitration staleness race is just an invalid handoff —
    RetryPromptPart + error pair + self-TailCall; NO HandoffStep (contract change); WARNING."""
    view = _view({"billing": None, "support": None})

    def _fn(messages: list[Any], info: AgentInfo) -> ModelResponse:
        view.set({"support": None})  # billing drops between render and arbitration
        return ModelResponse(parts=[_handoff_part("billing", "x")])

    agent = _agent(FunctionModel(_fn), peers=[Handoff("billing", "support")])
    ctx = _ctx_with_view(view)
    with caplog.at_level(logging.WARNING, logger="calfkit.nodes.agent"):
        result = await agent.run(ctx)

    assert isinstance(result, TailCall)
    assert result.target_topic == agent._return_topic  # self-retry, NOT a relinquish
    assert result.clear_overrides is False
    retry = ctx.state.tool_results.get("h1")
    assert isinstance(retry, RetryPromptPart)
    assert retry.content == _REJECT_UNREACHABLE.format(name="billing")
    draft = ctx._step_draft or []
    assert not any(isinstance(e, HandoffStep) for e in draft)  # rejections emit NO HandoffStep
    assert any(isinstance(e, ToolResultStep) and e.outcome == "denied" for e in draft)
    assert any(r.levelname == "WARNING" and "billing" in r.message and "#251" in r.message for r in caplog.records)


async def test_zero_live_peers_rejects_with_none_online_reason() -> None:
    """Plan test 7: the tool is always present, so a call with nobody online gets the clean
    §9 none-online rejection (never the vendor's unknown-tool retry)."""
    agent = _agent(_emit_once_then_text(_handoff_part("billing", "hi")), peers=[Handoff("billing")])
    ctx = _ctx_with_view(_view({}))
    result = await agent.run(ctx)
    assert isinstance(result, TailCall) and result.target_topic == agent._return_topic
    retry = ctx.state.tool_results.get("h1")
    assert isinstance(retry, RetryPromptPart) and retry.content == _REJECT_NONE_ONLINE


async def test_rejection_re_entry_re_invokes_the_model() -> None:
    """The surviving staleness-re-invoke guard, through the new machinery: the rejection
    rides tool_results, so the self-retry re-entry feeds DeferredToolResults and the model
    is ALWAYS re-invoked for a fresh decision."""
    model = _emit_once_then_text(_handoff_part("billing", "x"))
    agent = _agent(model, peers=[Handoff("billing")])
    view = _view({})  # nobody online → rejection
    ctx = _ctx_with_view(view)
    first = await agent.run(ctx)
    assert isinstance(first, TailCall) and first.target_topic == agent._return_topic
    assert ctx.state.tool_results["h1"].content == _REJECT_NONE_ONLINE  # the §9 reason
    view.set({"billing": "Billing."})  # peer online for the re-entry
    result = await agent.run(ctx)
    assert model._test_calls["n"] == 2  # the model WAS re-invoked with the retry visible
    assert isinstance(result, ReturnCall)
    assert "FRESH" in "".join(getattr(p, "text", "") for p in result.value)


async def test_invalid_handoff_plus_valid_tool_mixed_disposition() -> None:
    """Plan test 5: the rejection lands in tool_results while the valid tool DISPATCHES."""
    model = _emit_once_then_text(
        _handoff_part("ghost", "hi"),
        ToolCallPart(tool_name="search", args={}, tool_call_id="t1"),
    )
    agent = _agent(model, tools=[_user_tool("search")], peers=[Handoff("billing")])
    ctx = _ctx_with_view(_view({"billing": None}))
    result = await agent.run(ctx)
    assert isinstance(result, Call)  # the tool dispatched (single-call arm)
    assert result.tag == "t1"
    retry = ctx.state.tool_results.get("h1")
    assert isinstance(retry, RetryPromptPart)
    assert retry.content == _REJECT_UNREACHABLE.format(name="ghost")  # the §9 reason, not "no tool named"


async def test_invalid_handoff_plus_two_tools_takes_the_parallel_arm() -> None:
    """Plan test 5b: pending excludes the resolved rejection; the two valid tools return as
    the parallel list[Call] (which is what opens the 2-slot durable batch at the chokepoint
    — the fold itself is unchanged machinery, pinned by the fan-out suites and S4 kafka)."""
    model = _emit_once_then_text(
        _handoff_part("ghost", "hi"),
        ToolCallPart(tool_name="search", args={}, tool_call_id="t1"),
        ToolCallPart(tool_name="book", args={}, tool_call_id="t2"),
    )
    agent = _agent(model, tools=[_user_tool("search"), _user_tool("book")], peers=[Handoff("billing")])
    ctx = _ctx_with_view(_view({"billing": None}))
    result = await agent.run(ctx)
    assert isinstance(result, list) and [c.tag for c in result] == ["t1", "t2"]
    retry = ctx.state.tool_results.get("h1")
    assert isinstance(retry, RetryPromptPart)
    assert retry.content == _REJECT_UNREACHABLE.format(name="ghost")


async def test_rejected_handoff_with_message_agent_sibling_keeps_the_marker() -> None:
    """Plan test 5c (spec §5): the handoff fork must not disturb the message_agent arm —
    the peer Call still carries its ToolCallMarker."""
    model = _emit_once_then_text(
        _handoff_part("ghost", "hi"),
        ToolCallPart(tool_name="message_agent", args={"name": "support", "message": "q"}, tool_call_id="m1"),
    )
    agent = _agent(model, peers=[Messaging("support"), Handoff("billing")])
    ctx = _ctx_with_view(_view({"billing": None, "support": None}))
    result = await agent.run(ctx)
    assert isinstance(result, Call)
    assert isinstance(result.marker, ToolCallMarker) and result.marker.tool_name == "message_agent"
    retry = ctx.state.tool_results.get("h1")
    assert isinstance(retry, RetryPromptPart)
    assert retry.content == _REJECT_UNREACHABLE.format(name="ghost")


async def test_handle_less_agent_user_tool_named_handoff_dispatches_normally() -> None:
    """Plan test 5d (spec §3.0): no Handoff handle → no interception; the user's own
    ``handoff_to_agent`` tool is an ordinary tool and dispatches to its node."""
    model = _emit_once_then_text(ToolCallPart(tool_name=HANDOFF_TOOL, args={}, tool_call_id="u1"))
    agent = _agent(model, tools=[_user_tool(HANDOFF_TOOL)])
    ctx = _ctx_with_view(_view({}))
    result = await agent.run(ctx)
    assert isinstance(result, Call)
    assert result.tag == "u1"
    assert ctx.state.tool_results == {}  # no rejection — never intercepted


# --------------------------------------------------------------------------- #
# Re-entry validity (plan test 6) + the defensive fork (impossible state)      #
# --------------------------------------------------------------------------- #


async def test_peer_re_enters_cleanly_on_the_inherited_state() -> None:
    """Plan test 6 (the B2 pin): the closing request terminates latest_tool_calls' reverse
    walk, so peer B makes a clean model call on the carried state — no deferred-results
    id mismatch, no dangling calls."""
    model = _emit_once_then_text(_handoff_part("billing", "customer needs a refund"))
    a = _agent(model, peers=[Handoff("billing")])
    ctx = _ctx_with_view(_view({"billing": "Billing."}))
    result = await a.run(ctx)
    assert isinstance(result, TailCall)

    b_calls = {"n": 0}

    def _b_model(messages: list[Any], info: AgentInfo) -> ModelResponse:
        b_calls["n"] += 1
        return ModelResponse(parts=[ModelTextPart("on it")])

    b = Agent("billing", subscribe_topics="billing.in", model_client=FunctionModel(_b_model))
    b_ctx = _ctx_with_view(_view({}), state=ctx.state)  # B inherits A's carried state
    b_result = await b.run(b_ctx)
    assert b_calls["n"] == 1
    assert isinstance(b_result, ReturnCall)
    assert "on it" in "".join(getattr(p, "text", "") for p in b_result.value)


async def test_pending_handoff_at_reentry_raises_incomplete_reentry() -> None:
    """Corrupt-state tripwire (successor to the deleted sequential-arm fork test): a PENDING
    handoff call surviving to re-entry is impossible by construction — a handoff is finalized
    in the delivery that produced it — so it must trip the unconditional incomplete-re-entry
    RuntimeError, never skip or mis-dispatch to the registry lookup. Guards against a future
    pre-model handoff routing landing ahead of the gate."""
    agent = _agent(TestModel(), peers=[Handoff("billing")])
    ctx = _ctx_with_view(_view({"billing": None}))
    part = _handoff_part("billing", "x")
    ctx.state.extend_with_responses([ModelResponse(parts=[part])], agent.name)
    ctx.state.add_tool_call(part)  # registered but unresolved — the impossible state
    with pytest.raises(RuntimeError, match="incomplete tool calls in run"):
        await agent.run(ctx)


async def test_returning_handoff_a_to_b_to_a_re_enters_cleanly() -> None:
    """The A-return direction of spec §4's re-entry pin (review round 1): after A→B→A, A
    re-enters over its OWN persisted handoff turn (real call + self-owned closing request)
    plus B's surfaced turn — one clean model call, no deferred-results mismatch."""
    a_model = _emit_once_then_text(_handoff_part("billing", "take over"))
    a = _agent(a_model, peers=[Handoff("billing")])
    ctx = _ctx_with_view(_view({"billing": None}))
    assert isinstance(await a.run(ctx), TailCall)

    b_model = _emit_once_then_text(_handoff_part("triage", "back to you", call_id="h2"))
    b = Agent("billing", subscribe_topics="billing.in", model_client=b_model, peers=[Handoff("triage")])
    b_ctx = _ctx_with_view(_view({"triage": None}), state=ctx.state)
    assert isinstance(await b.run(b_ctx), TailCall)  # billing hands BACK to triage

    a2_ctx = _ctx_with_view(_view({"billing": None}), state=b_ctx.state)
    result = await a.run(a2_ctx)
    assert a_model._test_calls["n"] == 2  # exactly one clean re-entry model call
    assert isinstance(result, ReturnCall)
    assert "FRESH" in "".join(getattr(p, "text", "") for p in result.value)
