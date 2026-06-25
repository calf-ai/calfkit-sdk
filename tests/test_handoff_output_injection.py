"""PR-C Item 2 (part B): wiring the ``HandoffRequest`` union into the model run (§5.3).

``_handoff_output_override`` computes, from the live Handoff-scoped directory, the per-run ``output_type``
override + the empty-set ephemeral instruction. With >=1 live in-scope peer it returns
``[final_output_type, <HandoffRequest subclass>, DeferredToolRequests]`` so the model MAY hand off; with a
``Handoff`` handle but NO live peer it OMITS the member (an empty ``Literal`` is unbuildable) and returns
the "no agents online" note, which ``run()`` composes into the request-level ``instructions`` (self-heals
when a peer comes online). A non-handoff agent's run is unchanged.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, get_args

from calfkit._vendor.pydantic_ai import DeferredToolRequests
from calfkit._vendor.pydantic_ai.messages import ModelResponse
from calfkit._vendor.pydantic_ai.messages import TextPart as ModelTextPart
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit._vendor.pydantic_ai.models.test import TestModel
from calfkit.models.agents import AGENTS_VIEW_RESOURCE_KEY
from calfkit.models.state import State
from calfkit.nodes import Agent
from calfkit.peers import Handoff, Messaging
from calfkit.peers.handoff import _HANDOFF_NO_PEERS_NOTE, HandoffRequest
from tests.test_tool_errors import _make_ctx


def _view(cards: dict[str, str | None]) -> object:
    snap = {name: SimpleNamespace(description=desc) for name, desc in cards.items()}
    return SimpleNamespace(snapshot=lambda: snap)


def _ctx_with_view(view: object) -> Any:
    ctx = _make_ctx(State())
    ctx._resources = {AGENTS_VIEW_RESOURCE_KEY: view}
    ctx._ancestor_callers = frozenset()
    return ctx


def _agent(model: Any, **kw: Any) -> Agent[str]:
    return Agent("triage", subscribe_topics="triage.in", model_client=model, **kw)


def test_override_offers_handoff_when_a_peer_is_live() -> None:
    agent = _agent(TestModel(), peers=[Handoff("billing")])
    output_type, note = agent._handoff_output_override(_ctx_with_view(_view({"billing": "Billing."})))
    assert note is None
    assert output_type is not None
    # The override REPLACES the construction-time type, so it must carry the FULL list (dropping
    # DeferredToolRequests would break tool dispatch).
    assert agent.final_output_type in output_type
    assert DeferredToolRequests in output_type
    member = next(t for t in output_type if isinstance(t, type) and issubclass(t, HandoffRequest))
    assert member is not HandoffRequest  # a per-turn subclass over the live Literal, not the base


def test_override_omits_member_and_notes_when_no_peer_live() -> None:
    agent = _agent(TestModel(), peers=[Handoff("billing")])
    output_type, note = agent._handoff_output_override(_ctx_with_view(_view({})))  # billing offline
    assert output_type is None  # member omitted -> no override -> construction-time default
    assert note == _HANDOFF_NO_PEERS_NOTE


def test_override_is_noop_without_a_handoff_handle() -> None:
    agent = _agent(TestModel(), peers=[Messaging("billing")])
    assert agent._handoff_output_override(_ctx_with_view(_view({"billing": None}))) == (None, None)


async def test_empty_live_set_composes_ephemeral_instruction_into_the_run() -> None:
    captured: dict[str, Any] = {}

    def _capture(messages: list[Any], info: AgentInfo) -> ModelResponse:
        captured["instructions"] = " | ".join(getattr(m, "instructions", None) or "" for m in messages)
        captured["output_tools"] = [t.name for t in info.output_tools]
        return ModelResponse(parts=[ModelTextPart("ok")])

    agent = _agent(FunctionModel(_capture), peers=[Handoff("billing")])
    await agent.run(_ctx_with_view(_view({})))  # no peer live
    assert _HANDOFF_NO_PEERS_NOTE in str(captured["instructions"])  # the note rode request-level instructions
    assert not any("HandoffRequest" in name for name in captured["output_tools"])  # member omitted


async def test_live_peer_does_not_inject_the_note() -> None:
    captured: dict[str, Any] = {}

    def _capture(messages: list[Any], info: AgentInfo) -> ModelResponse:
        captured["instructions"] = " | ".join(getattr(m, "instructions", None) or "" for m in messages)
        return ModelResponse(parts=[ModelTextPart("ok")])

    agent = _agent(FunctionModel(_capture), peers=[Handoff("billing")])
    await agent.run(_ctx_with_view(_view({"billing": "Billing."})))  # peer live -> member offered, no note
    assert _HANDOFF_NO_PEERS_NOTE not in str(captured["instructions"])


# ── the directory the model SEES is live + rebuilt EVERY invocation (a join/leave changes it) ──


def _literal_names(member: type) -> tuple[str, ...]:
    """The names in the per-turn HandoffRequest subclass's ``name: Literal[...]`` field."""
    return get_args(member.model_fields["name"].annotation)


async def test_model_receives_the_live_directory_names_and_descriptions() -> None:
    # The agent SEES the live names+descriptions: the handoff output member's model-facing description (its
    # __doc__) carries the rendered directory. Symmetric to the empty-case `output_tools` absence assertion.
    captured: dict[str, Any] = {}

    def _capture(messages: list[Any], info: AgentInfo) -> ModelResponse:
        captured["descs"] = " | ".join(t.description or "" for t in info.output_tools)
        return ModelResponse(parts=[ModelTextPart("ok")])

    agent = _agent(FunctionModel(_capture), peers=[Handoff(discover=True)])  # agent is "triage"
    await agent.run(_ctx_with_view(_view({"billing": "Billing questions.", "refunds": "Refund handling."})))
    assert "billing — Billing questions." in captured["descs"]  # the model sees the live name + description
    assert "refunds — Refund handling." in captured["descs"]


def _offered_names(agent: Agent[Any], cards: dict[str, str | None]) -> tuple[str, ...]:
    out, _ = agent._handoff_output_override(_ctx_with_view(_view(cards)))
    return _literal_names(next(t for t in out if isinstance(t, type) and issubclass(t, HandoffRequest)))


def test_directory_is_re_resolved_every_invocation() -> None:
    # Rebuilt fresh each turn: a peer joining/leaving between invocations changes what the SAME agent is
    # offered. `_handoff_output_override` reads the live view every call (no agent-level caching) — if that
    # were cached, a join/leave would silently stop updating and this test would catch it. (Each call reads
    # its own ctx's view, simulating the live view changing across invocations.)
    agent = _agent(TestModel(), peers=[Handoff(discover=True)])
    assert _offered_names(agent, {"billing": None}) == ("billing",)
    assert _offered_names(agent, {"billing": None, "refunds": None}) == ("billing", "refunds")  # a peer JOINED
    assert _offered_names(agent, {"refunds": None}) == ("refunds",)  # billing LEFT


def test_discover_handoff_offers_all_live_minus_self() -> None:
    # M1: discover-mode resolves to every live agent minus self, at the override level (not just the directory).
    agent = _agent(TestModel(), peers=[Handoff(discover=True)])  # agent is "triage"
    out, note = agent._handoff_output_override(_ctx_with_view(_view({"billing": None, "refunds": None, "triage": "me"})))
    assert note is None
    member = next(t for t in out if isinstance(t, type) and issubclass(t, HandoffRequest))
    assert _literal_names(member) == ("billing", "refunds")  # self (triage) excluded


def test_handoff_surface_scoped_to_handoff_handles_not_messaging() -> None:
    # M2: per-capability independence at the RESOLVED surface — a discover Messaging handle does NOT widen the
    # handoff surface. With Messaging(discover=True) + Handoff("refunds"), the handoff Literal is "refunds"
    # only, even though "billing" is messageable-via-discover.
    agent = _agent(TestModel(), peers=[Messaging(discover=True), Handoff("refunds")])
    out, _ = agent._handoff_output_override(_ctx_with_view(_view({"billing": None, "refunds": None})))
    member = next(t for t in out if isinstance(t, type) and issubclass(t, HandoffRequest))
    assert _literal_names(member) == ("refunds",)  # billing is messageable but NOT a handoff target
