"""S2a: the reserved ``handoff_to_agent`` tool — construction reservation + always-inject
(handoff-tool-transport-spec §2/§3.0).

A ``Handoff`` handle injects the runtime-rendered ``handoff_to_agent`` def into the
external toolset every turn (empty directory → the "none reachable" sentinel body) and
reserves the name against user tools; a handle-less agent reserves nothing and injects
nothing (spec §3.0 gating — its user tool of that name is never intercepted).
"""

from __future__ import annotations

from typing import Any

import pytest

from calfkit._vendor.pydantic_ai.messages import ModelResponse
from calfkit._vendor.pydantic_ai.messages import TextPart as ModelTextPart
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit._vendor.pydantic_ai.models.test import TestModel
from calfkit.nodes.tool import Tools
from calfkit.peers import Handoff, Messaging
from calfkit.peers.directory import _NONE_REACHABLE
from calfkit.peers.handoff import _HANDOFF_TOOL_PREAMBLE, HANDOFF_TOOL
from tests._peer_fakes import agents_view as _view
from tests._peer_fakes import ctx_with_view as _ctx_with_view
from tests._peer_fakes import triage_agent as _agent
from tests._peer_fakes import user_tool as _user_tool


def _capturing_model(captured: dict[str, Any]) -> FunctionModel:
    def _fn(messages: list[Any], info: AgentInfo) -> ModelResponse:
        captured["tools"] = {t.name: t for t in info.function_tools}
        return ModelResponse(parts=[ModelTextPart("ok")])

    return FunctionModel(_fn)


# --------------------------------------------------------------------------- #
# Injection (spec §2)                                                          #
# --------------------------------------------------------------------------- #


async def test_handoff_tool_injected_when_handle_present() -> None:
    captured: dict[str, Any] = {}
    agent = _agent(_capturing_model(captured), peers=[Handoff("billing")])
    await agent.run(_ctx_with_view(_view({"billing": "Billing questions."})))
    tool = captured["tools"][HANDOFF_TOOL]
    assert tool.description.startswith(_HANDOFF_TOOL_PREAMBLE)
    assert "billing — Billing questions." in tool.description


async def test_handoff_tool_always_injected_with_sentinel_when_none_live() -> None:
    """Spec §2: no omit-plus-note — the tool rides every turn; an empty directory renders
    the sentinel body so the model keeps the capability."""
    captured: dict[str, Any] = {}
    agent = _agent(_capturing_model(captured), peers=[Handoff("billing")])
    await agent.run(_ctx_with_view(_view({})))
    assert captured["tools"][HANDOFF_TOOL].description == _HANDOFF_TOOL_PREAMBLE + _NONE_REACHABLE


async def test_no_handoff_handle_injects_nothing() -> None:
    captured: dict[str, Any] = {}
    agent = _agent(_capturing_model(captured), peers=[Messaging("billing")])
    await agent.run(_ctx_with_view(_view({"billing": None})))
    assert HANDOFF_TOOL not in captured["tools"]


async def test_directory_scoped_to_handoff_handles_not_messaging() -> None:
    """Per-capability independence: a discover Messaging handle does not widen the handoff
    directory."""
    captured: dict[str, Any] = {}
    agent = _agent(_capturing_model(captured), peers=[Messaging(discover=True), Handoff("refunds")])
    await agent.run(_ctx_with_view(_view({"billing": None, "refunds": None})))
    desc = captured["tools"][HANDOFF_TOOL].description
    assert "refunds" in desc
    assert "billing" not in desc


async def test_discover_directory_excludes_self() -> None:
    captured: dict[str, Any] = {}
    agent = _agent(_capturing_model(captured), peers=[Handoff(discover=True)])  # agent is "triage"
    await agent.run(_ctx_with_view(_view({"billing": None, "refunds": None, "triage": "me"})))
    desc = captured["tools"][HANDOFF_TOOL].description
    assert "billing" in desc and "refunds" in desc
    assert "triage" not in desc


async def test_directory_re_rendered_every_invocation() -> None:
    captured: dict[str, Any] = {}
    agent = _agent(_capturing_model(captured), peers=[Handoff(discover=True)])
    await agent.run(_ctx_with_view(_view({"billing": None})))
    first = captured["tools"][HANDOFF_TOOL].description
    await agent.run(_ctx_with_view(_view({"billing": None, "refunds": None})))  # a peer JOINED
    assert captured["tools"][HANDOFF_TOOL].description != first


# --------------------------------------------------------------------------- #
# Reservation (spec §2/§3.0)                                                   #
# --------------------------------------------------------------------------- #


def test_handoff_name_reserved_against_user_tool() -> None:
    with pytest.raises(ValueError, match="reserved"):
        _agent(TestModel(), tools=[_user_tool(HANDOFF_TOOL)], peers=[Handoff("billing")])


def test_both_names_reserved_when_both_handle_kinds_present() -> None:
    both = [Messaging("billing"), Handoff("refunds")]
    with pytest.raises(ValueError, match="reserved"):
        _agent(TestModel(), tools=[_user_tool("message_agent")], peers=both)
    with pytest.raises(ValueError, match="reserved"):
        _agent(TestModel(), tools=[_user_tool(HANDOFF_TOOL)], peers=both)


def test_handoff_name_unreserved_without_a_handoff_handle() -> None:
    """Spec §3.0: reservation (and the fork it protects) is per-handle-kind — a handle-less
    agent may own a user tool named ``handoff_to_agent`` and it stays an ordinary tool."""
    agent = _agent(TestModel(), tools=[_user_tool(HANDOFF_TOOL)])
    assert HANDOFF_TOOL in {b.name for b in agent.tools}


def test_reserved_names_also_guard_the_tools_selector_arm() -> None:
    """The reservation gate's SECOND arm (review round 1): named `Tools` selectors are
    checked too — a NAMED selector carrying a reserved name must not construct, for either
    built-in, or the §3.0 fork would hijack it at runtime. (`Tools(discover=True)`
    resolution remains the explicitly deferred gap, agent.py's gate comment.)"""
    with pytest.raises(ValueError, match="reserved"):
        _agent(TestModel(), tools=[Tools(HANDOFF_TOOL)], peers=[Handoff("billing")])
    with pytest.raises(ValueError, match="reserved"):
        _agent(TestModel(), tools=[Tools("message_agent")], peers=[Messaging("billing")])
