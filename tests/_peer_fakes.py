"""Shared peer-capability test fakes (the `_capability_fakes.py` house pattern).

The agents-view fake + ctx builder + common fixtures for the Messaging/Handoff test
suites — one home instead of a per-file copy (which had already forked into a static
and a mutable variant before consolidation)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from calfkit._vendor.pydantic_ai.messages import ToolCallPart
from calfkit._vendor.pydantic_ai.tools import ToolDefinition
from calfkit.models.agents import AGENTS_VIEW_RESOURCE_KEY
from calfkit.models.state import State
from calfkit.models.tool_dispatch import ToolBinding
from calfkit.nodes import Agent
from calfkit.peers.handoff import HANDOFF_TOOL
from tests.test_tool_errors import _make_ctx


class MutableAgentsView:
    """A duck-typed agents view whose snapshot can change between reads — covers both the
    static case and peers joining/leaving mid-turn (the render→arbitration staleness race)."""

    def __init__(self, cards: dict[str, str | None]) -> None:
        self._cards = cards

    def snapshot(self) -> dict[str, Any]:
        return {n: SimpleNamespace(description=d) for n, d in self._cards.items()}

    def set(self, cards: dict[str, str | None]) -> None:
        self._cards = cards


def agents_view(cards: dict[str, str | None]) -> MutableAgentsView:
    return MutableAgentsView(cards)


def ctx_with_view(view: object, state: State | None = None) -> Any:
    ctx = _make_ctx(state if state is not None else State())
    ctx._resources = {AGENTS_VIEW_RESOURCE_KEY: view}
    ctx._ancestor_callers = frozenset()
    return ctx


def triage_agent(model: Any, **kw: Any) -> Agent[Any]:
    return Agent("triage", subscribe_topics="triage.in", model_client=model, **kw)


def user_tool(name: str) -> ToolBinding:
    return ToolBinding(
        dispatch_topic=f"{name}.in",
        tool_def=ToolDefinition(name=name, description="x", parameters_json_schema={"type": "object", "properties": {}}),
    )


def handoff_part(name: str, message: str, call_id: str = "h1") -> ToolCallPart:
    return ToolCallPart(tool_name=HANDOFF_TOOL, args={"name": name, "message": message}, tool_call_id=call_id)
