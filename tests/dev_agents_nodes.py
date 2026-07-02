"""Importable nodes for the ``ck dev`` agent-lifecycle tests (spec §5.1 preflight).

Mirrors ``tests/provisioning_cli_nodes.py``: small, real node definitions the CLI resolves via
``module:attr`` targets. Covers every preflight classification — an agent (advertises on
``calf.agents``), a function tool (``calf.capabilities``), a mixed iterable, a same-named agent in
a second attr (the cross-target duplicate), and a consumer node (advertises on neither plane).
"""

from __future__ import annotations

from calfkit.nodes import ConsumerNode, ToolNodeDef, consumer
from calfkit.nodes.agent import Agent
from calfkit.providers.pydantic_ai.model_client import PydanticModelClient


class _FakeModel(PydanticModelClient):
    """Constructor-only model stand-in: preflight never runs a model."""

    @property
    def model_name(self) -> str:
        return "fake"

    @property
    def system(self) -> str:
        return "fake"

    async def request(self, *args: object, **kwargs: object) -> object:
        raise NotImplementedError


def _agent(name: str) -> Agent:
    return Agent(name, subscribe_topics=f"{name}.in", model_client=_FakeModel())


def _tool(name: str) -> ToolNodeDef:
    def _fn(x: int) -> int:
        return x

    _fn.__name__ = name
    return ToolNodeDef.create_tool_node(func=_fn, subscribe_topics=f"{name}.in", publish_topic=f"{name}.out")


# One agent / one tool — the two advertising kinds.
general = _agent("general")
get_weather = _tool("get_weather")

# A DIFFERENT attr resolving to the SAME advertised name: launching both targets in one
# invocation must be a duplicate-name usage error, never a silent dedupe (spec §5.1).
general_dupe = _agent("general")

# A mixed iterable: one worker co-hosting an agent and a tool (union readiness set).
support_team = [_agent("support"), _tool("get_time")]


@consumer(subscribe_topics="audit.in", name="audit_log")
def audit_log(ctx: object) -> None:
    """A zero-advert node: no presence-plane record, so not a launchable ck dev target."""


assert isinstance(audit_log, ConsumerNode)

# A consumer co-hosted WITH an advertising node: legitimate — the daemon is visible and
# stoppable via the agent's name; only an all-consumer target errors.
desk_with_audit = [_agent("desk"), audit_log]
