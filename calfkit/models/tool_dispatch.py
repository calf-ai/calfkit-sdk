from collections.abc import Callable, Sequence
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field
from pydantic.json_schema import SkipJsonSchema
from typing_extensions import Self

from calfkit._vendor.pydantic_ai.messages import ToolCallPart
from calfkit._vendor.pydantic_ai.tools import ToolDefinition

ArgsValidator = Callable[[dict[str, Any]], Any]
"""Validates LLM-emitted tool args pre-dispatch; raises ``pydantic.ValidationError`` on mismatch."""


class ToolBinding(BaseModel):
    """The agent-facing contract for one callable tool: what to advertise to the
    LLM and where to dispatch the call.

    This is the provisioning-time half of the agent↔tool dispatch contract
    (:class:`ToolCallRef` below is the invocation-time half). It is exactly the
    surface ``BaseAgentNodeDef.run`` consumes per tool — nothing about the
    providing node (deployment topology, lifecycle, subscribe topics) leaks in,
    so one node may contribute many bindings (a native toolbox, an MCP toolbox) or one
    (function tool node) without any shared base class.

    Doubles as the wire model for per-run tool overrides
    (``OverridesState.override_agent_tools``): ``validator`` is process-local
    and excluded from serialization, so a deserialized binding always carries
    ``validator=None`` and dispatches unvalidated — the schema-only carve-out,
    enforced by the type instead of by convention.
    """

    model_config = ConfigDict(frozen=True)

    tool_def: ToolDefinition
    """Advertised to the LLM verbatim; ``tool_def.name`` is the registry key the
    model's tool calls are matched against."""
    # min_length: an empty topic would make the binding undispatchable — fail
    # at construction, not at the first tool call.
    dispatch_topic: str = Field(min_length=1)
    """Target topic for the ``Call`` carrying the :class:`ToolCallRef`."""
    validator: SkipJsonSchema[ArgsValidator | None] = Field(default=None, exclude=True)
    """Validates args before dispatch; ``None`` dispatches unvalidated (e.g. a
    schema-only override binding, or an MCP tool where the server validates).
    Excluded from serialization and JSON schema: callables never ride the wire."""

    @property
    def name(self) -> str:
        return self.tool_def.name


@runtime_checkable
class ToolProvider(Protocol):
    """Structural contract for anything that contributes tools to an agent.

    One provider may yield many bindings (a native toolbox, an MCP toolbox) or one (function
    tool node). ``runtime_checkable`` checks only that ``tool_bindings`` exists —
    the distinctive method name is what keeps unrelated objects from
    duck-typing in accidentally.
    """

    def tool_bindings(self) -> Sequence[ToolBinding]: ...


def normalize_tool_bindings(tools: Sequence[ToolProvider | ToolBinding] | None) -> list[ToolBinding]:
    """Flatten a mix of raw bindings and providers into a binding list.

    Raw :class:`ToolBinding` entries pass through verbatim (overrides, tests,
    hand-rolled bindings with no node object in hand); anything satisfying
    :class:`ToolProvider` contributes ``tool_bindings()``, so one provider may
    yield many bindings (a native toolbox, an MCP toolbox). The ``isinstance`` protocol check
    is structural — it only proves a ``tool_bindings`` attribute exists — which
    is why the unmatched arm is a hard ``TypeError`` rather than a skip.

    Shared by ``Agent(tools=...)`` and the client's ``tool_overrides=`` so both
    accept the same surface.
    """
    out: list[ToolBinding] = []
    for t in tools or ():
        if isinstance(t, ToolBinding):
            out.append(t)
        elif isinstance(t, ToolProvider):
            out.extend(t.tool_bindings())
        else:
            raise TypeError(f"tools must be ToolBinding or ToolProvider instances, got {type(t).__name__}: {t!r}")
    return out


class ToolCallRef(BaseModel):
    """The per-invocation reference handed to a tool node: which tool call it must service.

    A tool invocation cannot recover its own ``tool_call_id`` from ``ctx.state``
    alone — in parallel mode every fanned-out ``Call`` carries a deep copy of the
    same state holding *all* pending tool calls — so the id must be passed in.

    ``extra="forbid"`` makes this a *closed* envelope: because the tool node's
    handler route is the universal ``'*'``, the schema is the only discriminator
    that stops a foreign routeless body from being mis-consumed by a tool node.

    Note: ``tool_call_id`` is intentionally **not** ``min_length``-constrained. An
    empty id is a defended-against edge case (the tool node falls back to a sentinel
    ``FailedToolCall`` marker, see ``ToolNodeDef.run``); constraining it here would
    make that defensive path unreachable via this channel.
    """

    model_config = ConfigDict(extra="forbid")
    tool_call_id: str
    args: dict[str, Any]
    name: str

    @classmethod
    def from_tool_call_part(cls, tool_call_part: ToolCallPart) -> Self:
        return cls(tool_call_id=tool_call_part.tool_call_id, args=tool_call_part.args_as_dict(), name=tool_call_part.tool_name)
