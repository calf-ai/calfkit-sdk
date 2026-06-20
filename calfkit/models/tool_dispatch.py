from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field
from pydantic.json_schema import SkipJsonSchema
from typing_extensions import Self

from calfkit._vendor.pydantic_ai.messages import ToolCallPart
from calfkit._vendor.pydantic_ai.tools import ToolDefinition

if TYPE_CHECKING:
    # Type-only: the Capability View lookup the resolver consumes lives a layer up
    # (it references CapabilityRecord). A runtime import would cycle; this does not.
    from calfkit.models.capability import CapabilityLookup

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


@dataclass(frozen=True)
class SelectorResult:
    """Outcome of resolving one MCP tool selector against the Capability View.

    Carries the bindings plus structured diagnostics so the agent owns the
    warn/degrade policy in one place and tests assert on data, not log text.

    Staleness and schema-version filtering are the :class:`ControlPlaneView`'s
    job now (it hides stale/newer-schema records), so the only ways a selection
    is ``unresolved`` are a missing toolbox, missing requested tools, or a
    record that fails binding expansion.
    """

    toolbox_id: str
    bindings: list[ToolBinding] = field(default_factory=list)
    missing_toolbox: bool = False
    missing_tools: tuple[str, ...] = ()
    invalid_record: bool = False

    @property
    def unresolved(self) -> bool:
        """True when anything the selector asked for could not be delivered."""
        return self.missing_toolbox or bool(self.missing_tools) or self.invalid_record


@runtime_checkable
class ToolSelector(Protocol):
    """Deferred tool declaration, resolved per turn against the Capability View.

    Implemented by :class:`~calfkit.mcp.mcp_toolbox.MCPToolbox` (the handle
    agents hold, including ``select()`` results) and by the hosting
    :class:`~calfkit.mcp.mcp_toolbox.MCPToolboxNode`, which delegates to it:
    passing either to an agent extracts only a lookup key — no session contact,
    no deployment. The ``view`` is a :class:`~calfkit.models.capability.CapabilityLookup`
    (anything with ``get(toolbox_id) -> CapabilityRecord | None``), so the agent layer
    needs no ktables import and tests can use plain dicts.
    """

    def resolve_tools(self, view: "CapabilityLookup") -> SelectorResult: ...


def split_tool_declarations(
    tools: Sequence["ToolProvider | ToolBinding | ToolSelector"] | None,
) -> tuple[list[ToolBinding], list[ToolSelector]]:
    """Partition ``tools=`` into immediate bindings and deferred selectors.

    Selector-ness is checked BEFORE provider-ness so a selector type that also
    grew a ``tool_bindings`` attribute could never be mistakenly expanded at
    construction time.
    """
    bindings: list[ToolBinding] = []
    selectors: list[ToolSelector] = []
    for t in tools or ():
        if isinstance(t, ToolBinding):
            bindings.append(t)
        elif isinstance(t, ToolSelector):
            selectors.append(t)
        elif isinstance(t, ToolProvider):
            bindings.extend(t.tool_bindings())
        else:
            raise TypeError(f"agent tools must be ToolBinding, ToolProvider, or ToolSelector instances, got {type(t).__name__}: {t!r}")
    return bindings, selectors


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

    Note: ``tool_call_id`` is intentionally **not** ``min_length``-constrained — a
    rejecting constraint here would poison inbound decode of an otherwise-routable body
    (the same reason the reply models clamp rather than reject). An empty id is a
    tolerated edge; the agent mints non-empty ``tool_call_id``s in practice.
    """

    model_config = ConfigDict(extra="forbid")
    tool_call_id: str
    args: dict[str, Any]
    name: str

    @classmethod
    def from_tool_call_part(cls, tool_call_part: ToolCallPart) -> Self:
        return cls(tool_call_id=tool_call_part.tool_call_id, args=tool_call_part.args_as_dict(), name=tool_call_part.tool_name)
