from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field
from pydantic.json_schema import SkipJsonSchema
from typing_extensions import Self

from calfkit._vendor.pydantic_ai.messages import ToolCallPart
from calfkit._vendor.pydantic_ai.tools import ToolDefinition

if TYPE_CHECKING:
    # Type-only: the Capability View the resolver consumes lives a layer up
    # (it references CapabilityRecord). A runtime import would cycle; this does not.
    from calfkit.models.capability import EnumerableCapabilityView

ArgsValidator = Callable[[dict[str, Any]], Any]
"""Validates LLM-emitted tool args pre-dispatch; raises ``pydantic.ValidationError`` on mismatch.

Two implementations satisfy this contract: a local tool node's signature-built validator, and the
advertised-schema validator :func:`~calfkit.models.args_schema.schema_args_validator` builds for a
wire-crossing binding. The latter translates jsonschema's violations into the *same*
``pydantic.ValidationError``, so the agent's dispatch loop stays blind to which one ran."""


class ToolBinding(BaseModel):
    """The agent-facing contract for one callable tool: what to advertise to the
    LLM and where to dispatch the call.

    This is the provisioning-time half of the agent↔tool dispatch contract
    (:class:`ToolCallRef` below is the invocation-time half). It is exactly the
    surface ``BaseAgentNodeDef.run`` consumes per tool — nothing about the
    providing node (deployment topology, lifecycle, subscribe topics) leaks in,
    so one node may contribute many bindings (a native toolbox, an MCP toolbox) or one
    (function tool node) without any shared base class.

    ``validator`` is process-local
    and excluded from serialization, so a deserialized binding always carries
    ``validator=None``. Such a binding is not unvalidated — at dispatch the agent
    validates its args against the advertised ``tool_def.parameters_json_schema``
    (:func:`~calfkit.models.args_schema.schema_args_validator`); the ``validator``
    field only distinguishes a local *signature* validator from that schema fallback.
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
    """A local tool node's signature-built validator, or ``None`` for a wire-crossing binding
    (discovered, MCP) — in which case the agent falls back to a validator built from
    ``tool_def.parameters_json_schema`` at dispatch. Excluded from serialization and JSON schema:
    callables never ride the wire."""

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
    """Outcome of resolving one tool selector against the capability view.

    Cardinality-neutral: a selector may resolve one target (a tool node), one target
    with many tools (an MCP toolbox), or many targets (a ``Tools`` handle). Carries the
    bindings plus structured diagnostics so the agent owns the warn/degrade policy in one
    place and tests assert on data, not log text. A frozen value object — every field is
    immutable (``bindings`` is a tuple), so equal results compare and hash equal.

    The :class:`ControlPlaneView` owns staleness + schema-version filtering, so the only
    ways a selection is ``unresolved`` are: an absent target, an ``include``-pinned tool
    missing from a present record (MCP ``include`` only), a present-but-unexpandable
    record, or a present record of the wrong node kind (the over-pull guard).
    """

    bindings: tuple[ToolBinding, ...] = ()
    missing_targets: tuple[str, ...] = ()  # requested identities with no live record
    missing_tools: tuple[str, ...] = ()  # include-pinned tool names absent from a present record (MCP include only)
    invalid_targets: tuple[str, ...] = ()  # identities whose record was present but failed binding expansion
    wrong_kind_targets: tuple[str, ...] = ()  # identities present but of the wrong node_kind for this selector

    @property
    def unresolved(self) -> bool:
        """True when anything the selector asked for could not be delivered."""
        return bool(self.missing_targets or self.missing_tools or self.invalid_targets or self.wrong_kind_targets)


@runtime_checkable
class ToolSelector(Protocol):
    """Deferred tool declaration, resolved per turn against the Capability View.

    Implemented by :class:`~calfkit.nodes.toolbox.Toolboxes` (the family selector agents
    hold for toolboxes), the hosting :class:`~calfkit.mcp.mcp_toolbox.MCPToolboxNode`
    (which calls the same kernel), and by
    :class:`~calfkit.nodes.tool.Tools` for function tool nodes: passing any of them
    to an agent extracts only a lookup key — no session contact, no deployment. The
    ``view`` is an :class:`~calfkit.models.capability.EnumerableCapabilityView` (``get``
    + ``snapshot``), so the agent layer needs no ktables import. A by-name selector uses
    only ``get(target_id)`` (so plain ``dict``s suffice in its tests); a discover selector
    enumerates via ``snapshot()``.
    """

    def resolve_tools(self, view: "EnumerableCapabilityView") -> SelectorResult: ...


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

    Raw :class:`ToolBinding` entries pass through verbatim (tests,
    hand-rolled bindings with no node object in hand); anything satisfying
    :class:`ToolProvider` contributes ``tool_bindings()``, so one provider may
    yield many bindings (a native toolbox, an MCP toolbox). The ``isinstance`` protocol check
    is structural — it only proves a ``tool_bindings`` attribute exists — which
    is why the unmatched arm is a hard ``TypeError`` rather than a skip.
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
