"""Capability control-plane wire model (spec §3.2).

A :class:`CapabilityRecord` is one advertiser instance's advertisement on the
capability control-plane topic — an MCP toolbox or a function tool node: dispatch
topic and tool definitions, keyed on the wire by ``node_id`` × ``worker_id`` (the
control-plane instance key, held by the substrate — never in the value). Records are
calfkit-owned models — never the vendored ``ToolDefinition`` — because compacted
records outlive deploys, so the wire shape must not move when the vendored library does.

Reader policy: tolerant (unknown fields ignored — a newer writer may add fields).
Staleness and newer-``schema_version`` filtering are owned by the reader's
:class:`~calfkit.controlplane.ControlPlaneView` (it hides such records and logs the
skip), so the resolver here only maps a live record to tool bindings.
"""

from __future__ import annotations

from typing import Any, Protocol

from pydantic import AwareDatetime, BaseModel, ConfigDict, ValidationError

from calfkit._vendor.pydantic_ai.tools import ToolDefinition
from calfkit.controlplane import ControlPlaneRecord
from calfkit.models.tool_dispatch import SelectorResult as SelectorResult  # re-export: resolution API lives here
from calfkit.models.tool_dispatch import ToolBinding

CAPABILITY_SCHEMA_VERSION = 1
"""The record schema major this reader understands. Bump only on breaking
wire changes; additive fields ride on the tolerant reader instead."""

CAPABILITY_TOPIC = "calf.capabilities"
"""The cluster-wide compacted control-plane topic capability advertisers advertise to —
both MCP toolboxes and function tool nodes.

A control-plane topic (not MCP-specific transport): one record schema per topic.
Both the ``@advertises`` factory and the reader's ``ControlPlaneView`` bind to it."""


class CapabilityToolDef(BaseModel):
    """The minimal tool surface an LLM needs — name, description, JSON schema."""

    model_config = ConfigDict(extra="ignore")

    name: str
    description: str | None = None
    parameters_json_schema: dict[str, Any]


class CapabilityRecord(ControlPlaneRecord):
    """One advertiser instance's current advertisement on the capability control plane.

    A :class:`~calfkit.controlplane.ControlPlaneRecord` (identity ``node_id`` ×
    ``worker_id`` is the wire key; the worker stamps ``started_at`` /
    ``last_heartbeat_at`` / ``heartbeat_interval`` / ``node_kind``) plus the capability
    content: the dispatch topic and the tool list. ``content_updated_at`` advances only
    when ``tools`` actually changes — separate from the liveness heartbeat — so liveness
    and content currency stay distinct. It is **advisory**: the substrate cannot
    enforce its monotonicity (the writing node owns the contract; a hand-rolled
    factory could pass ``now()``), and no production reader gates on it yet — it is
    wire surface for future ops/observability consumers.

    **Precondition for the collapsed view:** all replicas of one advertiser (same
    ``node_id``) must advertise *equivalent* content. The view collapses replicas to one
    record by most-recent heartbeat — it does not merge tool sets — so two replicas with
    divergent tool lists would flap the agent's visible surface tick-to-tick. The dispatch
    topic is name-derived and identical across replicas, so routing is never wrong; only
    tool *visibility* would flap. Keep an advertiser's replicas pointed at the same tool
    set — trivially true for a function tool node (its schema is static).

    Inherits the base's tolerant, frozen ``model_config`` (``extra="ignore"``); the
    value object is rebuilt per heartbeat tick, never mutated.
    """

    schema_version: int = CAPABILITY_SCHEMA_VERSION
    dispatch_topic: str
    tools: list[CapabilityToolDef]
    content_updated_at: AwareDatetime


def _namespace_prefix(node_kind: str, name: str) -> str:
    """LLM-facing tool-name prefix for a capability record.

    Toolbox tools are namespaced ``<name>__`` so the cluster tool namespace is
    collision-free (ADR-0018); function tool-node names stay bare to preserve the
    ``node_id == tool name == capability key`` identity (ADR-0013). This is a
    read-time projection: the wire record's ``CapabilityToolDef.name`` stays bare
    (so re-advertising re-projects; no ``schema_version`` bump), and dispatch strips
    it back in ``MCPToolboxNode.run``.
    """
    return f"{name}__" if node_kind == "toolbox" else ""


def record_to_bindings(record: CapabilityRecord, *, name: str) -> list[ToolBinding]:
    """Expand a record into validator-less :class:`ToolBinding`s.

    Validator-less by construction (no local function to build a signature validator from — only
    the advertised schema): the agent validates args at dispatch against
    ``parameters_json_schema`` (a non-coercing subset check), and the advertiser stays
    authoritative on receipt — the toolbox's MCP server, or the tool node's own function schema.

    ``name`` is the advertiser's identity (its ``node_id`` / capability key — the
    control-plane wire key, never a field in the record value), supplied by the caller.
    Toolbox tools are namespaced ``<name>__<tool>`` for the LLM; tool-node names stay
    bare. See :func:`_namespace_prefix`.
    """
    prefix = _namespace_prefix(record.node_kind, name)
    return [
        ToolBinding(
            tool_def=ToolDefinition(
                name=f"{prefix}{tool.name}",
                description=tool.description,
                parameters_json_schema=tool.parameters_json_schema,
            ),
            dispatch_topic=record.dispatch_topic,
        )
        for tool in record.tools
    ]


CAPABILITY_VIEW_RESOURCE_KEY = "calfkit.controlplane.capability_view"
"""Worker resource-bag key under which the Capability View (a
:class:`CapabilityLookup`, in practice a ``ControlPlaneView[CapabilityRecord]``)
is published to hosted nodes."""


class CapabilityLookup(Protocol):
    """The minimal read surface the resolver needs: ``get(target_id)``.

    Satisfied by a ``ControlPlaneView[CapabilityRecord]`` (production) and by a
    plain ``dict`` (tests), so the agent layer needs no ktables import. The view
    already hides stale and newer-schema records, so a returned record is live
    and supported.
    """

    def get(self, target_id: str) -> CapabilityRecord | None: ...


def resolve_capability(
    view: CapabilityLookup,
    target_id: str,
    *,
    include: tuple[str, ...] | None = None,
    expected_kind: str | None = None,
) -> SelectorResult:
    """Resolve ``target_id`` (optionally filtered) against the capability view.

    Public API: this is the resolution kernel behind ``MCPToolbox`` and ``Tools``;
    custom ``ToolSelector`` implementations may call it directly. The view owns
    staleness + schema-version filtering, so this only maps a present record to bindings.

    ``expected_kind`` is the over-pull guard: when given, a present record whose
    ``node_kind`` differs (e.g. a ``Tools`` selector hitting a toolbox record, or vice
    versa) is rejected as ``wrong_kind_targets`` rather than bound.

    Never raises on bad records: the tolerant reader admits shapes that fail binding
    expansion (e.g. empty ``dispatch_topic``); those surface as ``invalid_targets`` so a
    poisoned advertisement cannot crash a turn.
    """
    record = view.get(target_id)
    if record is None:
        return SelectorResult(missing_targets=(target_id,))
    if expected_kind is not None and record.node_kind != expected_kind:
        return SelectorResult(wrong_kind_targets=(target_id,))
    try:
        bindings = record_to_bindings(record, name=target_id)
    except ValidationError:
        # The one tolerant-reader bad-data path: an empty dispatch_topic fails
        # ToolBinding's min_length. A poisoned advert degrades (invalid_targets),
        # not crashes the turn. A NON-validation error here is a logic bug — let
        # it propagate (fail loud) rather than masking it as bad wire data.
        return SelectorResult(invalid_targets=(target_id,))
    missing_tools: tuple[str, ...] = ()
    if include is not None:
        # ``include`` pins BARE server-side tool names (C5): a dev knows the tool as
        # `search`, not `github__search`. Strip the toolbox namespace prefix to compare.
        wanted = set(include)
        prefix = _namespace_prefix(record.node_kind, target_id)
        tagged = [(b.name.removeprefix(prefix), b) for b in bindings]
        present_bare = {bare for bare, _ in tagged}  # bare names this record advertises
        bindings = [b for bare, b in tagged if bare in wanted]
        missing_tools = tuple(n for n in include if n not in present_bare)
    return SelectorResult(bindings=tuple(bindings), missing_tools=missing_tools)


class EnumerableCapabilityView(CapabilityLookup, Protocol):
    """:class:`CapabilityLookup` plus bulk enumeration: ``snapshot() -> {node_id: record}``.

    The discover-mode read surface. Satisfied by a ``ControlPlaneView[CapabilityRecord]``
    (which already exposes ``snapshot()``) and by the tests' ``_FakeView``. The
    single-target path (:func:`resolve_capability`, ``MCPToolbox``) keeps the narrower
    :class:`CapabilityLookup` — it never enumerates (Interface Segregation): point-lookup
    clients must not depend on a ``snapshot()`` they never call.
    """

    def snapshot(self) -> dict[str, CapabilityRecord]: ...


def resolve_all_capabilities(view: EnumerableCapabilityView, *, node_kind: str) -> SelectorResult:
    """Bind every live record of ``node_kind`` (the discover-mode kernel).

    A POSITIVE FILTER, not the over-pull guard: a record of another kind is out of scope
    (skipped), not ``wrong_kind_targets`` — so ``missing_targets`` / ``missing_tools`` /
    ``wrong_kind_targets`` are always empty. Only a poisoned record of the RIGHT kind (e.g.
    an empty ``dispatch_topic`` failing ``ToolBinding``'s ``min_length``) degrades to
    ``invalid_targets``; one bad record never crashes the turn (mirrors
    :func:`resolve_capability`).
    """
    bindings: list[ToolBinding] = []
    invalid: list[str] = []
    for node_id, record in view.snapshot().items():
        if record.node_kind != node_kind:
            continue
        try:
            # ``name`` is required since ADR-0018; for ``node_kind == "tool"`` the prefix is
            # "" so the snapshot key adds no namespace (and it stays correct for any kind).
            bindings.extend(record_to_bindings(record, name=node_id))
        except ValidationError:
            invalid.append(node_id)
    return SelectorResult(bindings=tuple(bindings), invalid_targets=tuple(invalid))
