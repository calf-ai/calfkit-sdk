"""Capability control-plane wire model (spec §3.2).

A :class:`CapabilityRecord` is one MCP Toolbox instance's advertisement on the
capability control-plane topic: dispatch topic and tool definitions, keyed on the
wire by ``toolbox_id`` × ``worker_id`` (the control-plane instance key, held by the
substrate — never in the value). Records are calfkit-owned models — never the
vendored ``ToolDefinition`` — because compacted records outlive deploys, so the
wire shape must not move when the vendored library does.

Reader policy: tolerant (unknown fields ignored — a newer writer may add fields).
Staleness and newer-``schema_version`` filtering are owned by the reader's
:class:`~calfkit.controlplane.ControlPlaneView` (it hides such records and logs the
skip), so the resolver here only maps a live record to tool bindings.
"""

from __future__ import annotations

from typing import Any, Protocol

from pydantic import AwareDatetime, BaseModel, ConfigDict

from calfkit._vendor.pydantic_ai.tools import ToolDefinition
from calfkit.controlplane import ControlPlaneRecord
from calfkit.models.tool_dispatch import SelectorResult as SelectorResult  # re-export: resolution API lives here
from calfkit.models.tool_dispatch import ToolBinding

CAPABILITY_SCHEMA_VERSION = 1
"""The record schema major this reader understands. Bump only on breaking
wire changes; additive fields ride on the tolerant reader instead."""

CAPABILITY_TOPIC = "calf.capabilities"
"""The cluster-wide compacted control-plane topic toolboxes advertise to.

A control-plane topic (not MCP-specific transport): one record schema per topic.
Both the ``@advertises`` factory and the reader's ``ControlPlaneView`` bind to it."""


class CapabilityToolDef(BaseModel):
    """The minimal tool surface an LLM needs — name, description, JSON schema."""

    model_config = ConfigDict(extra="ignore")

    name: str
    description: str | None = None
    parameters_json_schema: dict[str, Any]


class CapabilityRecord(ControlPlaneRecord):
    """One toolbox instance's current advertisement on the capability control plane.

    A :class:`~calfkit.controlplane.ControlPlaneRecord` (identity ``toolbox_id`` ×
    ``worker_id`` is the wire key; the worker stamps ``started_at`` /
    ``last_heartbeat_at`` / ``heartbeat_interval``) plus the capability content: the
    dispatch topic and the tool list. ``content_updated_at`` advances only when
    ``tools`` actually changes — separate from the liveness heartbeat — so a consumer
    can tell "instance alive" from "tool list fresh" (the CRITICAL-3 split).

    Inherits the base's tolerant, frozen ``model_config`` (``extra="ignore"``); the
    value object is rebuilt per heartbeat tick, never mutated.
    """

    schema_version: int = CAPABILITY_SCHEMA_VERSION
    dispatch_topic: str
    tools: list[CapabilityToolDef]
    content_updated_at: AwareDatetime


def record_to_bindings(record: CapabilityRecord) -> list[ToolBinding]:
    """Expand a record into validator-less :class:`ToolBinding`s.

    Wire-crossing tools dispatch unvalidated (the schema-only carve-out): the
    toolbox's MCP server is the argument validator of record.
    """
    return [
        ToolBinding(
            tool_def=ToolDefinition(
                name=tool.name,
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
    """The minimal read surface the resolver needs: ``get(toolbox_id)``.

    Satisfied by a ``ControlPlaneView[CapabilityRecord]`` (production) and by a
    plain ``dict`` (tests), so the agent layer needs no ktables import. The view
    already hides stale and newer-schema records, so a returned record is live
    and supported.
    """

    def get(self, toolbox_id: str) -> CapabilityRecord | None: ...


def resolve_capability(
    view: CapabilityLookup,
    toolbox_id: str,
    *,
    include: tuple[str, ...] | None = None,
) -> SelectorResult:
    """Resolve ``toolbox_id`` (optionally filtered) against the Capability View.

    Public API: this is the resolution kernel behind ``MCPToolbox`` and
    ``MCPToolboxNode.resolve_tools``; custom ``ToolSelector`` implementations may
    call it directly. The view owns staleness + schema-version filtering, so this
    only maps a present record to bindings.

    Never raises on bad records: the tolerant reader admits shapes that fail
    binding expansion (e.g. empty ``dispatch_topic``); those surface as
    ``invalid_record`` so a poisoned advertisement cannot crash a turn.
    """
    record = view.get(toolbox_id)
    if record is None:
        return SelectorResult(toolbox_id=toolbox_id, missing_toolbox=True)
    try:
        bindings = record_to_bindings(record)
    except Exception:
        return SelectorResult(toolbox_id=toolbox_id, invalid_record=True)
    missing: tuple[str, ...] = ()
    if include is not None:
        wanted = set(include)
        bindings = [b for b in bindings if b.name in wanted]
        missing = tuple(name for name in include if name not in {b.name for b in bindings})
    return SelectorResult(toolbox_id=toolbox_id, bindings=bindings, missing_tools=missing)
