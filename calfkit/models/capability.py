"""Capability control-plane wire model (spec §3.2).

A :class:`CapabilityRecord` is one MCP Toolbox's advertisement on the
capability topic: identity, dispatch topic, and tool definitions, keyed on the
wire by ``toolbox_id``. Records are calfkit-owned models — never the vendored
``ToolDefinition`` — because compacted records outlive deploys, so the wire
shape must not move when the vendored library does.

Reader policy: tolerant (unknown fields ignored — a newer writer may add
fields), and records with a newer ``schema_version`` decode fine but must be
*skipped with a log* by the consumer; :func:`is_unsupported_schema` is that
policy's predicate.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import AwareDatetime, BaseModel, ConfigDict

from calfkit._vendor.pydantic_ai.tools import ToolDefinition
from calfkit.models.tool_dispatch import SelectorResult as SelectorResult  # re-export: resolution API lives here
from calfkit.models.tool_dispatch import ToolBinding

CAPABILITY_SCHEMA_VERSION = 1
"""The record schema major this reader understands. Bump only on breaking
wire changes; additive fields ride on the tolerant reader instead."""


class CapabilityToolDef(BaseModel):
    """The minimal tool surface an LLM needs — name, description, JSON schema."""

    model_config = ConfigDict(extra="ignore")

    name: str
    description: str | None = None
    parameters_json_schema: dict[str, Any]


class CapabilityRecord(BaseModel):
    """One toolbox's current advertisement; superseded whole by its next publish."""

    model_config = ConfigDict(extra="ignore")

    schema_version: int = CAPABILITY_SCHEMA_VERSION
    toolbox_id: str
    dispatch_topic: str
    tools: list[CapabilityToolDef]
    published_at: AwareDatetime


def is_unsupported_schema(record: CapabilityRecord) -> bool:
    """True when ``record`` was written by a newer schema major than this
    reader understands — the consumer must skip it and log, not act on it."""
    return record.schema_version > CAPABILITY_SCHEMA_VERSION


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


CAPABILITY_VIEW_RESOURCE_KEY = "calfkit.mcp.capability_view"
"""Worker resource-bag key under which the Capability View (a
``Mapping[str, CapabilityRecord]``) is published to hosted nodes."""


def resolve_capability(
    view: Any,
    toolbox_id: str,
    *,
    include: tuple[str, ...] | None = None,
    strict: bool = False,
) -> SelectorResult:
    """Resolve ``toolbox_id`` (optionally filtered) against the view.

    Public API: this is the resolution kernel behind ``MCPToolbox`` and
    ``MCPToolboxNode.resolve_tools``; custom ``ToolSelector`` implementations may
    call it directly.

    Never raises on bad records: the tolerant reader admits shapes that fail
    binding expansion (e.g. empty ``dispatch_topic``); those surface as
    ``invalid_record`` so a poisoned advertisement cannot crash a turn.
    """
    record = view.get(toolbox_id)
    if record is None:
        return SelectorResult(toolbox_id=toolbox_id, strict=strict, missing_toolbox=True)
    if is_unsupported_schema(record):
        return SelectorResult(toolbox_id=toolbox_id, strict=strict, skipped_newer_schema=True)
    try:
        bindings = record_to_bindings(record)
    except Exception:
        return SelectorResult(toolbox_id=toolbox_id, strict=strict, invalid_record=True)
    missing: tuple[str, ...] = ()
    if include is not None:
        wanted = set(include)
        bindings = [b for b in bindings if b.name in wanted]
        missing = tuple(name for name in include if name not in {b.name for b in bindings})
    stale = max(0.0, (datetime.now(tz=timezone.utc) - record.published_at).total_seconds())
    return SelectorResult(
        toolbox_id=toolbox_id,
        strict=strict,
        bindings=bindings,
        missing_tools=missing,
        stale_seconds=stale,
    )
