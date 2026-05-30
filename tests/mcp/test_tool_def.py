"""Unit tests for ``calfkit.mcp._tool_def``.

Coverage focus:
- ``McpToolDef.from_mcp_tool`` correctly translates camelCase SDK fields
  to snake_case Python fields.
- ``McpToolAnnotations.from_mcp`` correctly translates annotation hints.
- Convenience properties (``read_only``, ``destructive``, etc.) apply the
  MCP spec defaults (destructive=True, open_world=True) when annotations
  are absent or partially populated.
- ``to_dict()`` round-trip is JSON-safe and omits ``None``.
"""

from __future__ import annotations

import json

import pytest
from mcp.types import Tool as McpSdkTool
from mcp.types import ToolAnnotations as McpSdkAnnotations

from calfkit.mcp._tool_def import McpToolAnnotations, McpToolDef

# ---------------------------------------------------------------------------
# from_mcp_tool / from_mcp adapters
# ---------------------------------------------------------------------------


def test_from_mcp_tool_minimal() -> None:
    """A tool with only required fields produces a valid McpToolDef."""
    sdk_tool = McpSdkTool(
        name="search",
        inputSchema={"type": "object", "properties": {"q": {"type": "string"}}},
    )
    td = McpToolDef.from_mcp_tool(sdk_tool)

    assert td.name == "search"
    assert td.description is None
    assert td.input_schema == {"type": "object", "properties": {"q": {"type": "string"}}}
    assert td.output_schema is None
    assert td.title is None
    assert td.annotations is None
    assert td.meta is None


def test_from_mcp_tool_full() -> None:
    """All optional fields propagate through the adapter."""
    sdk_ann = McpSdkAnnotations(
        title="Display Title",
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    )
    sdk_tool = McpSdkTool(
        name="send",
        title="Send Email",
        description="Sends an email",
        inputSchema={"type": "object", "required": ["to"]},
        outputSchema={"type": "object", "properties": {"message_id": {"type": "string"}}},
        annotations=sdk_ann,
        _meta={"version": "2"},
    )
    td = McpToolDef.from_mcp_tool(sdk_tool)

    assert td.name == "send"
    assert td.title == "Send Email"
    assert td.description == "Sends an email"
    assert td.input_schema == {"type": "object", "required": ["to"]}
    assert td.output_schema == {"type": "object", "properties": {"message_id": {"type": "string"}}}
    assert td.meta == {"version": "2"}

    # Annotations were translated to snake_case
    assert td.annotations is not None
    assert td.annotations.title == "Display Title"
    assert td.annotations.read_only_hint is True
    assert td.annotations.destructive_hint is False
    assert td.annotations.idempotent_hint is True
    assert td.annotations.open_world_hint is False


def test_annotations_from_mcp_none() -> None:
    """from_mcp(None) returns None (no wrapper instance)."""
    assert McpToolAnnotations.from_mcp(None) is None


def test_annotations_partial_propagation() -> None:
    """Only the explicitly set hint should be non-None in the result."""
    sdk_ann = McpSdkAnnotations(readOnlyHint=True)
    ann = McpToolAnnotations.from_mcp(sdk_ann)
    assert ann is not None
    assert ann.read_only_hint is True
    assert ann.destructive_hint is None
    assert ann.idempotent_hint is None
    assert ann.open_world_hint is None
    assert ann.title is None


# ---------------------------------------------------------------------------
# Convenience properties (MCP spec defaults)
# ---------------------------------------------------------------------------


def test_defaults_when_annotations_absent() -> None:
    """A tool with no annotations gets the MCP spec's conservative defaults.

    Spec: destructive=True, open_world=True, read_only=False, idempotent=False.
    """
    td = McpToolDef(name="t", input_schema={"type": "object"})
    assert td.read_only is False
    assert td.destructive is True  # spec default — conservative
    assert td.idempotent is False
    assert td.open_world is True  # spec default — conservative


def test_defaults_when_annotation_field_absent() -> None:
    """An annotation object with some hints set leaves the unset hints at the defaults."""
    td = McpToolDef(
        name="t",
        input_schema={"type": "object"},
        annotations=McpToolAnnotations(read_only_hint=True),
    )
    assert td.read_only is True  # explicit
    assert td.destructive is True  # spec default (unset)
    assert td.idempotent is False  # spec default (unset)
    assert td.open_world is True  # spec default (unset)


def test_explicit_overrides_defaults() -> None:
    """Explicit annotation values override the defaults."""
    td = McpToolDef(
        name="t",
        input_schema={"type": "object"},
        annotations=McpToolAnnotations(
            read_only_hint=True,
            destructive_hint=False,
            idempotent_hint=True,
            open_world_hint=False,
        ),
    )
    assert td.read_only is True
    assert td.destructive is False
    assert td.idempotent is True
    assert td.open_world is False


# ---------------------------------------------------------------------------
# to_dict / serialization
# ---------------------------------------------------------------------------


def test_to_dict_minimal_omits_none() -> None:
    """Minimal McpToolDef serialises to a compact dict with no None entries."""
    td = McpToolDef(name="t", input_schema={"type": "object"})
    d = td.to_dict()
    assert d == {"name": "t", "input_schema": {"type": "object"}}
    # JSON-safe round trip
    json.dumps(d)


def test_to_dict_full_includes_all_set_fields() -> None:
    td = McpToolDef(
        name="t",
        title="T",
        description="desc",
        input_schema={"type": "object"},
        output_schema={"type": "object", "properties": {"r": {"type": "string"}}},
        annotations=McpToolAnnotations(read_only_hint=True, destructive_hint=False),
        meta={"k": "v"},
    )
    d = td.to_dict()
    assert d == {
        "name": "t",
        "title": "T",
        "description": "desc",
        "input_schema": {"type": "object"},
        "output_schema": {"type": "object", "properties": {"r": {"type": "string"}}},
        "annotations": {"read_only_hint": True, "destructive_hint": False},
        "meta": {"k": "v"},
    }
    json.dumps(d)


def test_to_dict_omits_empty_annotations() -> None:
    """An ``McpToolAnnotations()`` with all-None fields should not surface as
    an empty ``annotations: {}`` blob in the output — that would be noise.
    """
    td = McpToolDef(
        name="t",
        input_schema={"type": "object"},
        annotations=McpToolAnnotations(),
    )
    d = td.to_dict()
    assert "annotations" not in d


# ---------------------------------------------------------------------------
# Immutability
# ---------------------------------------------------------------------------


def test_frozen_dataclass() -> None:
    """McpToolDef and McpToolAnnotations are frozen — mutation raises."""
    td = McpToolDef(name="t", input_schema={"type": "object"})
    with pytest.raises(Exception):  # FrozenInstanceError (dataclasses)
        td.name = "other"  # type: ignore[misc]

    ann = McpToolAnnotations(read_only_hint=True)
    with pytest.raises(Exception):
        ann.read_only_hint = False  # type: ignore[misc]
