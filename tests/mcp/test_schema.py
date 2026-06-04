"""Unit tests for ``calfkit.mcp.mcp_json_schema``.

Coverage focus:
- The emitted schema is a JSON-Schema draft 2020-12 document with the
  expected top-level shape (``$schema``, ``title``, ``required``).
- The ``mcpServers`` map's ``additionalProperties`` is an ``anyOf`` of the
  two server-config ``$ref``s, with both definitions present in ``$defs``.
- The schema is permissive: it carries NO ``"additionalProperties": false``
  anywhere (drop-in extras allowed) and NO ``$id`` key.
- Rendering is deterministic so a CLI ``--check`` cannot flap.
"""

from __future__ import annotations

import json
from pathlib import Path

from calfkit.mcp import mcp_json_schema


def test_schema_top_level_shape() -> None:
    schema = mcp_json_schema()
    assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert isinstance(schema["title"], str) and schema["title"]
    assert schema["type"] == "object"
    assert schema["required"] == ["mcpServers"]


def test_schema_server_union_is_anyof_of_two_refs() -> None:
    schema = mcp_json_schema()
    additional = schema["properties"]["mcpServers"]["additionalProperties"]
    any_of = additional["anyOf"]
    assert isinstance(any_of, list)
    assert len(any_of) == 2
    refs = {entry["$ref"] for entry in any_of}
    assert refs == {"#/$defs/StdioServerConfig", "#/$defs/HttpServerConfig"}


def test_schema_defs_contain_both_models() -> None:
    schema = mcp_json_schema()
    defs = schema["$defs"]
    assert "StdioServerConfig" in defs
    assert "HttpServerConfig" in defs


def test_schema_has_no_id_key() -> None:
    assert "$id" not in mcp_json_schema()


def test_schema_has_no_additional_properties_false() -> None:
    """Permissive doc-schema: never forbids unknown keys (drop-in leniency)."""
    rendered = json.dumps(mcp_json_schema())
    assert '"additionalProperties": false' not in rendered
    assert '"additionalProperties":false' not in rendered


def test_schema_render_is_deterministic() -> None:
    first = json.dumps(mcp_json_schema(), indent=2)
    second = json.dumps(mcp_json_schema(), indent=2)
    assert first == second


def test_committed_schema_file_matches_generator() -> None:
    """The committed ``mcp.schema.json`` must equal the generator's exact output.

    Pins the serialization form (``indent=2`` + trailing newline) that the CLI
    writes and CI's ``calfkit mcp schema --check`` depends on, so drift is
    caught locally rather than only in CI.
    """
    committed = Path(__file__).resolve().parents[2] / "calfkit" / "mcp" / "mcp.schema.json"
    expected = json.dumps(mcp_json_schema(), indent=2) + "\n"
    assert committed.read_text(encoding="utf-8") == expected
