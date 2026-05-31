"""Unit tests for ``calfkit.mcp._codegen`` renderer.

Pure tests — no MCP SDK / no I/O. Verifies deterministic output, edge
cases in identifier conversion, and JSON-safe value rendering.
"""

from __future__ import annotations

import ast
import datetime

from calfkit.mcp._codegen import (
    _render_value,
    _to_class_name,
    _to_constant_name,
    diff_modules,
    render_module,
)
from calfkit.mcp._tool_def import McpToolAnnotations, McpToolDef


def _td(name: str, **kwargs: object) -> McpToolDef:
    """Minimal McpToolDef helper with a valid input schema by default."""
    kwargs.setdefault("input_schema", {"type": "object", "properties": {}})
    return McpToolDef(name=name, **kwargs)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Identifier conversion — _to_constant_name
# ---------------------------------------------------------------------------


def test_constant_name_simple() -> None:
    assert _to_constant_name("search") == "SEARCH"


def test_constant_name_hyphen() -> None:
    assert _to_constant_name("list-tools") == "LIST_TOOLS"


def test_constant_name_dot() -> None:
    assert _to_constant_name("gmail.send") == "GMAIL_SEND"


def test_constant_name_underscore_preserved() -> None:
    assert _to_constant_name("my_tool") == "MY_TOOL"


def test_constant_name_mixed_specials() -> None:
    assert _to_constant_name("a.b-c_d") == "A_B_C_D"


def test_constant_name_leading_digit_gets_underscore_prefix() -> None:
    """Python identifiers cannot start with a digit."""
    assert _to_constant_name("12bad") == "_12BAD"


def test_constant_name_empty_falls_back() -> None:
    assert _to_constant_name("") == "_TOOL"


def test_constant_name_only_specials_falls_back() -> None:
    """All-special string collapses to nothing → fallback."""
    assert _to_constant_name("---") == "_TOOL"


def test_constant_name_runs_of_specials_collapse() -> None:
    """Multiple consecutive specials become a single underscore."""
    assert _to_constant_name("a---b") == "A_B"


# ---------------------------------------------------------------------------
# Identifier conversion — _to_class_name
# ---------------------------------------------------------------------------


def test_class_name_simple() -> None:
    assert _to_class_name("gmail") == "Gmail"


def test_class_name_hyphen() -> None:
    assert _to_class_name("my-server") == "MyServer"


def test_class_name_dot() -> None:
    assert _to_class_name("my.srv.v2") == "MySrvV2"


def test_class_name_mixed_specials() -> None:
    assert _to_class_name("server-everything") == "ServerEverything"


def test_class_name_leading_digit_gets_prefix() -> None:
    # The function only PascalCases when there's a separator to split on;
    # ``12srv`` has none, so we get ``_12srv`` (digit-prefix added, casing
    # preserved). Pin the exact value so the docstring + behaviour stay
    # aligned.
    assert _to_class_name("12srv") == "_12srv"


def test_class_name_leading_digit_with_separator_pascal_cases_parts() -> None:
    assert _to_class_name("12-srv") == "_12Srv"


def test_class_name_empty_falls_back() -> None:
    assert _to_class_name("") == "_Server"


def test_class_name_preserves_single_char_segments() -> None:
    assert _to_class_name("a-b-c") == "ABC"


# ---------------------------------------------------------------------------
# Value rendering — primitives
# ---------------------------------------------------------------------------


def test_render_string() -> None:
    assert _render_value("hello") == "'hello'"


def test_render_int() -> None:
    assert _render_value(42) == "42"


def test_render_bool() -> None:
    """Python bool renders as True/False — not JSON true/false."""
    assert _render_value(True) == "True"
    assert _render_value(False) == "False"


def test_render_none() -> None:
    assert _render_value(None) == "None"


def test_render_empty_dict() -> None:
    assert _render_value({}) == "{}"


def test_render_empty_list() -> None:
    assert _render_value([]) == "[]"


def test_render_simple_dict() -> None:
    out = _render_value({"k": 1})
    assert "'k': 1" in out
    assert out.startswith("{")
    assert out.endswith("}")


def test_render_nested_dict_is_valid_python() -> None:
    """Output must parse as valid Python source — round-trip via ast.literal_eval (safe)."""
    val = {"outer": {"inner": [1, 2, {"deep": True}]}}
    rendered = _render_value(val)
    # ast.literal_eval is safe (only accepts literal nodes); no eval() call.
    assert ast.literal_eval(rendered) == val


# ---------------------------------------------------------------------------
# render_module — happy paths
# ---------------------------------------------------------------------------


_FIXED_TIME = datetime.datetime(2026, 5, 30, 12, 0, 0, tzinfo=datetime.timezone.utc)


def test_render_module_minimal() -> None:
    out = render_module(server_name="x", tools=[], generated_at=_FIXED_TIME)
    # Output starts with the file-level ruff exemption so generated files
    # don't trip line-length lint on long tool descriptions / JSON schemas.
    assert out.startswith("# ruff: noqa: E501")
    # Has the banner and class even with zero tools
    assert "DO NOT EDIT BY HAND" in out
    assert "class X:" in out
    assert "ALL: list[McpToolDef] = []" in out
    # No-tools docstring noted
    assert "No tools declared" in out


def test_render_module_single_tool() -> None:
    out = render_module(
        server_name="gmail",
        tools=[_td("search", description="Search emails")],
        generated_at=_FIXED_TIME,
    )
    # Tool has no annotations → import is narrowed to McpToolDef only
    assert "from calfkit.mcp import McpToolDef" in out
    assert "McpToolAnnotations" not in out
    assert "SEARCH = McpToolDef(" in out
    assert "name='search'" in out
    assert "description='Search emails'" in out
    assert "class Gmail:" in out
    assert "SEARCH = SEARCH" in out
    # ALL list is rendered one-per-line for diff friendliness
    assert "ALL: list[McpToolDef] = [\n        SEARCH,\n    ]" in out


def test_render_module_multiple_tools_sorted() -> None:
    """Tools are sorted by name for deterministic output (CI drift detection)."""
    out = render_module(
        server_name="gmail",
        tools=[_td("send"), _td("archive"), _td("search")],
        generated_at=_FIXED_TIME,
    )
    # Find positions of each constant in the output
    pos_archive = out.find("ARCHIVE = McpToolDef(")
    pos_search = out.find("SEARCH = McpToolDef(")
    pos_send = out.find("SEND = McpToolDef(")
    assert pos_archive < pos_search < pos_send
    # ALL list (one per line) also reflects sorted order
    assert "ALL: list[McpToolDef] = [\n        ARCHIVE,\n        SEARCH,\n        SEND,\n    ]" in out


def test_render_module_deterministic_across_invocations() -> None:
    """Same inputs → byte-identical output (required for --check drift)."""
    tools = [_td("search"), _td("send")]
    out1 = render_module(server_name="gmail", tools=tools, generated_at=_FIXED_TIME)
    out2 = render_module(server_name="gmail", tools=list(reversed(tools)), generated_at=_FIXED_TIME)
    assert out1 == out2


def test_render_module_omits_none_fields() -> None:
    """Optional fields default to None and should not appear in the output."""
    out = render_module(
        server_name="x",
        tools=[_td("t")],
        generated_at=_FIXED_TIME,
    )
    assert "output_schema=" not in out
    assert "title=" not in out
    assert "annotations=" not in out
    assert "meta=" not in out


def test_render_module_includes_annotations_when_set() -> None:
    out = render_module(
        server_name="x",
        tools=[
            _td(
                "t",
                annotations=McpToolAnnotations(read_only_hint=True, destructive_hint=False),
            )
        ],
        generated_at=_FIXED_TIME,
    )
    assert "annotations=McpToolAnnotations(read_only_hint=True, destructive_hint=False)" in out


def test_render_module_includes_output_schema_when_set() -> None:
    out = render_module(
        server_name="x",
        tools=[_td("t", output_schema={"type": "object", "properties": {"r": {"type": "string"}}})],
        generated_at=_FIXED_TIME,
    )
    assert "output_schema={" in out
    assert "'type': 'object'" in out


def test_render_module_source_in_docstring() -> None:
    out = render_module(
        server_name="x",
        tools=[_td("t")],
        source="npx -y @mcp/server-x",
        generated_at=_FIXED_TIME,
    )
    assert "Source: npx -y @mcp/server-x" in out


def test_render_module_no_source_says_not_recorded() -> None:
    out = render_module(server_name="x", tools=[_td("t")], generated_at=_FIXED_TIME)
    assert "Source: (not recorded)" in out


def test_render_module_includes_timestamp() -> None:
    out = render_module(server_name="x", tools=[_td("t")], generated_at=_FIXED_TIME)
    assert "2026-05-30T12:00:00Z" in out


def test_render_module_output_is_valid_python() -> None:
    """The generated module must parse without syntax errors."""
    out = render_module(
        server_name="gmail",
        tools=[
            _td("search", description="find", annotations=McpToolAnnotations(read_only_hint=True)),
            _td("send", input_schema={"type": "object", "required": ["to"], "properties": {"to": {"type": "string"}}}),
        ],
        generated_at=_FIXED_TIME,
    )
    # compile() raises SyntaxError on invalid Python
    compile(out, "<generated>", "exec")


def test_render_module_output_is_importable() -> None:
    """The generated module, when written + imported, exposes the expected names."""
    import sys
    import tempfile
    from importlib import util

    out = render_module(
        server_name="gmail",
        tools=[_td("search"), _td("send")],
        generated_at=_FIXED_TIME,
    )
    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as f:
        f.write(out)
        path = f.name
    try:
        spec = util.spec_from_file_location("gen_test", path)
        assert spec is not None and spec.loader is not None
        module = util.module_from_spec(spec)
        sys.modules["gen_test"] = module
        spec.loader.exec_module(module)
        assert hasattr(module, "SEARCH")
        assert hasattr(module, "SEND")
        assert hasattr(module, "Gmail")
        assert len(module.Gmail.ALL) == 2
        assert {t.name for t in module.Gmail.ALL} == {"search", "send"}
    finally:
        sys.modules.pop("gen_test", None)


# ---------------------------------------------------------------------------
# diff_modules
# ---------------------------------------------------------------------------


def test_diff_modules_identical_returns_empty() -> None:
    assert diff_modules("hello\n", "hello\n") == ""


def test_diff_modules_different_returns_diff() -> None:
    diff = diff_modules(expected="line1\nline2\n", actual="line1\nDIFFERENT\n")
    assert "DIFFERENT" in diff
    assert "line2" in diff
    assert "current" in diff  # file label
    assert "generated" in diff


def test_diff_modules_against_empty() -> None:
    diff = diff_modules(expected="newline\n", actual="")
    assert "newline" in diff
