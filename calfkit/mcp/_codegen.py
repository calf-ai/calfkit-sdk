"""Pure renderer for ``calfkit mcp codegen``.

Takes a server name + list of :class:`McpToolDef` and emits a Python module
that re-instantiates them as importable constants. Designed to be:

- **Deterministic** — same inputs → byte-identical output (sorted tools,
  stable formatting). Critical for ``--check`` drift detection in CI.
- **Self-contained** — generated modules import only from ``calfkit.mcp``
  public surface, no transitive calfkit internals.
- **Trustably re-runnable** — leading "do not edit by hand" banner so PR
  reviewers and IDEs see at a glance that this file is regenerated.

Naming conventions (v1 plan §11 Q9):

- Server name "gmail" → class name ``Gmail`` (capitalize first letter).
- Server name "my-srv.v2" → ``MySrvV2`` (split on non-alphanumeric, title-case each, join).
- Tool name "search" → constant ``SEARCH``.
- Tool name "list-tools" → constant ``LIST_TOOLS``.
- Tool name "12bad" → constant ``_12BAD`` (prefix underscore — Python identifiers
  cannot start with a digit).
"""

from __future__ import annotations

import datetime
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

    from calfkit.mcp._tool_def import McpToolDef


_NON_IDENT_CHAR = re.compile(r"[^A-Za-z0-9]+")
_LEADING_DIGIT = re.compile(r"^(\d)")


# ---------------------------------------------------------------------------
# Identifier conversion
# ---------------------------------------------------------------------------


def _to_constant_name(tool_name: str) -> str:
    """Map an MCP tool name to a valid UPPER_SNAKE_CASE Python constant name.

    Examples:
        "search"     → "SEARCH"
        "list-tools" → "LIST_TOOLS"
        "gmail.send" → "GMAIL_SEND"
        "12bad"      → "_12BAD"   (digit prefix → leading underscore)
        ""           → "_TOOL"    (empty fallback)
    """
    if not tool_name:
        return "_TOOL"
    # Replace any run of non-alphanumeric characters with a single underscore
    cleaned = _NON_IDENT_CHAR.sub("_", tool_name).strip("_")
    if not cleaned:
        return "_TOOL"
    upper = cleaned.upper()
    # Python identifiers cannot start with a digit
    if _LEADING_DIGIT.match(upper):
        upper = "_" + upper
    return upper


def _to_class_name(server_name: str) -> str:
    """Map an MCP server name to a PascalCase Python class name.

    Examples:
        "gmail"       → "Gmail"
        "my-server"   → "MyServer"
        "my_srv.v2"   → "MySrvV2"
        "12srv"       → "_12srv" → "_12Srv"  (prefix _ then PascalCase)
        ""            → "_Server"
    """
    if not server_name:
        return "_Server"
    parts = [p for p in _NON_IDENT_CHAR.split(server_name) if p]
    if not parts:
        return "_Server"
    pascal = "".join(part[:1].upper() + part[1:] for part in parts)
    if _LEADING_DIGIT.match(pascal):
        pascal = "_" + pascal
    return pascal


# ---------------------------------------------------------------------------
# Value rendering
# ---------------------------------------------------------------------------


def _render_value(value: object, indent: int = 0) -> str:
    """Render a JSON-safe Python value as source code.

    Uses ``repr()`` for primitives (which produces valid Python source for
    str/int/float/bool/None) and recurses into dict/list with controlled
    indentation. Empty collections render compactly.
    """
    pad = "    " * indent
    inner_pad = "    " * (indent + 1)

    if isinstance(value, dict):
        if not value:
            return "{}"
        items = []
        for k, v in value.items():
            items.append(f"{inner_pad}{_render_value(k)}: {_render_value(v, indent + 1)}")
        return "{\n" + ",\n".join(items) + ",\n" + pad + "}"
    if isinstance(value, list):
        if not value:
            return "[]"
        items = [f"{inner_pad}{_render_value(v, indent + 1)}" for v in value]
        return "[\n" + ",\n".join(items) + ",\n" + pad + "]"
    # Primitives: str / int / float / bool / None all repr() correctly to
    # valid Python source.
    return repr(value)


def _render_annotations(td: McpToolDef) -> str | None:
    """Render the ``annotations=`` kwarg, or None if no annotation fields are set."""
    ann = td.annotations
    if ann is None:
        return None
    kwargs: list[str] = []
    if ann.title is not None:
        kwargs.append(f"title={ann.title!r}")
    if ann.read_only_hint is not None:
        kwargs.append(f"read_only_hint={ann.read_only_hint!r}")
    if ann.destructive_hint is not None:
        kwargs.append(f"destructive_hint={ann.destructive_hint!r}")
    if ann.idempotent_hint is not None:
        kwargs.append(f"idempotent_hint={ann.idempotent_hint!r}")
    if ann.open_world_hint is not None:
        kwargs.append(f"open_world_hint={ann.open_world_hint!r}")
    if not kwargs:
        return None
    return "McpToolAnnotations(" + ", ".join(kwargs) + ")"


def _render_tool_def(td: McpToolDef, *, constant_name: str) -> str:
    """Render one ``McpToolDef`` as a constant assignment statement.

    Fields are emitted in declared order: name, description, input_schema,
    output_schema, title, annotations, meta. Optional fields are omitted
    when None / empty to keep diffs tight.
    """
    lines = [f"{constant_name} = McpToolDef("]
    lines.append(f"    name={td.name!r},")
    if td.description is not None:
        lines.append(f"    description={td.description!r},")
    lines.append(f"    input_schema={_render_value(td.input_schema, indent=1)},")
    if td.output_schema is not None:
        lines.append(f"    output_schema={_render_value(td.output_schema, indent=1)},")
    if td.title is not None:
        lines.append(f"    title={td.title!r},")
    ann_str = _render_annotations(td)
    if ann_str is not None:
        lines.append(f"    annotations={ann_str},")
    if td.meta is not None and td.meta:
        lines.append(f"    meta={_render_value(td.meta, indent=1)},")
    lines.append(")")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Module rendering
# ---------------------------------------------------------------------------


def render_module(
    *,
    server_name: str,
    tools: Sequence[McpToolDef],
    source: str | None = None,
    generated_at: datetime.datetime | None = None,
) -> str:
    """Render a generated schemas module as a single Python source string.

    Args:
        server_name: Logical MCP server name (becomes the class name).
        tools: Discovered :class:`McpToolDef` instances. Sorted by name in
            the output for deterministic diffs.
        source: Human-readable provenance string (the original CLI command
            or URL). Embedded in the docstring for traceability.
        generated_at: UTC timestamp to embed (defaults to ``utcnow``).
            Parameter exists so tests can pin to a fixed time.

    Returns:
        A complete Python module source string, including the
        ``do not edit by hand`` banner, imports, per-tool constants, and
        the per-server class with ``.ALL`` and per-tool attributes.
    """
    sorted_tools = sorted(tools, key=lambda t: t.name)
    class_name = _to_class_name(server_name)

    ts = (generated_at or datetime.datetime.now(datetime.timezone.utc)).strftime("%Y-%m-%dT%H:%M:%SZ")
    src_line = f"Source: {source}" if source else "Source: (not recorded)"

    lines: list[str] = []
    # Module docstring
    lines.append('"""Generated MCP tool schemas for the `' + server_name + "` server.")
    lines.append("")
    lines.append("DO NOT EDIT BY HAND. Re-run `calfkit mcp codegen` when the upstream MCP")
    lines.append("server changes; the auto-generated content here will be overwritten.")
    lines.append("")
    lines.append(src_line)
    lines.append(f"Generated: {ts}")
    lines.append(f"Tool count: {len(sorted_tools)}")
    lines.append('"""')
    lines.append("")
    lines.append("from __future__ import annotations")
    lines.append("")
    lines.append("from calfkit.mcp import McpToolAnnotations, McpToolDef")
    lines.append("")
    lines.append("")

    # Per-tool constants
    constants_in_order: list[tuple[str, McpToolDef]] = []
    for td in sorted_tools:
        const_name = _to_constant_name(td.name)
        constants_in_order.append((const_name, td))
        lines.append(_render_tool_def(td, constant_name=const_name))
        lines.append("")

    # Per-server class
    lines.append("")
    lines.append(f"class {class_name}:")
    if not constants_in_order:
        lines.append(f'    """No tools declared for the `{server_name}` MCP server."""')
        lines.append("")
        lines.append("    ALL: list[McpToolDef] = []")
    else:
        lines.append(f'    """All tools exposed by the `{server_name}` MCP server."""')
        lines.append("")
        for const_name, _td in constants_in_order:
            lines.append(f"    {const_name} = {const_name}")
        lines.append("")
        all_items = ", ".join(c for c, _ in constants_in_order)
        lines.append(f"    ALL: list[McpToolDef] = [{all_items}]")
    lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Drift detection
# ---------------------------------------------------------------------------


def diff_modules(expected: str, actual: str) -> str:
    """Return a unified diff (string) between expected and actual module source.

    Empty string if identical. Used by the ``--check`` CLI mode to report
    drift without writing.
    """
    import difflib

    if expected == actual:
        return ""
    diff_lines = difflib.unified_diff(
        actual.splitlines(keepends=True),
        expected.splitlines(keepends=True),
        fromfile="current (on disk)",
        tofile="generated (from MCP server)",
        n=3,
    )
    return "".join(diff_lines)
