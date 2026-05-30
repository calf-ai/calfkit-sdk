"""User-facing MCP tool schema dataclass — the codegen target.

``McpToolDef`` is the type that ``calfkit mcp codegen`` writes into Python
modules and that ``McpServer(tools=[...])`` accepts. It carries all the
schema information a calfkit agent needs to advertise an MCP tool to the
LLM, without depending on a live MCP server connection at runtime.

Design notes:

- **snake_case Python attributes**. The vendored MCP SDK (``mcp 1.27+``)
  exposes the same fields in camelCase (``inputSchema``, ``readOnlyHint``);
  we translate at the boundary via :meth:`McpToolDef.from_mcp_tool` so the
  user-facing surface follows Python conventions and matches calfkit's
  internal style.

- **Annotations are wrapped, not flattened**. MCP's spec adds new annotation
  fields over time (e.g. ``returnDirect`` proposed for a future revision);
  carrying ``McpToolAnnotations`` as its own type keeps schema evolution
  isolated. Convenience boolean properties (``read_only``, ``destructive``,
  etc.) apply the MCP spec's default semantics — see
  ``docs/mcp-adaptor-design.md`` §4.3.

- **Frozen dataclasses**. v1 treats schemas as immutable artifacts from
  codegen; mutation is not part of the user contract. Future filter /
  rename machinery (Phase 2) produces new instances via ``dataclasses.replace``.

See ``docs/mcp-v1-plan.md`` §6.1 for the codegen workflow and
``docs/mcp-adaptor-implementation-plan.md`` §5 for class-design context.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from mcp.types import Tool as McpSdkTool
    from mcp.types import ToolAnnotations as McpSdkAnnotations


# MCP spec defaults for the four annotation hints (see design doc §4.3):
# - read_only_hint defaults to False (i.e. tools are assumed to modify state)
# - destructive_hint defaults to True (assume non-additive change)
# - idempotent_hint defaults to False (assume re-running is not safe)
# - open_world_hint defaults to True (assume external/untrusted reach)
_ANNOTATION_DEFAULTS: dict[str, bool] = {
    "read_only_hint": False,
    "destructive_hint": True,
    "idempotent_hint": False,
    "open_world_hint": True,
}


@dataclass(frozen=True)
class McpToolAnnotations:
    """Snake-case mirror of ``mcp.types.ToolAnnotations``.

    All fields default to ``None`` (annotation absent). Use the convenience
    properties on :class:`McpToolDef` to read effective values with the MCP
    spec's defaults applied.
    """

    title: str | None = None
    read_only_hint: bool | None = None
    destructive_hint: bool | None = None
    idempotent_hint: bool | None = None
    open_world_hint: bool | None = None

    @classmethod
    def from_mcp(cls, ann: McpSdkAnnotations | None) -> McpToolAnnotations | None:
        if ann is None:
            return None
        return cls(
            title=ann.title,
            read_only_hint=ann.readOnlyHint,
            destructive_hint=ann.destructiveHint,
            idempotent_hint=ann.idempotentHint,
            open_world_hint=ann.openWorldHint,
        )

    def to_dict(self) -> dict[str, Any]:
        """JSON-safe dict for serialization (e.g. codegen output).

        Omits keys whose values are ``None`` so the generated code stays
        compact and the diff is stable under codegen reruns when an
        upstream MCP server adds optional annotation fields.
        """
        out: dict[str, Any] = {}
        if self.title is not None:
            out["title"] = self.title
        if self.read_only_hint is not None:
            out["read_only_hint"] = self.read_only_hint
        if self.destructive_hint is not None:
            out["destructive_hint"] = self.destructive_hint
        if self.idempotent_hint is not None:
            out["idempotent_hint"] = self.idempotent_hint
        if self.open_world_hint is not None:
            out["open_world_hint"] = self.open_world_hint
        return out


@dataclass(frozen=True)
class McpToolDef:
    """Schema descriptor for one MCP tool — what codegen produces.

    Equivalent to ``mcp.types.Tool`` in information content, but with
    snake_case fields and a frozen-dataclass shape suitable for committing
    to source via ``calfkit mcp codegen``. Consumers:

    - :class:`calfkit.mcp.McpServer` accepts a ``list[McpToolDef]`` as
      ``tools=`` and yields one ``BaseToolNodeSchema`` per entry.
    - The Phase 3 ``McpBridge`` reads the ``name`` to dispatch tool calls
      to the right MCP method on the shared session.
    """

    name: str
    description: str | None = None
    input_schema: dict[str, Any] = field(default_factory=lambda: {"type": "object", "properties": {}})
    output_schema: dict[str, Any] | None = None
    title: str | None = None
    annotations: McpToolAnnotations | None = None
    meta: dict[str, Any] | None = None

    @classmethod
    def from_mcp_tool(cls, tool: McpSdkTool) -> McpToolDef:
        """Adapt an ``mcp.types.Tool`` (camelCase SDK type) into the
        snake-case user-facing type. Used by both ``McpSession.list_tools``
        (Phase 1) and the codegen CLI (Phase 5).
        """
        return cls(
            name=tool.name,
            title=tool.title,
            description=tool.description,
            input_schema=tool.inputSchema,
            output_schema=tool.outputSchema,
            annotations=McpToolAnnotations.from_mcp(tool.annotations),
            meta=tool.meta,
        )

    # ----- Convenience accessors that apply MCP spec defaults -----

    @property
    def read_only(self) -> bool:
        """Whether the tool is annotated read-only. Spec default: ``False``."""
        if self.annotations is None or self.annotations.read_only_hint is None:
            return _ANNOTATION_DEFAULTS["read_only_hint"]
        return self.annotations.read_only_hint

    @property
    def destructive(self) -> bool:
        """Whether the tool is annotated destructive. **Spec default: ``True``**.

        The conservative default means tools without explicit annotations
        are treated as potentially destructive — important for downstream
        consumers (e.g. confirmation prompts) that must not assume safety.
        """
        if self.annotations is None or self.annotations.destructive_hint is None:
            return _ANNOTATION_DEFAULTS["destructive_hint"]
        return self.annotations.destructive_hint

    @property
    def idempotent(self) -> bool:
        """Whether the tool is annotated idempotent. Spec default: ``False``.

        Used by the Phase 3 dedup cache to decide whether redelivery is
        safe to short-circuit (idempotent) or must be served from cache
        (non-idempotent).
        """
        if self.annotations is None or self.annotations.idempotent_hint is None:
            return _ANNOTATION_DEFAULTS["idempotent_hint"]
        return self.annotations.idempotent_hint

    @property
    def open_world(self) -> bool:
        """Whether the tool reaches an open world (external services).
        **Spec default: ``True``**.
        """
        if self.annotations is None or self.annotations.open_world_hint is None:
            return _ANNOTATION_DEFAULTS["open_world_hint"]
        return self.annotations.open_world_hint

    def to_dict(self) -> dict[str, Any]:
        """JSON-safe dict for codegen output / caching. Omits ``None`` fields
        so generated code stays compact and diff-stable.
        """
        out: dict[str, Any] = {"name": self.name, "input_schema": self.input_schema}
        if self.title is not None:
            out["title"] = self.title
        if self.description is not None:
            out["description"] = self.description
        if self.output_schema is not None:
            out["output_schema"] = self.output_schema
        if self.annotations is not None:
            ann_dict = self.annotations.to_dict()
            if ann_dict:
                out["annotations"] = ann_dict
        if self.meta is not None:
            out["meta"] = self.meta
        return out
