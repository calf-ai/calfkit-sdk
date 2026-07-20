"""Call-side toolbox selection: the ``Toolboxes`` family selector and its ``Toolbox`` entry spec.

The toolbox counterpart of :class:`~calfkit.nodes.tool.Tools` (spec:
``docs/designs/toolbox-discovery-spec.md``, ADR-0045): ``Toolboxes`` declares an agent's toolbox
surface — named entries XOR discover — and ``Toolbox`` is the frozen per-box entry carrying the
``include=`` trust boundary. Agents never speak MCP; they hold these identity-only values and the
deployed :class:`~calfkit.mcp.mcp_toolbox.MCPToolboxNode` services the calls.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from calfkit._handle_names import normalize_toolbox_entries
from calfkit.models.capability import (
    EnumerableCapabilityView,
    SelectorResult,
    resolve_all_capabilities,
    resolve_capability,
)
from calfkit.models.tool_dispatch import ToolBinding


@dataclass(frozen=True)
class Toolbox:
    """Identity-only entry spec naming one toolbox inside a :class:`Toolboxes` selector.

    A name plus optional ``include=`` scoping — the static per-toolbox trust boundary (bare
    server-side tool names; a server growing its tool list cannot enlarge the agent's surface).
    ``include=()`` is legal and means *explicit exclusion*: the box is bound with zero tools.

    Deliberately **not** a selector: it exists only as an entry, and passing one directly in
    ``tools=[...]`` raises a teaching error (wrap it in ``Toolboxes(...)``). ``MCPToolbox`` is a
    code-level alias of this class for MCP-flavored call sites, not a distinct concept.
    """

    name: str
    include: tuple[str, ...] | None = None

    # Hand-written so the advertised parameter type is ``Sequence`` (spec D3) while the stored
    # field stays a tuple — the dataclass-generated ``__init__`` would bind the parameter to the
    # field annotation and wrongly reject ``include=["a"]`` under type checking (family pattern:
    # ``Tools``/``Toolboxes`` also hand-write their ``__init__``).
    def __init__(self, name: str, include: Sequence[str] | None = None) -> None:
        if isinstance(include, str):
            raise ValueError("Toolbox include= must be a sequence of tool names, not a bare string")
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "include", tuple(include) if include is not None else None)
        if not self.name:
            raise ValueError("Toolbox name must be non-empty")


def _desugar(entry: object) -> Toolbox:
    if isinstance(entry, str):
        return Toolbox(entry)
    if isinstance(entry, Toolbox):
        return entry
    raise TypeError(f"Toolboxes entries must be toolbox names or Toolbox specs, got {type(entry).__name__}")


@dataclass(frozen=True)
class Toolboxes:
    """The call-side family selector declaring an agent's toolbox surface, passed in ``tools=[...]``.

    Two modes, exactly one (the family's discover-XOR-items rail, shared with
    ``Tools``/``Messaging``/``Handoff``):

    - **named** — ``Toolboxes("github", "slack")`` or mixed entries
      ``Toolboxes(Toolbox("github", include=("create_issue",)), "jira")``: bare names desugar to
      :class:`Toolbox` at construction, so the canonical form is always a tuple of entries.
      Duplicate toolbox names raise — an entry carries policy (``include=``), so a duplicate is a
      conflict, never the name rail's silent dedupe.
    - **discover** — ``Toolboxes(discover=True)``: every live toolbox, resolved fresh each turn.
      The exclusive author of the agent's toolbox surface (no other ``Toolboxes`` handle and no
      eager toolbox node may accompany it).

    Frozen value semantics: equal declarations compare and hash equal.
    """

    entries: tuple[Toolbox, ...]
    discover: bool = False

    # ``*positional`` varargs (the common case) plus a keyword-only ``entries=`` list — the
    # element-descriptive mirror of the siblings' ``names=``/``.names`` shape.
    def __init__(self, *positional: str | Toolbox, entries: Sequence[str | Toolbox] | None = None, discover: bool = False) -> None:
        object.__setattr__(
            self,
            "entries",
            normalize_toolbox_entries("Toolboxes", positional=positional, entries=entries, discover=discover, desugar=_desugar),
        )
        object.__setattr__(self, "discover", discover)

    def resolve_tools(self, view: EnumerableCapabilityView) -> SelectorResult:
        """Resolve against the capability view.

        Discover mode binds every live toolbox (``resolve_all_capabilities``); named mode
        resolves each entry (``resolve_capability`` with the entry's ``include``), concatenating
        all five ``SelectorResult`` fields — including ``missing_tools``, which named ``Tools``
        can never populate (it has no ``include=``) but a ``Toolbox`` entry's ``include=`` can.
        """
        if self.discover:
            return resolve_all_capabilities(view, node_kind="toolbox")
        bindings: list[ToolBinding] = []
        missing: list[str] = []
        missing_tools: list[str] = []
        invalid: list[str] = []
        wrong_kind: list[str] = []
        for entry in self.entries:
            result = resolve_capability(view, entry.name, include=entry.include, expected_kind="toolbox")
            bindings.extend(result.bindings)
            missing.extend(result.missing_targets)
            missing_tools.extend(result.missing_tools)
            invalid.extend(result.invalid_targets)
            wrong_kind.extend(result.wrong_kind_targets)
        return SelectorResult(
            bindings=tuple(bindings),
            missing_targets=tuple(missing),
            missing_tools=tuple(missing_tools),
            invalid_targets=tuple(invalid),
            wrong_kind_targets=tuple(wrong_kind),
        )
