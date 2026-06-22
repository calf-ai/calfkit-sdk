---
status: accepted
---

# Open-ended tool discovery rides the `Tools` handle as an exclusive discover mode

ADR-0013 gave agents a by-name handle (`Tools(*names)`) onto function tool nodes, but
left "discover *every* tool node, not a named set" as deferred future work (spec §14.2).
We add that as a **mode on the existing handle** — `Tools(discover=True)` — rather than a
new `AllTools` type or an `Agent` constructor flag. It resolves per turn against the same
`ControlPlaneView[CapabilityRecord]`, binding **only `node_kind == "tool"` records** (it
never absorbs an MCP toolbox's tools), via a new bulk kernel `resolve_all_capabilities`
that walks the view's existing `snapshot()`. Because it is still an ordinary `ToolSelector`,
it trips the existing read-side view-registration trigger (`worker.py` keys on
`_tool_selectors`) with **zero new worker wiring** — which is precisely what the deferred
note feared would "need a new view-registration trigger." Modeling it as a selector rather
than a flag dissolves that.

## The tool-surface contract (enforced at construction)

The agent's `tools=[...]` surface obeys one contract, checked when the agent is built:

1. **No duplicate tool names.** Across every developer-provided source — eager tool nodes,
   eager bindings/providers, and named `Tools(...)` — no tool name may appear twice
   (order-independent). A duplicate is a config mistake and **raises**.
2. **Discover owns the tool-node surface.** If `Tools(discover=True)` is present, no eager
   tool node and no named `Tools(...)` may sit alongside it — discover *is* the agent's
   tool-node surface, so any explicit tool-node reference is redundant with it. **Raises**.
3. **`MCPToolbox` is orthogonal.** A toolbox is `node_kind == "toolbox"`, not a tool, so it
   composes freely with discover *and* with named `Tools`. The narrow rule, not the broad
   "discover must be the sole entry."

This is enforced by **construction-time checks over the raw `tools=` list**, where the type
information is still intact (`BaseToolNodeDef` / `Tools` instances) — *before*
`split_tool_declarations` flattens a tool node into a bare `ToolBinding`. The duplicate
check is a pure name comparison; the discover-exclusivity reads the raw entry types. The
illegal combinations therefore never reach runtime, so **there is no runtime collision
policy** and **no provenance carried on `ToolBinding`**.

## Considered options

- **A separate `AllTools` type** — rejected: a second type to import and learn for what is
  the same concept ("function tool nodes for this agent") expressed by enumeration instead
  of by name. The mode keeps one handle, one mental model.
- **An `Agent(discover_tools=...)` constructor flag** — rejected: a flag lives *outside* the
  `tools=[...]` list, so it would not be a `ToolSelector` and would need a brand-new
  view-registration trigger (the deferred note's own suggestion). It also breaks composition
  with the rest of the `tools=` surface.
- **Resolve collisions at runtime (binding-merge existing-wins, dispatch-topic comparison)
  with a provenance marker on `ToolBinding`** — rejected: the construction-time contract
  prevents tool-node collisions *upstream*, where the raw `tools=` types are still intact, so
  there is nothing to resolve at the binding merge. Pushing it downstream forced a
  `source_kind` provenance field (because `split_tool_declarations` flattens the tool node
  away) and a per-turn collision policy — complexity that vanishes once the contract is
  enforced where the types are known.
- **Broad exclusivity (discover must be the sole `tools=` entry, MCP excluded)** — rejected:
  it couples discover and MCP toolboxes, two orthogonal planes (`node_kind=="tool"` vs
  `"toolbox"`), purely for enforcement simplicity. The narrow rule keeps orthogonal concerns
  orthogonal — a toolbox is not a tool.
- **One widened `CapabilityLookup` carrying both `get` and `snapshot`** — rejected on
  Interface-Segregation grounds: the point-lookup clients (`resolve_capability`, the
  MCP/named paths) would depend on a `snapshot()` they never call. Instead
  `EnumerableCapabilityView(CapabilityLookup)` adds enumeration as a separate role; the
  bulk kernel consumes it, the point kernel keeps the minimal surface.
- **Reshaping `tools=` handling so the discover check needs no retained state** — investigated
  (two grounded passes incl. an adversarial one) and **rejected**. The incremental `add_tools`
  path can't see the raw `BaseToolNodeDef` type (the split flattens an eager tool node into a
  bare `ToolBinding`), so the discover-exclusivity check retains a small typed
  `list[BaseToolNodeDef]`. Two reshapes were weighed:
  - *Third `eager_tool_nodes` bucket from `split_tool_declarations`* — not worth it. It does **not**
    remove the retained state (incremental `add_tools` must accumulate it regardless of how the
    split returns), and it adds a model→node layering edge (`tool_dispatch.py` would `isinstance`
    a concrete node class) plus a silent-break risk in the duck-typed provisioner
    (`provisioner.py:62` reads dispatch topics off `self.tools`). *(Correction: an earlier draft of
    this note claimed the split's return is consumed by the shipped by-name/MCP paths — that is
    wrong. `split_tool_declarations` has exactly two callers, both in `agent.py`; the by-name path
    uses `resolve_capability`, MCP uses `record_to_bindings`, client overrides use
    `normalize_tool_bindings` — none consume the split.)*
  - *A unified `ToolSource` protocol* (collapse `ToolBinding`/`ToolProvider`/`ToolSelector` to one
    list) — rejected as a wash/regression: it relocates rather than removes complexity (the
    eager/deferred partition resurfaces as a `view_dependent` flag + an eager-bindings cache; the
    frozen wire `ToolBinding` needs a ceremony adapter; the provisioner integration regresses;
    `resolve()` either drops `SelectorResult` diagnostics or forces eager sources to fabricate
    them), and it re-opens the locked "no protocol merge" (L9/L19, non-goal §3).
  The accepted interim is the **agent-local typed `list[BaseToolNodeDef]`**: extracted in
  `_add_tools` from the raw entries (which `agent.py` already imports `BaseToolNodeDef` to read),
  the split left unchanged so tool-node bindings still flow into `self.tools` (provisioner and
  registry untouched). It is small, correct, and proportionate to the wart.

## Consequences

- **No wire change.** Discover is call-side only: it reuses the `node_kind` field shipped in
  ADR-0013, adds no `CapabilityRecord` field, and triggers no `schema_version` bump.
- **`Tools` gains a `discover: bool` field** with the invariant *exactly one of {non-empty
  names, `discover=True`}*; the empty handle remains a construction error (never an implicit
  "everything"), preserving the fail-loud safety rail against an accidental empty splat.
- **One small protocol + one kernel** are added (`EnumerableCapabilityView`,
  `resolve_all_capabilities`); the named/MCP point-lookup path is untouched.
- **Prerequisite: MCP tool-name namespacing (ADR-0018).** Namespacing MCP tool names as
  `<server>__<tool>` makes the cluster tool namespace collision-free (tool-node names are
  globally unique by contract; MCP names become disjoint from them), which is what lets an
  `MCPToolbox` compose with discover/named `Tools` without any cross-plane name clash. It is
  a separate, isolated change to the MCP surface and lands first.
- **An empty cluster degrades silently** (discover names nothing, so there is no "missing"
  to report); a DEBUG count log aids the "why does my agent have no tools?" case, and the
  existing degraded-view WARNING still covers a broken view.
