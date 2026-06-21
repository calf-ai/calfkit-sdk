---
status: accepted
---

# MCP tool names are namespaced by their toolbox node

Function tool node names are globally unique by the `node_id` contract, but MCP tool names
live inside a toolbox and are governed by the MCP server, not by that contract. Today an
`MCPToolbox` advertises each tool to the LLM under its **bare** server-side name
(`record_to_bindings` builds `ToolDefinition(name=tool.name, ...)`), so a toolbox tool
`search` can collide with a tool node `search`, and two toolboxes can both expose `search`.
We **namespace** the LLM-facing name as:

```
<toolbox_node_name>__<tool_name>          e.g.  github__search
```

where **`<toolbox_node_name>` is the calfkit `MCPToolboxNode`'s `node_id`** — the `name` you
construct the toolbox with (`MCPToolbox("github")` / `MCPToolboxNode("github", ...)`), which
is also the capability key and the `mcp_server.<name>` topic suffix. It is **not** the MCP
server's own protocol-level name (`serverInfo.name`); calfkit never reads that for naming.
`<tool_name>` is the bare server-side tool name. The bare server-side name is still what is
sent to the MCP server on dispatch (the prefix is stripped first).

This makes the cluster's tool namespace **collision-free**: tool-node names (bare, e.g.
`add`) and MCP tool names (`github__search`) are disjoint, and two toolboxes can no longer
clash (`github__search` vs `jira__search`). That is the property that lets an `MCPToolbox`
sit alongside a function-tool discover/named surface without any cross-plane name collision
(see ADR-0014).

This is a **prerequisite, isolated change** to the MCP surface — it improves named MCP use
on its own, independent of any tool-discovery work, and lands first in its own PR.

## Notes

- **The prefix is the toolbox node's `node_id`.** Both the expansion (prefix) and the
  dispatch (strip) key on `MCPToolboxNode.node_id` (== the toolbox's construction `name`),
  not on anything fetched from the MCP server. At expansion the resolver supplies it
  (`resolve_capability`'s `target_id`, which *is* the toolbox's `node_id` / capability key);
  at dispatch the node uses its own `self.node_id`. The two are the same string, which is why
  the strip round-trips exactly. Do not derive the prefix from `serverInfo.name`, the
  `dispatch_topic`, or any server-reported value.
- **Separator & name validity.** `:` is not a valid tool-name character for major providers
  (OpenAI and Anthropic restrict tool names to roughly `[a-zA-Z0-9_-]`), so the separator is
  `__` (double underscore), the common MCP-client convention. The provider charset was
  confirmed at implementation against the vendored sanitizer
  (`calfkit/_vendor/pydantic_ai/_output.py`, `[^a-zA-Z0-9-_]`). **Length and charset are
  document-only (no runtime enforcement):** `{toolbox_name}__{tool_name}` must fit the
  provider tool-name limit (Anthropic 128, OpenAI 64) and stay within the charset. The
  toolbox name is a deployment identity, so the framework documents the constraint rather
  than policing it — a violation surfaces as a provider 400 at turn time, exactly as a bad
  *bare* name already would. Concatenation never truncates (truncation would create new
  collisions); deployers keep toolbox and tool names short and charset-safe.
- **Toolbox-scoped, not in the shared kernel.** The record→`ToolDefinition` expansion
  (`record_to_bindings`, `calfkit/models/capability.py`) is **shared** by MCP toolboxes and
  function tool nodes. Namespacing must apply **only to `node_kind == "toolbox"` records** —
  applying it unconditionally in the shared kernel would prefix tool-node names too and break
  the `node_id == tool name == capability key` identity (ADR-0013). The implementation factors
  the rule into one `_namespace_prefix(node_kind, name)` helper (returns `f"{name}__"` only
  for toolbox records, `""` otherwise), used both by `record_to_bindings` and by
  `resolve_capability`'s `include` filter — so the prefix is guarded on
  `record.node_kind == "toolbox"`, never applied blanket.
- **Dispatch de-prefixing.** The MCP server only knows the bare tool name, so the namespaced
  name is stripped back before `session.call_tool(...)`. The `MCPToolboxNode` strips its own
  prefix on receipt via `payload.name.removeprefix(f"{self.node_id}__")` — **not** a generic
  `split("__")`, which would corrupt a server-side tool name that legitimately contains `__`.
  Stripping exactly the node's own `<self.node_id>__` keeps the agent's dispatch path generic
  (it never understands the namespacing) and is robust to embedded separators.
- **`include` stays bare (the trust boundary).** `MCPToolbox(name, include=...)` pins tool
  names by their **bare** server-side name (a dev knows the tool as `search`, not
  `github__search`); the resolver strips the prefix before comparing, and the
  `missing_tools` diagnostic is reported bare. Namespacing is purely the LLM-facing
  projection — no user-facing API takes a namespaced name.

## Consequences

- **Breaking change to the LLM-facing MCP tool surface** (`search` → `github__search`). Any
  agent prompt or wiring relying on the bare name changes. Pre-1.0, no deployments to
  preserve — a clean break (consistent with the project's hard-breaks-over-compat stance).
- **No control-plane / wire-record change — a read-time projection.** The capability record
  still carries the **bare** server-side tool names; namespacing is a read-time projection
  applied when expanding a record into the LLM-facing `ToolDefinition` (`_namespace_prefix`
  in `record_to_bindings`), and undone on dispatch. The compacted `calf.capabilities` topic
  is untouched and there is **no `schema_version` bump** — re-advertising re-projects from
  bare.
