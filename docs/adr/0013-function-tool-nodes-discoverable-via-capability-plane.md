---
status: accepted
---

# Function tool nodes are discoverable via the capability plane

`MCPToolboxNode` advertises on the capability control plane (ADR-0012) so an agent can
hold an identity-only `MCPToolbox` handle and discover its tools at runtime. Function tool
nodes had no such path: an agent could only use one by importing the live `ToolNodeDef`,
which baked the tool's JSON schema into the agent's process — even though dispatch always
crosses Kafka. We make tool nodes a **second adopter** of the same plane. `BaseToolNodeDef`
declares an `@advertises("calf.capabilities", CapabilityRecord)` factory (always-on; the
schema is static, so the factory just reads it), and `Tools(*names)` is the by-name,
deployment-free handle an agent passes in `tools=[...]` — the call-side counterpart to a
deployed tool node, mirroring the `MCPToolbox`/`MCPToolboxNode` split (ADR-0009). It
resolves per turn against the shared `ControlPlaneView[CapabilityRecord]`, reusing MCP's
resolution kernel.

The eager path is kept: passing a live tool node still bakes the schema in with a local
arg validator (fail-fast before dispatch); a discovered binding has no validator, so bad
args are dispatched and fault at the tool node, riding the rail back (ADR-0003). Two
handles onto one node — the caller picks zero-infra-and-local-validation vs. decoupled-and-discovered.

Three changes let the plane serve both adopters safely:

- A worker-stamped `node_kind` on every control-plane record (`ControlPlaneStamp`) is the
  **over-pull guard**: `resolve_capability(..., expected_kind=)` rejects a record of the
  wrong kind, so `Tools` can never bind a multi-tool toolbox record (nor `MCPToolbox` a
  tool-node record). It also lets the view observe (log) cross-kind `node_id` collisions.
- A tool node's identity becomes the tool name — the `tool_` `node_id` prefix is dropped —
  so `node_id == the LLM-facing tool name == the capability key`, and an agent references a
  node by the same name it calls. `agent_tool(func, name=...)` / `create_tool_node(..., name=...)`
  override it (the disambiguation knob for the cluster-wide-unique identity contract).
- The `SelectorResult` / `resolve_capability` vocabulary is de-MCP'd to be
  cardinality-neutral (`missing_targets` / `invalid_targets` / `wrong_kind_targets`, tuple
  `bindings`), since `Tools` resolves N targets where `MCPToolbox` resolves one.

Considered and rejected: **opt-in advertising** (an eager-only tool node that does not
advertise) — designed and deferred, since always-on keeps the common case configuration-free
at the cost of making `calf.capabilities` a boot dependency for any tool-node worker;
**namespacing the capability key by owner kind** to make heterogeneous-owner collisions
unrepresentable — rejected because it breaks the clean `node_id == tool name` identity, and
the collision is a facet of the global `node_id`-uniqueness contract the framework already
documents (the `node_kind` view warning makes the cross-kind case observable).

Consequences, breaking (pre-1.0, no deployments): `node_kind` is a **required** field on the
control-plane record — recreate `calf.capabilities` on upgrade — and a tool node's `node_id`
changes from `tool_<fn>` to `<fn>`, which appears on the wire in envelope headers and fault
origin. Full design: `docs/designs/runtime-tool-discoverability-spec.md`.
