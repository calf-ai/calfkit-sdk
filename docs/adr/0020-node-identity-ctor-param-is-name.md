---
status: accepted
---

# Node identity constructor parameter is `name`, not `node_id`

A node's identity — the unique, stable, topic-mapped routing key — is stored on `BaseNodeSchema`
as `node_id` and exposed as both `self.node_id` and the read-only `self.name` alias. The
**constructor surface**, however, names this argument **`name`**: developers write
`Agent("weather_agent", ...)` / `Agent(name="weather_agent", ...)`, not `node_id=`. This is a
deliberate developer-experience choice — `name` reads more naturally at the call site than the
routing-key-precise `node_id` — applied progressively across the node family.

This is the orthogonal axis to [ADR-0009](0009-call-side-handle-takes-the-bare-name.md): 0009
decides *which class* of a two-type pair carries the `Node` suffix; this ADR decides *what the
identity parameter is called*. The MCP toolbox types led the migration
(`MCPToolboxNode(name=...)`, PRs #254/#255); the `Agent` ctor followed (PR #272), then the
`@consumer` decorator and `ConsumerNode` (PR #274). The function-tool-node factories
(`agent_tool` / `create_tool_node`) already take `name` (PR #266). What still uses `node_id` is
the **raw base construction** — `BaseNodeDef(node_id=...)` / `NodeDef` / the raw `ToolNodeDef(node_id=...)`
dataclass form — and the underlying `BaseNodeSchema.node_id` storage field, which remain the
migration target.

## Notes

- **Surface-only.** Each constructor maps `name` onto the base node's `node_id` storage
  (`super().__init__(node_id=name, ...)`). The `self.node_id` field and the `self.name` property
  both remain and read the same value, so internal code, topic derivation, and the control plane
  are untouched.
- **No wire change.** Identity is the Kafka key (`node_id × worker_id`), never carried in a
  serialized record value, so this rename never touches the wire format or any `schema_version`.
- **Why `name` over the "more honest" `node_id`.** `node_id` arguably describes the value better
  (a unique, stable routing id, distinct from a mutable display label). The naming was weighed
  and `name` chosen for call-site readability — a DX-over-precision call. Recorded here so the
  `name` surface is not "corrected" back to `node_id` by a future reader.

## Consequences

- **A clean pre-1.0 break per migrated type.** `Agent(node_id=...)` (keyword) stops working;
  positional `Agent("name", ...)` is unaffected — the idiomatic form throughout the repo, README,
  and examples. No deprecation shim, consistent with the project's hard-breaks-over-compat stance.
- **A mixed surface during migration.** Until the family-wide pass lands, some constructors take
  `name` (MCP toolbox types, `Agent`) and others still take `node_id`. The `.name` / `.node_id`
  accessors are uniform regardless, so reads are unaffected.
