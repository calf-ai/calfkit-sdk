---
status: accepted
---

# The MCP capability plane is built on the control-plane substrate

The shipped MCP capability plane was hand-rolled: `MCPToolboxNode` owned a flat
`KafkaTableWriter` keyed by `toolbox_id`, its own heartbeat loop, and its own
tombstone; the worker read a flat `KafkaTable`; config lived in
`MCPDiscoveryConfig`; and selectors carried a `strict` flag. That plane carried
three confirmed defects — CRITICAL-1 (a replica's clean tombstone blanks a node
still alive on another worker, because the key is flat), CRITICAL-3 (a single
`published_at` re-stamped every heartbeat masks whether the *tool list* is fresh),
and CRITICAL-4 (the agent never reads the table's `status`/`failure`, so a
degraded reader silently serves a frozen view). We migrate the plane onto the
generic control-plane substrate (ADR-0010, ADR-0011), which exists precisely to
fix these once for every plane.

We therefore make the capability plane an **adopter** of the substrate. The
toolbox keeps only the MCP-specific part — a cached tool list refreshed at
session setup and on every `tools/list_changed` — and exposes it through one
`@advertises("calf.capabilities", CapabilityRecord)` factory; the worker-owned
`ControlPlanePublisher` heartbeats and tombstones it (instance-keyed by
`node_id × worker_id`, ADR-0010/0011), and the worker reads a
`ControlPlaneView[CapabilityRecord]` that collapses the instances to one live
record per toolbox. `CapabilityRecord` becomes a `ControlPlaneRecord` subclass:
identity leaves the value (it is the wire key), and `published_at` splits into the
inherited `last_heartbeat_at` (liveness, stamped every tick) and a node-tracked
`content_updated_at` (advanced only on a real tool change) — which is the CRITICAL-3
fix expressed in the schema.

Because this is a clean wire break (the grouped composite key is incompatible with
the old flat topic) and we are pre-1.0, we take it whole rather than shimming:
topic `mcp.capabilities` → `calf.capabilities`; `MCPDiscoveryConfig` folded into the
substrate's `ControlPlaneConfig` (the topic is now fixed in the decorator, not
configurable); resource key `calfkit.mcp.capability_view` →
`calfkit.controlplane.capability_view`.

Two behavioural decisions follow, and both simplify the read path:

- **Staleness is hard-filtered, not advisory.** The collapsed view hides a stale
  toolbox (`get()` returns `None`), so an all-stale selection resolves as
  `missing_toolbox` and the agent degrades — rather than the old "use last-known
  tools + log" path. The view owns *both* staleness and schema-version filtering,
  so `SelectorResult` drops `stale_seconds` and `skipped_newer_schema`, and the
  resolver shrinks to "is it here, and does it have the tools I asked for".
  CRITICAL-4 becomes **log-only**: the agent warns when `view.status` is
  `degraded`/`failed`, and never raises on reader health.

- **The `strict` selector flag is removed** (along with `MCPToolResolutionError`).
  Its only effect was failing a turn before the model ran when a selection could
  not fully resolve. Under hard-filter staleness that would fire on a transient
  heartbeat gap, turning a blip into a hard failure; and `include` already carries
  the real trust boundary (it pins which tool names an agent may see). So an
  unresolved selection now *always* warns and degrades.

Considered and rejected: **keeping the per-node writer/heartbeat/tombstone** (the
whole point of the substrate is to centralise the resurrection-safe ordering once);
**a one-release dual-read shim** for the topic rename (pre-1.0, a clean cut is
cheaper than carrying two codecs); **preserving `strict`/advisory staleness**
(they fight the collapsed view and add surface for a knob `include` already
covers).

Consequences: the agent read path is a near-drop-in — `resolve_capability` still
only calls `view.get(toolbox_id)`. Tool-list changes now propagate by **pull**
(ADR-0011): a `tools/list_changed` updates the cache and lands at the next
heartbeat tick (≤ `heartbeat_interval`), not instantly; an immediate `publish_now()`
push is left as a future additive extension. The capability plane is now one of
several substrate adopters, so agent discovery and the operator-ACL/reconfig planes
reuse the same machinery rather than re-deriving transport.
