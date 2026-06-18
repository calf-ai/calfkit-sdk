# Capability-Plane Migration & Ops CLI

- **Status:** Draft (design)
- **Date:** 2026-06-16
- **Builds on:** [Node Presence Control Plane — Substrate & Shared Primitive](./node-presence-substrate-spec.md)
- **Scope:** (1) the shared `ControlPlaneRecord` base + record schemas; (2) migrating the shipped MCP capability plane onto the shared primitive (fixing CRITICAL-1/3/4); (3) the `calfkit nodes` ops view. These ship together in Phase 1, on top of the substrate.
- **Related:** [MCP Capability Discovery](./mcp-capability-discovery-spec.md), ADR-0001, ADR-0002.

---

## 1. Summary

The substrate spec extracts a shared control-plane primitive. This spec (a) defines the shared record base and the concrete `PresenceRecord` / `CapabilityRecord` schemas, (b) **migrates the shipped MCP capability plane** from its bespoke node-driven writer onto the worker-owned `ControlPlanePublisher` + `ControlPlaneView` — which fixes three live defects (replica flap, heartbeat-masks-staleness, frozen-view blindness) — and (c) adds an out-of-process **ops fleet view** (`calfkit nodes`) over `calf.presence`.

## 2. Goals / Non-goals

**Goals**
- One shared `ControlPlaneRecord` base so the staleness filter is generic across planes.
- Move the capability plane onto the primitive with **no loss of behaviour** for tool selectors, while instance-keying records and fixing CRITICAL-1/3/4.
- A read-only ops CLI for the live fleet.

**Non-goals**
- No new capability semantics — tools resolution behaves as today, just on the new transport/keying.
- No error handling / deadlines (inherited from the SDK-wide error-propagation work).
- Discovery, the Ask layer, and trust — see the [discovery spec](./agent-discovery-and-ask-spec.md).

## 3. Record schemas

The shared `ControlPlaneRecord` base and `PresenceRecord` are defined in the [substrate spec](./node-presence-substrate-spec.md#8-records-the-shared-base-and-presencerecord). `AgentCard` (topic `calf.agents`) is defined in the [discovery spec](./agent-discovery-and-ask-spec.md). This spec defines the migrated `CapabilityRecord`, which extends the same base:

```python
class CapabilityRecord(ControlPlaneRecord):      # topic: calf.capabilities  (migrated; toolboxes)
    schema_version: int = CAPABILITY_SCHEMA_VERSION
    dispatch_topic: str
    tools: list[CapabilityToolDef]
    content_updated_at: AwareDatetime            # when `tools` last changed — SEPARATE from last_heartbeat_at
    # inherits: schema_version, node_id (= toolbox_id), worker_id, started_at, last_heartbeat_at
```

### The CRITICAL-3 fix is in the schema

The shipped bug: the heartbeat re-stamped a single `published_at`, so a failed tool re-list was never detectable as stale — the liveness refresh masked content rot. Splitting **liveness** (`last_heartbeat_at`, refreshed every tick) from **content currency** (`content_updated_at`, refreshed only when `tools` actually change) lets a consumer distinguish "instance alive, tool list stale." `PresenceRecord` has no mutable content and needs only `last_heartbeat_at`.

## 4. Capability-plane migration

The shipped plane (`calfkit/mcp/mcp_toolbox.py`, `calfkit/models/capability.py`) is node-driven, keyed `toolbox_id`, on topic `mcp.capabilities`. It moves onto the primitive:

### 4.1 Writer: toolbox becomes a content contributor

`MCPToolboxNode` stops owning its own `KafkaTableWriter`, heartbeat loop, and tombstone. Instead it registers a `current_record()` contributor with the worker's `ControlPlanePublisher`:

- The toolbox still owns its tool list (the MCP session). On startup and on each MCP `tools/list_changed`, it updates its cached `CapabilityRecord` content and bumps `content_updated_at`.
- The worker's single heartbeat loop pulls that cached record each tick, refreshes `last_heartbeat_at`, and publishes it instance-keyed (`group=toolbox_id`, `member=worker_id`) to `calf.capabilities`.
- On clean shutdown the worker tombstones the toolbox's `(toolbox_id, worker_id)` key in its ordered pass.

This deletes the bespoke per-node loop/tombstone (DRY), **instance-keys** the record (fixes CRITICAL-1), and inherits the liveness/content split (fixes CRITICAL-3). The toolbox's careful shutdown ordering (flag-first, cancel-then-tombstone) is now the publisher's, applied uniformly.

> A toolbox node now appears in **both** `calf.presence` (as a live node of `kind=toolbox`) and `calf.capabilities` (with its tools). Modest liveness duplication, but each consumer reads exactly one topic.

### 4.2 Reader: capability view on the primitive

The worker's existing capability-view `@resource` becomes a `ControlPlaneView[CapabilityRecord]` over `calf.capabilities` (via the grouped table). Wiring `status`/`failure` closes **CRITICAL-4** (frozen-view blindness).

Selector resolution changes from a flat single-key lookup to a node-centric, liveness-aware one:

- Today: `view.get(toolbox_id)` → one record.
- After: `view.instances(toolbox_id, include_stale=False)` → live instances; pick one (replicas advertise identical tools, so any live instance's `tools` is correct). This is the **reader-side merge** that makes instance-keying transparent to the agent layer.

The agent still consumes a `Mapping`-shaped abstraction and never imports ktables (existing layering rule preserved).

### 4.3 De-MCP renames (Phase 0, breaking — pre-1.0 hard breaks)

The capability plane is no longer MCP-specific transport; the names should reflect the generalised control plane:

- Topic: `mcp.capabilities` → `calf.capabilities` (on the wire — breaking).
- Config: `MCPDiscoveryConfig` → `DiscoveryConfig` (public class — breaking); its `heartbeat_interval` / `catchup_timeout` become the shared control-plane knobs, plus a `stale_after` (default `3 × heartbeat_interval`).
- Resource key: `CAPABILITY_VIEW_RESOURCE_KEY` re-namespaced (`calfkit.mcp.capability_view` → `calfkit.controlplane.capability_view`).
- Remove `Worker.name` (unused dead code) — its own small cleanup PR.

Sequencing note: these renames touch areas the in-flight run/handler-unification and MCP work also touch; coordinate ordering so the breaking changes land coherently.

## 5. Ops CLI — `calfkit nodes`

Ops usually wants an *out-of-process* fleet view (from a laptop or sidecar), not code inside a worker. Add `calfkit/cli/nodes.py` alongside the existing `cli/run.py`, `cli/topics.py`:

- Opens a transient grouped table / `ControlPlaneView` over `calf.presence`, waits for catch-up, prints the live fleet grouped by `node_id` — `kind`, instance count, per-instance `worker_id` + `age`, stale markers — then exits.
- Flags: `--watch` (refresh), `--stale` (include stale, annotated), `--kind agent` (filter), `--json` (machine output).
- Reuses the existing CLI bootstrap (bootstrap-servers resolution, the `_loader` patterns). It is read-only — it never writes the control plane.

This is the concrete surface that makes "ops/observability" real for Phase 1; programmatic access is documented as "construct a `ControlPlaneView` over `calf.presence` yourself."

## 6. Provisioning

`calf.presence` and `calf.capabilities` both need `cleanup.policy=compact`. calfkit's provisioner applies a flat config to all data topics and cannot set per-topic compaction, so compaction is delegated to ktables `ensure_topic` (dev) or to ops (prod, where provisioning is off by default). Document the required topic config; this is consistent with how the shipped capability topic is handled today.

## 7. Phasing

Part of **Phase 1** (with the substrate). Order within Phase 1:
1. Shared `ControlPlaneRecord` base + `PresenceRecord` + `CapabilityRecord` (with `content_updated_at`).
2. De-MCP renames + remove `Worker.name` (Phase 0 cleanup, can precede).
3. Migrate the capability writer/reader onto the primitive (instance-keyed; CRITICAL-1/3/4 fixed).
4. `calfkit nodes` CLI.

## 8. Testing

- **Unit:** the shared base + both records (schema versioning, tolerant reader); selector resolution against a fake `ControlPlaneView` (dict-backed), including the live-instance merge.
- **Integration (Redpanda lane):**
  - **Capability parity:** an agent with MCP tool selectors still resolves the same tools after migration.
  - **Replica regression (CRITICAL-1):** two workers host the same toolbox; one shuts down cleanly; tools still resolve from the surviving instance.
  - **Liveness/content split (CRITICAL-3):** a refreshed heartbeat does not move `content_updated_at`; a `tools/list_changed` does.
  - **Frozen-view (CRITICAL-4):** a degraded/failed reader surfaces via `status`/`failure`.
  - **Ops CLI:** prints expected fleet rows; `--stale`/`--kind`/`--json` behave.

## 9. Open questions

- Exact resource-key namespace and `DiscoveryConfig` field set.
- Whether the capability topic rename warrants a one-release dual-read shim, or a clean cut (default: clean cut, pre-1.0).
- CLI command name (`nodes` vs `fleet`).
