# Node Presence Control Plane — Substrate & Shared Primitive

- **Status:** Draft (design)
- **Date:** 2026-06-16
- **Scope:** The foundational presence substrate and the reusable control-plane primitive it is built on. This is the base layer; two follow-on specs build on it:
  - [Capability-Plane Migration & Ops CLI](./capability-plane-migration-and-ops-spec.md)
  - [Agent Discovery & Ask](./agent-discovery-and-ask-spec.md)
- **Related:** [MCP Capability Discovery](./mcp-capability-discovery-spec.md) (the shipped precedent this generalises), ADR-0001 (compacted capability topic). External dependency: `ryan-yuuu/ktables#16` (grouped/multimap table).

---

## 1. Summary

Add a compacted-Kafka **presence control plane** that materialises a live view of every node (tool / agent / consumer / toolbox) on the network. Each worker emits a heartbeat record per hosted node to a compacted topic and tombstones it on clean shutdown; every worker broadcast-reads the whole topic, so any process can answer *"which nodes are online, and where?"*.

The shipped **MCP capability plane** (`calfkit/mcp/mcp_toolbox.py`, `calfkit/models/capability.py`) already does heartbeat + tombstone + compacted-view for toolboxes. This spec **extracts the reusable mechanism** from that precedent into a shared primitive and applies it to general node presence. The two follow-on specs migrate the capability plane onto the same primitive and build discovery on top.

## 2. Motivation

- **Ops / observability** wants a fleet view: which tools/agents are up, on which workers, how long they've been silent.
- **Agent-to-agent discovery** (a later phase) needs a registry of reachable peers to read from.
- **DRY across control planes.** calfkit already has the capability plane and will gain an operator ACL plane (#231) and a runtime-reconfig plane (#164). All four share the same read shape (a compacted view materialised behind a catch-up gate). Building presence as yet another bespoke plane would be the "needed-patch-rules-mean-the-factoring-is-wrong" anti-pattern. This spec factors the common mechanism once.

The presence *view* is the easy, transferable part. The hard problems (request/reply, trust, budgets) are messaging, and live in the discovery spec — not here.

## 3. Goals / Non-goals

**Goals**
- A shared, reusable control-plane primitive: a read-side `ControlPlaneView` and a worker-owned write-side `ControlPlanePublisher`.
- A `PresenceRecord` and a `calf.presence` compacted topic carrying every node's liveness + identity.
- Instance-keyed records (`node_id` × `worker_id`) so replicas don't clobber each other.
- Advisory staleness (no TTL, no active eviction).
- Wire ktables' reader-death signals (`status`/`failure`) so a frozen view is observable.

**Non-goals (explicitly out of scope here)**
- Any error handling, deadlines, retries, dangling-call recovery, or recursion budgets — these are inherited from the SDK-wide error-propagation work, not specced here.
- Crash *detection* beyond advisory staleness. A crashed node's record lingers until it reads as stale; there is no reaper.
- Discovery, messaging, trust enforcement, the ops CLI, and the capability migration — covered in the two follow-on specs.

## 4. Background: what the precedent already proves

From a source-grounded read of the shipped capability plane:

- **Transport works.** ktables (`>=0.2.0`) gives a groupless **broadcast** view (`group_id=None`, all partitions, replay from offset 0) — every worker sees every key, the GlobalKTable model. Writes are `acks=all` + idempotent. Tombstone = `delete(key)` (null value). `barrier()` gives read-your-own-writes. `status`/`failure` expose reader death.
- **Lifecycle seams exist.** `after_startup` (broker live — spawn a loop) and `on_shutdown` (broker still live — cancel + tombstone) are inherited by **both** `Worker` and every `BaseNodeDef` (`calfkit/worker/lifecycle.py`). `MCPToolboxNode` already runs a heartbeat loop and an ordered tombstone via these.
- **Known sharp edges to fix while generalising** (confirmed live on `main`):
  - **CRITICAL-1 — replica flap:** records keyed per-node (`toolbox_id`) mean one replica's clean tombstone blanks the record cluster-wide. Fixed here by instance-keying.
  - **CRITICAL-3 — heartbeat masks content staleness:** re-stamping a single `published_at` hides stale content. Fixed by the liveness/content timestamp split (relevant once content-bearing records migrate — see the migration spec).
  - **CRITICAL-4 — frozen-view blindness:** calfkit never reads `table.status`/`failure`. Fixed by wiring them into `ControlPlaneView`.

These fixes are *why* unifying (rather than adding a parallel plane) is worthwhile: the primitive fixes them once for every plane.

## 5. The shared primitive

Two halves, both reusable by the capability plane, the ACL plane (#231), and the reconfig plane (#164).

### 5.1 Read side — `ControlPlaneView[R]`

A thin, typed wrapper over a ktables grouped/multimap table (see §6), opened as a **worker-level `@resource`** and materialised as a node-centric view in the resource bag behind the catch-up gate.

Responsibilities:
- Bind one record model `R` to one topic (1:1, by construction).
- Present a **node-centric** API that hides the composite `node_id × worker_id` key (callers ask about `node_id`).
- Apply the **advisory staleness** filter (§7) — the one piece of domain semantics the view owns.
- Surface reader health (`status`, `failure`, `is_caught_up`) so a frozen/degraded view is observable, not silent.

```python
class ControlPlaneView(Generic[R]):
    # liveness-aware reads (node-centric; worker_id hidden)
    def is_online(self, node_id: str) -> bool: ...          # any live instance
    def instances(self, node_id: str, *, include_stale: bool = False) -> dict[str, R]: ...
    def online_nodes(self) -> set[str]: ...                 # node_ids with >=1 live instance
    def snapshot(self, *, include_stale: bool = False) -> dict[str, dict[str, R]]: ...

    # health passthrough (closes frozen-view blindness)
    @property
    def status(self) -> TableStatus: ...                    # unstarted|loading|caught_up|degraded|failed
    @property
    def failure(self) -> BaseException | None: ...
    async def barrier(self, timeout: float | None = None) -> bool: ...
```

The view depends only on the `ControlPlaneRecord` base (§ records spec) for `last_heartbeat_at`; it is otherwise generic over `R`. Consumers never import ktables (the existing layering rule).

### 5.2 Write side — `ControlPlanePublisher`

A single **worker-owned** resource that runs **one** heartbeat loop and **one** ordered tombstone pass for all of the worker's hosted nodes.

Drive model — **worker owns the heartbeat, nodes contribute content** — on a principle:

> Liveness is a per-process fact; record content is a per-node fact.

A node can't be "down" while its worker is "up" (there is no per-node crash independent of the process), so driving the heartbeat per-node would emit N signals for one liveness fact and duplicate the tricky cancel-then-tombstone ordering N times. Instead:

- The worker runs the loop, owns the instance identity (`Worker.id`), and owns the tombstone ordering.
- Each node exposes a `current_record()` contributor returning its present record content. A plain node contributes presence-only; richer nodes (toolbox, discoverable agent) contribute more (see follow-on specs). The worker treats records opaquely — it never knows about MCP or tools.

Lifecycle wiring (reusing the proven seams):
- `after_startup` (broker live): publish each node's record once, then spawn the heartbeat loop.
- Heartbeat tick: for each hosted node, refresh `last_heartbeat_at` and `set(group=node_id, member=worker_id, value=record)`.
- `on_shutdown` (broker still live): set a shutdown flag synchronously first, cancel-and-await the loop, then tombstone every `(node_id, worker_id)` key the worker owns. This ordering (copied from `MCPToolboxNode._tombstone_on_shutdown`) prevents a straggler `set()` from resurrecting a tombstoned record.

Crash behaviour is unchanged from the precedent and accepted: a hard crash skips `on_shutdown`, so the record is **not** tombstoned and instead goes stale (§7).

## 6. Keying & the grouped/multimap dependency

**Records are instance-keyed:** `group = node_id`, `member = worker_id` (= `Worker.id`, already documented as a stable wire identity "e.g. for fleet presence"). Readers group by `node_id` to get the set of live instances.

This is required for correctness, not convenience. Multiple replicas of the same node (same `node_id`) run across workers — the default scaling path. If they shared one key, a single replica's clean tombstone would blank the logical node (CRITICAL-1), and concurrent updates would be a lost-update race. Giving each writer **its own key** (`node_id × worker_id`) yields **single-writer-per-key by construction** — no read-modify-write, nothing to serialise, clean independent tombstones. The "collection of instances per node" is then a read-side grouping.

> Note on alternatives considered and rejected: a single node-keyed record with a collection value would require cross-process read-modify-write, which Kafka can't make safe (no broker compare-and-set). Making it safe via single-writer-per-partition forces a co-partitioned aggregator (a stateful consume-aggregate-produce service with ownership/rebalance) — heavy infrastructure this project avoids, and it still wouldn't detect crashes without a TTL. Instance-keying + read-side grouping is the lightweight, canonical secondary-index pattern (Kafka Streams `groupBy` / Faust `group_by`).

**The grouping is packaged in ktables**, not hand-rolled in calfkit, as a grouped/multimap table — `set/delete(group, member, value)` writes + group-keyed reads over a composite key (length-prefixed injective codec, compute-on-read projection). Filed as **`ryan-yuuu/ktables#16`**; calfkit will require `ktables>=0.3.0`. `ControlPlaneView` is the thin typed wrapper that adds the staleness filter; the composite-key mechanics live in ktables.

Because ktables has **no TTL/eviction** and **no change hook**, the view is compute-on-read (fine at control-plane cardinality). A future ktables `changes()` stream (noted in #16) would later enable an incremental index and push-notifications, but is not required here.

## 7. Staleness (advisory)

A tombstone only fires on **clean** shutdown; a crash leaves the record in place, and ktables never evicts. So "is this node actually online?" is a reader-side judgement, not a transport guarantee:

- Each record carries `last_heartbeat_at`, refreshed every heartbeat tick.
- `ControlPlaneView` computes `age = now - last_heartbeat_at` and treats a member as **stale** when `age > stale_after` (configurable; default `3 × heartbeat_interval`, matching the shipped 90s).
- The view **never deletes** stale records — it annotates/filters. Two surfaces:
  - **Annotated (ops):** show all instances with `age`/`is_stale`. Ops *wants* to see a node silent for 5 minutes — that's the signal; hiding it would blind the fleet view.
  - **Filtered (selection):** `online_nodes()` / `instances(..., include_stale=False)` exclude stale instances so a consumer never selects a probably-dead target.

Documented assumptions (not guarantees):
- **Clock skew.** `last_heartbeat_at` is the publisher's wall clock; `age` is the reader's. The generous `3×` threshold sits far above normal NTP skew (<1s). This is documented, not enforced with logical clocks — consistent with "document deployment, don't police it."
- **Crashes linger.** Between a crash and the staleness threshold, a dead node still appears (filtered out only after `stale_after`). Real crash *recovery* (failing pending work) is the error-propagation layer's concern, not this substrate's.

This deliberately rejects TTL hard-eviction and any active sweeper — the same decision the capability plane made, and the one that keeps the substrate infrastructure-free.

## 8. Records: the shared base and `PresenceRecord`

A thin shared base carries identity + liveness, so `ControlPlaneView`'s staleness filter depends on exactly one contract and never on a concrete record type. The base is reused by every plane (`CapabilityRecord` in the migration spec, `AgentCard` in the discovery spec, and later #231/#164):

```python
class ControlPlaneRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")   # tolerant reader (additive forward-compat)
    schema_version: int                          # default set per subclass; major bump => old readers skip
    node_id: str                                 # = group key (also in value: self-describing wire record)
    worker_id: str                               # = member key (the instance; Worker.id)
    started_at: AwareDatetime                    # this instance's boot time -> uptime + restart detection
    last_heartbeat_at: AwareDatetime             # liveness basis; refreshed every heartbeat tick

class PresenceRecord(ControlPlaneRecord):        # topic: calf.presence
    schema_version: int = PRESENCE_SCHEMA_VERSION
    kind: NodeKind          # agent | tool | consumer | toolbox
    dispatch_topic: str     # where this node is addressable
```

Keeping `node_id`/`worker_id` in the value (redundant with the key) matches the existing `CapabilityRecord`, which already stores its id in the value — the wire record stays self-describing. Schema evolution follows the existing pattern: per-record `schema_version` + tolerant reader for additive fields; a major bump makes old readers treat newer records as "node invisible" rather than crash.

- `worker_name` is **not** included — `Worker.name` is currently-unused dead code and is removed (its own cleanup PR). If friendly ops names prove desirable once the fleet view exists, re-introduce them then, with `PresenceRecord` as the real consumer.
- **Topic `calf.presence`** is compacted (`cleanup.policy=compact`). Presence is **default-on** for every node — ops needs the whole fleet. (Being a *messageable peer* is a separate opt-in, handled in the discovery spec.)
- Provisioning: calfkit's own provisioner applies a flat config to all data topics and cannot set per-topic compaction, so compaction is delegated to ktables `ensure_topic` (dev) / ops (prod) — documented, consistent with the existing capability topic.

## 9. What this substrate unlocks

The primitive is deliberately generic so the following build on it without re-deriving transport:

- **[Capability-Plane Migration & Ops CLI](./capability-plane-migration-and-ops-spec.md)** — migrates the shipped MCP capability plane onto `ControlPlaneView`/`ControlPlanePublisher` (fixing CRITICAL-1/3/4) and adds the `calfkit nodes` ops view over `calf.presence`.
- **[Agent Discovery & Ask](./agent-discovery-and-ask-spec.md)** — adds `calf.agents` (opt-in discoverable agents) and the caller-side request/reply adapter, reading presence/agents views to select live peers.
- **Operator ACL plane (#231)** — an operator-authored `AccessPolicyRecord` plane reusing the read-side `ControlPlaneView` (keyed `node_id`, no tombstone — operator-authored, not self-published).
- **Runtime-reconfig plane (#164)** — the same read-side shape for node reconfiguration.

## 10. Phasing & dependencies

- **Phase 0 (prereqs):** `ktables#16` lands → `ktables>=0.3.0`; extract `ControlPlaneView` + `ControlPlanePublisher`; remove `Worker.name`.
- **Phase 1 (this substrate):** `PresenceRecord` + `calf.presence` + worker-owned heartbeat for all nodes + `ControlPlaneView` with `status`/`failure` wired.
- Capability migration and discovery follow in their own specs.

## 11. Testing

- **Unit:** `PresenceRecord` schema versioning / tolerant reader; the staleness filter and node-centric grouping tested over a fake grouped table (a plain dict), zero Kafka.
- **Integration (Redpanda lane):** heartbeat publish → view reflects it after `barrier()`; clean shutdown tombstones the instance; **the 2-worker same-node replica regression test** — two workers host the same `node_id`; one shuts down cleanly; the node remains online via the surviving instance (the CRITICAL-1 proof).

## 12. Open questions

- Final names: `ControlPlaneView` / `ControlPlanePublisher`, resource-bag key namespace (`calfkit.controlplane.*`).
- `heartbeat_interval` / `stale_after` config home (generalise the existing `MCPDiscoveryConfig` knob — see the migration spec's rename).
- Whether `calf.presence` warrants >1 partition (presence is low-volume; single partition is fine — partition count is not load-bearing here).
