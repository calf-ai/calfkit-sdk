# Control-Plane Substrate — Generic Discoverability Machinery

- **Status:** Draft (design) — converged in the 2026-06-18 design discussion
- **Date:** 2026-06-18
- **Scope:** v1 is the **reusable control-plane substrate itself** — the machinery that lets any node type advertise a record to a compacted topic and lets any worker read a live view of it. It ships as pure machinery with **no concrete plane**; the known consumers (capability discovery, agent discovery) adopt it in their own later PRs.
- **Supersedes (for v1):** [`node-presence-substrate-spec.md`](./node-presence-substrate-spec.md). That spec framed the substrate around an always-on **presence** plane and an ops fleet view; v1 drops presence and the ops CLI entirely (see §3). The substrate-spec's §5.2 "every node contributes a presence record" framing is corrected here: presence is gone, and the contributor mechanism exists only for opt-in advertisers.
- **Related:** ADR-0010 (instance-keyed storage, collapsed view, staleness cadence on the record), ADR-0011 (worker-owned publisher), ADR-0001 (the compacted capability topic this generalises). The follow-on adopter specs — [capability-plane migration](./capability-plane-migration-and-ops-spec.md) and [agent discovery & Ask](./agent-discovery-and-ask-spec.md) — build on this and are otherwise out of scope here.
- **Dependency:** `ktables>=0.3.0` (already on `main`): provides `GroupedKafkaTable` / `GroupedKafkaTableWriter`.

---

## 1. Summary

Add a reusable **control-plane substrate**: the machinery a node type uses to advertise a record to a compacted Kafka topic, and the machinery a worker uses to read a live, node-centric view of that topic. It has three pieces:

- **`ControlPlaneRecord`** — the shared base every concrete record extends (identity + liveness).
- **`ControlPlanePublisher`** — one worker-owned writer that heartbeats every hosted node's advertised records and tombstones them on clean shutdown.
- **`ControlPlaneView[R]`** — the per-worker read-side materialisation of one control-plane topic, presented as a collapsed `node_id → record` map with an advisory-staleness filter.

The substrate is **generic over the record type and parameterised by topic**: each use case (capability discovery, agent discovery, and the future access-policy and reconfig planes) instantiates it on its **own** topic with its **own** record subclass. v1 ships and tests the machinery only; it does not ship a concrete plane.

This generalises the shipped MCP capability plane (`calfkit/mcp/mcp_toolbox.py`, `calfkit/models/capability.py`), which today hand-rolls heartbeat + tombstone + a single-keyed view per toolbox. That plane is **not** migrated in this PR — it keeps running on its bespoke machinery until its own migration PR adopts this substrate.

## 2. Motivation

- **One mechanism, many planes.** calfkit already has the capability plane and will gain agent discovery (#241), an operator ACL plane (#231), and a runtime-reconfig plane (#164). All share the same read/write shape (a compacted topic materialised behind a catch-up gate, self-published with heartbeat + tombstone). Building each bespoke is the "needed-patch-rules-mean-the-factoring-is-wrong" anti-pattern; this factors the common mechanism once.
- **Discoverability is the v1 use.** The immediate need is for node types to be discoverable to the rest of the mesh via a global ktable. The *registry* (advertise + find) is the transferable, well-understood part; messaging (request/reply) is a separate, harder concern handled in the discovery spec, not here.
- **Fix latent defects once.** Generalising lets the substrate fix, for every adopter, the defects the bespoke capability plane carries: the replica flap (ADR-0010) and frozen-view blindness (§9).

## 3. Goals / Non-goals

**Goals**
- A generic, reusable control-plane primitive: `ControlPlaneRecord` base, `ControlPlanePublisher` (write), `ControlPlaneView[R]` (read).
- A node-type-level plug-in interface (`@advertises`) so a use case declares "instances of this type publish record `R` to topic `T`," contributing only content.
- Instance-keyed storage (`node_id × worker_id`) with a collapsed `node_id → record` view.
- Advisory staleness (no TTL, no eviction).
- Reader-death observability (`status` / `failure`) wired through the view.
- Designed against the capability + agent record shapes so those planes adopt it mechanically.

**Non-goals (explicitly out of scope)**
- **No concrete plane.** v1 ships the machinery; capability migration, agent discovery, ACL, and reconfig are their own PRs.
- **No messaging.** Request/reply ("Ask"), peer-as-tool adapters, and trust enforcement live in the discovery spec.
- **No presence plane and no ops CLI.** The always-on ops fleet roster is dropped from v1 (its only consumer was ops/observability, off the request path); it may return later as just another adopter of this substrate.
- **No push path in v1.** Content propagation is pull-only; `publish_now()` is reserved as an additive extension (§6).
- **No error handling** — deadlines, retries, dangling-call recovery, recursion budgets — these are the SDK-wide error-propagation layer's concern.
- **No capability-plane changes** and **no de-MCP renames** (`mcp.capabilities` → `calf.capabilities`, `MCPDiscoveryConfig` → …); those land with the capability migration PR.

## 4. Vocabulary

Defined in `CONTEXT.md` (Control plane section): **Control plane**, **Control Plane Record**, **Control Plane View**, **Control Plane Publisher**, **Advisory staleness**. This spec uses those terms precisely.

## 5. The shared record base

A thin base carries identity + liveness so the view's staleness filter depends on exactly one contract and never on a concrete record type. Every concrete plane's record extends it.

```python
class ControlPlaneRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")    # tolerant reader (additive forward-compat)
    schema_version: int                          # default set per subclass; a major bump => old readers skip
    node_id: str                                 # = group key (kept in the value: self-describing wire record)
    worker_id: str                               # = member key (the instance; Worker.id)
    started_at: AwareDatetime                    # this instance's boot time -> uptime + restart detection
    last_heartbeat_at: AwareDatetime             # liveness basis; refreshed every heartbeat tick
    heartbeat_interval: float                    # the WRITER's cadence (seconds); lets any reader judge
                                                 # staleness without knowing the writer's config (§9)
```

The worker stamps all five identity/liveness fields; a concrete record adds only its use-case content. Carrying `heartbeat_interval` **on the record** is deliberate (the C1 decision): staleness is judged on the read side, and a reader is routinely a *different* process than the writer — and may be a config-less out-of-process reader — so the staleness threshold cannot live only in the reader's config; it must travel on the wire (§9, §11). The worker hands the stamped fields to the content factory as an **identity envelope**:

```python
class ControlPlaneIdentity(BaseModel):           # frozen; built fresh by the worker each tick
    node_id: str
    worker_id: str
    started_at: AwareDatetime
    last_heartbeat_at: AwareDatetime
    heartbeat_interval: float                    # from the publisher's ControlPlaneConfig
```

Schema evolution follows the existing capability pattern: per-record `schema_version` + a tolerant reader for additive fields; a major bump makes old readers treat newer records as "node invisible" (skip + log) rather than crash. The base provides `last_heartbeat_at` (the liveness basis); a content-bearing record that needs to distinguish "alive but content unchanged" adds its own `content_updated_at` (e.g. capability — see the migration spec), refreshed only when content actually changes, never by the heartbeat.

## 6. The plug-in interface — `@advertises`

A node **type** declares what it advertises with a class-level marker decorator, modelled directly on the existing `@handler` / `RegistryMixin` machinery (`calfkit/_registry.py`):

```python
class SomeNode(BaseNodeDef):

    @advertises(topic="calf.capabilities", record=CapabilityRecord)
    def _capability_record(self, identity: ControlPlaneIdentity) -> CapabilityRecord:
        return CapabilityRecord(
            **identity.model_dump(),                 # worker-stamped identity + liveness
            dispatch_topic=self.subscribe_topics[0],
            tools=self._current_tools,               # per-instance, node-owned content
            content_updated_at=self._tools_changed_at,
        )
```

(The example uses `CapabilityRecord` illustratively — it is a future adopter's record, not part of v1. `dispatch_topic`/`tools`/`content_updated_at` are concrete-record fields the substrate base knows nothing about; `subscribe_topics[0]` is the addressable-node convention and is only meaningful for a node with a public inbox — not every plane's record is addressable, e.g. the operator ACL plane.)

Design properties (all locked):

- **Per node type, structural.** The `(topic, record)` binding lives at class scope. Instances of one type therefore advertise to the *same* topic with the *same* schema — required, because a topic's view decodes exactly one `R`; instances differ only in content and identity. The binding is collected per subclass via `__init_subclass__` (the `@handler` pattern: marker-not-wrapper — the method is returned unchanged; MRO-merged so a subclass override wins; fail-fast at class-definition on a duplicate topic, raising `RegistryConfigError`).
- **One topic ⇒ one record schema (a global, deployment-time invariant).** A topic's `ControlPlaneView` decodes exactly one `R`, so *every* advert on a given topic — across *all* node types, not only instances of one type — must use the same record schema. The per-class duplicate-topic check above cannot catch two *different* types colliding on one topic with different schemas; that is a deployment-time contract (documented, not policed, per the framework's "document, don't police" rule). The failure mode if violated is silent: a foreign-schema payload fails the view's decode and is poison-skipped one layer below, dropping that node from the view.
- **Opt-in.** A node is on a plane **iff** it declares an `@advertises` method for it. A node with none contributes nothing. A type may declare more than one advert (one per plane it participates in).
- **Content-only contributor.** The decorated method is a `factory(identity) -> R`: the worker builds and passes the identity envelope, the node composes a complete, valid record in one shot — no placeholder fields, no "these are ignored" contract.
- **Worker is use-case-blind.** The worker calls the factory and publishes the result opaquely; it never imports any concrete record type.

**Content propagation is pull-only.** The worker loop calls each advert factory once per heartbeat tick, so a content change (a node mutating its own cached content, e.g. on an MCP `tools/list_changed`) lands at the **next tick** (≤ `heartbeat_interval`). Pull is *correct* (the factory reads the node's current content) and supports the liveness/content split (the worker stamps `last_heartbeat_at` fresh every tick while the record's own `content_updated_at` only moves on a real change). All writes funnel through the publisher (§7), so a future `publish_now()` nudge for immediate propagation is a clean additive extension — deferred until a consumer proves it needs sub-tick latency (only content-mutating planes like capability would; agent cards are effectively static).

> **Contract — a factory must not re-stamp content timestamps on a tick.** Because the substrate re-runs the factory on *every* tick (unlike the shipped capability plane, which caches the built record and only re-stamps its timestamp), any "content currency" field a concrete record carries — e.g. capability's `content_updated_at` — must be returned from **a value the node tracks and advances only when its content actually changes** (the `self._tools_changed_at` in the example), never `now()` computed inside the factory. Writing `now()` in the factory re-rots the heartbeat-masks-content-staleness defect (CRITICAL-3) on every tick. The worker is use-case-blind and cannot enforce this (it does not know which field is "content"), so it is a documented contract, tested at the substrate layer with a content-bearing test record (§14).

## 7. Write side — `ControlPlanePublisher`

One **worker-owned** publisher runs **one** heartbeat loop and **one** ordered tombstone pass for every hosted node, per ADR-0011.

**Auto-registration, sequenced (zero user wiring).** The wiring follows the existing `_maybe_register_capability_view` pattern, stated explicitly because the substrate spans multiple topics:

1. During `register_handlers` (pre-broker), the worker scans the hosted node **types** for `@advertises` bindings (available at class-definition time via `__init_subclass__`, like `_handlers`).
2. If any exist, it computes the **union of advert topics** and — **synchronously here, before the resource phase** — registers the publisher plus one writer `@resource` per distinct topic (`self.resource(key)(...)`, idempotent by name). Registering the writer resources at this point is what guarantees they exist when the resource phase runs.
3. The resource phase opens each `GroupedKafkaTableWriter` (`.json(...)`, `ensure_topic` gated on provisioning), landing them in the worker bag.
4. `after_startup` publishes each advert once, then spawns the single heartbeat loop.

Adverts are bound at `register_handlers` and `_prepared` is latched there, so a node added via `add_nodes()` **after** the worker is prepared does **not** join a plane (no writer is registered for its topics) — the same boundary the capability path already has. Declare all advertising nodes before the worker starts.

**One writer per distinct topic.** The publisher opens **one `GroupedKafkaTableWriter` per distinct advert topic** across all hosted nodes; the single loop routes each `(node, advert)` write to that advert's topic writer.

**Instance-keyed publish.** Per tick, for each hosted node and each of its adverts, the publisher builds a fresh `ControlPlaneIdentity` (`node_id=node.node_id`, `worker_id=worker.id`, `started_at`, `last_heartbeat_at=now`, `heartbeat_interval` from config), calls the advert factory, and writes:

```python
writer.set(group=node_id, member=worker_id, value=record)
```

**Lifecycle** (lifting the proven `MCPToolboxNode` structure to the worker, applied uniformly):

- *resource phase* — open the per-topic writers (`GroupedKafkaTableWriter.json(...)`, `ensure_topic` gated on provisioning).
- *`after_startup`* (broker live) — publish each advert once, then spawn the single heartbeat loop.
- *`on_shutdown`* (broker live, before stop) — set the shutdown flag **synchronously first** (no await above it), cancel-and-await the loop, then run the tombstone pass. The tombstone target is the **declared cross-product** `{(node_id, topic) : node ∈ hosted, advert ∈ type(node)'s adverts}` — `writer.delete(group=node_id, member=worker_id)` on each, **idempotent** (deleting a key never written is a harmless no-op), so the publisher needs no per-key success bookkeeping that could drift from the loop's in-flight state. **Resurrection-safety invariant:** once the flag is set, no new `set()` may be issued by *either* the tick loop *or* a future `publish_now()`, and the flag is set synchronously before the loop is cancelled — generalising `MCPToolboxNode._tombstone_on_shutdown` so a straggler write can never land after its `delete()`.

**Crash behaviour** (unchanged, accepted): a hard crash skips `on_shutdown`, so the record is **not** tombstoned and instead goes stale (§9).

**Bootstrap & provisioning.** Bootstrap servers are derived via the existing chain (`worker._derive_bootstrap_servers()` → `client.server_urls` → broker connection kwargs → explicit `ControlPlaneConfig.bootstrap_servers`). Topic compaction (`cleanup.policy=compact`) is delegated to ktables `ensure_topic` in dev/CI (provisioning enabled) or to ops in production (provisioning off by default), consistent with how the capability topic is handled today — calfkit's own provisioner cannot set compaction on a single topic in isolation (its `topic_configs` apply flat to every data topic it creates), whereas ktables `ensure_topic` defaults to `cleanup.policy=compact` for exactly this topic.

## 8. Read side — `ControlPlaneView[R]`

A thin, typed wrapper over a `GroupedKafkaTable`, generic over `R` and knowing only the `ControlPlaneRecord` base (for `last_heartbeat_at`). Per ADR-0010 the storage is instance-keyed but the view is **collapsed**:

```python
class ControlPlaneView(Generic[R]):
    # collapsed, liveness-aware reads (one live record per node; worker_id hidden)
    def online_nodes(self) -> set[str]: ...        # node_ids with >= 1 live instance
    def get(self, node_id: str) -> R | None: ...   # one live record, or None
    def snapshot(self) -> dict[str, R]: ...        # node_id -> live record (the registry map)

    # health passthrough (closes frozen-view blindness)
    @property
    def status(self) -> TableStatus: ...           # unstarted|loading|caught_up|degraded|failed
    @property
    def failure(self) -> BaseException | None: ...
    @property
    def is_caught_up(self) -> bool: ...
    async def barrier(self, timeout: float | None = None) -> bool: ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
```

- **Collapse rule (precise).** For each group (`node_id`): first **filter** members to those that are decodable, **supported** (`schema_version ≤` the reader's version for `R`), and **live** (`age ≤ stale_after`, §9). If that filtered set is empty the node is **not online** and `get()` returns `None`; otherwise `get()` returns `max(filtered, key=last_heartbeat_at)`. `online_nodes()` is the set of groups whose filtered set is non-empty, and `snapshot()` maps each such group to its collapsed record. This makes `get(g) is not None ⟺ g ∈ online_nodes()` an **invariant** — the order is *filter-then-tie-break*, never tie-break-then-filter (the latter could select a member the filter rejected, making `get()` and `online_nodes()` disagree about the same node). Ties break on the *writer's* `last_heartbeat_at`, which is best-effort under clock skew — acceptable because replicas advertise equivalent content. The result is `Mapping`-shaped (`node_id → record`).
- **Schema-skip lives in the view, not the consumer.** Because the collapsed view returns `R` directly (with no per-consumer resolution layer to host it), version tolerance is folded into the same filter as staleness: a member whose `schema_version` exceeds the reader's version for `R` is skipped (and logged), so `get()`/`snapshot()` only ever return supported, live records — version tolerance is a substrate concern, not something each adopter re-derives. (A payload that fails to *decode* at all — an incompatible schema on the topic — is poison-skipped by ktables one layer below and logged as a decode error; see the one-topic-one-schema invariant in §6.)
- **Advisory staleness is the view's one piece of domain logic** (§9). `online_nodes`/`get`/`snapshot` are live-only by construction.
- **Gating-agnostic.** The view exposes catch-up signals but does **not** impose boot-gating. Each consumer chooses: a capability view gates its `@resource` on catch-up (agents need tools at boot); a discovery view loads in the background. Gating is a consumer-wiring decision, not a property of the view.
- **Consumer-instantiated; not auto-wired in v1.** Because v1 has no concrete plane, nothing opens a view in production — v1 ships the view as a class exercised by tests. Each adopter PR opens its own view over its own topic (capability over `calf.capabilities`, etc.). (Contrast the publisher, which *is* auto-wired so any advertiser works out of the box.) The consumer supplies the view's bootstrap servers (its own `ControlPlaneConfig.bootstrap_servers`, or its worker's `_derive_bootstrap_servers()`); a fully out-of-process reader needs only bootstrap + topic and **no writer config at all**, because the staleness cadence rides on the records (§9).
- **Layering rule preserved.** The view is typed as a `Mapping`-like abstraction; consumers never import ktables, and a plain dict satisfies the type for unit tests.

Per-instance access (`instances(node_id) -> dict[worker_id, R]`) is intentionally **not** exposed in v1; it is a purely additive future addition (the grouped storage already holds the data) for a use case that needs all replicas, e.g. load-balancing. Note that capability tool-resolution does **not** need it — the collapsed `get(toolbox_id)` *is* the "pick one live instance" reader-side merge. The only deferred consumers are the ops fleet view and replica-aware load-balancing; the capability-migration spec's current `instances(...)` wording predates the collapse decision and should be reconciled to `get(...)` when that PR lands.

## 9. Staleness (advisory)

A tombstone fires only on **clean** shutdown; a crash leaves the record in place and ktables never evicts. So "is this node online?" is a reader-side judgement, not a transport guarantee:

- Each record carries `last_heartbeat_at` (refreshed every tick) **and `heartbeat_interval`** — the writer's cadence (§5).
- The view treats a member as **stale** when `age = now - last_heartbeat_at > stale_after`, where `stale_after` defaults to `N × record.heartbeat_interval` (`N = 3`) — **derived from the cadence carried on the record**, so a reader judges staleness against the *writer's* declared interval regardless of the reader's own config (and a config-less out-of-process reader works with no extra knowledge). A consumer may set an explicit `stale_after` override/floor, but the default needs no reader/writer config agreement. The collapsed reads exclude stale members so a consumer never selects a probably-dead instance.
- The view **never deletes** — there is no TTL and no sweeper.

Documented assumptions (not enforced): `last_heartbeat_at` is the publisher's wall clock and `age` is the reader's; the generous `N = 3` threshold sits well above normal NTP skew. Between a crash and the threshold, a dead instance still appears (filtered only after `stale_after`); real crash *recovery* (failing pending work) is the error-propagation layer's concern, not this substrate's.

Wiring `status`/`failure` through the view closes **frozen-view blindness**: a degraded/failed reader is observable rather than silently serving a stale snapshot.

## 10. Storage & keying

Per ADR-0010: storage is a `GroupedKafkaTable`, `group = node_id`, `member = worker_id`. Each writer owns its own key (single-writer-per-key by construction) — no read-modify-write, no lost-update race, independent tombstones. This is what makes correct multi-replica operation possible and is why the keying is fixed now (it is the on-the-wire composite message key; changing it later is a topic re-key). The composite key uses ktables' default length-prefixed injective codec; the projection to a `node_id → record` map is compute-on-read (fine at control-plane cardinality — one small record per node-instance, compacted). A future ktables change-stream would later enable an incremental index, but is not required.

## 11. Config — `ControlPlaneConfig`

A new generic, worker-level config holds the knobs (topics are **not** here — they live in `@advertises`):

```python
@dataclass(frozen=True)
class ControlPlaneConfig:
    # WRITE side (publisher)
    heartbeat_interval: float = 30.0     # loop cadence; also STAMPED on every record (§5) so a reader
                                         #   can judge staleness without knowing this value
    # READ side (view)
    stale_after: float | None = None     # optional override/floor; None => derive N * record.heartbeat_interval
    catchup_timeout: float = 30.0        # bound on the view's catch-up gate
    # BOTH sides
    bootstrap_servers: str | None = None # split-cluster escape hatch; else derived from the client
```

The knobs split by side: `heartbeat_interval` is the publisher's (and is stamped on records, so it reaches readers); `stale_after` and `catchup_timeout` are the view's; `bootstrap_servers` applies to whichever side opens a table. A reader in a different process than the writer needs **no** agreement on `heartbeat_interval` — it arrives on the record (the C1 decision).

`ControlPlaneConfig` is introduced *alongside* the still-live `MCPDiscoveryConfig` (which keeps serving the un-migrated capability plane). That temporary duplication is intentional and collapses when the capability migration PR folds `MCPDiscoveryConfig` into `ControlPlaneConfig`.

## 12. Naming

The `ControlPlane*` family (not `Discovery*`): the substrate is broader than discovery — the same machinery carries the future access-policy and reconfig planes, which are not discovery. "Discovery" is the v1 *use case*; "control plane" is the *category*. Resource-bag keys live under the `calfkit.controlplane.*` namespace.

## 13. What this unlocks — and how adopters use it

The substrate is deliberately generic so each adopter is mechanical: **define a record subclass, declare an `@advertises` factory, and (on the read side) open a `ControlPlaneView` over the topic.**

- **Capability-plane migration** ([spec](./capability-plane-migration-and-ops-spec.md)) — `CapabilityRecord(ControlPlaneRecord)` with `tools` + `content_updated_at`; `MCPToolboxNode` replaces its bespoke writer/heartbeat/tombstone with one `@advertises("calf.capabilities", CapabilityRecord)` factory; the worker's capability view becomes a (boot-gated) `ControlPlaneView[CapabilityRecord]`. Instance-keying fixes the replica flap; the collapsed `view.get(toolbox_id)` keeps the agent-side resolution API unchanged. (Reconcile that spec's §4.2 `instances(...)` wording to `get(...)`; its ops fleet view needs the deferred `instances()`/annotated reads, added additively when ops lands.)
- **Agent discovery** ([spec](./agent-discovery-and-ask-spec.md)) — `AgentCard(ControlPlaneRecord)` with `ask_topic` + `skills`; a discoverable agent declares `@advertises("calf.agents", AgentCard)`; peer selection opens a `ControlPlaneView[AgentCard]`. (The Ask messaging layer is separate.)
- **Operator ACL plane (#231)** and **runtime-reconfig plane (#164)** — reuse the read-side `ControlPlaneView`; their write side differs (operator-authored, not self-published heartbeats), which is exactly why per-use-case topics + a generic view (rather than one shared topic) is the right factoring.

## 14. Testing

- **Unit (no broker):**
  - `ControlPlaneRecord` / `ControlPlaneIdentity` schema versioning + tolerant reader.
  - `@advertises` collection: per-type binding, MRO override, duplicate-topic → `RegistryConfigError`, marker-not-wrapper (the decorated method stays directly callable).
  - The view's collapse + staleness filter over a fake grouped table (a plain dict): stale exclusion, the filter-then-tie-break ordering, the `get(g) is not None ⟺ g ∈ online_nodes()` invariant, and skipping a member whose `schema_version` exceeds the reader's.
  - **Staleness derives from the record's `heartbeat_interval`** — a reader configured with a *different* `heartbeat_interval` still judges a writer's records correctly.
  - **Liveness/content split (the §6 contract):** an unchanged-content tick keeps a content-bearing test record's `content_updated_at` fixed while `last_heartbeat_at` advances; a content change moves both.
  - The publisher's tick (publish-once + heartbeat cadence) and ordered tombstone against a fake writer (records `set`/`delete` calls), including the flag-first ordering and the declared-cross-product tombstone set across multiple topics.
- **Integration (kafka lane, real broker):**
  - advertise → `ControlPlaneView` reflects it after `barrier()`.
  - clean shutdown tombstones the instance; the view drops it.
  - **2-worker same-node replica regression** (the ADR-0010 / CRITICAL-1 proof): two workers host the same `node_id`; one shuts down cleanly; the node stays online via the surviving instance.
  - degraded/failed reader surfaces via `status`/`failure`.
- v1 ships a **test-only** `ControlPlaneRecord` subclass and a test `BaseNodeDef` declaring `@advertises` as the integration vehicle (there is no production advertiser in v1, so the publisher is dormant in real deployments until the first adopter).
- ktables' own semantics (catch-up gate, tombstones, reader death) are **not** re-tested in calfkit — they are covered by the ktables suite; calfkit tests its policy layers (collapse, staleness, advert collection, publisher ordering) only.

## 15. Open questions

- Exact resource-bag key strings under `calfkit.controlplane.*` (e.g. the publisher key, per-topic writer keys).
- Whether `@advertises` should reuse `RegistryMixin` directly or get a parallel sibling mixin (same mechanism, separate registry) — an implementation detail to settle in the build.
- The collapse selection (live-filter then most-recent `last_heartbeat_at`) is specified in §8; revisit only if a real adopter needs a different selection policy.
