# MCP Capability Discovery — Design Spec

**Status:** IMPLEMENTED, then migrated onto the control-plane substrate (2026-06-19). ⚠️ The §3.3/§8.5 node-owned writer + heartbeat + tombstone, the flat `mcp.capabilities` topic, `MCPDiscoveryConfig`, and the `strict` selector flag are **superseded** — see ADR-0012 and [`mcp-capability-substrate-migration-plan.md`](./mcp-capability-substrate-migration-plan.md). What still holds: the wire model (`CapabilityRecord`/`CapabilityToolDef`), the `include` trust boundary, and per-turn agent resolution. The plane is now instance-keyed on `calf.capabilities` and read through a `ControlPlaneView`.
**Depends on:** the `ToolBinding`/`ToolProvider` contract on `feat/mcp_bridge_v2` (`calfkit/models/tool_dispatch.py`)

## 1. Problem

An `MCPToolboxNode` node fronts one MCP server and services tool calls sent to its
dispatch topic (`mcp_server.<name>`). Its tools, however, are only knowable at
runtime: `list_tools` is an async RPC against a live MCP session that exists
only after the toolbox's resource phase. Agents declare tools at construction
time, synchronously, and may run in a **different worker process** than the
toolbox. `MCPToolboxNode.tool_bindings()` therefore raises `NotImplementedError`
today.

This spec defines how runtime-discovered MCP tools become `ToolBinding`s
available to any agent in the cluster.

## 2. Decision summary

| # | Decision | Choice |
|---|----------|--------|
| 1 | Topology | Cross-process from day one: toolbox and consuming agent may live in different workers |
| 2 | Mechanism | A single cluster-wide **compacted capability topic**; toolboxes publish their tool list keyed by toolbox id (control plane). Live per-turn queries rejected (§7) |
| 3 | Reader | One subscriber + one materialized dict (**Capability View**) per agent-hosting worker, stored in the worker resource bag; agents read it per turn |
| 4 | Agent declaration | Pass the `MCPToolboxNode` instance itself in `tools=[...]` (same object that gets deployed via `add_nodes` — the tool-node pattern); `toolbox.select(include=[...], strict=True)` for scoped/strict. Resolved per turn as a deferred selector |
| 5 | Boot ordering | Worker gates serving on view catch-up (bounded wait ~30s; on timeout, serve + loud log) |
| 6 | Unresolved selector | Warn + run the turn degraded; `strict=True` fails the turn before the model runs |
| 7 | Wire format | Calfkit-owned versioned pydantic model; never the vendored `ToolDefinition` |
| 8 | Liveness | Toolboxes heartbeat by re-publishing their record (~30s). v1 readers only **log** staleness; behavioral use of staleness (hiding tools, strict-fail) deferred |
| 9 | Removal | Toolbox publishes a tombstone on **clean shutdown**; crash leaves the record standing (staleness signals it). See accepted trade-off in §6 |
| 10 | Toolbox boot failure | Worker boot fails (existing resource-phase semantics: session `initialize()` errors abort startup) |

## 3. The control plane

### 3.1 Topic

One topic for the whole cluster (working name `mcp.capabilities`),
`cleanup.policy=compact`. Compaction is broker-side: the broker retains the
latest record per **key**; producers and consumers need nothing special.
Steady-state content ≈ one record per toolbox — a small key-value table.

Provisioning: **needs a dedicated creation path.** Verified during design:
`ProvisioningConfig.topic_configs` is a flat config dict applied to *every*
data topic the provisioner creates (`provisioner.py:170-172`) — putting
`cleanup.policy=compact` there would compact all data topics. The capability
topic must be created with its own config (a dedicated ensurer call, or
extend `topic_configs` to per-topic-name keying). (Doc bug noted:
`provisioning/config.py:36` says "Per-topic config overrides", which
misleadingly suggests per-topic-name targeting.)

Creation responsibility: the dangerous path is **broker auto-creation**
(implicit materialization with default configs — `cleanup.policy=delete`),
which is always forbidden; an *explicit* idempotent ensure with the correct
compact config is safe from either side. Dev/CI (provisioning enabled):
**both** the toolbox-hosting worker and agent-side readers ensure the topic at
startup via the same dedicated ensurer (`TopicAlreadyExists` → no-op;
CreateTopics is atomic, so a boot race is benign) — this removes any
bring-up-order requirement: a reader that boots first gates instantly on the
empty topic and picks up the toolbox's record later as a live update.
Production (provisioning disabled, the default): nobody creates; the topic is
ops-governed infrastructure (consistent with `docs/topic-provisioning.md`)
and a missing topic fails reader resource setup loudly. Creation is one-time
per cluster: topics are durable metadata and survive broker restarts; only
storage loss (ephemeral dev containers) requires re-creation — which the
idempotent ensure-on-boot heals. A mis-created policy is repairable in place
(`cleanup.policy` is a dynamic topic config).

Data plane (per-toolbox call topics, `mcp_server.<name>`) is unchanged and
strictly separate: the capability topic carries metadata about what exists,
never tool calls or workflow state.

### 3.2 Capability Record (wire format)

Calfkit-owned models — **not** the vendored `ToolDefinition`, whose fields
change with vendor bumps while compacted records live indefinitely:

```python
class CapabilityToolDef(BaseModel):
    name: str
    description: str | None = None
    parameters_json_schema: dict[str, Any]

class CapabilityRecord(BaseModel):
    schema_version: int = 1
    toolbox_id: str
    dispatch_topic: str
    tools: list[CapabilityToolDef]
    published_at: AwareDatetime
```

Reader policy: tolerant (ignore unknown fields); skip + log records with a
newer `schema_version`. Kafka message key = `toolbox_id` (bytes). Tombstone =
null value under the key.

### 3.3 Writer (toolbox)

The toolbox publishes its record:

1. **On connect** — in `after_startup` (the serving phase exists precisely
   because it has the live broker): `list_tools()` → `CapabilityRecord` →
   keyed publish.
2. **On change** — a `tools/list_changed` handler registered on the MCP
   `ClientSession` (the pydantic-ai pattern; it is the only surveyed framework
   that consumes this notification) re-lists and re-publishes.
3. **Heartbeat** — re-publish the same record every ~30s with a fresh
   `published_at`. No new message type; compaction absorbs the churn.
4. **Tombstone on clean shutdown** (`on_shutdown`). A crash publishes
   nothing — the record stands and goes stale.

Boot failure: session setup lives in the resource phase, where errors abort
worker startup. Kept deliberately — a crash-looping worker is visible to ops,
and agents elsewhere keep using the last-published record meanwhile.

### 3.4 Reader (worker)

A worker hosting ≥1 agent with MCP selectors registers **one** extra
subscriber on the capability topic:

- `auto_offset_reset="earliest"`, **unique per-process group id** — every
  worker replica must replay the full topic; sharing a group would partition
  records across replicas.
- Handler: last-write-wins upsert `view[toolbox_id] = (record, received_at)`;
  delete on tombstone. LWW also absorbs not-yet-compacted duplicates during
  replay.
- The view lives in the worker resource bag (the faust-table equivalent,
  ~50 lines; no faust dependency).

**Catch-up gate:** during startup, before consumers serve, block until the
subscriber position reaches the topic end offset (an empty topic catches up
instantly, so first deploys don't wait). Bounded by ~30s; on timeout, log
loudly and serve anyway — a lagging metadata topic degrades MCP tools but must
not crash-loop a worker whose other nodes are healthy.

**Compaction is an optimization, not a correctness dependency.** The reader is
LWW-by-key, so replaying an uncompacted topic yields the identical final view —
this is forced anyway, because Kafka never compacts the active segment and
compaction lags the dirty ratio, so duplicates reach every reader regardless
(and `TestKafkaBroker`, which doesn't compact, exercises the real semantics).
Degradation without compaction is replay *size*, not wrongness: heartbeats
(~2,880 records/toolbox/day) mean a misconfigured `cleanup.policy=delete` topic
still always holds a fresh record inside any sane retention window — boots get
slower (linearly in retention × toolboxes), tools never get lost. The genuinely
non-trivial implementation work is the boot choreography, not the dict: the
end-offset catch-up check (reaching the aiokafka consumer under FastStream —
see §8), and consumer identity (prefer `group_id=None` — no committed offsets,
always-earliest — over minting unique group ids that accumulate as broker
metadata garbage).

## 4. The agent side

### 4.1 Selector

```python
agent = Agent(
    "researcher",
    subscribe_topics="researcher.input",
    model_client=client,
    tools=[
        weather_tool,                                  # ordinary ToolProvider
        MCPTools("docs_server", include=["search"]),   # selector placeholder
    ],
)
```

`MCPTools` is a declarative reference, not a `ToolProvider`: it names a toolbox
and optionally filters tool names (the filter is the trust boundary — a server
suddenly exposing new tools cannot silently enlarge an agent's tool surface).
`_normalize_tools` keeps selectors aside rather than expanding them.

### 4.2 Per-turn resolution

At the top of `run()` — where `tools_registry` is already rebuilt per turn for
overrides — each selector resolves against the Capability View into
validator-less `ToolBinding`s (`tool_def` rebuilt from `CapabilityToolDef`,
`dispatch_topic` from the record) and merges with static ctor tools. Fresh
every turn; no agent mutation; no RPC.

Unresolved (toolbox absent from view, or `include` name not advertised):

- default: log a warning, run the turn with what resolved;
- `MCPTools(..., strict=True)`: fail the turn before the model runs.

Staleness (v1): if the resolved record's heartbeat is older than the
threshold, log it. No behavioral change — hiding stale tools from the model
and strict-on-stale are explicitly deferred to a v2 decision.

## 5. Failure-mode walkthrough

| Event | Control plane | Agent experience |
|---|---|---|
| Toolbox crashes | Record stands; heartbeats stop | Tools still offered; calls fail visibly (`FailedToolCall`); staleness logged after threshold |
| Toolbox clean shutdown (deploy) | Tombstone, then re-publish on boot | Tools vanish for the deploy window, then return (accepted trade-off, §6) |
| Toolbox decommissioned for good | Tombstone (clean shutdown) | Selector unresolved: warn + degrade, or strict-fail |
| MCP server changes tools | `tools/list_changed` → re-publish | Next turn sees the new list |
| Agent worker boots | Replays topic, gated ~30s | First turn already has full view |
| Capability topic lagging | Gate timeout → serve + loud log | Possibly missing MCP tools until convergence; warnings on resolve |

## 6. Accepted trade-off: tombstone-on-clean-shutdown

A rolling deploy (SIGTERM → clean shutdown) tombstones the record, so the
toolbox's tools vanish cluster-wide until the new instance re-publishes —
while a *crash* leaves the advertisement standing. Graceful restarts are thus
briefly more disruptive than crashes. Accepted consciously (self-cleaning, no
decommission CLI needed for v1), with the explicit ideal that advertisement
should track liveness — which the heartbeat field enables a future v2 to act
on. Revisit if deploy-window tool gaps hurt in practice.

## 7. Rejected alternatives

- **Live `list_tools` query per turn** (the in-process pattern of pydantic-ai
  / OpenAI Agents SDK): over Kafka this couples every agent turn to every
  toolbox's availability, adds 2×N hops of pre-model latency, and requires new
  fan-in state machinery in `run()`. Cached variants converge back to a
  change-notification topic — i.e., this design with extra steps. See ADR-0001.
- **Per-toolbox capability topics**: readers would need the topic list up
  front (defeating discovery) and adding a toolbox would churn subscriptions.
  One topic + keys gives compaction per toolbox for free.
- **Faust tables (forked OR embedded as an app in the worker lifecycle)**:
  rejected on both buy-vs-build axes. *Vendor health* (verified on PyPI +
  GitHub 2026-06-09): `faust` dead since 2020-02; `faust-streaming` last
  released 0.11.3 on 2024-08-23 with exactly one commit in the ~16 months
  since (2026-03-30: an aiokafka 0.13 compat fix — for the aiokafka version
  calfkit ships — that remains unreleased, so the PyPI package is broken
  against our lockfile); 147 open issues; drags `mode-streaming`.
  *Interface fit*: faust tables own their internal
  `{app_id}-{table}-changelog` topic — toolboxes publish to OUR topic in OUR
  versioned format, so embedding faust means either writing into faust
  internals (unsupported) or a copy-agent that double-stores every record
  while the table reduces to `table[k] = v`; and standard tables SHARD keys
  across the consumer group (each worker a slice — wrong), so every worker
  would need its own embedded app (`GlobalTable` + mode supervisor + own
  consumers + default-on aiohttp server) inside FastStream's lifecycle. The
  one thing faust would contribute — recovery/"caught-up" signaling — is a
  single position-vs-end-offsets check here. (Quix Streams, the maintained
  alternative, has the same partition-sharded framework-owned-changelog
  shape; the ecosystem has no maintained Python `GlobalKTable` equivalent —
  consume-from-earliest-into-a-map is the standard non-JVM answer.) The LWW
  dict is ~50 lines; compaction needs zero client-library support (verified:
  FastStream subscriber `auto_offset_reset` + keyed publish).
- **Other Python Kafka-state libraries** (full survey 2026-06-09: quix-streams,
  bytewax, pathway, kstreams, fluvii, winton-kafka-streams, streamiz): none
  fits, for the same two structural reasons as faust. Every library with
  state implements *partition-sharded* KTable-style stores (no GlobalKTable
  equivalent exists in maintained Python) backed by *framework-owned*
  changelog/recovery storage — none materializes a view from an existing
  externally-written topic. The three healthy engines (quix-streams 3.23.7
  2026-05; pathway 0.31.0 2026-05; bytewax repo-active/release-stale 2024-11)
  all own the process and run librdkafka or embedded Rust, not asyncio/
  aiokafka. The one embeddable aiokafka-native option, kstreams (0.30.1
  2026-01), has no store abstraction at all (explicit README TODO). fluvii
  (2023) and winton-kafka-streams (2019) are dead; streamiz is .NET.
  Consume-from-earliest into a per-process LWW dict is the standard non-JVM
  answer (it is how Kafka Connect materializes its own config topic).
- **TTL-expiry instead of tombstones**: rebuilds liveness-checking as hard
  state removal; heartbeat-as-information (reader-side judgment) kept instead.

## 8. Implementation design

### 8.1 Module map

| Piece | Location | Contents |
|---|---|---|
| Wire models | `calfkit/models/capability.py` | `CapabilityToolDef`, `CapabilityRecord` (§3.2), `CAPABILITY_SCHEMA_VERSION` |
| Table + writer | **`ktables` (PyPI dependency, >=0.1.1)** | `KafkaTable[CapabilityRecord]` (the Capability View) + `KafkaTableWriter` (toolbox publisher transport) |
| Selector protocol | `calfkit/models/tool_dispatch.py` | `ToolSelector` protocol (deferred per-turn resolution) — keeps `nodes/agent.py` free of any `calfkit.mcp` import |
| MCP selector | `calfkit/mcp/mcp_toolbox.py` | `MCPToolboxNode` itself implements `ToolSelector`; `.select(include=..., strict=...)` returns a frozen internal scoped selector (no user-facing selector class) |
| Toolbox publisher | `calfkit/mcp/mcp_toolbox.py` | record publish on connect, `tools/list_changed` handler, heartbeat task, shutdown tombstone |
| Worker wiring | `calfkit/worker/worker.py` (+ config) | auto-registered worker `@resource` when any hosted agent declares selectors |

### 8.2 The table layer is the `ktables` package

The materialized-view mechanics were extracted, hardened (3-round adversarial
review), and published as the standalone `ktables` package
(https://pypi.org/project/ktables/ — grown from this design's PoC; calfkit
now depends on it rather than vendoring). It provides everything §3.4
requires: group-less full-replay reader with the end-offsets catch-up gate
(`catchup_timeout`, serve-degraded on expiry), LWW upsert / null-value
tombstones, poison-record tolerance, reader-death observability
(`status` ∈ unstarted/loading/caught_up/degraded/failed + `failure`),
idempotent `acks=all` writer, and explicit idempotent `ensure_topic()`.

The Capability View is therefore literally `KafkaTable[CapabilityRecord]`
(constructed via `KafkaTable.json(model=CapabilityRecord)`), and calfkit owns
only: the wire model, `MCPDiscoveryConfig`, the lifecycle wiring (§8.3), the
toolbox publishing policy (§8.5), and selector resolution (§8.4). Calfkit-side
version-tolerance (skip + log records with newer `schema_version`) lives in
the selector-resolution layer, since ktables decodes any valid
`CapabilityRecord` JSON.

### 8.3 Reader-side wiring (worker)

**No coordination on toolbox ids — by design.** The table is an *unfiltered*
materialization of the whole capability topic: every toolbox's record,
cluster-wide, regardless of what any hosted agent asked for (Kafka cannot
subscribe to a subset of keys, and the data is tiny metadata — §"everyone
holds the full map"). Agents never register interest in ids; each one simply
looks up its own selectors' keys per turn. The only cross-node decision is
binary — does ANY hosted agent declare selectors? — which gates whether the
worker creates the resource at all. Nothing to negotiate, no refcounts, no
per-id subscription management.

Wiring sequence:

1. **Agent ctor**: `_normalize_tools` returns `(bindings, selectors)`;
   selectors (objects implementing `ToolSelector`) are stored on
   `agent._tool_selectors`. Checked BEFORE the `ToolProvider` structural
   check, so an `MCPToolboxNode` in `tools=[...]` is never mistaken for a
   sync provider.
2. **`Worker.register_handlers`**: scan hosted nodes for non-empty
   `_tool_selectors`. If any (and the resource isn't already registered —
   names are unique per owner, so the guard is a lookup), register a
   worker-level resource programmatically:

   ```python
   if any(getattr(n, "_tool_selectors", None) for n in self._nodes):
       self.resource(CAPABILITY_VIEW_KEY)(self._capability_view_resource)

   async def _capability_view_resource(self, ctx) -> AsyncIterator[Mapping[str, CapabilityRecord]]:
       cfg = self._mcp_discovery  # MCPDiscoveryConfig
       table = KafkaTable.json(
           bootstrap_servers=cfg.bootstrap_servers or self._broker_url(),
           topic=cfg.topic, model=CapabilityRecord,
           catchup_timeout=cfg.catchup_timeout,
           ensure_topic=self._provisioning_enabled(),   # §3.1
       )
       await table.start()    # replay + bounded gate; serve-degraded built in
       yield table            # lands in the WORKER bag → merged into every
       await table.stop()     #   node's ctx.resources via _effective_resources
   ```

3. **Per turn**: the agent reads
   `ctx.resources[CAPABILITY_VIEW_KEY]` — present in every hosted node's
   view because worker resources merge under node resources. A missing key
   with selectors declared (agent driven outside a worker, e.g. a bare-`run()`
   unit test) is a warn-and-degrade, `strict=True` raises.

**Layering rule that falls out: the agent layer never imports ktables.** The
resolution code types the view as `Mapping[str, CapabilityRecord]` —
`KafkaTable` satisfies it, and so does a **plain dict**, which is the entire
agent-side test strategy (no broker, no fakes framework: seed a dict, run a
turn). Only the worker wiring file touches `ktables`.

**Toolbox-id uniqueness is the existing node-id invariant, not a new one.**
Two toolboxes sharing a `node_id` would LWW-clobber each other's records —
the same operational error class as two calf nodes sharing an id/topic today,
and it surfaces visibly (tool lists flapping between heartbeats). Nothing new
to enforce; document alongside the existing uniqueness expectation.

**Zero-config by design: the control plane is invisible.** The user provides
the broker URL exactly once, in the existing pattern:

```python
client = Client.connect("kafka:9092")          # the only URL the user types
worker = Worker(client, nodes=[agent, docs_toolbox])
```

Today `Client.connect` resolves `server_urls` (arg → `CALFKIT_MESH_URL` →
`"localhost"`, `base.py:144`) but does not retain it. PR A adds retention: the
resolved value is stored on the client and exposed as a read-only
`client.server_urls`. Derivation chain, in order:

1. `worker._client.server_urls` — calfkit-owned, no FastStream internals.
   The toolbox's writer reaches the same value via its `_worker`
   back-reference (assigned in `add_nodes`, before lifecycle runs).
2. Fallback for clients not built via `connect()`: a guarded helper reading
   the broker's `_connection_kwargs["bootstrap_servers"]` (verified present
   in FastStream 0.6.6; same guarded-internal pattern as `ensurer.py:42`),
   raising a clear error naming the override if the attribute ever moves.
3. Explicit `MCPDiscoveryConfig.bootstrap_servers` — pure escape hatch.

`MCPDiscoveryConfig(topic="mcp.capabilities", catchup_timeout=30.0,
heartbeat_interval=30.0, bootstrap_servers=None)` is therefore entirely
optional — defaults apply when absent. **The worker is the single config
surface**: toolboxes take no discovery config of their own and inherit the
hosting worker's via the `_worker` back-reference (defaults when unhosted) —
one config door into the control plane, not two. `bootstrap_servers` remains the
override for the advanced split-cluster case (control plane on a different
Kafka cluster than the data plane).

**Security boundary (v1 scope):** retaining the URL deliberately relaxes half
of the #188 pin (`test_client_never_captures_security_kwargs` documents the
split) — addressing data may be retained on the client; **credentials may
not**. Consequence: the v1 control plane targets clusters reachable with
bootstrap servers alone (dev/PLAINTEXT or network-trusted). Secured-cluster
support for the control plane is future work via an explicit security
pass-through on `MCPDiscoveryConfig` — never a client-side capture of raw
security kwargs.

### 8.4 Agent-side resolution

The toolbox is passed to agents exactly like a tool node — one object, both
deployable and advertisable (`worker.add_nodes(docs)`; `tools=[docs]`).
Passing it never touches the MCP session: the agent extracts only the
deferred lookup key (`node_id`) plus any `.select()` scoping. Cross-process
works because the agent's worker imports the toolbox *definition* (shared
codebase) without deploying it — the same way tool-node objects travel today.
**Reference type (added for #212, adjudicated 2026-06-10):** distributed
agent hosts reference a toolbox by name via the public frozen
`MCPToolbox(toolbox_id, include=None, strict=False)` (exported beside
`MCPToolboxNode`; `select()` returns it; the toolbox's own resolution delegates
to it). A one-class union ctor (ADK-style `connection_params` accepting an
identity arm) was considered and rejected: ADK's union arms are
capability-invariant transports, while ours would be capability-variant
(deployable secret-bearing servant vs inert lookup key) — the differences
surface in PUBLIC operations (`add_nodes`, secrets, IDE), so they warrant a
type boundary; `add_nodes(ref)` is rejected eagerly with a teaching error.
From the union position we kept: co-located `tools=[toolbox]` stays primary,
one docs page for both types, ADK-shaped host params, intuitive error text.

The standalone `MCPTools` class is dropped; `MCPToolboxNode.tool_bindings()`
(which raised `NotImplementedError`) is deleted in favor of implementing
`ToolSelector`. `_normalize_tools` checks selector-ness BEFORE provider-ness
and sets selectors aside in `self._tool_selectors`.

Selector mechanics: `ToolSelector` (in `tool_dispatch.py`) is

```python
@runtime_checkable
class ToolSelector(Protocol):
    def resolve_tools(self, view: Mapping[str, "CapabilityRecord"]) -> "SelectorResult": ...
```

`MCPToolboxNode.resolve_tools` looks up `view.get(self.node_id)`;
`toolbox.select(include=..., strict=...)` returns a small frozen
`_ScopedSelector(toolbox_id, include, strict)` delegating to the same code.
`SelectorResult` carries `bindings` plus structured diagnostics
(`missing_toolbox`, `missing_tools`, `stale_seconds`, `skipped_newer_schema`)
so `run()` owns the warn/strict/log policy in one place and tests can assert
on diagnostics instead of log text. Resolution must also guard
`record_to_bindings` (a malformed record — e.g. empty `dispatch_topic` —
decodes under the tolerant reader but fails binding expansion): catch, count
as a diagnostic, skip-and-log, never crash the turn. Schema-version tolerance (skip + log
records with a newer major) lives here, since ktables decodes any valid JSON
into the model.

At the top of `run()`, selectors resolve against
`ctx.resources["calfkit.mcp.capability_view"]` (`table.get(toolbox_id)`):
build validator-less `ToolBinding`s from the record (`tool_def` rebuilt from
`CapabilityToolDef`, `dispatch_topic` from the record), apply the `include`
filter, then merge into `tools_registry` AFTER static tools with
**collision = log error + static wins** (a remote server must never silently
shadow a locally-defined tool). Unresolved selector: warn + degrade;
`strict=True` raises before the model call. Stale heartbeat (v1): log only.

**Overrides pin the tool surface (decided post-review, 2026-06-10):** when a
caller supplies per-run `override_agent_tools`, selector resolution is
skipped entirely for that turn — an override means "exactly these tools", so
MCP tools never inject into an overridden turn and a strict selector cannot
fail it.

### 8.5 Toolbox-side publishing

The toolbox owns a `KafkaTableWriter[CapabilityRecord]` (started in its
resource phase alongside the MCP session; `ensure_topic` gated on
provisioning). Policy revision vs. the original sketch: publish via the
ktables writer rather than the worker's FastStream broker — it costs one
extra producer connection per toolbox worker but buys the idempotent
`acks=all` defaults and symmetric codecs for free, and keeps the control
plane fully decoupled from the data plane's broker lifecycle.

In `after_startup`: `list_tools()` → `CapabilityRecord` →
`writer.set(toolbox_id, record)`; register the `tools/list_changed` message
handler (re-list + re-`set`); start the heartbeat task (re-`set` every
`heartbeat_interval`). In `on_shutdown`: cancel heartbeat,
`writer.delete(toolbox_id)` (§6 trade-off). Boot failure unchanged:
resource-phase errors abort worker startup.

### 8.6 Delivery plan (3 PRs, TDD each)

0. ~~PoC~~ **DONE** — graduated into the published `ktables` package
   (>=0.1.1, now a calfkit dependency), hardened via 3-round review.
1. **PR A — wire model + config**: `calfkit/models/capability.py`
   (`CapabilityRecord`, `CapabilityToolDef`), `MCPDiscoveryConfig`; pure unit
   tests (round-trip, version tolerance policy helper).
2. **PR B — toolbox publisher**: `KafkaTableWriter` resource on `MCPToolboxNode`,
   connect/`list_changed`/heartbeat/tombstone hooks; tests with a fake writer
   (publish assertions) + one real-broker integration test.
3. **PR C — worker view + selector + agent resolution**: worker `@resource`
   opening `KafkaTable.json(model=CapabilityRecord)` (auto-registered when a
   hosted agent declares selectors), `ToolSelector` protocol, `MCPTools`,
   `run()` resolution/merge/strict; fixes `MCPToolboxNode.tool_bindings()`'s error
   message to point at `MCPTools`. Closes the loop; PR #207 leaves draft.
   (Formerly PRs C+D; merged because the view wiring is now ~20 lines.)

Open detail to settle during PR C: pinning the capability topic to 1
partition at creation (recommended: yes; total order per key is free).
(Bootstrap-servers derivation: RESOLVED — zero-config via the broker, §8.3.)

### 8.7 Testing strategy

- **Agent resolution (PR C, no broker):** the view is typed as
  `Mapping[str, CapabilityRecord]`, so unit tests seed a plain dict into the
  node's resource bag and drive `run()` with `TestKafkaBroker` as usual —
  selector hits, include filters, collisions (static wins), unresolved
  warn/strict, staleness logging, newer-schema skip.
- **Toolbox publisher (PR B, no broker):** the writer is held as a node
  resource; tests inject a fake writer (records `set`/`delete` calls) into
  the bag and assert publish-on-connect / re-publish-on-list_changed /
  heartbeat cadence (advance via short interval) / tombstone-on-shutdown.
- **Worker wiring + real pipeline (PR C, broker-gated):** one integration
  test, skipped without a broker (the ktables test pattern): toolbox worker
  up → record lands; agent worker up → catch-up gate passes → a turn
  resolves bindings; toolbox clean-shutdown → tombstone → next turn warns.
- ktables' own semantics (gate, LWW, tombstones, reader death) are NOT
  re-tested in calfkit — they're covered by the ktables suite; calfkit tests
  its policy layers only.

## 9. Implementation notes (non-normative)

- `MCPToolboxNode.tool_bindings()` currently raises `NotImplementedError`; once
  selectors exist it should keep raising (a toolbox is not a sync provider) and
  the error message should point at `MCPTools`.
- The catch-up gate needs consumer end-offset access; FastStream exposes the
  underlying aiokafka consumer per subscriber — verify the cleanest hook
  during implementation (the StartupTopicEnsurer work, issue #180, shows the
  pattern for broker pre-start access).
- `TestKafkaBroker` does not compact (in-memory); tests exercise LWW semantics
  by publishing multiple records per key — compaction is a broker-side
  optimization the reader must not depend on.
