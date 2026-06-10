# MCP Capability Discovery — Design Spec

**Status:** converged design, pre-implementation (grilling session 2026-06-09)
**Depends on:** the `ToolBinding`/`ToolProvider` contract on `feat/mcp_bridge_v2` (`calfkit/models/tool_dispatch.py`)

## 1. Problem

An `MCPBridge` node fronts one MCP server and services tool calls sent to its
dispatch topic (`mcp_server.<name>`). Its tools, however, are only knowable at
runtime: `list_tools` is an async RPC against a live MCP session that exists
only after the bridge's resource phase. Agents declare tools at construction
time, synchronously, and may run in a **different worker process** than the
bridge. `MCPBridge.tool_bindings()` therefore raises `NotImplementedError`
today.

This spec defines how runtime-discovered MCP tools become `ToolBinding`s
available to any agent in the cluster.

## 2. Decision summary

| # | Decision | Choice |
|---|----------|--------|
| 1 | Topology | Cross-process from day one: bridge and consuming agent may live in different workers |
| 2 | Mechanism | A single cluster-wide **compacted capability topic**; bridges publish their tool list keyed by bridge id (control plane). Live per-turn queries rejected (§7) |
| 3 | Reader | One subscriber + one materialized dict (**Capability View**) per agent-hosting worker, stored in the worker resource bag; agents read it per turn |
| 4 | Agent declaration | `MCPTools(bridge_id, include=[...], strict=False)` selector placeholders in `tools=[...]` |
| 5 | Boot ordering | Worker gates serving on view catch-up (bounded wait ~30s; on timeout, serve + loud log) |
| 6 | Unresolved selector | Warn + run the turn degraded; `strict=True` fails the turn before the model runs |
| 7 | Wire format | Calfkit-owned versioned pydantic model; never the vendored `ToolDefinition` |
| 8 | Liveness | Bridges heartbeat by re-publishing their record (~30s). v1 readers only **log** staleness; behavioral use of staleness (hiding tools, strict-fail) deferred |
| 9 | Removal | Bridge publishes a tombstone on **clean shutdown**; crash leaves the record standing (staleness signals it). See accepted trade-off in §6 |
| 10 | Bridge boot failure | Worker boot fails (existing resource-phase semantics: session `initialize()` errors abort startup) |

## 3. The control plane

### 3.1 Topic

One topic for the whole cluster (working name `mcp.capabilities`),
`cleanup.policy=compact`. Compaction is broker-side: the broker retains the
latest record per **key**; producers and consumers need nothing special.
Steady-state content ≈ one record per bridge — a small key-value table.

Provisioning: **needs a dedicated creation path.** Verified during design:
`ProvisioningConfig.topic_configs` is a flat config dict applied to *every*
data topic the provisioner creates (`provisioner.py:170-172`) — putting
`cleanup.policy=compact` there would compact all data topics. The capability
topic must be created with its own config (a dedicated ensurer call, or
extend `topic_configs` to per-topic-name keying). Ops-governed creation in
production, consistent with `docs/topic-provisioning.md`. (Doc bug noted:
`provisioning/config.py:36` says "Per-topic config overrides", which
misleadingly suggests per-topic-name targeting.)

Data plane (per-bridge call topics, `mcp_server.<name>`) is unchanged and
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
    bridge_id: str
    dispatch_topic: str
    tools: list[CapabilityToolDef]
    published_at: AwareDatetime
```

Reader policy: tolerant (ignore unknown fields); skip + log records with a
newer `schema_version`. Kafka message key = `bridge_id` (bytes). Tombstone =
null value under the key.

### 3.3 Writer (bridge)

The bridge publishes its record:

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
- Handler: last-write-wins upsert `view[bridge_id] = (record, received_at)`;
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
(~2,880 records/bridge/day) mean a misconfigured `cleanup.policy=delete` topic
still always holds a fresh record inside any sane retention window — boots get
slower (linearly in retention × bridges), tools never get lost. The genuinely
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

`MCPTools` is a declarative reference, not a `ToolProvider`: it names a bridge
and optionally filters tool names (the filter is the trust boundary — a server
suddenly exposing new tools cannot silently enlarge an agent's tool surface).
`_normalize_tools` keeps selectors aside rather than expanding them.

### 4.2 Per-turn resolution

At the top of `run()` — where `tools_registry` is already rebuilt per turn for
overrides — each selector resolves against the Capability View into
validator-less `ToolBinding`s (`tool_def` rebuilt from `CapabilityToolDef`,
`dispatch_topic` from the record) and merges with static ctor tools. Fresh
every turn; no agent mutation; no RPC.

Unresolved (bridge absent from view, or `include` name not advertised):

- default: log a warning, run the turn with what resolved;
- `MCPTools(..., strict=True)`: fail the turn before the model runs.

Staleness (v1): if the resolved record's heartbeat is older than the
threshold, log it. No behavioral change — hiding stale tools from the model
and strict-on-stale are explicitly deferred to a v2 decision.

## 5. Failure-mode walkthrough

| Event | Control plane | Agent experience |
|---|---|---|
| Bridge crashes | Record stands; heartbeats stop | Tools still offered; calls fail visibly (`FailedToolCall`); staleness logged after threshold |
| Bridge clean shutdown (deploy) | Tombstone, then re-publish on boot | Tools vanish for the deploy window, then return (accepted trade-off, §6) |
| Bridge decommissioned for good | Tombstone (clean shutdown) | Selector unresolved: warn + degrade, or strict-fail |
| MCP server changes tools | `tools/list_changed` → re-publish | Next turn sees the new list |
| Agent worker boots | Replays topic, gated ~30s | First turn already has full view |
| Capability topic lagging | Gate timeout → serve + loud log | Possibly missing MCP tools until convergence; warnings on resolve |

## 6. Accepted trade-off: tombstone-on-clean-shutdown

A rolling deploy (SIGTERM → clean shutdown) tombstones the record, so the
bridge's tools vanish cluster-wide until the new instance re-publishes —
while a *crash* leaves the advertisement standing. Graceful restarts are thus
briefly more disruptive than crashes. Accepted consciously (self-cleaning, no
decommission CLI needed for v1), with the explicit ideal that advertisement
should track liveness — which the heartbeat field enables a future v2 to act
on. Revisit if deploy-window tool gaps hurt in practice.

## 7. Rejected alternatives

- **Live `list_tools` query per turn** (the in-process pattern of pydantic-ai
  / OpenAI Agents SDK): over Kafka this couples every agent turn to every
  bridge's availability, adds 2×N hops of pre-model latency, and requires new
  fan-in state machinery in `run()`. Cached variants converge back to a
  change-notification topic — i.e., this design with extra steps. See ADR-0001.
- **Per-bridge capability topics**: readers would need the topic list up
  front (defeating discovery) and adding a bridge would churn subscriptions.
  One topic + keys gives compaction per bridge for free.
- **Faust tables (forked OR embedded as an app in the worker lifecycle)**:
  rejected on both buy-vs-build axes. *Vendor health* (verified on PyPI +
  GitHub 2026-06-09): `faust` dead since 2020-02; `faust-streaming` last
  released 0.11.3 on 2024-08-23 with exactly one commit in the ~16 months
  since (2026-03-30: an aiokafka 0.13 compat fix — for the aiokafka version
  calfkit ships — that remains unreleased, so the PyPI package is broken
  against our lockfile); 147 open issues; drags `mode-streaming`.
  *Interface fit*: faust tables own their internal
  `{app_id}-{table}-changelog` topic — bridges publish to OUR topic in OUR
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

## 8. Implementation notes (non-normative)

- `MCPBridge.tool_bindings()` currently raises `NotImplementedError`; once
  selectors exist it should keep raising (a bridge is not a sync provider) and
  the error message should point at `MCPTools`.
- The catch-up gate needs consumer end-offset access; FastStream exposes the
  underlying aiokafka consumer per subscriber — verify the cleanest hook
  during implementation (the StartupTopicEnsurer work, issue #180, shows the
  pattern for broker pre-start access).
- `TestKafkaBroker` does not compact (in-memory); tests exercise LWW semantics
  by publishing multiple records per key — compaction is a broker-side
  optimization the reader must not depend on.
