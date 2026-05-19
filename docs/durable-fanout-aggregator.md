---
status: draft
authors:
  - python-sdk-dx-reviewer (public API & DX)
  - event-driven-architect (Kafka mechanics)
date: 2026-05-18
supersedes:
  - docs/drafts/aggregator-dx-design.md
scope: Canonical merged design for the durable fan-out aggregator — public API, wire format, Kafka mechanics, file-by-file changes, test plan, observability, and migration.
---

# Durable Fan-Out Aggregator — Canonical Design

> **Implementation status (v1 shipped on `feat/durable-fanout-aggregator`):**
> The implementation diverges from this doc in the following ways. They reflect
> the multi-partition v1 baseline and pure-FastStream pattern decisions made
> after this design was first written. Read these before relying on any
> specific section for implementation detail.
>
> 1. **Multi-partition by default.** All three topics (main, `{node_id}.fanout-state`,
>    `{node_id}.fanout-returns`) share the same partition count, auto-detected
>    from the agent's main topic at worker startup (default 6 when missing).
>    The "single-partition v1 / multi-partition v1.1" framing in §17.5/§17.9 is
>    obsolete — multi-partition is v1.
> 2. **Per-partition linearisation** (§7.1) is achieved via consumer-group
>    ownership of the returns topic plus `max_workers=1` on the aggregator's
>    subscriber. The same group_id is shared with the agent's main subscriber
>    so co-partitioning is automatic.
> 3. **Rehydration is rebalance-driven and partition-scoped** (§8.5).
>    `_StateStoreRebalanceListener.on_partitions_assigned` performs a one-shot
>    read of just the newly-assigned state-topic partitions via
>    `AIOKafkaConsumer` — not eager full-log rehydration in `Worker.run()`.
> 4. **No second consumer.** Standard Kafka Streams-style aggregation:
>    the returns subscriber owns reads and writes; there is no
>    "state-watcher" tailing the state topic in the background.
> 5. **`broker.config.admin_client` is the production access path** for topic
>    provisioning; the one small `AIOKafkaConsumer` usage in
>    `_kafka_state_store.py` is for the rehydration pattern FastStream's
>    `@broker.subscriber` decorator doesn't express. Both are accessed
>    through FastStream where possible.
> 6. **Standby replicas are explicitly out of scope for v1** (§2.2). Pure
>    consumer-group ownership handles failover.
> 7. **Implementation lives on six PRs** on the `feat/durable-fanout-aggregator`
>    branch: protocol headers / kafka_subscriptions / FanOutAggregator skeleton /
>    state store + topic admin / wire-up + remove `_pending_batches` / tests +
>    deprecation. The behavioural change ships in PR 5.

## 1. Executive summary

`BaseAgentNodeDef._pending_batches` is an in-process Python dict that holds the join-state of a parallel tool fan-out. It vanishes on process restart and on Kafka consumer-group rebalance — a known correctness hole acknowledged in the existing `RuntimeError` at `calfkit/nodes/agent.py:117-127`. This design replaces it with a durable, Kafka-backed aggregator that is:

- **Invisible by default.** Hello-world agent code does not change. The framework auto-constructs a `FanOutAggregator()` and the Worker wires its subscriber without user involvement.
- **Customizable through three override methods.** `merge`, `should_complete`, `on_partial`. All have sensible defaults; users override only when they need short-circuit completion, custom merging, or progress-event emission.
- **Durable on a compacted Kafka topic per agent.** Per-agent ownership: `{node_id}.fanout-state` (compacted, holds in-flight batch records keyed by `(correlation_id, fan_out_id)`) and `{node_id}.fanout-returns` (a normal returns channel the aggregator subscribes to). The compacted topic is the system of record; on Worker startup it is rehydrated into an in-memory write-through cache.
- **Observable across the fan-out boundary.** Per-batch OTel spans propagate via W3C `traceparent`/`tracestate` Kafka headers, so a single distributed trace covers dispatch → tool work → return → merge → resume. This is the calfkit-specific selling point that in-process agent SDKs cannot deliver.
- **Idempotent under at-least-once delivery.** Dedup is keyed by `(correlation_id, fan_out_id, tool_call_id)`. Duplicate returns are a no-op. Duplicate dispatches publish the same `fan_out_id` so the aggregator collapses them.

The design is built on the **public API frozen in [aggregator-dx-design.md](./drafts/aggregator-dx-design.md)**. This document adds the Kafka mechanics deliberately deferred to the event-driven architect: wire format, compacted-topic configuration, partitioning, lifecycle sequences, rehydration semantics, idempotency contract, header protocol, and file-by-file implementation plan.

Estimated implementation footprint: **~1,400–1,800 LoC** new code (aggregator subpackage + worker integration + tests), **~80 LoC** removed (`_pending_batches` and `_parallel_state_aggregation`).

---

## 2. Goals and non-goals

### 2.1 Goals (v1)

1. Replace `_pending_batches` with a durable join store; fix the known fragility on restart/rebalance.
2. Keep the existing public Agent API surface unchanged for the hello-world case; add exactly one optional kwarg (`aggregator=...`).
3. Make the aggregator a discoverable, subclassable companion class — not a method on `BaseAgentNodeDef`.
4. Survive at-least-once delivery: duplicate tool returns and duplicate dispatches both converge to the same merged state.
5. Survive partition rebalance and process restart: a new owner of the agent's partition rehydrates pending batches from the compacted topic and finishes them.
6. Propagate OTel `traceparent` across the fan-out boundary so distributed tracing works without user effort.
7. Provide an in-memory test harness (`InMemoryAggregator`) that runs without Docker or a real broker.

### 2.2 Non-goals (v1)

- **Exactly-once semantics via Kafka transactions.** We commit to at-least-once + idempotent merge. Transactional production is a v2 consideration.
- **Multi-partition aggregator topics.** v1 ships single-partition `{node_id}.fanout-state` and `{node_id}.fanout-returns` per agent. Multi-partition scale-out is a v2 problem.
- **A general "DLQ" framework.** Late arrivals are logged and dropped in v1. A configurable DLQ topic is a future addition.
- **Cross-agent fan-in / scatter-gather.** Aggregator joins are scoped to one agent's tool fan-out. Choreography across agents is a separate primitive (`Emit` / subscription patterns).
- **A RocksDB-backed state store.** v1 holds in-memory cache with full rehydration from the compacted log on startup. RocksDB-backing is a v2 consideration if memory bounds become an operational concern.
- **Soft-timeouts / progress-heartbeats.** v1 has a single firm idle timeout. Soft-progress hooks (e.g. `on_idle_progress(elapsed)`) are v2.

### 2.3 What survives unchanged

- `BaseNodeDef` keeps its single-handler contract for non-agent nodes; the new `kafka_subscriptions()` method defaults to "one subscription for `self.handler`."
- `Envelope`, `SessionRunContext`, `WorkflowState`, `CallFrame`, `Deps` — no wire-shape changes.
- `Call`, `ReturnCall`, `TailCall`, `Silent`, `Emit` — no return-type changes.
- The `_protocol.py` headers `x-calf-emitter` and `x-calf-emitter-kind` keep their meaning. Two new ones are added.

---

## 3. Background: the bug we are fixing

### 3.1 Current state of the code

`BaseAgentNodeDef.__init__` initialises:

```python
self._pending_batches: dict[str, PendingToolBatch] = dict()
```

(`calfkit/nodes/agent.py:52`).

On a parallel turn, after the LLM emits N tool calls, the agent does the following inside `run()` (`agent.py:206-221`):

1. Constructs N `Call[State]` actions, one per tool.
2. Stores a `PendingToolBatch(expected_tool_call_ids=frozenset(...), base_state=ctx.state.model_copy(deep=True))` in `self._pending_batches[ctx.deps.correlation_id]`.
3. Returns `list[Call]` — `_publish_action` (`base.py:167-183`) iterates and publishes one Kafka message per tool with `key=correlation_id.encode()`.

When each tool finishes and a `ReturnCall` arrives back at the agent, the gate at `agent.py:97-101` runs `_parallel_state_aggregation(ctx)` which:

1. Looks up the batch in `self._pending_batches.get(ctx.deps.correlation_id)`.
2. If found, copies any new `tool_results` into `batch.collected_results`.
3. If complete, merges into `batch.base_state`, sets `ctx.state = batch.base_state`, deletes the batch entry.
4. If incomplete, returns `Silent()` — no further publish, agent waits for the next return.

### 3.2 What goes wrong

`self._pending_batches` is process-local. Three failure modes:

1. **Worker process restart.** Between the fan-out publish and the first return, the worker is killed (deploy, OOM, crash). On restart, `_pending_batches` is empty. The first return arrives, the gate at `agent.py:99-127` raises the canonical `RuntimeError: This indicates lost PendingToolBatch state (e.g. partition rebalance or process restart).`
2. **Consumer-group rebalance.** A second worker joins the group. Kafka reassigns the partition holding this `correlation_id` to the new worker. Returns now arrive at a worker whose `_pending_batches` never saw the dispatch. Same error.
3. **Replay / duplicate delivery.** If a tool return is redelivered (consumer hasn't committed offset, broker re-pushed), `_parallel_state_aggregation` will idempotently re-copy the same `tool_call_id` into `batch.collected_results` — that's fine. But if the batch was already completed and the dict entry deleted, the redelivered return arrives at the gate with no batch present and triggers the same `RuntimeError`.

### 3.3 The invariant we need

> Given any `(correlation_id, fan_out_id)`, the set of `expected_tool_call_ids` and the dict `collected_results` must be recoverable by any process that owns the agent's partition, regardless of restart history.

Kafka itself is a perfectly good store for this. The fix is to put the batch record on a **compacted topic** keyed by `(correlation_id, fan_out_id)`, write through it on every state change, and rehydrate the in-memory cache from offset 0 on startup.

---

## 4. Authoring API (folded in from the DX design)

This section restates the user-visible API from the DX design (`docs/drafts/aggregator-dx-design.md` §3–§13). The Kafka mechanics that realise it are in §6–§9.

### 4.1 The hello-world case: zero new code

Existing agent code keeps working unchanged:

```python
from calfkit.nodes import Agent, agent_tool
from calfkit.providers import OpenAIResponsesModelClient

@agent_tool
def get_weather(location: str) -> str:
    return f"It's sunny in {location}"

@agent_tool
def get_news(topic: str) -> str:
    return f"Latest news on {topic}: ..."

agent = Agent(
    "concierge",
    system_prompt="You are a helpful assistant. Use tools in parallel when possible.",
    subscribe_topics="concierge.input",
    model_client=OpenAIResponsesModelClient(model_name="gpt-5.4-nano"),
    tools=[get_weather, get_news],
)
```

On a parallel turn, this agent now durably aggregates. The framework attached a default `FanOutAggregator()` at construction, the Worker discovered and registered its subscriber, and the user is unaware.

### 4.2 Custom aggregator behaviour

```python
from calfkit.nodes import Agent, FanOutAggregator
from calfkit.nodes.aggregator import AggregatorBatch

class FirstSuccessAggregator(FanOutAggregator):
    """Complete the batch as soon as one tool returns successfully."""
    async def should_complete(self, batch: AggregatorBatch) -> bool:
        return any(not isinstance(r, Exception) for r in batch.collected_results.values())

agent = Agent(
    "racing_agent",
    system_prompt="Answer using whichever backend responds first.",
    subscribe_topics="racing.input",
    model_client=...,
    tools=[search_google, search_bing, search_ddg],
    aggregator=FirstSuccessAggregator(),
)
```

The three override methods, restated:

```python
class FanOutAggregator(Generic[StateT, ResultT]):
    async def merge(
        self,
        base_state: StateT,
        results: Mapping[ToolCallId, ResultT],
    ) -> StateT: ...

    async def should_complete(self, batch: AggregatorBatch[StateT, ResultT]) -> bool: ...

    async def on_partial(
        self,
        batch: AggregatorBatch[StateT, ResultT],
        latest: AggregatedReturn[ResultT],
    ) -> None: ...
```

Defaults:

- `merge`: deep-copy `base_state`, call `add_tool_result` for each `(tool_call_id, result)`, return.
- `should_complete`: `frozenset(batch.collected_results) == batch.expected_tool_call_ids`.
- `on_partial`: no-op.

### 4.3 Public types and module structure

```
calfkit/
  nodes/
    aggregator/
      __init__.py        # FanOutAggregator, AggregatorBatch, AggregatedReturn, ToolCallId
      aggregator.py      # FanOutAggregator class
      state.py           # FanOutState, AggregatorBatch, AggregatedReturn, ToolCallId
      state_store.py     # _KafkaStateStore (private)
      errors.py          # AggregatorError, FanOutTimeoutError, AggregatorMergeError, AggregatorStateStoreError
      testing.py         # InMemoryAggregator
```

Public surface:

```python
from calfkit.nodes import FanOutAggregator
from calfkit.nodes.aggregator import AggregatorBatch, AggregatedReturn, ToolCallId
from calfkit.nodes.aggregator.testing import InMemoryAggregator
from calfkit.nodes.aggregator.errors import (
    AggregatorError, FanOutTimeoutError, AggregatorMergeError, AggregatorStateStoreError,
)
```

### 4.4 Configuration

```python
class FanOutAggregator(Generic[StateT, ResultT]):
    def __init__(
        self,
        *,
        idle_timeout: timedelta = timedelta(minutes=5),
        on_merge_error: Literal["abort", "retry", "drop"] = "abort",
        merge_retry_count: int = 0,
        kafka_topic_config: Mapping[str, str] | None = None,
        state_topic: str | None = None,        # default: f"{node_id}.fanout-state"
        returns_topic: str | None = None,      # default: f"{node_id}.fanout-returns"
    ) -> None: ...
```

Environment variables:

| Env var | Purpose | Default |
|---|---|---|
| `CALFKIT_AGGREGATOR_OTEL` | `"0"` disables auto-OTel, `"1"` forces it | Auto-detect on `opentelemetry` import |
| `CALFKIT_AGGREGATOR_IDLE_TIMEOUT_MS` | Default idle timeout for un-configured aggregators | `300000` |
| `CALFKIT_AGGREGATOR_LOG_LEVEL` | Logger level override for `calfkit.aggregator` namespace | Inherits root |

The full DX surface (testing API, comparison tables, naming rationale, etc.) is in [aggregator-dx-design.md](./drafts/aggregator-dx-design.md); not duplicated here.

---

## 5. Architecture overview

### 5.1 Per-agent topology

Each `Agent` instance owns two new Kafka topics:

```
{node_id}.fanout-state      compacted, 1 partition  — system of record for in-flight batches
{node_id}.fanout-returns    normal,    N partitions — fan-in channel for tool returns
```

Per-agent ownership is the locked-in decision. Multiple agents do not share these topics.

### 5.2 Message flow

```
                                              +----------------------+
                                              |  Agent.run() emits   |
                                              |  list[Call]          |
                                              +----------+-----------+
                                                         |
                                       (1) write batch record
                                       to fanout-state    +-----> {node_id}.fanout-state  (compacted)
                                                          |       key=(corr_id, fan_out_id)
                                                          |
                                       (2) dispatch N tool Calls
                                       with x-calf-fanout-id header
                                                          |
                          +-------------+ +-------------+ +-------------+
                          | tool_a Call | | tool_b Call | | tool_c Call |
                          +------+------+ +------+------+ +------+------+
                                 |               |               |
                                 v               v               v
                          +------+--+      +-----+---+    +------+--+
                          | tool_a  |      | tool_b  |    | tool_c  |
                          +----+----+      +----+----+    +----+----+
                               |                |              |
                          ReturnCall      ReturnCall      ReturnCall
                          with fanout-id  with fanout-id  with fanout-id
                          retargeted to retargeted to    retargeted to
                          fanout-returns  fanout-returns  fanout-returns
                               |                |              |
                               v                v              v
                          +----+----------------+--------------+----+
                          |  {node_id}.fanout-returns (N partitions)|
                          |  partition key = correlation_id          |
                          +-------------------+---------------------+
                                              |
                                              v
                          +-----------------------------------------+
                          |  Aggregator handler (per-agent companion)|
                          |    1. dedup on (corr, fanout, tool_call) |
                          |    2. write-through update fanout-state  |
                          |    3. user merge / should_complete /     |
                          |       on_partial                         |
                          +-----------------+-----------------------+
                                            |
                          on completion:    |
                          (a) tombstone fanout-state record          |
                          (b) re-enter agent main topic with merged  |
                              state (AggregatedReturn synthesised)   |
                                            |
                                            v
                                  agent's subscribe_topics
```

### 5.3 Key design choices

**Why a compacted topic, not a separate DB?**
The compacted topic is part of the same Kafka cluster the agent already depends on. No new infrastructure. Log compaction gives us key-based retention without TTL bookkeeping. The active in-flight set is bounded by `pending_batches × N`, which is small in practice (hundreds to low thousands).

**Why a separate returns topic?**
The agent's main `subscribe_topics` already handles `ReturnCall` envelopes from single-tool calls (via the existing call-stack `callback_topic`). For parallel fan-out, returns must hit the **aggregator** before they re-enter the agent's `run()`. Routing all returns to the main topic and disambiguating inside `run()` is what the current code does and is exactly what we are fixing. A dedicated `fanout-returns` topic separates the two consumer-group concerns cleanly.

**Why per-agent topics, not a global aggregator service?**
Per-agent topics co-partition cleanly with the agent's main subscribe topic — the same `correlation_id` lands on the same partition on both. This makes read-modify-write on the aggregator a linearisable single-consumer problem (no distributed lock). A shared global topic would require additional partition coordination across agents.

**Why retarget tool returns to the aggregator topic, not to the main topic?**
We rewrite the `callback_topic` field of the tool's `CallFrame` to `{node_id}.fanout-returns` at dispatch time (only in the parallel case; the single-tool sequential path still uses the main topic, unchanged). When the tool calls `ReturnCall(state=...)`, the existing `_publish_action` logic at `base.py:201-215` pops the frame and publishes to `frame.callback_topic` — which is now the aggregator topic. This is a one-line change inside the parallel branch of `_publish_action` and keeps the rest of the call-stack semantics intact.

---

## 6. Wire format

All wire-format types live in `calfkit/nodes/aggregator/state.py`. The naming convention "Compact" / "Plain" follows the existing distinction documented in agent memory (`project_envelope_basemodel_distinction.md`).

### 6.1 The compacted-topic value: `FanOutState`

`FanOutState` is the value published to `{node_id}.fanout-state`. The key is the dedup composite `(correlation_id, fan_out_id)` serialised as `f"{correlation_id}|{fan_out_id}"` (matching the partition-key convention; `|` is illegal in uuid7 hex). On completion we publish a tombstone (`value=None`) on this key.

```python
# calfkit/nodes/aggregator/state.py

from datetime import datetime
from typing import Any
from pydantic import BaseModel, ConfigDict, Field

from calfkit.models.state import State

class FanOutState(BaseModel):
    """Value type for the {node_id}.fanout-state compacted topic.

    Plain BaseModel (not CompactBaseModel) — wire shape must be stable.
    Always-serialised fields; deserialise must be tolerant of unknown fields.
    """

    model_config = ConfigDict(extra="ignore")

    # Identity (also the key — duplicated in value for self-contained replay)
    correlation_id: str
    fan_out_id: str = Field(description="uuid7 hex assigned by the agent at dispatch time")
    node_id: str = Field(description="The owning agent's node_id (the compacted topic name's prefix)")

    # Expected work
    expected_tool_call_ids: frozenset[str] = Field(
        description="The full set of tool_call_id values this batch waits on. Frozen at dispatch."
    )

    # Collected results so far. None values are not allowed — a missing key means
    # "not yet collected". A present key is unambiguously "received".
    collected_results: dict[str, Any] = Field(
        default_factory=dict,
        description="tool_call_id -> serialised tool result. Tool results are arbitrary, "
                    "carried as-is from State.tool_results. JSON-serialisable.",
    )

    # Snapshot of agent State at fan-out time (the basis the merge will be applied to)
    base_state: State

    # Lifecycle timestamps (epoch millis; integers, not floats, for compact JSON)
    started_at_ms: int
    last_updated_ms: int

    # Versioning of the wire shape — bump on backwards-incompatible changes
    schema_version: int = 1

    # Optional propagation
    traceparent: str | None = Field(
        default=None,
        description="W3C trace-context value captured at dispatch time; used to "
                    "stitch the completion span back to the originating trace.",
    )
    tracestate: str | None = None
```

**Why plain BaseModel, not CompactBaseModel:** the rules in agent memory (`exclude_unset=True` requires `= None` defaults, list mutation does not mark fields) apply to `State`/`CompactBaseModel`. For a value type written and read by the same component on a topic with strict round-trip, plain BaseModel is simpler and avoids the trap. `Envelope` already follows this convention for the same reason.

**Why `expected_tool_call_ids: frozenset[str]`, not a list:** the dedup invariant is set-based; a frozenset documents it. Pydantic serialises it to a JSON array; on read we coerce back. Order is irrelevant.

**Why store `base_state` in the record:** completion requires merging into the state snapshot captured at dispatch time, not whatever state the latest return brings (returns carry stale fragments). On rehydration after restart, the aggregator needs `base_state` to perform the merge — we cannot reconstruct it from envelopes alone. Cost: each fan-out writes one State-sized record. State is bounded by message_history which is bounded by the agent loop, so this is reasonable.

### 6.2 The returns-topic message: `FanOutReturnEnvelope`

The aggregator subscribes to `{node_id}.fanout-returns`. Each message is a regular calfkit `Envelope` — tools publish via the existing `_publish_action` pathway. **No new envelope type.** The only thing we change is the destination topic (via the rewritten `callback_topic`) and the headers we stamp on dispatch.

The aggregator's subscriber unpacks the standard `Envelope`:

- `envelope.context.state.tool_results` carries the new return entry — one `tool_call_id -> result` mapping.
- `envelope.internal_workflow_state.current_frame.frame_id` is the per-hop uuid7 from `CallFrame.frame_id` (`session_context.py:38`).
- Kafka headers carry `x-calf-fanout-id`, `x-calf-frame-id`, `x-calf-emitter`, `x-calf-emitter-kind`, and `traceparent`/`tracestate` (when OTel is enabled).

This is the **right call**: we do not invent a new envelope type for a transient transport pattern. The aggregator pulls what it needs from the existing shape.

### 6.3 The post-completion message: re-entry to the main topic

When the batch completes, the aggregator publishes one envelope to the agent's main subscribe topic. The envelope shape is again the standard `Envelope`. The aggregator constructs it by:

1. Taking the persisted `FanOutState.base_state`.
2. Running it through `aggregator.merge(base_state, collected_results)` to produce the final merged `State`.
3. Reusing the **first** received return's `internal_workflow_state` (with the call-stack appropriately unwound — see §8.3).

This re-entry message is what the agent's `run()` will see. From the agent's point of view, it's a normal inbound on its main subscribe topic, with merged state. The existing `_parallel_state_aggregation` gate and the `RuntimeError` are removed; the agent simply runs the next LLM iteration.

### 6.4 Header protocol additions

Two new constants in `calfkit/_protocol.py`:

```python
HDR_FANOUT_ID = "x-calf-fanout-id"
"""Kafka header identifying the fan-out batch this message participates in.

Present on:
  - Every outbound tool Call dispatched as part of a parallel batch.
  - Every ReturnCall from a tool participating in a parallel batch (because the
    tool republishes inbound headers via Response, see base.py:278).
  - Every aggregator-emitted re-entry to the agent's main topic.

Absent on:
  - Single-tool sequential dispatches.
  - Non-agent node hops.
  - Initial client invocations.
"""

HDR_FRAME_ID = "x-calf-frame-id"
"""Kafka header carrying CallFrame.frame_id (uuid7) for the hop.

Promoted from the body for cheap downstream dedup, header-based DLQ filters,
and external tooling (kafka-console-consumer, MirrorMaker SMTs). The frame_id
is the canonical cross-hop dedup key under at-least-once delivery.
"""
```

Header summary, by message:

| Message | `x-calf-emitter` | `x-calf-emitter-kind` | `x-calf-frame-id` | `x-calf-fanout-id` | `traceparent`/`tracestate` |
|---|---|---|---|---|---|
| Client initial invoke | client.{id} | `client` | (from frame) | absent | (if client-side OTel) |
| Agent single-tool dispatch | {agent.node_id} | `agent` | (from frame) | absent | (propagated) |
| Agent parallel dispatch (one per call) | {agent.node_id} | `agent` | (from frame, distinct per call) | {fan_out_id} | (propagated) |
| Tool ReturnCall (parallel) | {tool.node_id} | `tool` | (from new frame) | {fan_out_id} (echoed) | (propagated) |
| Aggregator re-entry to agent main topic | {agent.node_id} | `agent` | (synthesised new frame) | absent (batch is done) | (carried from base) |

**Echoing rule:** the existing `BaseNodeDef.handler` returns `Response(body, headers=self._emitter_headers())` at `base.py:278`. The Response headers include `HDR_EMITTER` and `HDR_EMITTER_KIND`. The fan-out and frame headers ride only on the outbound `broker.publish(...)` call sites at `base.py:176-181, 194-199, 209-214, 227-232`. Tools do not need to know they're in a fan-out — `_publish_action` reads them from the inbound `headers` dict (via FastStream `Context("message.headers")`) and forwards them.

Concretely: a small helper `_emit_headers(envelope, inbound_headers)` builds the outbound header dict by composing `_emitter_headers(envelope)` (with `HDR_FRAME_ID` added per the existing frame_id memory) with any inbound `x-calf-fanout-id` / `traceparent` / `tracestate` carried forward. This is a refactor of the existing four call sites.

### 6.5 Compacted topic configuration

```python
# calfkit/nodes/aggregator/state_store.py

DEFAULT_FANOUT_STATE_TOPIC_CONFIG: dict[str, str] = {
    "cleanup.policy": "compact",
    "min.cleanable.dirty.ratio": "0.1",     # compact aggressively; short-lived keys
    "min.compaction.lag.ms": "60000",       # 1 min: ensure consumers can read recent writes before compaction
    "delete.retention.ms": "60000",         # 1 min: tombstones retained briefly after compaction
    "segment.ms": "600000",                 # 10 min: small segments to enable frequent compaction
    "segment.bytes": "10485760",            # 10 MB
    "retention.ms": "-1",                   # infinite — compaction owns retention
    "max.compaction.lag.ms": "300000",      # 5 min: hard upper bound
}

DEFAULT_FANOUT_RETURNS_TOPIC_CONFIG: dict[str, str] = {
    "cleanup.policy": "delete",
    "retention.ms": "3600000",              # 1 hour: returns are ephemeral once aggregated
    "segment.ms": "600000",
}
```

**Rationale per setting (fanout-state):**

- `cleanup.policy=compact` — required: we want per-key retention only. Without this, the topic is delete-policy and a tombstone is meaningless.
- `min.compaction.lag.ms=60000` — protects against compaction racing with consumers that just read a key and need to re-read it during rehydration. 1 minute is a balance between aggressive cleanup and read safety.
- `delete.retention.ms=60000` — how long a tombstone is retained after the key is compacted away. Long enough for a rehydrating consumer to see the tombstone and know the batch is done; short enough that completed batches don't bloat the log.
- `segment.ms=600000`, `segment.bytes=10485760` — small segments make compaction run frequently on the active segment. Default Kafka segment size (1 GB / 7 days) would mean the active segment never closes for low-volume topics, blocking compaction. 10 MB / 10 min is right for the expected bounded state size.
- `max.compaction.lag.ms=300000` — hard upper bound on how stale the log can get before compaction is forced.

**Partition count and replication factor:**

- `fanout-state`: **1 partition** in v1. Single-partition guarantees total order of writes for a given `(correlation_id, fan_out_id)` key, which simplifies the read-modify-write contract. The trade-off is throughput cap; at expected scale (low-100s of in-flight batches per agent), one partition holds. Document a v2 path: hash-by-correlation_id across N partitions when needed, with the aggregator consuming all partitions (no consumer group, or one consumer-per-partition).
- `fanout-returns`: **partition count co-partitioned with the agent's primary subscribe topic.** If the main topic has 6 partitions, this one has 6, keyed by `correlation_id`. This keeps a given correlation's returns on a single aggregator instance.
- Replication factor: **3** for both, matching production Kafka defaults. Configurable via `kafka_topic_config` for dev / single-broker environments where 1 is acceptable.

### 6.6 Why `frozenset[str]` on Kafka

Pydantic serialises `frozenset[str]` to a JSON array. On deserialise we coerce back via Pydantic's `Field` discriminator. Round-trip preserves equality. The Kafka payload is JSON (FastStream default); this works.

### 6.7 Forbidden patterns reaffirmed

- **No `dict[str, Any]` on the wire** in user-facing types. `FanOutState.collected_results` is `dict[str, Any]` only because `tool_results` upstream is `dict[str, ToolCallResult | Any]`. This is a known weak point in the existing State model; the aggregator does not make it worse.
- **No `headers` field on `Envelope`.** User propagation context flows via Kafka transport headers per the existing `_protocol.py` channel. The aggregator's `traceparent`/`tracestate` are also Kafka headers, never body fields.

---

## 7. Partitioning and idempotency

### 7.1 The linearisation contract

For any `(correlation_id, fan_out_id)`, all reads and writes to `fanout-state` must be linearisable from the aggregator's perspective. That is, the read-modify-write sequence "load batch, merge new return, write back" must not interleave with a competing writer.

In v1, this is achieved trivially: `fanout-state` has 1 partition, so one Kafka consumer-group member reads and writes it. Two aggregator instances cannot race because the consumer group assigns the single partition to one member.

In v2 with N partitions, the contract requires hash-by-correlation_id on both producer and consumer, so writes for the same correlation always land on the same partition and thus the same consumer.

### 7.2 Co-partitioning with the agent's main topic

The agent's main `subscribe_topics` and `fanout-returns` are partitioned by `correlation_id` (the existing `_publish_action` already does `key=correlation_id.encode()` at `base.py:180, 198, 213, 231`). If the agent's main topic has 6 partitions, `fanout-returns` has 6 partitions; both use the default murmur2 partitioner; the same correlation_id lands on the same partition number on both topics.

**Consequence:** the aggregator consumer-group member that owns partition 3 of `fanout-returns` is the one whose aggregator instance handles all returns for correlation IDs hashing to partition 3. The agent worker that owns partition 3 of the main topic resumes those correlations. Co-location of state and processing is automatic.

### 7.3 What happens during a Kafka rebalance mid-fan-out

Scenario: agent worker A is mid-fan-out for `correlation_id=X` (`fan_out_id=F`). Two tools have returned, one is outstanding. Worker A has the partition 3 assignment for `fanout-state`, `fanout-returns`, and the main topic. Worker B joins the consumer group.

1. Kafka triggers rebalance. Partition 3 is reassigned to worker B.
2. Worker A loses its assignment. Any uncommitted offsets are not committed; the messages will be redelivered to worker B.
3. Worker B starts up: its aggregator instance for the agent already exists in the local process (registered at Worker init), but its in-memory cache for the agent is empty (no rehydration has happened yet).
4. Worker B's aggregator subscribes to `fanout-state` for rehydration (see §10).
5. Worker B reads `fanout-state` from offset 0; it finds the record for `(X, F)` written at dispatch time + any subsequent write-throughs. The in-memory cache is rebuilt.
6. Worker B starts consuming `fanout-returns` partition 3. Returns for tools 1 and 2 may be redelivered (if their offsets were never committed) — these are deduped via the idempotency contract in §7.5. The third tool's return arrives normally and triggers completion.

The user sees: a brief pause during rebalance, then the batch completes. No `RuntimeError`. No lost work.

### 7.4 What happens during a partition rebalance after completion

Scenario: batch `(X, F)` completed. Worker A wrote a tombstone to `fanout-state` and re-entered the agent's main topic. Then rebalance reassigns partition 3 to worker B.

1. Worker B rehydrates `fanout-state` from offset 0. It sees the dispatch record for `(X, F)`, then the tombstone. The tombstone is honoured: the key is excluded from the in-memory cache. No batch is loaded.
2. Worker B starts consuming `fanout-returns`. A redelivered return for `correlation_id=X`, `fan_out_id=F` arrives (possible if worker A's offset commit was lost).
3. The aggregator looks up `(X, F)` in its cache: not present. The aggregator looks up the key in `fanout-state` to disambiguate between "never seen" and "tombstoned" — for performance, the in-memory cache should record both presence (active batch) and absence (recent tombstone, retained for `delete.retention.ms`). If the key is in the "recently completed" set, the return is logged as a late arrival and dropped.
4. If the tombstone has already been compacted away (after `delete.retention.ms + min.cleanable.dirty.ratio` compaction triggered), the rehydration sees no record at all and the return is logged as "orphan; possibly late arrival or possibly bug" and dropped.

This is the §10.3 "late return" path. The reason `delete.retention.ms=60000` is set explicitly: we want tombstones retained long enough to disambiguate "tombstoned" from "never existed" during the realistic rebalance window.

### 7.5 Dedup contract

The composite key for dedup is `(correlation_id, fan_out_id, tool_call_id)`.

**Inbound on `fanout-returns`:**
1. Decode `correlation_id` (Kafka header), `fan_out_id` (Kafka header), and unpack the `tool_call_id` from `envelope.context.state.tool_results` (there should be exactly one new entry).
2. Look up `(correlation_id, fan_out_id)` in the in-memory cache.
   - **Not found, no tombstone known:** rehydrate from `fanout-state` (one targeted read of the recent log). If still not found: log warning "orphan return"; drop.
   - **Found in active set:** check if `tool_call_id` is already in `batch.collected_results`. If yes: dedup hit, log DEBUG, drop. If no: proceed.
   - **Found in recently-tombstoned set:** log INFO "late return after completion"; drop. (Optionally route to DLQ — v2.)
3. Update `batch.collected_results[tool_call_id] = result`, set `last_updated_ms`, write-through to `fanout-state`.
4. Call `on_partial(batch, latest)`.
5. Call `should_complete(batch)`. If true, run §8.4 completion path.

**Outbound dispatch dedup:**

A redelivered inbound on the agent's main topic re-runs `run()` and re-emits the parallel `list[Call]`. Without dedup, this would publish N more tool calls and create a second batch.

Two-stage guard:

1. **`fan_out_id` is derived deterministically from a stable source per agent turn.** Use `envelope.internal_workflow_state.current_frame.frame_id` (the uuid7 assigned to the inbound frame at `session_context.py:38`). Same redelivery → same frame_id → same `fan_out_id`.
2. **Before dispatching, the agent checks `fanout-state` for an existing record at `(correlation_id, fan_out_id)`.** If present and `expected_tool_call_ids` matches, the agent skips re-dispatch (the prior dispatch already happened; returns are en route). The agent returns `Silent()`.

This is the "idempotent dispatch" pattern. It is small additional cost (one cache lookup at dispatch time) and is critical for at-least-once correctness.

### 7.6 What is allowed to be slow

- Compaction. `min.compaction.lag.ms=60000` means a key can be uncompacted for at least a minute. Rehydration reads from offset 0; in pathological cases this could mean reading a few minutes of writes. Acceptable; rehydration is a startup-only cost.
- Late arrivals. A tool return that arrives 30 minutes after completion is dropped. No retry, no DLQ in v1.

### 7.7 What is not allowed to be slow

- The in-memory cache lookup on every return. O(1) dict access.
- The write-through to `fanout-state` on every return. One async publish; small message (State-sized).

---

## 8. Lifecycle sequences

### 8.1 Dispatch (parallel fan-out begins)

```
agent.run() returns list[Call]
  |
  v
_publish_action receives list[Call]
  |
  +--- compute fan_out_id = envelope.internal_workflow_state.current_frame.frame_id
  |
  +--- compute expected_tool_call_ids = frozenset(c.state.tool_results plus pending)
  |    Note: pending_tool_calls in agent.run() already determined the set; pass it through
  |
  +--- IDEMPOTENT DISPATCH CHECK
  |    aggregator._kafka_state_store.get((corr, fan_out_id))
  |    if found and matches expected: log "duplicate dispatch suppressed"; return envelope
  |
  +--- write FanOutState to {node_id}.fanout-state
  |    key   = f"{corr}|{fan_out_id}"
  |    value = FanOutState(
  |       correlation_id=corr, fan_out_id=fan_out_id, node_id=self.node_id,
  |       expected_tool_call_ids=expected,
  |       collected_results={},
  |       base_state=envelope.context.state.model_copy(deep=True),
  |       started_at_ms=now_ms, last_updated_ms=now_ms,
  |       traceparent=inbound_headers.get("traceparent"),
  |       tracestate=inbound_headers.get("tracestate"),
  |    )
  |    Wait for ack (acks=all) — this is the durability gate.
  |
  +--- for each Call in list[Call]:
  |       build wf_copy: copy workflow_state; invoke_frame(call, callback_topic=fanout-returns)
  |       NOTE: callback_topic is overridden to {node_id}.fanout-returns
  |             (vs. the existing default self.subscribe_topics[0])
  |       publish to wf_copy.current_frame.target_topic with:
  |         key      = correlation_id.encode()
  |         headers  = {
  |           HDR_EMITTER, HDR_EMITTER_KIND, HDR_FRAME_ID (new frame),
  |           HDR_FANOUT_ID: fan_out_id,
  |           traceparent: (propagated or new span),
  |           tracestate:  (propagated or new),
  |         }
  |
  v
return envelope (echo to caller; agent waits for aggregator to re-enter)
```

The order is critical: **write `FanOutState` first, then publish tool Calls.** If the state write is durable (acks=all, replication=3) but tool Calls fail to publish, the next redelivery of the inbound will re-dispatch (the idempotent check sees the existing state, but `expected_tool_call_ids` differs only if the new dispatch decided differently — same inputs ⇒ same outputs, so it matches; on match, dispatch is suppressed; ah but the tools haven't actually run!). Subtle: if the inbound is redelivered before any tool Call published successfully, the `FanOutState` exists but `collected_results` is empty and no tools were actually called. Redispatch must re-publish tool Calls in that case.

Resolution: the idempotent-dispatch check is "state record exists AND `expected_tool_call_ids` matches" → suppress. But on suppression we still need to *guarantee* the tools were called. Two-phase shape:

- Stage A: write `FanOutState(dispatched_calls=False, ...)` — durably record intent.
- Stage B: publish tool Calls.
- Stage C: write `FanOutState(dispatched_calls=True, ...)` — durably record completion of dispatch.

On redelivery, if stage A exists but stage C doesn't, retry stage B. If stage C exists, suppress.

This adds one round-trip to dispatch. Reasonable in practice; this is the at-least-once tax.

**Alternative (simpler):** since tool Calls are idempotent themselves under at-least-once (tools re-running with the same `correlation_id` and `tool_call_id` should be safe — that is, after all, the calfkit contract), we can skip the two-phase ceremony and just re-publish on redelivery. The aggregator's dedup at the returns side handles duplicate completions.

**Recommendation:** the simpler alternative. Document that tools must be idempotent on `(correlation_id, tool_call_id)`. This is the existing implicit contract; we make it explicit. Stage A and Stage B collapse into one write.

### 8.2 Tool return (one of N)

```
tool.run() returns ReturnCall(state=ctx.state)
  |
  v
_publish_action unwinds the tool's frame
  |
  +--- frame.callback_topic = "{agent_node_id}.fanout-returns" (was rewritten at dispatch)
  |
  +--- publish standard Envelope to fanout-returns with:
        key     = correlation_id.encode()
        headers = {
          HDR_EMITTER: tool.node_id, HDR_EMITTER_KIND: "tool",
          HDR_FRAME_ID: new_frame_id,
          HDR_FANOUT_ID: (echoed from inbound),
          traceparent, tracestate (echoed),
        }
```

The tool node has no knowledge of the aggregator. The rewritten `callback_topic` does all the work. Tools remain pure.

### 8.3 Aggregator handler (per return)

```
aggregator_handler(envelope, correlation_id, headers, broker):
  |
  +--- fan_out_id = decode_header_str(headers[HDR_FANOUT_ID])
  |    if fan_out_id is None: log WARN "missing fanout-id"; drop
  |
  +--- key = (correlation_id, fan_out_id)
  |
  +--- batch = self._cache.get(key)
  |    if batch is None:
  |       # not in cache — possibly tombstoned, possibly orphan
  |       # check the recently-tombstoned set
  |       if key in self._recently_completed:
  |          log INFO "late return after completion"; drop
  |          # optional v2: forward to DLQ topic
  |          return
  |       # not in cache, not tombstoned — orphan
  |       log WARN "orphan return"; drop
  |       return
  |
  +--- # Extract the new tool result
  |    new_returns = {tcid: r for tcid, r in envelope.context.state.tool_results.items()
  |                  if tcid in batch.expected_tool_call_ids
  |                  and tcid not in batch.collected_results}
  |
  +--- if not new_returns:
  |       # dedup hit (already collected) or no expected matches (bug)
  |       log DEBUG "no new returns; dedup or no-match"; ack and return
  |
  +--- # Update batch
  |    for tcid, result in new_returns.items():
  |       batch.collected_results[tcid] = result
  |    batch.last_updated_ms = now_ms
  |
  +--- # Write-through to compacted topic
  |    await self._kafka_state_store.put(key, batch.to_fanout_state())
  |
  +--- # Build immutable view for user hooks
  |    view = AggregatorBatch(
  |       correlation_id=corr, expected_tool_call_ids=batch.expected_tool_call_ids,
  |       collected_results=MappingProxyType(dict(batch.collected_results)),
  |       base_state=batch.base_state, started_at_ms=batch.started_at_ms,
  |    )
  |
  +--- # Call user hook
  |    latest = AggregatedReturn(
  |       tool_call_id=tcid,                 # only one expected per return
  |       tool_name=batch.base_state.tool_calls[tcid].tool_name,
  |       result=new_returns[tcid],
  |       received_at_ms=now_ms,
  |    )
  |    try:
  |       await self._aggregator.on_partial(view, latest)
  |    except Exception:
  |       log ERROR "on_partial raised"; continue (do not block completion)
  |
  +--- # Check completion
  |    if await self._aggregator.should_complete(view):
  |       go to §8.4 completion path
  |    else:
  |       ack and return (wait for more)
```

### 8.4 Completion path

```
# Triggered when should_complete() returns True
  |
  +--- try:
  |       merged_state = await self._aggregator.merge(
  |          batch.base_state, batch.collected_results)
  |    except Exception as e:
  |       handle per on_merge_error policy:
  |         "abort":  raise AggregatorMergeError(e) — FastStream will nack/redeliver
  |         "retry":  retry up to merge_retry_count times then abort
  |         "drop":   log + tombstone + drop (agent will not resume)
  |       return
  |
  +--- # Reconstruct the agent's re-entry envelope
  |    # The agent expects to consume on its main topic, with the merged State.
  |    # The call_stack must look as it did when the inbound that triggered the
  |    # original fan-out was being handled — i.e., one frame above the fan-out
  |    # frame. The fan-out frame itself is consumed (the "frame" of the parallel
  |    # dispatch is logically completed).
  |    re_entry_envelope = Envelope(
  |       context=SessionRunContext(state=merged_state, deps=batch.base_state_deps),
  |       internal_workflow_state=batch.base_workflow_state,  # snapshotted at dispatch
  |    )
  |
  +--- # Tombstone fanout-state record
  |    await self._kafka_state_store.tombstone(key)
  |
  +--- # Add to recently-tombstoned set (TTL = delete.retention.ms = 60s)
  |    self._recently_completed.add(key, ttl_ms=60_000)
  |
  +--- # Publish to agent's main topic
  |    await broker.publish(
  |       re_entry_envelope,
  |       topic=self._agent.subscribe_topics[0],     # agent main topic
  |       correlation_id=correlation_id,
  |       key=correlation_id.encode(),
  |       headers={
  |          HDR_EMITTER: self._agent.node_id,
  |          HDR_EMITTER_KIND: "agent",
  |          HDR_FRAME_ID: new_frame_id,
  |          # HDR_FANOUT_ID is absent — the batch is done, this is a resumption
  |          traceparent: batch.traceparent,
  |          tracestate:  batch.tracestate,
  |       },
  |    )
  |
  +--- log INFO "batch completed"; emit metric
  +--- ack and return
```

A subtle property: **the merged state goes to the agent's main topic, not to the original caller.** The agent then runs its next LLM iteration. The caller's reply only happens when the agent eventually returns a `ReturnCall` after all LLM iterations are done.

### 8.5 Worker startup rehydration

```
Worker.run():
  |
  +--- for each node in self._nodes:
  |    for each KafkaSubscription in node.kafka_subscriptions():
  |       register the subscriber on the broker
  |       (this includes the aggregator's fanout-returns subscriber)
  |
  +--- # Pre-flight: ensure aggregator topics exist with correct config
  |    for each agent in self._nodes:
  |       if agent has aggregator:
  |          await agent.aggregator._ensure_topics(broker)
  |          # checks topic exists, validates cleanup.policy=compact for state topic
  |          # creates with default config if missing (requires admin API)
  |          # logs WARN and continues if cluster forbids topic creation
  |
  +--- # Rehydration: build the in-memory cache from fanout-state
  |    for each agent in self._nodes:
  |       if agent has aggregator:
  |          await agent.aggregator._rehydrate(broker)
  |          # see §10 for rehydration steps
  |
  +--- # Start the FastStream broker (begins consuming all subscribed topics)
  |    await FastStream(self._client._connection).run(...)
```

Rehydration steps inside `agent.aggregator._rehydrate(broker)`:

```
1. Create a one-shot consumer on {agent.node_id}.fanout-state.
   - group_id = unique per worker instance (NOT a stable group; we want full read-from-zero)
   - auto_offset_reset = "earliest"
   - enable_auto_commit = False
2. Read until end-of-log (high watermark) timestamp is exceeded:
   - For each record:
     - if value is None (tombstone): drop key from cache; record in _recently_completed
       with received_at as TTL anchor
     - else: deserialise to FanOutState; insert into cache
3. Close the consumer.
4. Emit metric: calfkit_aggregator_rehydration_seconds.observe(duration)
5. Emit log: INFO "rehydration complete batches={n} returns={m}"
6. Mark aggregator as "ready"; release any awaiters.
```

**When does main topic consumption begin?**

Two options:

- **A. Rehydrate-then-serve (sequential).** Block all subscribers until rehydration is complete. Simple; safe; adds startup latency proportional to log size.
- **B. Rehydrate-while-serving (parallel).** Begin consuming main topic immediately; if a return arrives for a `(corr, fan_out_id)` not yet seen by rehydration, block that one handler until rehydration is past that offset. Lower startup latency; more complex.

**Recommendation: A for v1.** Rehydration of bounded state (low-thousands of records, JSON-serialised) is fast — sub-second to a few seconds. Startup latency is acceptable. The B path is a v2 optimisation if startup time becomes a problem in production.

Implementation detail: Worker.register_handlers() returns synchronously today and FastStream's `run()` does the subscribing. We need a hook *between* register and run: pre-flight + rehydration. The simplest path is to make `Worker.run()` async-await the rehydration step before calling `FastStream(...).run(...)`.

### 8.6 Graceful shutdown

```
Worker shutdown (e.g. SIGTERM):
  |
  +--- FastStream's lifespan handler triggers
  |
  +--- For each aggregator:
  |     - drain pending write-throughs (await any in-flight broker.publish to fanout-state)
  |     - flush logs
  |     - (optional in v2) commit current consumer offsets explicitly
  |
  +--- Stop the broker connection
```

We do NOT flush the cache to disk or write a final "checkpoint" record. The compacted log is the source of truth. On the next start, rehydration rebuilds the cache.

---

## 9. Idempotency and failure modes

Restated and consolidated from §7 plus new failure scenarios:

### 9.1 Failure-mode matrix

| Failure | Detection | Recovery | User-visible effect |
|---|---|---|---|
| Agent process crashes after writing FanOutState, before any tool dispatch | Inbound redelivered; idempotent dispatch check finds prior state | Re-dispatch tool Calls (per simpler alternative in §8.1) | Tool runs once or twice (idempotency required); batch completes normally |
| Agent process crashes after dispatching some tools, before all | Some tool Calls were published; others were not. Inbound redelivered. | Re-dispatch; deduped via tool's own idempotency on `(correlation_id, tool_call_id)` | Some tools run twice; results converge |
| Tool process crashes mid-execution | No return arrives within idle_timeout | Aggregator emits FanOutTimeoutError; agent receives it as an inbound exception | Configurable error handling at agent layer |
| Aggregator process crashes after receiving return, before write-through | Return's consumer offset never committed; redelivered to next owner | Next owner re-receives; dedup based on `tool_call_id` not in `collected_results` (post-rehydration); proceeds | None |
| Aggregator process crashes after write-through, before completion check | Cache lost; rehydrate finds updated state; should_complete re-runs | Completion fires (if applicable); re-entry happens | Slight delay; correct outcome |
| Aggregator process crashes after completion publish, before tombstone | re-entry message visible to agent; agent runs next LLM call; aggregator next-start sees no tombstone and considers the batch still active | The agent's next inbound might already be on the main topic. Aggregator's rehydration loads the (now-stale) state; subsequent late returns are deduped via collected_results. Eventually `idle_timeout` fires and the aggregator emits `FanOutTimeoutError` to a duplicate run that already completed. | Risk of duplicate FanOutTimeoutError firing for a completed batch. Mitigation: emit tombstone BEFORE re-entry publish. Trade: tombstone may exist for a batch whose re-entry was lost. Then the agent never resumes, and the next idle_timeout fires.... |

The last row is the trickiest. The atomicity gap between "tombstone write" and "re-entry publish" cannot be closed without Kafka transactions. Two non-transactional mitigations:

1. **Write tombstone first, then publish re-entry.** If tombstone succeeds but re-entry fails, the agent never resumes. On rehydration, the batch is gone. There is no way to recover the in-flight correlation from the aggregator's records. The agent is stuck waiting for a resumption that will never come.
2. **Publish re-entry first, then write tombstone.** If re-entry succeeds but tombstone fails, the agent resumes (and may complete its work). On rehydration, the batch reloads; idle_timeout will eventually fire. The fired timeout is "for a completed batch" — a confusing observability artifact. The agent's actual run is unaffected.

**Recommendation:** option 2 (re-entry first, then tombstone). The failure mode is a noisy false-positive `FanOutTimeoutError` for a batch that actually completed — annoying, observable, but the work was done correctly. Option 1 risks lost work, which is worse.

If exactly-once is needed, Kafka transactions (atomic write-and-publish across `fanout-state` and the agent's main topic) are the correct solution. This is a v2 consideration.

### 9.2 The dedup contract restated

The aggregator MUST be idempotent under at-least-once delivery on the following keys:

- **Same tool return arrives twice**: dedup via `tool_call_id in batch.collected_results` check. Drop the second.
- **Same dispatch happens twice**: dedup via idempotent-dispatch check (state record exists for `(corr, fan_out_id)`). Suppress the second.
- **Same completion publish happens twice**: the agent's main topic handler must be idempotent on `correlation_id`. The agent's next LLM iteration sees a deduped inbound and produces a single resume. This is the existing calfkit contract for redelivery on any handler.

### 9.3 Late-return policy

A return arriving for a `(correlation_id, fan_out_id)` that is in `_recently_completed` (tombstoned within the last 60s) is logged at INFO and dropped. A return arriving for a key not in cache and not in `_recently_completed` is logged at WARNING ("orphan return") and dropped. No DLQ in v1; the operator's diagnostic path is logs + metrics.

### 9.4 Merge error policy

The user's `merge()` can raise. The policy is `on_merge_error` on the `FanOutAggregator` constructor:

- `"abort"` (default): raise `AggregatorMergeError(orig)`. FastStream nacks the inbound; Kafka redelivers. If the underlying issue is transient (network blip), the next try succeeds. If permanent (bug in `merge`), the message redelivers forever — operator must intervene. **This is the standard Kafka at-least-once failure mode.**
- `"retry"`: retry `merge()` up to `merge_retry_count` times in-process before raising `AggregatorMergeError`.
- `"drop"`: log the error, write tombstone, drop the batch. Agent never resumes. Use only for fire-and-forget patterns. Document loudly.

---

## 10. State store internals

### 10.1 The in-memory cache

```python
# calfkit/nodes/aggregator/state_store.py

class _InFlightBatch:
    """Mutable in-memory representation. Mirrors FanOutState on the wire,
    but lives in process memory for hot-path mutation."""

    correlation_id: str
    fan_out_id: str
    expected_tool_call_ids: frozenset[str]
    collected_results: dict[str, Any]
    base_state: State
    started_at_ms: int
    last_updated_ms: int
    traceparent: str | None
    tracestate: str | None

    def to_fanout_state(self) -> FanOutState: ...

class _KafkaStateStore:
    """Write-through cache over {node_id}.fanout-state."""

    _cache: dict[tuple[str, str], _InFlightBatch]      # key = (corr_id, fan_out_id)
    _recently_completed: _TtlSet[tuple[str, str]]      # 60s TTL
    _broker: KafkaBroker
    _state_topic: str
    _ready: asyncio.Event

    async def rehydrate(self) -> None: ...
    async def get(self, key: tuple[str, str]) -> _InFlightBatch | None: ...
    async def put(self, key: tuple[str, str], state: FanOutState) -> None: ...
    async def tombstone(self, key: tuple[str, str]) -> None: ...
    def mark_completed(self, key: tuple[str, str]) -> None: ...
```

### 10.2 Memory bounds

Cache size scales with `pending_batches × N_tool_calls_per_batch`. For a single agent at 100 in-flight conversations × 10 tool calls per turn ≈ 1,000 active entries. Each entry contains a `State` snapshot — bounded by `message_history` length, in practice 10s of KB. Total: 10–100 MB per agent. Reasonable for v1.

When does this become a problem?

- Million-conversation-per-second agents.
- Long-lived conversations with huge message_history.
- Many parallel agents on one worker.

Future direction: RocksDB-backed cache (FastStream / Faust pattern), with the in-memory cache becoming an LRU over the disk store. Out of scope for v1.

### 10.3 Why no explicit checkpoints

We could write a periodic snapshot of the cache to a separate topic to reduce rehydration cost. We don't, for v1:

- Rehydration of bounded state is fast.
- Checkpoints add complexity (compacted topic semantics already give us per-key retention).
- The compacted log IS the snapshot; tombstoned keys are cheap.

If rehydration time becomes operationally meaningful (say >30s), revisit by either:
- Adding a periodic snapshot topic.
- Sharing the in-memory cache across worker instances (Sharding via consumer groups; v2).

---

## 11. Observability — API + internals

### 11.1 Logs

Namespace: `calfkit.aggregator`. Format mirrors `BaseNodeDef.handler()` (`correlation_id[:8]`, `node=`).

| Event | Level | Fields |
|---|---|---|
| Batch dispatch started | DEBUG | `correlation_id`, `node_id`, `fan_out_id`, `expected_count` |
| State record written (write-through) | DEBUG | `correlation_id`, `fan_out_id`, `collected_count`, `expected_count` |
| Return integrated | DEBUG | `correlation_id`, `tool_call_id`, `tool_name`, `progress` |
| Late return dropped | INFO | `correlation_id`, `fan_out_id`, `tool_call_id`, `elapsed_since_complete_ms` |
| Orphan return dropped | WARNING | `correlation_id`, `fan_out_id`, `tool_call_id` |
| Batch completed | INFO | `correlation_id`, `fan_out_id`, `node_id`, `duration_ms`, `result_count` |
| Batch idle-timed-out | WARNING | `correlation_id`, `fan_out_id`, `node_id`, `missing_ids`, `idle_ms` |
| Rehydration started | INFO | `node_id` |
| Rehydration complete | INFO | `node_id`, `restored_batches`, `tombstoned_keys`, `duration_ms` |
| State-store write failed | ERROR | `correlation_id`, `fan_out_id`, `error`, `retry_count` |
| Merge error | ERROR | `correlation_id`, `fan_out_id`, `error`, `policy_applied` |
| Duplicate dispatch suppressed | INFO | `correlation_id`, `fan_out_id` |

### 11.2 OTel spans

The aggregator emits one span per batch lifecycle: `calfkit.aggregator.batch`.

**Trace propagation across the fan-out boundary** — the unique calfkit angle:

1. **Inbound to agent:** `traceparent` header (if present) becomes the parent context for the agent's run span.
2. **At dispatch time:** `traceparent` is captured into `FanOutState.traceparent`. Each outbound tool Call's `traceparent` header is a new child span context (one per call), so each tool's run is a sibling of the others under the original parent.
3. **Tool processes its Call:** `traceparent` becomes parent of the tool's run span. Returns echo the original `traceparent`.
4. **Aggregator handles return:** opens a `aggregator.handle_return` span linked to the batch span; the latter is rooted under the dispatch span via `traceparent`.
5. **Completion → re-entry:** the re-entry envelope's `traceparent` is the original (captured) `traceparent` — the agent's next LLM iteration is rooted under the same trace.

Net effect: a Jaeger / Tempo / Datadog trace for one user request shows: client → agent.run (dispatch) → [tool_a, tool_b, tool_c in parallel] → aggregator.batch (completion) → agent.run (next iteration) → ... → final reply. One trace ID. Across process and host boundaries.

Span attributes on `calfkit.aggregator.batch`:

```
calfkit.node_id           = <agent node_id>
calfkit.correlation_id    = <correlation id>
calfkit.fan_out_id        = <uuid7>
calfkit.expected_count    = <int>
calfkit.completed_count   = <int>      (set on close)
calfkit.duration_ms       = <int>      (set on close)
calfkit.outcome           = "success" | "timeout" | "merge_error" | "dropped"
```

Span events: one per return integrated (`tool_returned`, attributes: `tool_call_id`, `tool_name`).

### 11.3 Metrics (Prometheus / OTel metrics)

```
calfkit_aggregator_batches_started_total{node_id="..."}                  counter
calfkit_aggregator_batches_completed_total{node_id="...", outcome="..."} counter
calfkit_aggregator_batch_duration_seconds{node_id="..."}                 histogram
calfkit_aggregator_batch_size{node_id="..."}                             histogram
calfkit_aggregator_returns_total{node_id="...", tool_name="..."}         counter
calfkit_aggregator_returns_late_total{node_id="..."}                     counter
calfkit_aggregator_returns_orphan_total{node_id="..."}                   counter
calfkit_aggregator_pending_batches{node_id="..."}                        gauge
calfkit_aggregator_rehydration_duration_seconds{node_id="..."}           histogram
calfkit_aggregator_rehydrated_batches{node_id="..."}                     gauge (set on rehydration complete)
calfkit_aggregator_state_store_writes_total{node_id="...", kind="..."}   counter   # kind=put|tombstone
calfkit_aggregator_state_store_write_errors_total{node_id="..."}         counter
```

Low cardinality. No `correlation_id` or `tool_call_id` labels. Operators answer "is anything stuck?" with `pending_batches > 0 and rate(returns) == 0`.

### 11.4 OTel opt-out

Auto-detect on `opentelemetry` import; `CALFKIT_AGGREGATOR_OTEL=0` disables. When disabled, no spans emitted; metrics and logs continue. This matches the existing `calfkit/_vendor/pydantic_ai/_instrumentation.py` convention.

---

## 12. File-by-file changes

Concrete plan. Sizes are order-of-magnitude estimates.

### 12.1 New files

#### `calfkit/nodes/aggregator/__init__.py` (~30 LoC)

```python
"""Durable join for parallel tool calls.

See ``FanOutAggregator`` for the public API. The companion construct is owned
by an ``Agent`` and is auto-attached at construction time when none is provided.
"""

from calfkit.nodes.aggregator.aggregator import FanOutAggregator
from calfkit.nodes.aggregator.errors import (
    AggregatorError, AggregatorMergeError, AggregatorStateStoreError, FanOutTimeoutError,
)
from calfkit.nodes.aggregator.state import AggregatedReturn, AggregatorBatch, ToolCallId

__all__ = [
    "FanOutAggregator",
    "AggregatorBatch",
    "AggregatedReturn",
    "ToolCallId",
    "AggregatorError",
    "FanOutTimeoutError",
    "AggregatorMergeError",
    "AggregatorStateStoreError",
]
```

#### `calfkit/nodes/aggregator/aggregator.py` (~400 LoC)

Houses `FanOutAggregator` class with:

- `__init__` (config knobs: `idle_timeout`, `on_merge_error`, `merge_retry_count`, `kafka_topic_config`, `state_topic`, `returns_topic`).
- `merge`, `should_complete`, `on_partial` (default implementations).
- `kafka_subscriptions(node_id, broker)` — builds the `_KafkaSubscription` for `fanout-returns`.
- `_handle_return(envelope, correlation_id, headers, broker)` — the actual Kafka handler.
- `_rehydrate(broker)` — invoked from Worker startup.
- `_ensure_topics(broker)` — pre-flight topic creation/validation.
- Internal references: `self._kafka_state_store: _KafkaStateStore`, `self._agent: Agent | None` (back-reference set when bound).

#### `calfkit/nodes/aggregator/state.py` (~250 LoC)

```python
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Generic, NewType

from pydantic import BaseModel, ConfigDict, Field

from calfkit._types import StateT
from calfkit.models.state import State

ToolCallId = NewType("ToolCallId", str)

@dataclass(frozen=True)
class AggregatedReturn(Generic[Any]):
    tool_call_id: ToolCallId
    tool_name: str
    result: Any
    received_at_ms: int

@dataclass(frozen=True)
class AggregatorBatch(Generic[StateT]):
    correlation_id: str
    fan_out_id: str
    expected_tool_call_ids: frozenset[ToolCallId]
    collected_results: Mapping[ToolCallId, Any]   # MappingProxyType for immutability
    base_state: StateT
    started_at_ms: int

    @property
    def is_complete(self) -> bool: ...
    @property
    def progress(self) -> float: ...

class FanOutState(BaseModel):
    """Wire format for the {node_id}.fanout-state compacted topic."""

    model_config = ConfigDict(extra="ignore")

    correlation_id: str
    fan_out_id: str
    node_id: str
    expected_tool_call_ids: frozenset[str]
    collected_results: dict[str, Any] = Field(default_factory=dict)
    base_state: State
    started_at_ms: int
    last_updated_ms: int
    schema_version: int = 1
    traceparent: str | None = None
    tracestate: str | None = None
```

#### `calfkit/nodes/aggregator/errors.py` (~80 LoC)

```python
class CalfkitError(Exception):
    """Base for all calfkit-raised errors. (New base — see §12.5.)"""

class AggregatorError(CalfkitError): ...

class FanOutTimeoutError(AggregatorError):
    def __init__(
        self,
        correlation_id: str,
        node_id: str,
        fan_out_id: str,
        missing_tool_call_ids: frozenset[str],
        elapsed: timedelta,
    ) -> None: ...

class AggregatorMergeError(AggregatorError):
    def __init__(self, original: Exception, correlation_id: str, fan_out_id: str) -> None: ...

class AggregatorStateStoreError(AggregatorError):
    def __init__(self, op: str, correlation_id: str | None, fan_out_id: str | None, original: Exception) -> None: ...
```

#### `calfkit/nodes/aggregator/state_store.py` (~450 LoC)

The `_KafkaStateStore` write-through cache. Methods per §10.1.

Key implementation notes:
- The rehydration consumer uses a uniquely-named group_id per worker instance to ensure read-from-zero.
- Write-through `put()` uses `acks=all` (Kafka producer config) to wait for durable commit.
- The `_recently_completed` TTL set uses asyncio-friendly bookkeeping (a `deque` of `(key, expiry_ms)` checked on insert).

#### `calfkit/nodes/aggregator/testing.py` (~300 LoC)

```python
class InMemoryAggregator(FanOutAggregator):
    """Drop-in for FanOutAggregator that skips the Kafka state-store layer.

    Backed by a plain dict; supports persist_to_disk for cross-restart tests.
    """

    def __init__(
        self,
        *,
        persist_to_disk: bool = False,
        path: Path | None = None,
        idle_timeout: timedelta = timedelta(minutes=5),
        on_merge_error: Literal["abort", "retry", "drop"] = "abort",
        merge_retry_count: int = 0,
    ) -> None: ...

    @classmethod
    def wrap(cls, base: FanOutAggregator) -> "InMemoryAggregator":
        """Take an existing aggregator subclass and wrap it with in-memory storage."""

    @classmethod
    def rehydrate_from_disk(cls, path: Path) -> "InMemoryAggregator": ...

    async def wait_for_partial_state(
        self, *, correlation_id: str | None = None, n_returns: int = 1
    ) -> AggregatorBatch: ...

    async def wait_for_completion(
        self, *, correlation_id: str, timeout: float = 5.0
    ) -> AggregatorBatch: ...

    def drop_correlation(self, correlation_id: str) -> None: ...

    def simulate_restart(self) -> "InMemoryAggregator": ...

    def set_clock(self, now_ms: int) -> None:
        """Inspired by Temporal's TestWorkflowEnvironment.advance_time().
        Lets tests inject a deterministic clock for timeout testing."""
```

#### `tests/test_durable_aggregator.py` (~700 LoC)

New test file for the v1 feature set. Outline in §13.

### 12.2 Edited files

#### `calfkit/nodes/agent.py`

Remove `_pending_batches`, `_parallel_state_aggregation`. Add `aggregator` parameter to `__init__`. Wire the aggregator into the parallel branch of `run()`:

**Before (lines 50-52, 68-79, 97-101, 122-127, 216-221):**
- `self._pending_batches: dict[str, PendingToolBatch] = dict()`
- `_parallel_state_aggregation` method
- Parallel-aggregation gate in `run()`
- The `RuntimeError` about lost state
- The `self._pending_batches[corr] = PendingToolBatch(...)` write

**After:**
- `self.aggregator: FanOutAggregator = aggregator or FanOutAggregator()`
- No `_pending_batches`, no `_parallel_state_aggregation`. The aggregation gate goes away — when the agent's main topic gets an inbound after aggregation, the merged state is already in `ctx.state`.
- The dispatch path returns `list[Call]` unchanged. The write-through to `fanout-state` happens inside `_publish_action` (see `calfkit/nodes/base.py` change below).

`add_tools()`, `instructions()` decorator — unchanged.

Estimated diff: ~50 lines removed, ~30 lines added.

#### `calfkit/nodes/base.py`

1. Add `kafka_subscriptions(self) -> list[_KafkaSubscription]` method on `BaseNodeDef`. Default returns one subscription for the main handler. `BaseAgentNodeDef` overrides to extend with the aggregator's subscription.

2. Inside `_publish_action`, the `list[Call]` branch is enhanced:
   - Compute `fan_out_id` from `envelope.internal_workflow_state.current_frame.frame_id`.
   - **Before publishing tool Calls**, write a `FanOutState` record to `{self.node_id}.fanout-state` (via `self.aggregator._kafka_state_store.put(...)`). This is the durability gate.
   - Override `wf_copy.invoke_frame(call, callback_topic=f"{self.node_id}.fanout-returns")` instead of `callback_topic=self.subscribe_topics[0]`.
   - Stamp `HDR_FANOUT_ID` and `HDR_FRAME_ID` on each outbound publish.

3. Add `HDR_FRAME_ID` to `_emitter_headers` — promoted from body per agent memory. Signature stays `dict[str, str]`.

Estimated diff: ~80 lines changed.

#### `calfkit/_protocol.py`

Add `HDR_FANOUT_ID` and `HDR_FRAME_ID` constants. ~20 lines added.

#### `calfkit/worker/worker.py`

`register_handlers` iterates `node.kafka_subscriptions()` instead of doing one subscription per node. `run()` adds pre-rehydration step before `FastStream(...).run()`.

```python
def register_handlers(self) -> None:
    if self._prepared: raise RuntimeError(...)
    for node in self._nodes:
        for sub in node.kafka_subscriptions():
            subscriber = self._client._connection.subscriber(
                *sub.topics,
                group_id=sub.group_id or self._group_id or node.name,
                max_workers=self._max_workers,
                **self._extra_subscribe_kwargs,
            )
            handler = subscriber(sub.handler)
            if sub.publish_topic:
                self._client._connection.publisher(sub.publish_topic, ...)(handler)
    self._prepared = True

async def run(self, **extra_run_args: Any) -> None:
    self.register_handlers()
    if not self._client._connection._connection:
        await self._client._connection.start()
    # Pre-flight + rehydration
    for node in self._nodes:
        if hasattr(node, "aggregator") and node.aggregator is not None:
            await node.aggregator._ensure_topics(self._client._connection)
            await node.aggregator._rehydrate(self._client._connection)
    # Begin serving
    await FastStream(self._client._connection).run(**extra_run_args)
```

Estimated diff: ~40 lines changed.

#### `calfkit/models/state.py`

Remove `PendingToolBatch` dataclass entirely (lines 128-143). Nothing else needs to change in this file.

Estimated diff: ~15 lines removed.

#### `calfkit/__init__.py`

Re-export `FanOutAggregator` from `calfkit.nodes`:

```python
from calfkit.nodes import (..., FanOutAggregator, ...)
__all__ = [..., "FanOutAggregator", ...]
```

Note: per the DX design §7.4, we do NOT re-export from the top-level `calfkit` module — keep the top-level surface minimal. **Override:** since the existing `calfkit/__init__.py` already re-exports `Agent`, consistency suggests re-exporting `FanOutAggregator` too. The DX design's intent is that the import path stays semantically meaningful (`from calfkit.nodes import FanOutAggregator` reads as "a node-level concern"). Either is defensible. Recommendation: re-export from `calfkit.nodes` only, not from `calfkit`, to match the DX design verbatim.

#### `calfkit/nodes/__init__.py`

Add `FanOutAggregator` re-export from `calfkit.nodes.aggregator`.

```python
from calfkit.nodes.aggregator import FanOutAggregator
__all__ = [..., "FanOutAggregator", ...]
```

#### `calfkit/exceptions.py`

Add `CalfkitError` base if not present (currently the module has only `DeserializationError`). Update `DeserializationError` to inherit from `CalfkitError`. The aggregator's error hierarchy hangs off `CalfkitError`.

Estimated diff: ~15 lines added.

#### `tests/conftest.py`

Add fixtures:

- `aggregator_topics_factory` — produces (state_topic, returns_topic) names for a deployed agent.
- `inmemory_aggregator_factory` — produces `InMemoryAggregator` instances for tests.
- Update existing `deploy_agent`-style fixtures so that they accept an optional `aggregator=` parameter.

Estimated diff: ~40 lines added.

#### `tests/test_concurrent_tool_calls.py`

Replace contents with one happy-path durable-aggregation test (covered fully in `tests/test_durable_aggregator.py`). The existing test can stay as-is — it still exercises the parallel path; with the new aggregator wired in, it transparently uses durable aggregation. **Recommendation: keep this test as a smoke test; do not delete.** Add a comment noting it now relies on `InMemoryAggregator` under `TestKafkaBroker`.

### 12.3 Removed files

None. Everything is additive or in-place edits.

### 12.4 The `CalfkitError` prerequisite

`calfkit/exceptions.py` currently has one class. Introducing `CalfkitError` as the base of the new hierarchy is a small prerequisite. Land it as part of the aggregator PR or as a small predecessor PR — author's choice.

### 12.5 Total LoC estimate

| Component | Added | Removed | Net |
|---|---|---|---|
| aggregator subpackage | ~1,500 | 0 | +1,500 |
| Tests | ~700 | ~80 | +620 |
| `agent.py` | ~30 | ~50 | -20 |
| `base.py` | ~80 | ~10 | +70 |
| `worker.py` | ~40 | ~10 | +30 |
| `state.py` | 0 | ~15 | -15 |
| `_protocol.py` | ~20 | 0 | +20 |
| `__init__.py` files | ~10 | 0 | +10 |
| `exceptions.py` | ~15 | 0 | +15 |
| **Total** | **~2,395** | **~165** | **+2,230** |

Reasonable for a feature that closes a known correctness hole and adds a public API surface.

---

## 13. Test plan

All tests use `uv run pytest`. Two categories: integration tests under `TestKafkaBroker` (the existing pattern) and unit tests under `InMemoryAggregator` (no broker).

### 13.1 Golden path

`test_three_tools_durably_aggregate` — three tools fan out, all return, aggregator merges, agent re-enters with full state, final reply contains all three tool outputs.

- Under `TestKafkaBroker`: verifies the full Kafka path including state-topic writes.
- Under `InMemoryAggregator`: verifies the API contract without broker overhead.

### 13.2 Crash recovery

`test_aggregator_survives_worker_restart`:

1. Start Worker A; client invokes agent with three tools.
2. After dispatch publishes (verified via state topic having one record), but before any tool return, stop Worker A.
3. Start Worker B in the same consumer group.
4. Let tool returns flow.
5. Assert agent re-enters and the reply is correct.

This is the central correctness test. Use `InMemoryAggregator(persist_to_disk=True)` for the in-memory variant (per DX design §4.4).

### 13.3 Partition rebalance

`test_aggregator_handles_rebalance_mid_fanout`:

1. Multi-partition `fanout-returns` (e.g. 3 partitions).
2. Two workers in the same consumer group.
3. Dispatch fan-out on Worker A.
4. After 1 of 3 returns, start Worker B (triggers rebalance).
5. Assert remaining 2 returns process on Worker B; batch completes; agent resumes.

Requires real broker or careful FastStream simulation. May land as a docker-compose-backed integration test outside `TestKafkaBroker` if `TestKafkaBroker` can't simulate rebalance.

### 13.4 Late arrival

`test_late_return_is_logged_and_dropped`:

1. Configure short `idle_timeout` (200ms).
2. Dispatch fan-out; let only 2 of 3 tools return; let timeout fire (agent gets `FanOutTimeoutError`).
3. Have the third tool return after a 1s delay.
4. Assert: aggregator logs "late return" at INFO; metric `calfkit_aggregator_returns_late_total` incremented; agent does not re-enter (already errored out).

### 13.5 Idempotent re-arrival

`test_duplicate_return_is_deduped`:

1. Dispatch fan-out with two tools.
2. Inject the same tool return twice (via `InMemoryAggregator` test hook or by manually publishing on `TestKafkaBroker`).
3. Assert: only one update to `batch.collected_results`; only one `on_partial` invocation.

### 13.6 Empty fan-out

`test_zero_tools_fanout_does_not_create_state`:

1. Pathological: LLM returns `DeferredToolRequests(calls=[])`.
2. Assert: no `FanOutState` record is written; no aggregator subscriber is triggered; the agent continues normally (likely with a final answer).

Implementation note: the dispatch code path guards against this — `list[Call]` with zero elements is not the parallel path; in `agent.py:191-204` the single-tool fast path requires `pending_tool_calls` non-empty. If empty, the agent should fall through to final answer.

### 13.7 Single-tool fan-out

`test_single_tool_uses_sequential_path`:

1. LLM returns exactly one tool call.
2. Assert: no `FanOutState` record written; the single-Call path (existing) is used.

The existing code at `agent.py:191-204` short-circuits for `len(pending_tool_calls) == 1` to the single-tool path. We preserve this. The aggregator subscriber still exists but is unused for this turn.

### 13.8 Tool error in batch

`test_tool_error_does_not_block_completion`:

1. Dispatch fan-out with three tools; one tool raises (returns `RetryPromptPart` or `ModelRetry`).
2. Assert: aggregator collects the error result; batch completes when all three returns are in (errors are still returns); agent receives the merged state including the error part.

### 13.9 Merge error policies

Three sub-tests:

- `test_merge_error_abort`: user `merge()` raises; assert `AggregatorMergeError` propagates; assert FastStream message is nacked (under `TestKafkaBroker`, hard to verify directly; assert via metric `calfkit_aggregator_state_store_write_errors_total` or by mocking the broker).
- `test_merge_error_retry`: `on_merge_error="retry"`, `merge_retry_count=2`, `merge()` raises twice then succeeds; assert batch completes; assert two retries observable via log.
- `test_merge_error_drop`: `on_merge_error="drop"`, `merge()` raises; assert tombstone written; assert agent never re-enters; assert `FanOutTimeoutError` does NOT fire (the batch was explicitly dropped, not timed out).

### 13.10 Custom `should_complete`

`test_first_success_aggregator`: `FirstSuccessAggregator` example from DX design. Three tools; first returns; assert batch completes before others arrive; assert remaining returns are deduped/late.

### 13.11 Custom `merge`

`test_summarizing_merge_returns_synthesised_state`: a `merge` implementation that combines results into one synthetic part; assert agent's resumed state has the synthetic part, not the raw results.

### 13.12 Idempotent dispatch

`test_redelivered_inbound_does_not_double_dispatch`:

1. Mock or stub the agent's main topic consumer to "redeliver" an inbound after the initial dispatch.
2. Assert: only one set of N tool calls is published; the second processing of the same inbound is suppressed (verified via metric `pending_batches` count and via tool-side spy that counts invocations).

### 13.13 Rehydration

`test_rehydration_loads_in_flight_batches`:

1. Pre-populate `fanout-state` (via `InMemoryAggregator(persist_to_disk=True)` or by direct publish on `TestKafkaBroker`).
2. Start Worker.
3. Assert: cache contains the in-flight batches; subsequent returns process correctly.

### 13.14 Tombstone after completion

`test_completion_writes_tombstone`:

1. Run a fan-out to completion under `TestKafkaBroker`.
2. Read the `fanout-state` topic.
3. Assert: the final record for `(corr, fan_out_id)` is a tombstone (value=None).

### 13.15 Test list summary

15 numbered test cases above. Each maps to ~1 pytest function or parametrised set. Total: ~25–35 pytest test functions across the new aggregator test module.

### 13.16 Fixtures

In `tests/conftest.py`, add:

- `inmemory_aggregator_factory(persist_to_disk: bool = False) -> InMemoryAggregator`
- `deploy_agent_with_custom_aggregator(aggregator_cls: type[FanOutAggregator])` — variant of existing `deploy_agent`
- `assert_fanout_state_record(broker, agent_id, correlation_id, fan_out_id, expected: FanOutState | None)` — helper that reads the state topic and asserts (None = tombstone)

---

## 14. Startup, deployment, topic provisioning

### 14.1 Worker startup sequence (recap)

```
1. Worker.__init__: nodes registered, aggregators auto-attached
2. Worker.run:
   a. Worker.register_handlers (synchronous):
      for each node:
         for each KafkaSubscription in node.kafka_subscriptions():
            broker.subscriber(...)(...)
   b. broker.start() — establish connection
   c. For each agent's aggregator:
      i. aggregator._ensure_topics(broker)
      ii. aggregator._rehydrate(broker)
   d. FastStream(broker).run() — begin consuming
```

### 14.2 Topic auto-creation vs explicit creation

Three modes:

- **Cluster allows topic auto-creation** (`auto.create.topics.enable=true`): the first publish to `fanout-state` will auto-create it — but with the *default* cleanup.policy=delete, which is WRONG. The aggregator's `_ensure_topics` must use the Kafka Admin API to (a) check the topic exists, (b) check `cleanup.policy=compact`, (c) if missing, create with the correct config; if wrong policy, log ERROR and (option A: hard fail / option B: continue with risk).
- **Cluster forbids topic auto-creation**: `_ensure_topics` must create explicitly via Admin API. If the broker user lacks permission, log ERROR and fail to start (it's a configuration error).
- **Managed Kafka (e.g. Confluent Cloud, AWS MSK Serverless)**: the user must pre-create topics. Provide a CLI helper.

### 14.3 CLI helper

```
uv run calfkit topics create-aggregator <node_id> [--state-topic NAME] [--returns-topic NAME] [--partitions N]
```

Outputs:
- The topic names that would be created.
- The config that would be applied.
- If `--apply` is passed and the broker is reachable, creates the topics.

Implementation: a new `calfkit/cli/` module. Could share Click / Typer dependency. Estimated +200 LoC, but out of scope for v1 if we accept "start-time auto-create via Admin API with sensible defaults" as the primary path.

**Recommendation:** ship v1 with start-time auto-create only (via Admin API in `_ensure_topics`). Defer the CLI to v1.1 if user demand surfaces.

### 14.4 Graceful shutdown (recap)

FastStream's lifespan handler triggers:
- Aggregators drain pending write-throughs (await any in-flight `broker.publish` on `fanout-state`).
- Compacted-log offsets are committed via Kafka's standard consumer offset commit.
- Broker connection closes.

No special action required: relying on FastStream's existing lifespan model.

---

## 15. Migration plan

### 15.1 Code removal

`_pending_batches` is an underscore-prefixed framework-internal attribute. Removing it is not a breaking change per Python conventions. Three sub-steps:

1. Remove the field declaration and all reads/writes in `agent.py`.
2. Remove `_parallel_state_aggregation` method.
3. Remove `PendingToolBatch` dataclass from `state.py`.

If anyone reached into `_pending_batches` in user code, they were violating the contract; we add a release note but otherwise treat it as breaking only by name.

### 15.2 Runtime behaviour change

Parallel fan-outs are now durable. The user-visible difference is **fewer mysterious "lost batch" errors on worker restart** — which is what we want. No code changes required by users.

### 15.3 New Kafka topics

Two new topics per agent. Operational impact:

- Managed Kafka users with strict topic auto-creation policies must pre-create them.
- Topic count grows by `2 × num_agents`. For a deployment with 20 agents, that's 40 new topics. Manageable.
- Disk usage: the state topic is bounded by `pending_batches × N_tool_calls × State_size`. For a 100-agent deployment with bursty workloads, peak ~10s of GB across all aggregator topics. Negligible compared to typical Kafka log volumes.

### 15.4 Release note headline

> **Parallel tool calls are now durable.** Agents that dispatch multiple tool calls in a single turn now use a Kafka-backed aggregator that survives worker restarts and consumer-group rebalances. No code changes are required to benefit. Two new Kafka topics are created per agent (`{agent}.fanout-state`, `{agent}.fanout-returns`); on managed Kafka with restricted auto-creation, pre-create them or grant the worker the Topic-Create ACL. See the [Aggregator documentation] for advanced customisation (short-circuit completion, custom merge, progress events).

### 15.5 Deprecations

- `sequential_only_mode=True` on `Agent` is soft-deprecated. It still works (the single-tool path covers it). Add `DeprecationWarning` on construction with `sequential_only_mode=True`. Remove in the next major.

### 15.6 Rollout sequence (recommended)

1. PR 1: introduce `CalfkitError` base.
2. PR 2: introduce `HDR_FANOUT_ID`, `HDR_FRAME_ID`; refactor `_emitter_headers` to include `HDR_FRAME_ID` (per the existing memory recommendation).
3. PR 3: introduce `BaseNodeDef.kafka_subscriptions()` method with default; no behaviour change (single subscription per node, same as today).
4. PR 4 (the big one): introduce `FanOutAggregator` subpackage, edit `agent.py` to use it, edit `base.py` for the rewritten `_publish_action` parallel branch, edit `worker.py` for rehydration. Tests.
5. PR 5: deprecation warning on `sequential_only_mode`.

Each PR is self-contained, reviewable, revertable. PR 4 is the only one that ships the behaviour change; everything before is preparation.

---

## 16. DX choices that need adjustment

This is where I (event-driven-architect) surgically push back on items in the DX design that don't survive the broker layer.

### 16.1 The "stateful aggregator with shared resources" claim (DX §6.5)

The DX design argues for a class-based API partly because aggregators may hold shared state — "a connection, a summarizer client, a rate limiter."

**Critique:** holding a network connection or a rate limiter inside a `FanOutAggregator` instance is fine for the *local in-process* concern (the merge function calling out to an LLM), but **the aggregator instance is process-local**. If two worker processes each run the same agent (consumer-group siblings), each holds its own aggregator instance. Their "shared state" is not shared across processes.

**Adjustment:** the docstring on `FanOutAggregator` should explicitly say:

> The aggregator instance is process-local. State held in `__init__` (e.g. an LLM client used by `merge()`) is per-process; do not rely on it for cross-process coordination. Cross-process state is the compacted topic.

This is a tiny clarification; it doesn't change the API. Just be honest about the boundary.

### 16.2 The "wrap" classmethod for `InMemoryAggregator` (DX §8.1)

The DX design proposes `InMemoryAggregator.wrap(MyCustomAggregator())` to take a user's subclass and make it in-memory-backed.

**Critique:** this works for `merge`, `should_complete`, `on_partial` — pure behaviour overrides. It doesn't trivially work if `MyCustomAggregator.__init__` sets up state that depends on the storage backend being Kafka (which would be a bad pattern, but possible). The `wrap` semantic must be **strictly limited to user behaviour overrides; storage details are managed by `InMemoryAggregator`**.

**Adjustment:** document the constraint:

> `InMemoryAggregator.wrap(base)` reuses `base.merge`, `base.should_complete`, and `base.on_partial`. Configuration on `base.__init__` that affects storage (e.g. `state_topic`, `kafka_topic_config`) is ignored. If you need to test with custom storage details, instantiate the real `FanOutAggregator` and use `TestKafkaBroker`.

Again, a tiny doc clarification; no API change.

### 16.3 The "OTel span propagated across the fan-out" claim (DX §9.1.2)

The DX design promises span propagation across the fan-out via Kafka headers. **This is achievable** but the DX doc undersells the constraint: the OTel SDK must be configured to use the W3C trace-context propagator (the default in recent OTel Python SDKs). If a user has overridden the propagator to something exotic (B3, Jaeger, etc.), our header names won't match.

**Adjustment:** the aggregator should not bake in `traceparent`/`tracestate` names; instead, use the OTel SDK's configured propagator to inject/extract headers via the standard `inject(carrier)` / `extract(carrier)` API. The carrier is `dict[str, str]`. This works regardless of which propagator is configured.

Pseudocode at dispatch:

```python
from opentelemetry import propagate
carrier: dict[str, str] = {}
propagate.inject(carrier)   # writes whatever the configured propagator says, e.g. traceparent or b3
outbound_headers = {**self._emitter_headers(envelope), **carrier, HDR_FANOUT_ID: fan_out_id}
```

At return / aggregator-handle:

```python
ctx = propagate.extract(headers)
with tracer.start_as_current_span("calfkit.aggregator.handle_return", context=ctx):
    ...
```

This is a one-paragraph correction to the DX claim. No API surface change for the user; just makes the implementation robust to propagator choice.

### 16.4 The `aggregator_handler` second subscriber registration (DX §3.3.2)

The DX design's `kafka_subscriptions()` mechanism is exactly right. One clarification: the **consumer group_id for the aggregator subscriber** must be the same as the agent's main group_id (so a single worker owns both partitions for a given correlation_id). If they differed, a rebalance could split ownership and the co-location guarantee would break.

**Adjustment:** document that the aggregator's `_KafkaSubscription.group_id` is hard-coded to the agent's group_id, not user-configurable. The `BaseNodeDef.kafka_subscriptions()` signature should not expose `group_id` as a customisation point at the `Agent` level.

This is an implementation detail, but it must be enforced.

### 16.5 The `simulate_restart()` semantics (DX §8.3)

The DX design proposes `simulate_restart()` on `InMemoryAggregator` to simulate a worker restart. With `persist_to_disk=True`, a new instance can rehydrate from disk; with the default (`False`), state is wiped.

**Critique:** the default-wipe behaviour models "we have NO durable store." But the *real* aggregator uses a compacted Kafka topic, not disk; the "no durable store" scenario is not what users will encounter in production. The test that *matters* is "the durable store survives." `persist_to_disk=False` simulates an unrealistic scenario.

**Adjustment:** make `persist_to_disk=True` the default for `InMemoryAggregator` (matches real-world). Keep `persist_to_disk=False` available for "what does my agent do if durability fails?" exploration. Document loudly: persist_to_disk=False is not a realistic prod scenario; it's for fault-injection only.

### 16.6 What survives unchanged

Everything else in the DX design. The three override methods, the module structure, the error hierarchy, the `aggregator=` constructor arg, the testing API shape, the comparison-to-other-SDKs framing, the topic naming convention — all hold under the Kafka mechanics.

---

## 17. Open questions

Honest list of things that need prototyping, validation, or further design before shipping.

### 17.1 Single-partition `fanout-state` throughput

v1 has 1 partition. At what fan-out rate does this become a bottleneck? Order-of-magnitude estimate: each batch is 1 write-on-dispatch + N writes-on-return + 1 tombstone, so ~(N+2) writes per batch. For 100 fan-outs/sec with 5 tool calls each: 700 writes/sec on the state topic. Far below single-partition Kafka throughput (~10k+ msg/sec on commodity hardware). Plausibly fine for v1; needs load-test.

**Action item:** include a benchmark in `tests/perf/` once the implementation lands.

### 17.2 Exactly-once via Kafka transactions

The §9 analysis shows a small window of "duplicate FanOutTimeoutError firing for a completed batch" or "lost completion if tombstone+publish ordering reverses." Kafka transactions (`transactional.id`) would close this gap atomically.

Cost: meaningful complexity (transactional producer, isolation level on consumers, write order constraints). Benefit: eliminates the noisy false-positive timeout error.

**v1 recommendation:** skip transactions. Live with the small operational artifact. Document loudly. Revisit in v2 if real users hit it.

### 17.3 DLQ for late arrivals

v1 logs + drops. A configurable DLQ topic (`{node_id}.fanout-dlq`) would let operators inspect late returns and trace why a tool was slow. Adds one config knob and one topic per agent.

**v1 recommendation:** skip. Reconsider if observability of late arrivals via logs alone proves insufficient.

### 17.4 Heterogeneous tool result types

`State.tool_results` is typed `dict[str, ToolCallResult | Any]`. The `Any` is wire-shape leakage. The aggregator's `merge` receives the same `Any`. If a user wants strongly-typed batch results, they have no path.

**Action:** out of scope for v1 (the existing weakness is not made worse). Long-term, tighten `tool_results` to a tagged union, propagate the tightening to `FanOutAggregator[StateT, ResultT]`.

### 17.5 Multi-partition `fanout-state`

Going beyond 1 partition for state. The aggregator's read-modify-write must remain linearisable per key. Standard pattern: hash-by-`correlation_id` consistently on the producer; the consumer-group rebalance auto-co-locates writers and readers for a given key.

Implementation cost: small (just change the producer's key hash). Operational cost: more partitions = more files = slightly more overhead on the broker. Probably worth doing in v1.1 once we know v1 works.

### 17.6 RocksDB-backed cache

If in-memory cache becomes a memory bound problem (millions of in-flight batches, very large message_history), back the cache with RocksDB (Faust / Kafka Streams pattern). Adds a runtime dep on a Python rocksdb binding. Significant complexity. v2+ work.

### 17.7 Multi-partition aggregator topic vs single-aggregator-instance ownership

Open: in a multi-worker deployment, do we want each worker to "own" a slice of correlations (consumer-group rebalance gives us this), or a single "aggregator leader" that handles all correlations for an agent (Kafka leader-election pattern)?

**v1 recommendation:** consumer-group ownership. The same model as the agent's main topic. Co-partitioning gives us the locality we need.

### 17.8 What does `fan_out_id` look like when the same agent inbound is processed twice in parallel?

Pathological: same `correlation_id`, same `frame_id` (since redelivery), two workers both decide to dispatch. The idempotent-dispatch check in §7.5 catches the second one — BUT the check is "state record exists." If both workers race on the write-through, both write `FanOutState` records. Since the key is `(correlation_id, fan_out_id)` and `fan_out_id` is deterministic from `frame_id`, both write the SAME key. Last-write-wins. Both then proceed to publish their own set of N tool Calls — duplicate dispatches.

**Resolution:** the second worker's publish is deduped by *the tool's idempotency on `(correlation_id, tool_call_id)`* — which is the existing calfkit contract. So tools run once. The aggregator dedups returns. Correctness is preserved.

The risk: tool publishes are not transactional with the state write; we cannot prevent duplicate dispatches in a true split-brain scenario. We rely on tool-side idempotency.

**Action:** document this contract clearly in the migration note. "Tools must be idempotent on `(correlation_id, tool_call_id)`" — this is already an implicit contract; we are making it explicit.

### 17.9 Should aggregator topics always be single-partition for simplicity?

Trade-off: simpler reasoning vs. throughput cap. v1 picks 1 for `fanout-state`, co-partitioned for `fanout-returns`. The co-partitioning lets `fanout-returns` scale; `fanout-state` is the throughput bound at the single-partition limit (~10k writes/sec on commodity hardware, ample for v1 workloads).

### 17.10 What if the agent's main topic has a different partitioner from `fanout-returns`?

We assume both use murmur2 (Kafka's default). If a user configures a custom partitioner on the main topic but not on `fanout-returns`, co-location breaks. The `_ensure_topics` pre-flight should validate partition count match and partitioner config match. **Action:** add a startup check.

---

## 18. Appendix

### 18.1 References to the DX draft

The user-facing API in this document is sourced from `docs/drafts/aggregator-dx-design.md` (python-sdk-dx-reviewer, 2026-05-18). That document is the authoritative source for:

- The hello-world quickstart shape (`§4.1`).
- The three override methods (`§4.2`, `§6.4`).
- The configuration knobs (`§5`).
- The module structure (`§6.1`).
- The error hierarchy (`§10.1`).
- The testing API (`§8`).
- The observability surface at the user level (`§9`).
- The migration story (`§12`).

This document supersedes it as the merged canonical design.

### 18.2 References to memory artifacts

Grounding facts for this design come from the agent-memory artifacts at `/Users/ryan/Projects/calf-sdk/.claude/agent-memory/event-driven-architect/`:

- `project_calfkit_architecture.md` — One execution unit (Node) over Kafka round-trips joined by correlation_id; the durability model that requires durable batch state.
- `project_calfkit_kafka_headers.md` — Existing wire protocol with str-typed Kafka headers (`x-calf-emitter`, `x-calf-emitter-kind`); new headers must follow this protocol.
- `project_envelope_basemodel_distinction.md` — `Envelope` uses plain BaseModel; `State` uses CompactBaseModel. `FanOutState` follows the `Envelope` convention.
- `project_pending_tool_batch_fragility.md` — Documents the exact bug this design fixes.
- `project_frame_id_as_dedup_key.md` — `CallFrame.frame_id` (uuid7) is the canonical cross-hop dedup key; promoted to `HDR_FRAME_ID`. The `fan_out_id` reuses `frame_id` of the dispatch frame for deterministic dedup.
- `project_kafka_headers_design_decision.md` — User propagation context belongs on Kafka transport headers, not Envelope body; this design uses `HDR_FANOUT_ID` and `traceparent` as Kafka headers, not body fields.

### 18.3 Reference to current code

Key sites in `calfkit/`:

- `nodes/agent.py:50-79` — current `_pending_batches` and `_parallel_state_aggregation`; removed.
- `nodes/agent.py:117-127` — current `RuntimeError` about lost state; the bug we fix.
- `nodes/agent.py:206-221` — current parallel dispatch; the site where `FanOutState` is written.
- `nodes/base.py:161-162` — current `_emitter_headers`; extended with `HDR_FRAME_ID`.
- `nodes/base.py:167-183` — current parallel-Call branch of `_publish_action`; rewritten to include state write and `HDR_FANOUT_ID` header.
- `nodes/base.py:247-278` — current `handler`; unchanged (the new aggregator handler is a separate method, registered via `kafka_subscriptions`).
- `worker/worker.py:33-55` — current `register_handlers`; rewritten to iterate `kafka_subscriptions()`.
- `worker/worker.py:57-61` — current `run`; rewritten to add pre-flight + rehydration.
- `models/state.py:128-143` — current `PendingToolBatch`; removed.
- `_protocol.py` — current `HDR_EMITTER`, `HDR_EMITTER_KIND`; extended with two new constants.
- `client/reply_dispatcher.py:33-34` — current emitter-header decode; unchanged.
- `tests/test_concurrent_tool_calls.py` — current smoke test; preserved as regression coverage.

### 18.4 Glossary

- **fan-out batch**: the set of N tool Calls dispatched in one turn by one agent for one correlation_id.
- **fan_out_id**: uuid7-hex identifier for a fan-out batch; derived deterministically from `CallFrame.frame_id` of the agent's current frame at dispatch time. Stable across redelivery.
- **correlation_id**: existing calfkit identifier for one logical conversation/invocation. Stable for the lifetime of the chain.
- **compacted topic**: Kafka topic with `cleanup.policy=compact`; per-key retention; tombstones delete keys.
- **write-through cache**: in-memory copy that is updated synchronously on every external write (so an external reader sees the same value at the same time).
- **rehydration**: rebuilding the in-memory cache from offset 0 of the compacted topic on startup.
- **co-partitioning**: two topics share partition count and key-hash so a given key lands on the same partition number on both topics.

### 18.5 Sequence diagram — happy path (text)

```
Client            Agent (run)        Aggregator(state)     Tool A         Tool B
  |                  |                   |                   |              |
  | execute_node     |                   |                   |              |
  |----------------> |                   |                   |              |
  |                  | (LLM emits 2 tool calls)              |              |
  |                  | dispatch parallel                     |              |
  |                  | write FanOutState                     |              |
  |                  |----------------> compacted topic      |              |
  |                  |                   <-- ack             |              |
  |                  | publish Call(A) hdr={fanout=F, frame=X}              |
  |                  |---------------------------------->    |              |
  |                  | publish Call(B) hdr={fanout=F, frame=Y}              |
  |                  |--------------------------------------------------->  |
  |                  | (run returns; agent hop done)         |              |
  |                  |                                       |              |
  |                  |                                  (tool A runs)       |
  |                  |                                       | ReturnCall   |
  |                  |                                       |              |
  |                  |                  +-----------+-------- callback=fanout-returns
  |                  |                  | tools return back |               |
  |                  |                  v                   |               |
  |                  |          Aggregator handler          |               |
  |                  |          dedup, write-through        |               |
  |                  |          on_partial(view, latest)    |               |
  |                  |                                                       (tool B runs)
  |                  |                                                       | ReturnCall
  |                  |                                                       |
  |                  |          Aggregator handler          <----------------+
  |                  |          dedup, write-through       
  |                  |          should_complete() == True
  |                  |          merge()
  |                  |          publish re-entry to agent main topic
  |                  |          tombstone fanout-state
  |                  |
  |                  | (agent.run reinvoked with merged state)
  |                  | (LLM emits final answer)
  |                  | ReturnCall to client reply topic
  | <----------------|
```

### 18.6 Sequence diagram — restart recovery (text)

```
Worker A                  Aggregator A         compacted topic        Worker B (joins later)
  |                          |                       |                       |
  | dispatch                 |                       |                       |
  |  write FanOutState ----> | -----------> {(corr,F): FanOutState(...)}     |
  | publish tool Calls       |                       |                       |
  | (Worker A crashes)       X                       |                       |
  |                                                  |                       |
  |       (tools complete; returns published to fanout-returns)              |
  |                                                  |                       |
  |       Kafka rebalance: partitions reassigned to Worker B                 |
  |                                                  |                       |
  |                                                  | <-- consumer (rehydrate)
  |                                                  | from offset 0
  |                                                  | --> Worker B builds  
  |                                                  |     cache: {(corr,F):FanOutState}
  |                                                  |
  |                                                  | (Worker B starts consuming fanout-returns)
  |                                                  | (returns arrive; aggregator handles them)
  |                                                  | (batch completes; re-entry published)
  |                                                  | (agent on Worker B handles re-entry)
```

---

End of canonical design.
