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

> **Implementation status (shipped on `feat/durable-fanout-aggregator`).**
> The implementation diverges from this doc in the following ways; treat this
> banner as authoritative when it contradicts the body below.
>
> 1. **Multi-partition by default.** All three topics (main, `{node_id}.fanout-state`,
>    `{node_id}.fanout-returns`) share the same partition count, auto-detected
>    from the agent's main topic at worker startup (default 6 when missing).
>    Co-partitioning is preserved by a custom
>    `FanOutAggregatorPartitioner` installed broker-wide at `Client.connect`
>    (`calfkit/client/base.py::BaseClient.connect`,
>    `calfkit/nodes/aggregator/_partitioner.py`). Earlier "single-partition
>    v1 / multi-partition v1.1" framing is obsolete; the body below has
>    been updated to match.
> 2. **Per-partition linearisation** (§7.1) is achieved via consumer-group
>    ownership of the returns topic plus `max_workers=1` on the aggregator's
>    subscriber (`calfkit/nodes/agent.py::BaseAgentNodeDef.kafka_subscriptions`).
> 3. **Rehydration is rebalance-driven and partition-scoped** (§8.5).
>    `_StateStoreRebalanceListener.on_partitions_assigned`
>    (`calfkit/nodes/aggregator/_rebalance.py`) drives
>    `_KafkaStateStore.rehydrate_partitions`
>    (`calfkit/nodes/aggregator/_kafka_state_store.py`) on the assigned
>    partitions — not eager full-log rehydration in `Worker.run()`. The
>    rehydration consumer uses **no** `group_id` and `assign()`s partitions
>    manually.
> 4. **No second consumer.** Standard Kafka Streams-style aggregation:
>    the returns subscriber owns reads and writes; there is no
>    "state-watcher" tailing the state topic in the background.
> 5. **`broker.config.admin_client` is the production access path** for topic
>    provisioning (`calfkit/nodes/aggregator/_topic_admin.py`); the one
>    small `AIOKafkaConsumer` usage in `_kafka_state_store.py` is for the
>    rehydration pattern FastStream's `@broker.subscriber` doesn't express.
> 6. **`KafkaConfig` is threaded through `Client.connect` → `Worker._prepare_aggregators`
>    → `FanOutAggregator.setup` → `_KafkaStateStore`** so the transient
>    rehydration consumer inherits the broker's SASL/SSL configuration
>    (`calfkit/client/kafka_config.py`).
> 7. **Public-API split:** the user-facing `FanOutAggregator` holds only
>    behaviour overrides plus `merge_error_policy`; all Kafka mechanics
>    live behind a single `_runtime: _FanOutRuntime` attribute
>    (`calfkit/nodes/aggregator/_runtime.py`,
>    `calfkit/nodes/aggregator/aggregator.py::FanOutAggregator.runtime`).
> 8. **Structured exception context.** `AggregatorMergeError` carries
>    `correlation_id` / `fan_out_id`; `AggregatorStateStoreError` carries
>    `state_topic` (`calfkit/nodes/aggregator/errors.py`).
> 9. **`HDR_DEGRADED_MERGE`** (`calfkit/_protocol.py`) is stamped on the
>    aggregated return when `MergeErrorPolicy.FALLBACK_TO_DEFAULT` is in
>    effect and the user's `merge()` raised. Operators can filter on this
>    header to surface silently-degraded batches.
> 10. **`idle_timeout_seconds` and `FanOutTimeoutError` are removed.**
>    The reaper was never built; rather than ship dead code, the parameter
>    and exception were dropped. A follow-up design will be added under
>    `docs/` and registered in `ROADMAP.md` when the reaper is prioritised
>    — no roadmap entry exists today. `last_updated_ms` is still recorded
>    on `FanOutState` for observability.
> 11. **`merge_retry_count` is not a knob.** `MergeErrorPolicy.RETRY`
>    retries `merge()` exactly once; no configuration.

## 1. Executive summary

`BaseAgentNodeDef._pending_batches` *was* an in-process Python dict that held the join-state of a parallel tool fan-out — both the dict and the explanatory `RuntimeError` it raised on cache-miss have been removed by the shipped design. It vanished on process restart and on Kafka consumer-group rebalance — the known correctness hole this design replaces with a durable, Kafka-backed aggregator that is:

- **Invisible by default.** Hello-world agent code does not change. The framework auto-constructs a `FanOutAggregator()` and the Worker wires its subscriber without user involvement.
- **Customizable through three override methods.** `merge`, `should_complete`, `on_partial`. All have sensible defaults; users override only when they need short-circuit completion, custom merging, or progress-event emission.
- **Durable on a compacted Kafka topic per agent.** Per-agent ownership: `{node_id}.fanout-state` (compacted, holds in-flight batch records keyed by `(correlation_id, fan_out_id)`) and `{node_id}.fanout-returns` (a normal returns channel the aggregator subscribes to). The compacted topic is the system of record; on Worker startup it is rehydrated into an in-memory write-through cache.
- **Observable across the fan-out boundary** (design target — not
  shipped; see §11.2). The intended end-state is per-batch OTel spans
  propagating via W3C ``traceparent`` / ``tracestate`` Kafka headers,
  so a single distributed trace covers dispatch → tool work → return
  → merge → resume. The forward-compatibility hooks
  (``FanOutState.traceparent`` / ``tracestate``) exist on the durable
  wire format today so the OTel integration will not require a
  schema migration on the compacted topic.
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
- **A general "DLQ" framework.** Late arrivals are logged and dropped in v1. A configurable DLQ topic is a future addition.
- **Cross-agent fan-in / scatter-gather.** Aggregator joins are scoped to one agent's tool fan-out. Choreography across agents is a separate primitive (`Emit` / subscription patterns).
- **A RocksDB-backed state store.** v1 holds in-memory cache with full rehydration from the compacted log on startup. RocksDB-backing is a v2 consideration if memory bounds become an operational concern.
- **Idle-timeout reaping of stuck batches.** v1 ships without an idle-timeout reaper. Batches live until they complete or are evicted by partition rebalance. A partition-scoped periodic sweep is tracked as a follow-up in `ROADMAP.md`. Soft-progress hooks (e.g. `on_idle_progress(elapsed)`) are v2.

### 2.3 What survives unchanged

- `BaseNodeDef` keeps its single-handler contract for non-agent nodes; the new `kafka_subscriptions()` method defaults to "one subscription for `self.handler`."
- `Envelope`, `SessionRunContext`, `WorkflowState`, `CallFrame`, `Deps` — no wire-shape changes.
- `Call`, `ReturnCall`, `TailCall`, `Silent`, `Emit` — no return-type changes.
- The `_protocol.py` headers `x-calf-emitter` and `x-calf-emitter-kind` keep their meaning. Two new ones are added.

---

## 3. Background: the bug this design fixed (historical)

> This section describes the pre-aggregator code paths that the shipped
> `FanOutAggregator` replaced. `PendingToolBatch`, `_pending_batches`, and
> `_parallel_state_aggregation` are all gone; field names referenced below
> (`collected_results` etc.) refer to that removed type, not to the shipped
> `FanOutState.received`.

### 3.1 What the code used to do

`BaseAgentNodeDef.__init__` initialised an in-process dict:

```python
self._pending_batches: dict[str, PendingToolBatch] = dict()
```

On a parallel turn, after the LLM emitted N tool calls, the agent stored a
`PendingToolBatch(expected_tool_call_ids=..., base_state=...)` keyed by
`correlation_id` and returned `list[Call]`. When each tool's `ReturnCall`
came back, an aggregation gate inside `run()` looked up the batch, copied
new tool_results into `batch.collected_results`, and either completed or
waited.

### 3.2 What went wrong

`self._pending_batches` was process-local. Three failure modes:

1. **Worker process restart.** Between dispatch and the first return, the worker is killed (deploy, OOM, crash). On restart, `_pending_batches` is empty. The first return arrives and the gate raises the canonical `RuntimeError: This indicates lost PendingToolBatch state (e.g. partition rebalance or process restart).`
2. **Consumer-group rebalance.** A second worker joins the group. Kafka reassigns the partition holding this `correlation_id` to the new worker. Returns now arrive at a worker whose `_pending_batches` never saw the dispatch. Same error.
3. **Replay / duplicate delivery.** If a tool return is redelivered (consumer hasn't committed offset, broker re-pushed), the aggregation gate would idempotently re-copy the same `tool_call_id` — fine. But if the batch had already completed and the dict entry was deleted, the redelivered return triggered the same `RuntimeError`.

### 3.3 The invariant we needed

> Given any `(correlation_id, fan_out_id)`, the set of `expected_tool_call_ids` and the per-batch received-tool-results map must be recoverable by any process that owns the agent's partition, regardless of restart history.

Kafka itself is a perfectly good store for this. The fix is to put the batch record on a **compacted topic** keyed by `(correlation_id, fan_out_id)`, write through it on every state change, and rehydrate the in-memory cache from the log on partition assignment.

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
        return any(not isinstance(r, Exception) for r in batch.received.values())

agent = Agent(
    "racing_agent",
    system_prompt="Answer using whichever backend responds first.",
    subscribe_topics="racing.input",
    model_client=...,
    tools=[search_google, search_bing, search_ddg],
    aggregator=FirstSuccessAggregator(),
)
```

The three override methods, restated (shipped signatures —
`calfkit/nodes/aggregator/aggregator.py::FanOutAggregator`):

```python
class FanOutAggregator:
    async def merge(self, batch: AggregatorBatch) -> AggregatedReturn: ...

    async def should_complete(self, batch: AggregatorBatch) -> bool: ...

    async def on_partial(
        self,
        batch: AggregatorBatch,
        newly_received: frozenset[str],
    ) -> None: ...
```

Defaults:

- `merge`: deep-copy `batch.base_state`, call `add_tool_result` for each `(tool_call_id, result)` in `batch.received`, return as `AggregatedReturn(state=...)`.
- `should_complete`: `batch.is_complete_by_count` — true when `expected_tool_call_ids <= frozenset(received.keys())`.
- `on_partial`: no-op.

### 4.3 Public types and module structure

```
calfkit/
  nodes/
    aggregator/
      __init__.py            # FanOutAggregator, MergeErrorPolicy, AggregatorBatch,
                             # AggregatedReturn, FanOutState, errors
      aggregator.py          # FanOutAggregator, MergeErrorPolicy (public)
      state.py               # FanOutState, AggregatorBatch, AggregatedReturn
      errors.py              # AggregatorError, AggregatorMergeError, AggregatorStateStoreError
      testing.py             # InMemoryAggregator
      _runtime.py            # _FanOutRuntime (framework-managed runtime state)
      _kafka_state_store.py  # _KafkaStateStore (production durable store)
      _in_memory_store.py    # _InFlightBatch, _TtlSet, _InMemoryStateStore (test scaffolding)
      _topic_admin.py        # ensure_aggregator_topics, STATE_TOPIC_CONFIG
      _partitioner.py        # FanOutAggregatorPartitioner, composite-key helpers
      _rebalance.py          # _StateStoreRebalanceListener
```

Public surface (shipped — `calfkit/nodes/aggregator/__init__.py`):

```python
from calfkit.nodes.aggregator import (
    FanOutAggregator,
    MergeErrorPolicy,
    AggregatorBatch,
    AggregatedReturn,
    FanOutState,
    AggregatorError,
    AggregatorMergeError,
    AggregatorStateStoreError,
)
from calfkit.nodes.aggregator.testing import InMemoryAggregator
```

For convenience, the top-level :mod:`calfkit` package re-exports
``FanOutAggregator``, ``AggregatorBatch``, ``AggregatedReturn``,
``MergeErrorPolicy``, ``AggregatorError``, ``AggregatorMergeError``,
``AggregatorStateStoreError``, and ``KafkaConfig`` (see
``calfkit/__init__.py``). Hello-world snippets can import them directly
from ``calfkit``; the canonical home remains
``calfkit.nodes.aggregator``.

### 4.4 Configuration

The shipped constructor takes exactly one knob —
`calfkit/nodes/aggregator/aggregator.py::FanOutAggregator.__init__`:

```python
class FanOutAggregator:
    def __init__(
        self,
        *,
        merge_error_policy: MergeErrorPolicy = MergeErrorPolicy.ABORT,
    ) -> None: ...
```

`MergeErrorPolicy` (an `enum.Enum` with `str` values):

| Value | Behaviour |
|---|---|
| `ABORT` (default) | Raise `AggregatorMergeError` and let FastStream nack/redeliver. |
| `RETRY` | Retry `merge()` exactly once; fall back to ABORT on the second failure. Not configurable beyond on/off. |
| `FALLBACK_TO_DEFAULT` | Log the error, fall back to the default merge, stamp `HDR_DEGRADED_MERGE=1` on the published envelope, complete normally. The fallback merge itself is wrapped in try/except — if the default merge also raises, the framework treats it as `ABORT`. |

Topic names are derived from `node_id` (`{node_id}.fanout-state`,
`{node_id}.fanout-returns`); compacted-topic config is the
`STATE_TOPIC_CONFIG` constant in
`calfkit/nodes/aggregator/_topic_admin.py` (see §6.5). Neither is a
user knob in v1. `idle_timeout_seconds` was removed entirely — see §17.11
(deferred idle-timeout reaper; no roadmap entry yet).

The full DX surface (testing API, comparison tables, naming rationale, etc.) is in [aggregator-dx-design.md](./drafts/aggregator-dx-design.md); not duplicated here.

---

## 5. Architecture overview

### 5.1 Per-agent topology

Each `Agent` instance owns two new Kafka topics:

```
{node_id}.fanout-state      compacted, N partitions — system of record for in-flight batches
{node_id}.fanout-returns    normal,    N partitions — fan-in channel for tool returns
```

Both share the same partition count `N` as the agent's main topic (auto-detected
at startup; default 6 when the main topic doesn't exist yet —
`calfkit/nodes/aggregator/_topic_admin.py::ensure_aggregator_topics`). The
`FanOutAggregatorPartitioner` keeps composite-key (`{corr}|{fan_out_id}`)
records co-partitioned with `correlation_id`-keyed records, so the same
correlation lands on the same partition number across all three topics
(`calfkit/nodes/aggregator/_partitioner.py`).

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
                          (a) re-enter agent main topic with merged  |
                              state (AggregatedReturn synthesised)   |
                          (b) tombstone fanout-state record          |
                          (publish-first, tombstone-second — see §8.4 |
                          for the rationale and the redelivery       |
                          contract that depends on this ordering)    |
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
We rewrite the `callback_topic` field of the tool's `CallFrame` to `{node_id}.fanout-returns` at dispatch time (only in the parallel case; the single-tool sequential path still uses the main topic, unchanged). When the tool calls `ReturnCall(state=...)`, the existing `_publish_action` logic in `base.py::BaseNodeDef._publish_action` (the `ReturnCall` branch) pops the frame and publishes to `frame.callback_topic` — which is now the aggregator topic. This is a one-line change inside the parallel branch of `_publish_action` and keeps the rest of the call-stack semantics intact.

---

## 6. Wire format

All wire-format types live in `calfkit/nodes/aggregator/state.py`. The naming convention "Compact" / "Plain" follows the existing distinction documented in agent memory (`project_envelope_basemodel_distinction.md`).

### 6.1 The compacted-topic value: `FanOutState`

`FanOutState` is the value published to `{node_id}.fanout-state`. The key is the dedup composite `(correlation_id, fan_out_id)` serialised as `f"{correlation_id}|{fan_out_id}"` (matching the partition-key convention; `|` is illegal in uuid7 hex). On completion we publish a tombstone (`value=None`) on this key.

Shipped at `calfkit/nodes/aggregator/state.py::FanOutState`:

```python
class FanOutState(BaseModel):
    """Value type for the {node_id}.fanout-state compacted topic.

    Plain BaseModel (not CompactBaseModel) — wire shape must be stable.
    Always-serialised fields; deserialise must be tolerant of unknown fields.
    """

    model_config = ConfigDict(extra="ignore", frozen=True)

    correlation_id: str
    fan_out_id: str          # derived from CallFrame.frame_id at dispatch
    expected_tool_call_ids: frozenset[str]
    received: dict[str, Any] = Field(default_factory=dict)
    base_state: State
    started_at_ms: int
    last_updated_ms: int
    agent_topic: str         # where the aggregated return is published

    # Durable degraded-marker (see §7.5): set on dispatch when the
    # idempotent-dispatch path overwrote a drifted in-flight batch and
    # discarded prior received results. Persists across rehydration so
    # the completion publish still stamps HDR_DEGRADED_MERGE even if the
    # original dispatch worker crashed.
    degraded: bool = False

    # Optional W3C trace-context propagation
    traceparent: str | None = None
    tracestate: str | None = None
```

Earlier drafts of this doc additionally proposed `node_id: str` and
`schema_version: int = 1` fields; neither shipped. The owning agent's
`node_id` is recoverable from the state topic name (`{node_id}.fanout-state`),
and schema evolution is handled by Pydantic's `extra="ignore"` plus the
"never remove a field" convention.

**Why `frozenset[str]` / `dict[str, Any]` and not a `ToolCallId` NewType:**
an earlier iteration wrapped tool-call ids in `ToolCallId = NewType("ToolCallId", str)`
to document the dedup-triple's distinct components at the type level. In
practice the wrapping was inconsistent (returns parsed from
``State.tool_results`` arrived as plain ``str`` and required casting at
every boundary), added friction, and provided no extra type safety. The
NewType was retired; the module-level docstring on
``calfkit/nodes/aggregator/state.py`` retains the conceptual note.

**Why plain BaseModel, not CompactBaseModel:** the rules in agent memory (`exclude_unset=True` requires `= None` defaults, list mutation does not mark fields) apply to `State`/`CompactBaseModel`. For a value type written and read by the same component on a topic with strict round-trip, plain BaseModel is simpler and avoids the trap. `Envelope` already follows this convention for the same reason.

**Why `expected_tool_call_ids: frozenset[str]`, not a list:** the dedup invariant is set-based; a frozenset documents it. Pydantic serialises it to a JSON array; on read we coerce back. Order is irrelevant.

**Why store `base_state` in the record:** completion requires merging into the state snapshot captured at dispatch time, not whatever state the latest return brings (returns carry stale fragments). On rehydration after restart, the aggregator needs `base_state` to perform the merge — we cannot reconstruct it from envelopes alone. Cost: each fan-out writes one State-sized record. State is bounded by message_history which is bounded by the agent loop, so this is reasonable.

**Why store `agent_topic` in the record:** completion publishes the
`AggregatedReturn` to the agent's main topic, and rehydration may run on
a worker that wasn't the one that wrote the record. The agent's main
topic name is the canonical recovery anchor.

### 6.2 The returns-topic message: standard `Envelope`

The aggregator subscribes to `{node_id}.fanout-returns`. Each message is a regular calfkit `Envelope` — tools publish via the existing `_publish_action` pathway. **No new envelope type.** The only thing that changes is the destination topic (via the rewritten `callback_topic`) and the headers stamped on dispatch.

The aggregator's subscriber unpacks the standard `Envelope`:

- `envelope.context.state.tool_results` carries the new return entry — one `tool_call_id -> result` mapping.
- `envelope.internal_workflow_state.current_frame.frame_id` is the per-hop uuid7 from `CallFrame.frame_id` (`session_context.py:38`).
- Kafka headers carry ``x-calf-fanout-id``, ``x-calf-frame-id``,
  ``x-calf-emitter``, ``x-calf-emitter-kind``. ``traceparent`` /
  ``tracestate`` are a design target (see §11.2); the wire format
  reserves the field names but no shipped code path threads them
  today.

This is the **right call**: we do not invent a new envelope type for a transient transport pattern. The aggregator pulls what it needs from the existing shape.

### 6.3 The post-completion message: re-entry to the main topic

When the batch completes, the aggregator publishes one envelope to the
agent's main subscribe topic (``FanOutState.agent_topic``). The envelope
shape is the standard ``Envelope``. The aggregator constructs it by:

1. Taking the persisted ``FanOutState.base_state`` and running it
   through ``aggregator.merge(batch)`` where ``batch.received`` holds
   the accumulated tool results, to produce an
   ``AggregatedReturn(state=..., degraded=...)``.
2. **Deep-copying the inbound's ``internal_workflow_state``** (the
   current tool return's, not the first one received) and replacing the
   top-of-stack ``CallFrame`` with a copy whose ``frame_id`` is a fresh
   ``uuid_utils.uuid7().hex``. Concretely (see
   ``calfkit/nodes/agent.py::_aggregator_handler`` lines 602–605):

   ```python
   re_entry_state = envelope.internal_workflow_state.model_copy(deep=True)
   previous_frame = re_entry_state.unwind_frame()
   new_frame = dataclasses.replace(previous_frame, frame_id=uuid_utils.uuid7().hex)
   re_entry_state.call_stack.push(new_frame)
   ```

The frame-id replacement is load-bearing: ``fan_out_id`` is derived
deterministically from ``CallFrame.frame_id``, so a subsequent parallel
fan-out emitted by the same invocation (a new LLM turn following the
just-completed batch) needs a distinct ``frame_id`` on top of the call
stack. Without the replacement, the next dispatch's ``fan_out_id``
collides with the just-tombstoned key and the idempotent-dispatch check
takes the ``was_recently_completed`` branch — a silent skip, and the
agent hangs. The original behaviour (reusing the first received
return's state untouched) was the source of this bug; the explicit
re-stamp is the fix.

The re-entry envelope is then published with
``correlation_id`` preserved and the new ``HDR_FRAME_ID`` carried in the
headers; ``HDR_FANOUT_ID`` is intentionally absent because the batch is
done.

From the agent's point of view, this is a normal inbound on its main
subscribe topic, with merged state. The existing
``_parallel_state_aggregation`` gate and the ``RuntimeError`` are
removed; the agent simply runs the next LLM iteration.

### 6.4 Header protocol additions

Two new constants in `calfkit/_protocol.py`:

```python
HDR_FANOUT_ID = "x-calf-fanout-id"
"""Kafka header identifying the fan-out batch this message participates in.

Present on:
  - Every outbound tool Call dispatched as part of a parallel batch.
  - Every ReturnCall from a tool participating in a parallel batch (because the
    tool republishes inbound headers via Response, see
    base.py::BaseNodeDef.handler).
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

A third aggregator-specific header is added in this branch
(`calfkit/_protocol.py::HDR_DEGRADED_MERGE`, value `"x-calf-degraded-merge"`).
It is stamped on the aggregated return ONLY when
`MergeErrorPolicy.FALLBACK_TO_DEFAULT` fell back to the default merge
after the user's `merge()` raised; value is the string `"1"`. Operators
and observability tooling filter on this header to surface silently-
degraded batches.

Header summary, by message:

| Message | `x-calf-emitter` | `x-calf-emitter-kind` | `x-calf-frame-id` | `x-calf-fanout-id` | `x-calf-degraded-merge` | `traceparent`/`tracestate` |
|---|---|---|---|---|---|---|
| Client initial invoke | client.{id} | `client` | (from frame) | absent | absent | (not shipped — §11.2) |
| Agent single-tool dispatch | {agent.node_id} | `agent` | (from frame) | absent | absent | (not shipped — §11.2) |
| Agent parallel dispatch (one per call) | {agent.node_id} | `agent` | (from frame, distinct per call) | {fan_out_id} | absent | (not shipped — §11.2) |
| Tool ReturnCall (parallel) | {tool.node_id} | `tool` | (from new frame) | {fan_out_id} (echoed) | absent | (not shipped — §11.2) |
| Aggregator re-entry — normal | {agent.node_id} | `agent` | (synthesised new frame) | absent | absent | (not shipped — §11.2) |
| Aggregator re-entry — FALLBACK_TO_DEFAULT or drift-overwrite | {agent.node_id} | `agent` | (synthesised new frame) | absent | `"1"` | (not shipped — §11.2) |

`HDR_DEGRADED_MERGE` is stamped on the re-entry envelope when EITHER the
merge produced a degraded result (``AggregatedReturn.degraded=True``
from the ``FALLBACK_TO_DEFAULT`` fallback path) OR the dispatch path
flagged the batch as degraded (``FanOutState.degraded=True`` from a
drift-overwrite that discarded prior received results — see §7.5). The
dispatch-time flag survives rehydration on the durable record, so a
worker restart between dispatch and completion cannot lose the signal.

**Echoing rule:** the existing ``BaseNodeDef.handler`` returns
``Response(body, headers=self._emitter_headers())``. The Response
headers include ``HDR_EMITTER`` and ``HDR_EMITTER_KIND``. The fan-out
and frame headers are added explicitly at the publish call sites that
need them (the parallel-fan-out dispatcher and the aggregator-handler's
completion publish) — they are not folded into
``_emitter_headers``. Tools republish inbound headers via the existing
``_publish_action`` pathway, which carries ``HDR_FANOUT_ID`` through
from the tool's inbound to its ``ReturnCall``.

The shipped ``calfkit/nodes/base.py`` defines ``_emitter_headers`` (which
returns only ``HDR_EMITTER`` and ``HDR_EMITTER_KIND``); no
``_emit_headers`` helper exists. ``traceparent`` / ``tracestate`` are
not threaded by current code; the fields on ``FanOutState`` are
forward-compatibility hooks for the OTel work described in §11.2.

### 6.5 Compacted topic configuration

Shipped at `calfkit/nodes/aggregator/_topic_admin.py::STATE_TOPIC_CONFIG`:

```python
STATE_TOPIC_CONFIG: dict[str, str] = {
    "cleanup.policy": "compact",
    "min.compaction.lag.ms": "60000",      # 1 min: matches recently-completed TTL
    "delete.retention.ms": "60000",        # 1 min: tombstones retained long enough to disambiguate
    "segment.ms": "604800000",             # 7 days: Kafka default; compaction triggers via min.compaction.lag.ms
}
```

The returns topic uses Kafka defaults (no per-topic config overrides).

**Rationale per setting (fanout-state):**

- `cleanup.policy=compact` — required: per-key retention only. Without this, the topic is delete-policy and a tombstone is meaningless.
- `min.compaction.lag.ms=60000` — matches the `_recently_completed` TTL in the in-memory cache. Protects against compaction racing with consumers that just read a key and need to re-read it during rehydration.
- `delete.retention.ms=60000` — how long a tombstone is retained after the key is compacted away. Long enough for a rehydrating consumer to see the tombstone and know the batch is done; short enough that completed batches don't bloat the log.
- `segment.ms=604800000` (7 days) — Kafka's default; small `min.compaction.lag.ms` already drives frequent enough compaction without forcing tiny segments.

Earlier drafts proposed `min.cleanable.dirty.ratio`, `segment.bytes`,
`retention.ms`, and `max.compaction.lag.ms`. None shipped — the four keys
above are sufficient in practice and Kafka's defaults are reasonable for
the unspecified parameters.

**Validation of pre-existing topics.**
`_topic_admin._ensure_topic` (the helper that backs
`ensure_aggregator_topics`) validates the partition count AND each key
in ``STATE_TOPIC_CONFIG`` against the live topic configuration when the
state topic already exists. If any required key is missing or has a
drifted value (e.g. an operator manually set
``cleanup.policy=delete``), startup raises
``AggregatorStateStoreError`` with the offending key/value rather than
silently proceeding with broken compaction. This closes the
"misconfigured pre-existing topic" gap called out in §14.2.

The live-config lookup itself goes through
``_describe_topic_configs``, which wraps the broker's
``describe_configs`` response: a non-zero per-resource error code (most
commonly ``TOPIC_AUTHORIZATION_FAILED`` on managed brokers where the
worker's principal lacks ``DESCRIBE_CONFIGS``) is surfaced as
``AggregatorStateStoreError`` with a clear message pointing at the
missing ACL, rather than the misleading
``cleanup.policy=None`` mismatch the validator would otherwise produce.
Operators upgrading into the durable aggregator on a managed broker
should grant ``DescribeConfigs`` on the state topic before the worker
restarts.

**Partition count and replication factor:**

- Both topics: **partition count auto-detected from the agent's main
  topic** at startup (default 6 when the main topic doesn't exist).
  Co-partitioning is preserved by the `FanOutAggregatorPartitioner`,
  which extracts the `correlation_id` prefix from the
  `{corr}|{fan_out_id}` composite keys before hashing.
- Replication factor: **3** for both, matching production Kafka defaults
  (`_topic_admin.py::ensure_aggregator_topics` accepts a
  `replication_factor` kwarg for dev / single-broker environments).

### 6.6 Why `frozenset[str]` on Kafka

Pydantic serialises `frozenset[str]` to a JSON array. On deserialise we coerce back via Pydantic's `Field` discriminator. Round-trip preserves equality. The Kafka payload is JSON (FastStream default); this works.

### 6.7 Forbidden patterns reaffirmed

- **No `dict[str, Any]` on the wire** in user-facing types.
  ``FanOutState.received`` is ``dict[str, Any]`` because the upstream
  ``State.tool_results`` is ``dict[str, ToolCallResult | Any]``. This
  is a known weak point in the existing State model; the aggregator
  does not make it worse. An earlier iteration wrapped tool-call ids
  in a ``ToolCallId = NewType("ToolCallId", str)`` to add a documentary
  type-level distinction from ``correlation_id`` / ``fan_out_id``, but
  the wrapping was inconsistent at the State boundary (callers had to
  cast at every entry), produced no extra type safety, and added
  friction. The NewType was retired; the
  ``calfkit/nodes/aggregator/state.py`` module docstring keeps the
  conceptual note.
- **No `headers` field on `Envelope`.** User propagation context flows via Kafka transport headers per the existing `_protocol.py` channel. The aggregator's `traceparent`/`tracestate` are also Kafka headers, never body fields.

---

## 7. Partitioning and idempotency

### 7.1 The linearisation contract

For any `(correlation_id, fan_out_id)`, all reads and writes to `fanout-state` must be linearisable from the aggregator's perspective. That is, the read-modify-write sequence "load batch, merge new return, write back" must not interleave with a competing writer.

This is achieved via three composed properties:

1. **Co-partitioning.** The `FanOutAggregatorPartitioner` extracts `correlation_id` from composite keys before hashing, so writes for the same `correlation_id` always land on the same partition number across main / fanout-state / fanout-returns (`calfkit/nodes/aggregator/_partitioner.py`).
2. **Single owner per partition.** The aggregator's returns subscriber is a consumer-group member; Kafka guarantees one member owns a given partition at a time.
3. **Serial dispatch within the owner.** The aggregator's subscription registers with `max_workers=1` (`calfkit/nodes/agent.py::BaseAgentNodeDef.kafka_subscriptions`), so a single asyncio task drives read-modify-write for all owned partitions on this worker — no in-process race for the same key.

Together these make per-key writes linearisable across the cluster.

### 7.2 Co-partitioning with the agent's main topic

The agent's main `subscribe_topics` and `fanout-returns` are partitioned by `correlation_id` (the existing `_publish_action` already does `key=correlation_id.encode()` on every outbound call inside `base.py::BaseNodeDef._publish_action`). If the agent's main topic has 6 partitions, `fanout-returns` has 6 partitions; both use the `FanOutAggregatorPartitioner` installed at `Client.connect`; the same correlation_id lands on the same partition number on both topics.

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

1. Worker B's rebalance listener fires `on_partitions_assigned` for partition 3 of `fanout-returns`. The state store rehydrates the same partition of `fanout-state` from beginning. It sees the dispatch record for `(X, F)`, then the tombstone — the tombstone drops the key from the cache and adds it to `_recently_completed` (TTL = 60s).
2. Worker B begins consuming `fanout-returns` on partition 3. A redelivered return for `correlation_id=X`, `fan_out_id=F` arrives (possible if worker A's offset commit was lost).
3. The aggregator handler calls `state_store.was_recently_completed(key)` → true → log INFO "late return after completion" and drop.
4. If the tombstone has already been compacted away (after `delete.retention.ms` elapsed and Kafka compaction triggered), rehydration sees no record at all and the return is logged WARN "orphan return" and dropped.

This is the §10.3 "late return" path. The reason `delete.retention.ms=60000` is set explicitly: we want tombstones retained long enough to disambiguate "tombstoned" from "never existed" during the realistic rebalance window.

### 7.5 Dedup contract

The composite key for dedup is `(correlation_id, fan_out_id, tool_call_id)`.

**Inbound on the agent's main topic and `fanout-returns`** — both
subscriptions are registered with
``ack_policy=AckPolicy.NACK_ON_ERROR`` (see
``calfkit/nodes/agent.py::BaseAgentNodeDef.kafka_subscriptions``). The
agent's main subscription is force-set to ``NACK_ON_ERROR`` (overriding
``BaseNodeDef``'s default ``ACK_FIRST``) because the dispatch path's
durable state-store write and per-Call publish loop MUST rewind the
inbound offset on raise — otherwise an inbound is committed-then-lost
and the user-facing request hangs forever. The fanout-returns
subscription uses the same ack policy so a handler raise (publish
failure, transient state-store error, ``AggregatorMergeError`` under
ABORT) rewinds the consumer offset and the message is redelivered
within the same consumer session, rather than silently swallowed by
``ACK_FIRST``'s before-handler commit.

**Inbound on `fanout-returns`** — handler at
`calfkit/nodes/agent.py::BaseAgentNodeDef._aggregator_handler`:

1. Decode `correlation_id` (FastStream context), `fan_out_id` (Kafka header `HDR_FANOUT_ID`), and the `tool_call_id` from `envelope.context.state.tool_results`.
2. Look up `(correlation_id, fan_out_id)` in the state-store cache (`state_store.get(key)`):
   - **Recently completed** (`state_store.was_recently_completed(key)` returns true): log INFO "late return after completion"; drop. (Optionally route to DLQ — v2.)
   - **Not found:** log WARN "orphan return"; drop. Rehydration runs on partition assignment, so a cache miss here means the batch was never started on this partition or was already tombstoned past the `delete.retention.ms` window.
   - **Found in active set:** filter out `tool_call_id`s already in `batch.received` (dedup) and `tool_call_id`s outside `batch.expected_tool_call_ids` (defensive). If nothing new remains: log DEBUG "no new returns; dedup or no-match"; ack and return.
3. Build the post-merge batch via `_InFlightBatch.with_received(...)` (copy-on-write — see `_in_memory_store.py`), then `await state_store.put(key, new_batch, partition=...)`. The durable publish completes before the cache write, so a crash between the two leaves the durable log as the source of truth.
4. Call `aggregator.on_partial(batch_view, newly_received)`.
5. Call `aggregator.should_complete(batch_view)`. If true, run §8.4 completion path.

**Outbound dispatch dedup — the shipped idempotent-dispatch check**
(at `calfkit/nodes/agent.py::BaseAgentNodeDef._publish_parallel_with_aggregator`):

A redelivered inbound on the agent's main topic re-runs `run()` and re-emits
the parallel `list[Call]`. Without dedup, this would publish N more tool
calls and create a second batch. The check has three branches keyed off
the durable state at `(correlation_id, fan_out_id)`:

1. **Deterministic `fan_out_id`.** `fan_out_id = envelope.internal_workflow_state.current_frame.frame_id`. Same inbound redelivery → same frame_id → same `fan_out_id`.
2. **Recently-completed:** `state_store.was_recently_completed(key)` → skip the fan-out entirely (return). The aggregated return has already been published; re-publishing would either duplicate Tool Calls or rewind a completed batch.
3. **In-flight with matching `expected_tool_call_ids`:** `state_store.get(key)` returns a batch whose `expected_tool_call_ids` matches the newly-computed set → preserve the durable `received` map (do NOT call `state_store.put`) and re-publish the tool Calls. Tools are responsible for being idempotent on `(correlation_id, tool_call_id)` — the standard calfkit contract — so duplicate dispatches converge.
4. **In-flight with drifted `expected_tool_call_ids`:**
   ``state_store.get(key)`` returns a batch whose
   ``expected_tool_call_ids`` differs from the newly-computed set. This
   is a real upstream bug (the agent loop produced a different set of
   tool calls on re-entry) and discards the prior ``received`` map.
   Log ``ERROR "redispatch with different expected_tool_call_ids;
   overwriting"`` and fall through to the first-time-dispatch write —
   but the fresh ``_InFlightBatch`` is constructed with ``degraded=True``
   so the eventual completion publish stamps ``HDR_DEGRADED_MERGE`` on
   the re-entry envelope. Downstream observability gets a quantifiable
   signal for the silent data loss. Storing ``degraded`` on the durable
   record (rather than a process-local set) is what lets the signal
   survive worker restart and ``NACK_ON_ERROR`` redelivery — a
   rehydrating worker reads the flag back from the compacted topic.
5. **First-time dispatch:** no record exists → build the initial
   ``_InFlightBatch`` (with ``agent_topic=self.subscribe_topics[0]``
   and ``degraded=False``) and ``await state_store.put(key,
   initial_batch)`` before publishing any tool Call.

This is the "idempotent dispatch" pattern. It costs one cache lookup at
dispatch time and one durable publish on first dispatch, and is critical
for at-least-once correctness.

### 7.6 What is allowed to be slow

- Compaction. `min.compaction.lag.ms=60000` means a key can be uncompacted for at least a minute. Rehydration reads from offset 0; in pathological cases this could mean reading a few minutes of writes. Acceptable; rehydration is a startup-only cost.
- Late arrivals. A tool return that arrives 30 minutes after completion is dropped. No retry, no DLQ in v1.

### 7.7 What is not allowed to be slow

- The in-memory cache lookup on every return. O(1) dict access.
- The write-through to `fanout-state` on every return. One async publish; small message (State-sized).

---

## 8. Lifecycle sequences

### 8.1 Dispatch (parallel fan-out begins)

Shipped at `calfkit/nodes/agent.py::BaseAgentNodeDef._publish_parallel_with_aggregator`:

```
agent.run() returns list[Call]
  |
  v
_publish_parallel_with_aggregator(calls, envelope, correlation_id, broker, partition):
  |
  +--- fan_out_id = envelope.internal_workflow_state.current_frame.frame_id
  +--- expected_tool_call_ids = frozenset of call.input_args[0] for each call
  +--- key = (correlation_id, fan_out_id)
  |
  +--- IDEMPOTENT-DISPATCH BRANCHING (see §7.5):
  |      state_store.was_recently_completed(key)? → return (skip)
  |      state_store.get(key) is not None and matches expected? → preserve, fall through to step "publish Calls"
  |      state_store.get(key) is not None but drifted?         → log ERROR, overwrite below
  |      state_store.get(key) is None                          → first-time dispatch, write below
  |
  +--- (first-time / overwrite branch) build initial _InFlightBatch
  |      with base_state = calls[0].state deep-copy,
  |      received = {}, agent_topic = self.subscribe_topics[0]
  |    await state_store.put(key, initial_batch, partition=partition)
  |    The publish completes before the cache write — durable log is the recovery anchor.
  |
  +--- for each Call in calls:
  |       wf_copy = envelope.internal_workflow_state.model_copy(deep=True)
  |       wf_copy.invoke_frame(call, callback_topic=returns_topic)  # = {node_id}.fanout-returns
  |       publish wf_copy.current_frame.target_topic with:
  |         key      = correlation_id.encode()  # partitioner extracts the correlation_id
  |         headers  = {
  |           HDR_EMITTER, HDR_EMITTER_KIND,
  |           HDR_FRAME_ID: wf_copy.current_frame.frame_id,
  |           HDR_FANOUT_ID: fan_out_id,
  |           # traceparent/tracestate are not threaded today — see §11.2
  |         }
```

The order is critical: **write `FanOutState` first, then publish tool Calls.**
If the state write succeeds but tool Calls fail to publish, the next
redelivery of the inbound will hit the "in-flight with matching
`expected_tool_call_ids`" branch and re-publish the Calls. This relies on
tools being idempotent on `(correlation_id, tool_call_id)` — the standard
calfkit contract — so duplicate dispatches converge.

If both `state_store.put` and any subsequent Call publish are non-atomic
under at-least-once: at worst, tools may run twice with the same
`tool_call_id`; tool-side idempotency makes this a no-op at the level of
aggregated results. Kafka transactions could close the window atomically;
that is a v2 consideration (§17.2).

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
          # traceparent/tracestate echo is not shipped — see §11.2
        }
```

The tool node has no knowledge of the aggregator. The rewritten `callback_topic` does all the work. Tools remain pure.

### 8.3 Aggregator handler (per return)

```
aggregator_handler(envelope, correlation_id, headers, broker, partition):
  |
  +--- fan_out_id = decode_header_str(headers[HDR_FANOUT_ID])
  |    if fan_out_id is None: log WARN "missing fanout-id"; drop
  |
  +--- key = (correlation_id, fan_out_id)
  |
  +--- if state_store.was_recently_completed(key):
  |       log INFO "late return after completion"; drop
  |       return
  |
  +--- batch = state_store.get(key)
  |    if batch is None:
  |       # not in cache, not recently completed — orphan
  |       log WARN "orphan return"; drop
  |       return
  |
  +--- # Extract the new tool result
  |    new_ids = {tcid for tcid in envelope.context.state.tool_results
  |              if tcid in batch.expected_tool_call_ids
  |              and tcid not in batch.received}
  |
  +--- if new_ids:
  |       # Build the post-merge batch via copy-on-write
  |       updated_batch = batch.with_received({**batch.received, **new_results},
  |                                           last_updated_ms=now_ms)
  |       # Write-through to compacted topic, then update the cache
  |       await state_store.put(key, updated_batch, partition=partition)
  |       batch = updated_batch
  |       view = self.aggregator._batch_view(batch)
  |       # Exceptions from on_partial propagate (no try/except wrapper in the
  |       # framework) — they abort the batch and FastStream redelivers the
  |       # inbound. on_partial is expected to be a thin observability hook;
  |       # if it does work that can fail, the user owns the try/except.
  |       await self.aggregator.on_partial(view, frozenset(new_ids))
  |    else:
  |       # No new tcids. Could be a plain duplicate, OR a redelivery after
  |       # a previously-merged batch failed to publish or tombstone (the
  |       # durable record already has all results). Build the view and let
  |       # should_complete decide which path to take.
  |       view = self.aggregator._batch_view(batch)
  |       if not await self.aggregator.should_complete(view):
  |          log DEBUG "duplicate/empty return; idempotent drop"; return
  |       log INFO "re-attempting completion for already-merged batch (redelivery)"
  |       # fall through to completion path
  |
  +--- # Check completion (either freshly advanced or redelivered-and-already-complete)
  |    if await self.aggregator.should_complete(view):
  |       go to §8.4 completion path
  |    else:
  |       ack and return (wait for more)
```

The `AggregatorBatch` view exposes the fields shipped on the dataclass
(see `calfkit/nodes/aggregator/state.py`):
`correlation_id`, `fan_out_id`, `expected_tool_call_ids`, `received`,
`base_state`, `started_at_ms`, `last_updated_ms`. `AggregatedReturn`
carries only `state` and `degraded` — the framework needs no other
fields to publish the re-entry envelope.

### 8.4 Completion path

```
# Triggered when should_complete() returns True
  |
  +--- try:
  |       aggregated = await self.aggregator.merge(view)
  |       # aggregated is an AggregatedReturn(state=..., degraded=False)
  |    except Exception as e:
  |       handle per aggregator.merge_error_policy:
  |         ABORT:               raise AggregatorMergeError(
  |                                  correlation_id=key[0],
  |                                  fan_out_id=key[1],
  |                                  state_topic=self.aggregator.runtime.state_topic,
  |                              )  # FastStream NACK_ON_ERROR rewinds offset; message redelivers
  |         RETRY:               retry merge() exactly once; on second failure, fall through to ABORT
  |         FALLBACK_TO_DEFAULT: log; fall back to the default merge (apply received to base_state);
  |                              return AggregatedReturn(state=..., degraded=True) so
  |                              HDR_DEGRADED_MERGE is stamped on the published envelope;
  |                              if the default merge also raises, fall through to ABORT
  |
  +--- # Build the re-entry envelope. Replace the top-of-stack frame's
  |    # frame_id with a fresh uuid7 so the next parallel fan-out within
  |    # the same invocation derives a distinct fan_out_id (see §6.3).
  |    re_entry_state = envelope.internal_workflow_state.model_copy(deep=True)
  |    previous_frame = re_entry_state.unwind_frame()
  |    new_frame = dataclasses.replace(previous_frame, frame_id=uuid_utils.uuid7().hex)
  |    re_entry_state.call_stack.push(new_frame)
  |    re_entry_envelope = Envelope(
  |       context=SessionRunContext(state=aggregated.state, deps=envelope.context.deps),
  |       internal_workflow_state=re_entry_state,
  |    )
  |
  +--- # Publish to agent's main topic (FanOutState.agent_topic) FIRST,
  |    # then tombstone. Rationale below.
  |    headers = {
  |       HDR_EMITTER: self.node_id, HDR_EMITTER_KIND: "agent",
  |       HDR_FRAME_ID: new_frame.frame_id,
  |       # HDR_FANOUT_ID is absent — the batch is done, this is a resumption.
  |       # Stamp HDR_DEGRADED_MERGE if EITHER the merge produced a degraded
  |       # result (aggregated.degraded) OR the dispatch path flagged the
  |       # batch as degraded (batch.degraded — drifted-redispatch overwrite).
  |    }
  |    if aggregated.degraded or batch.degraded:
  |        headers[HDR_DEGRADED_MERGE] = "1"
  |    await broker.publish(re_entry_envelope, topic=batch.agent_topic,
  |                         correlation_id=correlation_id,
  |                         key=correlation_id.encode(), headers=headers)
  |
  +--- # Tombstone the fanout-state record so late returns are recognised
  |    # as late and the recently-completed TTL window starts ticking.
  |    await state_store.tombstone(key, partition=msg_partition)
  |
  +--- log INFO "batch completed"
  +--- ack and return
```

A subtle property: **the merged state goes to the agent's main topic, not to the original caller.** The agent then runs its next LLM iteration. The caller's reply only happens when the agent eventually returns a `ReturnCall` after all LLM iterations are done.

**Why publish-first, tombstone-second.** Earlier drafts of this design
specified tombstone-first ordering on the theory that the durable log
should never contradict itself — a stale state record outliving a
successful re-entry would, in principle, confuse subsequent
rebalances. In practice, shipping the tombstone-first ordering exposed
a worse failure mode: if `state_store.put` succeeded on a prior
delivery but the downstream agent-topic publish then failed,
FastStream redelivered the inbound tool return to the aggregator
handler. On redelivery, `new_ids` was empty (every tcid was already in
`batch.received`) and the handler short-circuited on the
duplicate-return drop — the merged record sat orphaned in the durable
log and the agent never re-entered. Operators saw a stuck batch with
no way to drive it forward short of manual intervention.

The shipped ordering — **publish first, tombstone second** — makes
the completion path idempotent under at-least-once redelivery. If the
publish or tombstone fails, FastStream redelivers; the aggregator
handler reaches a code path in
`BaseAgentNodeDef._aggregator_handler` that detects "no new tcids but
`should_complete` is True" and **re-attempts merge → publish →
tombstone**. Genuine duplicates (where `should_complete`
is still False) still take the drop path. The trade-off is described
in the §9.1 failure-mode matrix and the dedup contract in §9.2.

The downstream cost is that the agent's main-topic handler must be
idempotent on `correlation_id` for the merged-state envelope, since a
crash between publish and tombstone (or between two redelivered
completion attempts) can result in the same merged envelope being
delivered to the agent twice. The dedup mechanism is the existing
idempotent-dispatch check at `agent.py::_publish_parallel_with_aggregator`'s
redelivery branch: redelivery hits `state_store.was_recently_completed(key)`
or `existing.expected_tool_call_ids == expected_tool_call_ids` and
short-circuits the second dispatch. The same check covers the
publish-twice case: the agent re-enters with the merged state on the
first delivery, computes its next LLM iteration, dispatches a new
fan-out (or returns); when the second copy of the merged envelope
arrives, the agent's resumed run already advanced past that
correlation-id state, and the duplicate is either deduped at the
state-store layer or handled idempotently by the agent loop's own
contract.

### 8.5 Worker startup and rehydration

Topic provisioning happens once at worker startup; rehydration is
framework-managed and partition-scoped, triggered by Kafka rebalance
events — NOT by an eager full-log read at boot.

```
Worker.run():
  |
  +--- await Worker._prepare_aggregators():
  |       await broker.connect()  # makes broker.config.admin_client available
  |       for each agent in self._nodes:
  |          await agent.aggregator.setup(
  |              broker,
  |              node_id=agent.node_id,
  |              main_topic=agent.subscribe_topics[0],
  |              kafka_config=self._client.kafka_config,
  |          )
  |       # FanOutAggregator.setup:
  |       #   1. ensure_aggregator_topics(broker, node_id, main_topic)
  |       #      -> reads main_topic's partition count via admin.describe_topics
  |       #      -> creates {node_id}.fanout-state with STATE_TOPIC_CONFIG
  |       #      -> creates {node_id}.fanout-returns with default config
  |       #      -> validates partition count on existing topics
  |       #   2. construct _KafkaStateStore (with kafka_config kwargs forwarded)
  |       #   3. construct _StateStoreRebalanceListener
  |       #   4. publish runtime state to FanOutAggregator._runtime
  |
  +--- Worker.register_handlers():
  |       iterates node.kafka_subscriptions() for each node; passes the
  |       aggregator subscription's listener through to broker.subscriber.
  |
  +--- await FastStream(broker).run(**extra_run_args)
       FastStream begins consuming; aiokafka assigns partitions; the
       rebalance listener fires on_partitions_assigned, which calls
       state_store.rehydrate_partitions(partition_ids) BEFORE the
       returns subscriber begins handling messages for those partitions.
```

Rehydration runs inside
`calfkit/nodes/aggregator/_rebalance.py::_StateStoreRebalanceListener.on_partitions_assigned`,
which delegates to
`calfkit/nodes/aggregator/_kafka_state_store.py::_KafkaStateStore.rehydrate_partitions`:

1. Construct a transient `AIOKafkaConsumer` with
   `bootstrap_servers=kafka_config.bootstrap_servers`,
   `**kafka_config.client_kwargs` (SASL/SSL settings inherited from
   `Client.connect`), and **no `group_id`** — the consumer uses
   `assign()` to take partitions explicitly so it doesn't participate
   in group coordination.
2. Seek to beginning on the assigned state-topic partitions.
3. Loop `consumer.getmany(timeout_ms=1000)` until each partition's
   end-offset has been crossed. Each record is applied via
   `_apply_record`: tombstones drop the key from cache and add it to
   `_recently_completed`; non-tombstones parse to `FanOutState` and
   populate the cache. After
   `_REHYDRATE_MAX_EMPTY_POLLS=5` consecutive empty polls with
   partitions still outstanding, `rehydrate_partitions` raises
   `AggregatorStateStoreError` rather than silently activating the
   partition with partial state.
4. Mark partitions owned; the rebalance listener returns and aiokafka
   resumes the returns subscriber on those partitions.

On revoke (`on_partitions_revoked`), the listener calls
`state_store.evict_partitions(...)` synchronously to drop cache entries
for revoked partitions BEFORE Kafka reassigns them to another worker.

Earlier drafts proposed an eager `_rehydrate(broker)` call in
`Worker.run()` with a uniquely-named per-worker `group_id` for the
rehydration consumer. Neither shipped: rebalance-driven rehydration is
strictly better (no startup-time stalls, no orphan partitions, no
group-coordination complexity).

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
| Tool process crashes mid-execution | No return arrives | Batch remains pending until partition rebalance evicts it (v1 has no idle-timeout reaper — see §17.11). Operator-visible via stuck `pending_batches`. | Batch stays in-flight; operator must redrive or rely on rebalance eviction |
| Aggregator process crashes after receiving return, before write-through | Return's consumer offset never committed; redelivered to next owner | Next owner re-receives; dedup based on `tool_call_id` not in `batch.received` (post-rehydration); proceeds | None |
| Aggregator process crashes after write-through, before completion check | Cache lost; rebalance rehydration finds updated state; should_complete re-runs | Completion fires (if applicable); re-entry happens | Slight delay; correct outcome |
| Aggregator process crashes after re-entry publish, before tombstone | The durable record still says "in flight" (no tombstone); the merged envelope did reach the agent topic. Inbound redelivered to aggregator handler after rebalance. | Handler re-runs: `new_ids` is empty (all results already in `batch.received`) but `should_complete` is True → re-attempts merge → publish → tombstone (see `BaseAgentNodeDef._aggregator_handler`). | Merged-state envelope is delivered to the agent topic again; agent dedups by `correlation_id` (existing calfkit contract). |
| Aggregator process crashes after merge / before re-entry publish | Same as above — the durable record still says "in flight", no envelope reached the agent. | Same redelivery path re-attempts merge → publish → tombstone idempotently. | None — the merged envelope is delivered exactly the same as a non-crash path. |

The last two rows are why the shipped ordering is **publish-first,
then tombstone**, and why the handler distinguishes "no new tcids and
not yet complete" (idempotent drop) from "no new tcids and already
complete" (re-attempt completion). Earlier drafts specified
tombstone-first ordering for log-self-consistency reasons; in practice
that ordering produced a stuck-batch failure mode (durable record
tombstoned, agent never re-entered) with no automated recovery path.
The publish-first ordering trades that for at-least-once re-entry on
the agent topic — the agent's main handler is responsible for dedup
on the merged envelope, which is the same contract calfkit already
relies on for redelivery of any inbound (see §9.2). Atomic
write-and-publish across `fanout-state` and the agent's main topic
would close the gap and remove the dedup requirement; that is a v2
consideration (§17.2).

### 9.2 The dedup contract restated

The aggregator MUST be idempotent under at-least-once delivery on the following keys:

- **Same tool return arrives twice**: dedup via `tool_call_id in batch.received` check. Drop the second.
- **Same dispatch happens twice**: dedup via idempotent-dispatch check at `_publish_parallel_with_aggregator` (see §7.5). Recently-completed → skip; in-flight matching → preserve and re-publish; in-flight drifted → log ERROR and overwrite.
- **Same completion publish happens twice**: the agent's main topic handler must be idempotent on `correlation_id`. The agent's next LLM iteration sees a deduped inbound and produces a single resume. This is the existing calfkit contract for redelivery on any handler.

### 9.3 Late-return policy

A return arriving for a `(correlation_id, fan_out_id)` that is in `_recently_completed` (tombstoned within the last 60s) is logged at INFO and dropped. A return arriving for a key not in cache and not in `_recently_completed` is logged at WARNING ("orphan return") and dropped. No DLQ in v1; the operator's diagnostic path is logs + metrics.

### 9.4 Merge error policy

The user's `merge()` can raise. The policy is `merge_error_policy` on the
`FanOutAggregator` constructor — a `MergeErrorPolicy` enum
(`calfkit/nodes/aggregator/aggregator.py::MergeErrorPolicy`):

- `MergeErrorPolicy.ABORT` (default): raise
  `AggregatorMergeError(correlation_id=..., fan_out_id=...)`. FastStream
  nacks the inbound; Kafka redelivers. If transient, the next try
  succeeds. If permanent (bug in `merge`), the message redelivers
  forever — operator must intervene. The exception carries structured
  context for grouping in Sentry / Statsig.
- `MergeErrorPolicy.RETRY`: retry `merge()` exactly once. Useful for a
  transient downstream call (e.g., LLM timeout). On the second failure,
  behaves as `ABORT`. There is no `merge_retry_count` knob — retry
  count is fixed at one.
- `MergeErrorPolicy.FALLBACK_TO_DEFAULT`: log the error, fall back to the
  default merge (apply `batch.received` to a copy of `batch.base_state`),
  complete normally, and stamp `HDR_DEGRADED_MERGE=1` on the published
  envelope so operators can detect silently-degraded batches. The agent
  resumes with default-merged state. The fallback merge is itself wrapped
  in try/except — if the framework's default merge also raises (e.g., a
  broken ``State.add_tool_result``), the framework treats it as `ABORT`
  rather than letting the exception escape silently.

---

## 10. State store internals

### 10.1 The in-memory cache

Shipped at `calfkit/nodes/aggregator/_in_memory_store.py` and
`calfkit/nodes/aggregator/_kafka_state_store.py`:

```python
@dataclass(frozen=True)
class _InFlightBatch:
    """Immutable in-memory representation; mirrors FanOutState on the wire.

    Frozen, with ``received`` exposed as a ``Mapping`` (the dataclass'
    ``__post_init__`` wraps the underlying dict in
    ``types.MappingProxyType``) so callers cannot perform in-place
    mutation. Updates flow through :meth:`with_received`, which returns
    a fresh instance with the new ``received`` and ``last_updated_ms``.
    """
    correlation_id: str
    fan_out_id: str
    expected_tool_call_ids: frozenset[str]
    base_state: State
    started_at_ms: int
    last_updated_ms: int
    agent_topic: str
    degraded: bool = False
    received: Mapping[str, Any] = field(default_factory=dict)
    traceparent: str | None = None
    tracestate: str | None = None

    def to_fanout_state(self) -> FanOutState: ...
    @classmethod
    def from_fanout_state(cls, state: FanOutState) -> _InFlightBatch: ...
    def with_received(self, received, *, last_updated_ms) -> _InFlightBatch: ...


class _KafkaStateStore:
    """Partition-scoped in-memory cache backed by the compacted state topic.

    Standard Kafka-Streams-style: the returns subscriber owns reads and
    writes; rehydrate_partitions is called from the rebalance listener
    on partition assignment.
    """

    # Reads
    def get(self, key) -> _InFlightBatch | None: ...
    def was_recently_completed(self, key) -> bool: ...

    # Writes (durable publish first, then cache update)
    def partition_for_key(self, key) -> int: ...
    async def put(self, key, batch, *, partition=None) -> None: ...
    async def tombstone(self, key, *, partition=None) -> None: ...
    def mark_completed(self, key) -> None: ...

    # Rebalance lifecycle (called by _StateStoreRebalanceListener)
    async def rehydrate_partitions(self, partition_ids: set[int]) -> None: ...
    def evict_partitions(self, partition_ids: set[int]) -> None: ...
```

A few load-bearing properties:

- **Partition-scoped.** Only partitions this worker currently owns are
  in the cache; `evict_partitions` drops entries on revoke,
  `rehydrate_partitions` rebuilds them on assignment.
- **Composite keying.** `put`/`tombstone` use
  `_partitioner.build_composite_key(correlation_id, fan_out_id)` so the
  partitioner can extract `correlation_id` before hashing.
- **Immutable batches; replace-on-update.** ``_InFlightBatch`` is a
  frozen dataclass and its ``received`` field is exposed as a
  ``Mapping`` backed by ``MappingProxyType``. The agent handler builds
  the post-merge batch via ``batch.with_received(...)`` — a fresh
  instance — BEFORE the durable publish, so a failed publish leaves
  the cached instance unchanged and FastStream's
  ``ack_policy=NACK_ON_ERROR`` redelivery re-merges cleanly. In-place
  mutation of ``received`` is structurally prevented, not merely
  conventional.

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

This table is aspirational — the shipped code logs at INFO / DEBUG /
WARN / ERROR on the events below, but not all rows are wired to
metrics yet. Treat as a design target, not a contract.

| Event | Level | Fields |
|---|---|---|
| Batch dispatch persisted | DEBUG | `correlation_id`, `node_id`, `fan_out_id`, `expected_count` |
| Redelivered inbound — recently completed | INFO | `correlation_id`, `fan_out_id` |
| Redelivered inbound — in-flight matching | INFO | `correlation_id`, `fan_out_id`, `received_count`, `expected_count` |
| Redispatch with drifted `expected_tool_call_ids` | ERROR | `correlation_id`, `fan_out_id`, `prev`, `new` |
| Return integrated | DEBUG | `correlation_id`, `tool_call_id`, `tool_name`, `progress` |
| Late return dropped | INFO | `correlation_id`, `fan_out_id`, `tool_call_id` |
| Orphan return dropped | WARNING | `correlation_id`, `fan_out_id`, `tool_call_id` |
| Batch completed | INFO | `correlation_id`, `fan_out_id`, `node_id`, `duration_ms`, `result_count` |
| Aggregator partitions assigned | INFO | `partition_ids` |
| Aggregator partitions revoked | INFO | `partition_ids` |
| Rehydration complete (per-rebalance) | INFO | `partition_ids`, `cached_keys` |
| State-topic poison record | ERROR | `partition`, `key`, `error` (raises `AggregatorStateStoreError`) |
| Merge error | ERROR | `correlation_id`, `fan_out_id`, `error`, `policy_applied` |
| Merge degraded (FALLBACK_TO_DEFAULT fallback) | WARN | `correlation_id`, `fan_out_id`, `error` |

Idle-timed-out logging is intentionally absent — see §17.11.

### 11.2 OTel spans

The span / trace-propagation design below is a **design target — not
shipped**. The shipped aggregator does not open any OTel spans and does
not propagate `traceparent` / `tracestate` end-to-end. The
`FanOutState.traceparent` and `FanOutState.tracestate` fields exist in
the durable wire format (see `calfkit/nodes/aggregator/state.py`) so a
future OTel integration can carry them through the fan-out without a
schema migration on the compacted topic — but no code path currently
reads or writes them. Treat this section as the intended end-state for
operators evaluating the shape of future traces.

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

### 11.3 Metrics (Prometheus / OTel metrics) — design target

The metrics catalogue below is aspirational; the shipped code does not
emit OTel metrics yet. Logs alone (§11.1) cover the "is anything stuck?"
operator path for v1.

```
calfkit_aggregator_batches_started_total{node_id="..."}                  counter
calfkit_aggregator_batches_completed_total{node_id="...", outcome="..."} counter
calfkit_aggregator_batch_duration_seconds{node_id="..."}                 histogram
calfkit_aggregator_batch_size{node_id="..."}                             histogram
calfkit_aggregator_returns_total{node_id="...", tool_name="..."}         counter
calfkit_aggregator_returns_late_total{node_id="..."}                     counter
calfkit_aggregator_returns_orphan_total{node_id="..."}                   counter
calfkit_aggregator_pending_batches{node_id="..."}                        gauge
calfkit_aggregator_state_store_writes_total{node_id="...", kind="..."}   counter   # kind=put|tombstone
calfkit_aggregator_state_store_write_errors_total{node_id="..."}         counter
calfkit_aggregator_degraded_merges_total{node_id="..."}                  counter   # MergeErrorPolicy.FALLBACK_TO_DEFAULT fallbacks
```

Low cardinality. No `correlation_id` or `tool_call_id` labels. Operators answer "is anything stuck?" with `pending_batches > 0 and rate(returns) == 0`.

### 11.4 OTel opt-out

Auto-detect on `opentelemetry` import; `CALFKIT_AGGREGATOR_OTEL=0` disables. When disabled, no spans emitted; metrics and logs continue. This matches the existing `calfkit/_vendor/pydantic_ai/_instrumentation.py` convention.

---

## 12. File-by-file layout (shipped)

The implementation ships in one PR on `feat/durable-fanout-aggregator`.
The layout below reflects what is on the branch, not a plan. For the
authoritative module structure see §4.3.

### 12.1 New files

| File | Purpose |
|---|---|
| `calfkit/nodes/aggregator/__init__.py` | Public re-exports (see §4.3). |
| `calfkit/nodes/aggregator/aggregator.py` | `FanOutAggregator` (behaviour overrides + `merge_error_policy` + `setup`) and `MergeErrorPolicy`. |
| `calfkit/nodes/aggregator/state.py` | `FanOutState` (wire format, with durable `degraded` flag), `AggregatorBatch` (immutable view), `AggregatedReturn`. |
| `calfkit/nodes/aggregator/errors.py` | `AggregatorError`, `AggregatorMergeError` (carries `correlation_id`/`fan_out_id`), `AggregatorStateStoreError` (carries `state_topic`). |
| `calfkit/nodes/aggregator/_runtime.py` | `_FanOutRuntime` frozen dataclass — framework-managed state (topics, partition count, state store, rebalance listener). |
| `calfkit/nodes/aggregator/_kafka_state_store.py` | `_KafkaStateStore` — partition-scoped cache, durable publishes via `broker.publish`, transient `AIOKafkaConsumer` for `rehydrate_partitions`. |
| `calfkit/nodes/aggregator/_in_memory_store.py` | `_InFlightBatch` (frozen dataclass; ``received`` wrapped in ``MappingProxyType``), `_TtlSet`, `_InMemoryStateStore` (test scaffolding). |
| `calfkit/nodes/aggregator/_topic_admin.py` | `ensure_aggregator_topics`, `STATE_TOPIC_CONFIG`. |
| `calfkit/nodes/aggregator/_partitioner.py` | `FanOutAggregatorPartitioner`, `build_composite_key`, `parse_composite_key`, `has_composite_delimiter`. |
| `calfkit/nodes/aggregator/_rebalance.py` | `_StateStoreRebalanceListener`. |
| `calfkit/nodes/aggregator/testing.py` | `InMemoryAggregator` (drop-in `FanOutAggregator` for unit tests). |
| `calfkit/client/kafka_config.py` | `KafkaConfig` dataclass — snapshot of bootstrap servers + client kwargs captured by `Client.connect`. |
| `tests/test_durable_aggregator.py` and friends | Test suite (see §13). |

Earlier drafts of this section detailed pseudo-code for each new file. Read
the shipped source for the authoritative shape; this doc only annotates
the load-bearing choices.

### 12.2 Edited files

#### `calfkit/nodes/agent.py`

- `_pending_batches`, `_parallel_state_aggregation`, and the `RuntimeError` about lost state are removed.
- `__init__` gains an `aggregator: FanOutAggregator | None = None` kwarg; default-constructs `FanOutAggregator()` when omitted.
- `kafka_subscriptions` overrides `BaseNodeDef.kafka_subscriptions` to append the aggregator returns subscription (`max_workers=1`, listener from `runtime.rebalance_listener` when `setup()` has run).
- New `_publish_parallel_with_aggregator` method (called from `_publish_action`'s parallel branch) handles the durable-dispatch path — see §7.5 and §8.1.
- New `_aggregator_handler` method — the FastStream handler for `fanout-returns` — see §8.3.
- New ``_ensure_aggregator_ready`` method — strict validation, not
  lazy synthesis. Both the parallel-dispatch and aggregator-handler
  paths call it before touching ``aggregator.runtime``; if
  ``aggregator._runtime is None`` (setup never ran) it raises
  ``AggregatorStateStoreError`` pointing the operator at
  ``calfkit.nodes.aggregator.testing.setup_for_tests`` for paths that
  intentionally bypass ``Worker.run()``. The previous behaviour
  (silent fixture-style synthesis when ``_runtime`` was missing) was
  removed so production code never grows a hidden no-broker fallback.

#### `calfkit/nodes/base.py`

- Adds `BaseNodeDef.kafka_subscriptions(self) -> list[_KafkaSubscription]` — default returns one subscription for the main handler. `BaseAgentNodeDef` (in `agent.py`) overrides to extend with the aggregator's returns subscription.
- The parallel branch of `_publish_action` delegates to `BaseAgentNodeDef._publish_parallel_with_aggregator` (defined in `agent.py`) when the node is an agent; non-agents keep the existing list-of-Calls behaviour.
- `_emitter_headers` is unchanged in signature (`dict[str, str]`); the aggregator headers (`HDR_FANOUT_ID`, `HDR_FRAME_ID`, `HDR_DEGRADED_MERGE`) are added explicitly at the publish call sites that need them, not folded into the base method.

#### `calfkit/_protocol.py`

Adds `HDR_FANOUT_ID`, `HDR_FRAME_ID`, and `HDR_DEGRADED_MERGE` constants (see §6.4).

#### `calfkit/worker/worker.py`

`register_handlers` iterates `node.kafka_subscriptions()` and threads the
aggregator subscription's `listener` through to `broker.subscriber`.
`run()` calls `_prepare_aggregators` BEFORE `register_handlers` so the
runtime is populated before subscriptions are built — see shipped code at
`calfkit/worker/worker.py`:

```python
async def _prepare_aggregators(self) -> None:
    broker = self._client._connection
    await broker.connect()  # makes broker.config.admin_client available

    kafka_config = self._client.kafka_config
    if kafka_config is None and any(isinstance(n, BaseAgentNodeDef) for n in self._nodes):
        raise AggregatorStateStoreError(
            "Worker requires Client.kafka_config to wire up the durable "
            "fan-out aggregator. ..."
        )

    for node in self._nodes:
        if isinstance(node, BaseAgentNodeDef):
            await node.aggregator.setup(
                broker,
                node_id=node.node_id,
                main_topic=node.subscribe_topics[0],
                kafka_config=kafka_config,
            )

async def run(self, **extra_run_args: Any) -> None:
    await self._prepare_aggregators()
    self.register_handlers()
    await FastStream(self._client._connection).run(**extra_run_args)
```

Note the contrast with earlier drafts: there is no `_ensure_topics` /
`_rehydrate` pair on `FanOutAggregator`; topic provisioning happens
inside `setup()` (via `_topic_admin.ensure_aggregator_topics`), and
rehydration is framework-managed by the rebalance listener (§8.5).

#### `calfkit/models/state.py`

Remove `PendingToolBatch` dataclass entirely (lines 128-143). Nothing else needs to change in this file.

Estimated diff: ~15 lines removed.

#### `calfkit/__init__.py`

The shipped top-level package re-exports the full aggregator surface
plus ``KafkaConfig``:

```python
from calfkit.nodes import (..., FanOutAggregator, ...)
from calfkit.nodes.aggregator import (
    AggregatedReturn,
    AggregatorBatch,
    AggregatorError,
    AggregatorMergeError,
    AggregatorStateStoreError,
    MergeErrorPolicy,
)
from calfkit.client import KafkaConfig
__all__ = [
    ..., "FanOutAggregator", "AggregatedReturn", "AggregatorBatch",
    "AggregatorError", "AggregatorMergeError", "AggregatorStateStoreError",
    "MergeErrorPolicy", "KafkaConfig", ...,
]
```

Earlier drafts argued against top-level re-export ("keep the surface
minimal; `from calfkit.nodes import FanOutAggregator` reads as a
node-level concern"). The shipped position is the opposite: parallel
fan-out is a first-class user concern and the convenience of
``from calfkit import FanOutAggregator, MergeErrorPolicy`` outweighs
the surface-area cost. The canonical home remains
``calfkit.nodes.aggregator``; the top-level re-exports are
straightforward aliases.

#### `calfkit/nodes/__init__.py`

Add `FanOutAggregator` re-export from `calfkit.nodes.aggregator`.

```python
from calfkit.nodes.aggregator import FanOutAggregator
__all__ = [..., "FanOutAggregator", ...]
```

#### `calfkit/exceptions.py`

The `CalfkitError` base class is present (existing). `AggregatorError`,
`AggregatorMergeError`, and `AggregatorStateStoreError` all inherit from
it.

#### `tests/conftest.py`

Test fixtures wire up `InMemoryAggregator` and provide `deploy_agent`
helpers that accept an `aggregator=` parameter.

#### `tests/test_concurrent_tool_calls.py`

Retained as smoke coverage; the durable-aggregator test suite lives in
`tests/test_durable_aggregator.py` (and friends).

### 12.3 Removed files

`models/state.py::PendingToolBatch` is removed alongside
`_pending_batches` and `_parallel_state_aggregation`.

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

1. Dispatch fan-out with three tools.
2. Let all three return so the batch completes and writes a tombstone.
3. Replay one of the returns after completion (simulating an at-least-once redelivery from Kafka).
4. Assert: aggregator logs "late return" at INFO; metric `calfkit_aggregator_returns_late_total` incremented; agent does not re-enter (batch is already in `_recently_completed`).

(Pre-completion timeout scenarios are deferred — v1 ships without an idle-timeout reaper; see `ROADMAP.md`.)

### 13.5 Idempotent re-arrival

`test_duplicate_return_is_deduped`:

1. Dispatch fan-out with two tools.
2. Inject the same tool return twice (via `InMemoryAggregator` test hook or by manually publishing on `TestKafkaBroker`).
3. Assert: only one update to `batch.received`; only one `on_partial` invocation.

### 13.6 Empty fan-out

`test_zero_tools_fanout_does_not_create_state`:

1. Pathological: LLM returns `DeferredToolRequests(calls=[])`.
2. Assert: no `FanOutState` record is written; no aggregator subscriber is triggered; the agent continues normally (likely with a final answer).

Implementation note: the dispatch code path guards against this — `list[Call]` with zero elements is not the parallel path; the single-tool fast path in `BaseAgentNodeDef.run` requires `pending_tool_calls` non-empty. If empty, the agent should fall through to final answer.

### 13.7 Single-tool fan-out

`test_single_tool_uses_sequential_path`:

1. LLM returns exactly one tool call.
2. Assert: no `FanOutState` record written; the single-Call path (existing) is used.

The existing code in `BaseAgentNodeDef.run` short-circuits for `len(pending_tool_calls) == 1` to the single-tool path. We preserve this. The aggregator subscriber still exists but is unused for this turn.

### 13.8 Tool error in batch

`test_tool_error_does_not_block_completion`:

1. Dispatch fan-out with three tools; one tool raises (returns `RetryPromptPart` or `ModelRetry`).
2. Assert: aggregator collects the error result; batch completes when all three returns are in (errors are still returns); agent receives the merged state including the error part.

### 13.9 Merge error policies

Three sub-tests, one per `MergeErrorPolicy`:

- `test_merge_error_abort`: default `merge_error_policy=ABORT`; user `merge()` raises; assert `AggregatorMergeError` (with `correlation_id` / `fan_out_id` attributes) propagates; assert FastStream message is nacked.
- `test_merge_error_retry`: `merge_error_policy=RETRY`; `merge()` raises once then succeeds; assert batch completes after exactly one retry. Retry count is fixed at one — no `merge_retry_count` knob.
- `test_merge_error_fallback_to_default`: `merge_error_policy=FALLBACK_TO_DEFAULT`; `merge()` raises; assert the default merge runs as fallback; assert published envelope carries `HDR_DEGRADED_MERGE=1`; assert tombstone written; assert agent resumes with default-merged state.
- `test_fallback_to_default_when_default_also_raises_falls_through_to_abort`: `merge_error_policy=FALLBACK_TO_DEFAULT`; user merge raises; framework default merge ALSO raises; assert `AggregatorMergeError` propagates (treated as ABORT) rather than the inner exception leaking out.

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

- `inmemory_aggregator_factory(persist_to_disk: bool = True) -> InMemoryAggregator`
- `deploy_agent_with_custom_aggregator(aggregator_cls: type[FanOutAggregator])` — variant of existing `deploy_agent`
- `assert_fanout_state_record(broker, agent_id, correlation_id, fan_out_id, expected: FanOutState | None)` — helper that reads the state topic and asserts (None = tombstone)

### 13.17 InMemoryAggregator harness fidelity

``InMemoryAggregator`` is intentionally a tight mirror of the
production path. Two behaviours that matter when comparing test results
to production:

- ``_batch_view`` deep-copies ``batch.base_state`` so a user
  ``merge`` that mutates ``batch.base_state`` in place cannot corrupt
  the harness's cached ``_InFlightBatch`` — same contract as the
  production ``FanOutAggregator._batch_view``.
- ``_run_merge`` honours ``MergeErrorPolicy`` exactly like the
  production ``_handle_merge_error``: ``ABORT`` and post-RETRY failures
  raise ``AggregatorMergeError`` (with ``correlation_id`` /
  ``fan_out_id`` / ``state_topic`` attrs);
  ``FALLBACK_TO_DEFAULT`` wraps the default-merge fallback in
  try/except and treats a double-failure as ``ABORT`` rather than
  letting the inner exception leak out and bypass the configured
  policy.

These were a source of test-vs-prod drift in earlier iterations and
are now load-bearing harness invariants. Tests that rely on the merge
error policy should treat ``InMemoryAggregator`` and the real
``FanOutAggregator`` as interchangeable on this dimension.

---

## 14. Startup, deployment, topic provisioning

### 14.1 Worker startup sequence (recap)

```
1. Worker.__init__: nodes registered, aggregators auto-attached
2. Worker.run:
   a. Worker._prepare_aggregators:
      - await broker.connect()
      - for each BaseAgentNodeDef in self._nodes:
          await node.aggregator.setup(
              broker, node_id, main_topic, kafka_config=client.kafka_config)
        which internally:
          - ensure_aggregator_topics(...) via broker.config.admin_client
          - constructs _KafkaStateStore with kafka_config kwargs
          - constructs _StateStoreRebalanceListener
   b. Worker.register_handlers:
      for each node, for each KafkaSubscription in node.kafka_subscriptions():
        broker.subscriber(*topics, listener=sub.listener, max_workers=...)(handler)
   c. FastStream(broker).run() — begin consuming.
      On partition assignment, the rebalance listener drives
      state_store.rehydrate_partitions BEFORE the returns subscriber
      processes any messages for those partitions.
```

### 14.2 Topic auto-creation vs explicit creation

Three modes:

- **Cluster allows topic auto-creation** (`auto.create.topics.enable=true`): the first publish to `fanout-state` would auto-create it — but with the *default* `cleanup.policy=delete`, which is WRONG. The shipped `_topic_admin.ensure_aggregator_topics` uses the Kafka Admin API to (a) check the topic exists, (b) create with `STATE_TOPIC_CONFIG` if missing, (c) validate partition count if it exists. It does NOT validate `cleanup.policy` on an existing state topic — that's a known gap; an operator with a misconfigured pre-existing topic will silently get incorrect compaction behaviour.
- **Cluster forbids topic auto-creation**: `ensure_aggregator_topics` creates explicitly via Admin API. If the broker user lacks permission, `AggregatorStateStoreError` is raised and the worker fails to start (it's a configuration error).
- **Managed Kafka (e.g. Confluent Cloud, AWS MSK Serverless)**: the user must pre-create topics. No CLI ships in v1.

### 14.3 CLI helper (not shipped)

Earlier drafts proposed a `uv run calfkit topics create-aggregator <node_id>` CLI. Not shipped in v1 — start-time auto-create via the Admin API in `ensure_aggregator_topics` is the primary path. Defer the CLI to v1.1 if user demand surfaces.

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

### 15.6 Rollout

The implementation shipped in one PR on `feat/durable-fanout-aggregator`.
The earlier multi-PR breakdown in this section (introduce `CalfkitError`,
then `HDR_FANOUT_ID` / `HDR_FRAME_ID`, then `kafka_subscriptions`, then
the aggregator subpackage, then `sequential_only_mode` deprecation) is
left behind as a historical note on review staging — useful for
reading the branch's commit log, not for understanding the shipped
behaviour.

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

The DX design proposes ``simulate_restart()`` on ``InMemoryAggregator``
to simulate a worker restart. The shipped harness
(``calfkit/nodes/aggregator/testing.py::InMemoryAggregator``) defaults
``persist_to_disk=True`` so a new instance can rehydrate from a JSON
file on disk; ``persist_to_disk=False`` is available for
fault-injection ("what does my agent do if durability fails?") but is
not the realistic default.

Earlier drafts had the default the other way around — ``False`` —
modelling "we have NO durable store." That is not what users encounter
in production (the real aggregator uses a compacted Kafka topic, not
disk); ``persist_to_disk=True`` matches the durable-by-default model
of the production path.

**``RestartSimulatedError``.** When ``simulate_restart()`` fires, any
in-flight ``await`` returned by the harness's completion or
partial-state waiters (``wait_for_completion(key)``,
``wait_for_partial_state(key, ...)``) is failed with
``RestartSimulatedError`` rather than left hanging or silently
cancelled. Tests that expect ``simulate_restart`` mid-await should
catch the exception explicitly so a hang during refactor is impossible
to confuse with a successful await. The exception lives in
``testing.py`` (NOT ``errors.py``) so production code catching
``AggregatorError`` cannot accidentally swallow a test-only signal.

### 16.6 What survives unchanged

Everything else in the DX design. The three override methods, the module structure, the error hierarchy, the `aggregator=` constructor arg, the testing API shape, the comparison-to-other-SDKs framing, the topic naming convention — all hold under the Kafka mechanics.

---

## 17. Open questions

Honest list of things that need prototyping, validation, or further design before shipping.

### 17.1 Per-partition `fanout-state` throughput

Per-batch: 1 write-on-dispatch + N writes-on-return + 1 tombstone, i.e.
~(N+2) writes per batch. At 100 fan-outs/sec with 5 tool calls each:
~700 writes/sec on the state topic, divided across `partition_count`
partitions. Far below per-partition Kafka throughput (~10k+ msg/sec on
commodity hardware). Plausibly fine; needs load-test.

**Action item:** include a benchmark in `tests/perf/` once load-testing
infrastructure is in place.

### 17.2 Exactly-once via Kafka transactions

The §9 analysis shows a small window of "stale state lingers in-cache for a completed batch" or "lost completion if tombstone+publish ordering reverses." Kafka transactions (`transactional.id`) would close this gap atomically.

Cost: meaningful complexity (transactional producer, isolation level on consumers, write order constraints). Benefit: eliminates the stale-state observability artifact.

**v1 recommendation:** skip transactions. Live with the small operational artifact. Document loudly. Revisit in v2 if real users hit it.

### 17.3 DLQ for late arrivals

v1 logs + drops. A configurable DLQ topic (`{node_id}.fanout-dlq`) would let operators inspect late returns and trace why a tool was slow. Adds one config knob and one topic per agent.

**v1 recommendation:** skip. Reconsider if observability of late arrivals via logs alone proves insufficient.

### 17.4 Heterogeneous tool result types

`State.tool_results` is typed `dict[str, ToolCallResult | Any]`. The `Any` is wire-shape leakage. The aggregator's `merge` receives the same `Any`. If a user wants strongly-typed batch results, they have no path.

**Action:** out of scope for v1 (the existing weakness is not made worse). Long-term, tighten `tool_results` to a tagged union, propagate the tightening to `FanOutAggregator[StateT, ResultT]`.

### 17.5 Multi-partition `fanout-state` (shipped — resolved)

This was an open question in earlier drafts; shipped in v1. The state
topic shares the agent main topic's partition count;
`FanOutAggregatorPartitioner` extracts `correlation_id` from composite
keys to preserve co-partitioning. See §6.5, §7.1, §8.5.

### 17.6 RocksDB-backed cache

If in-memory cache becomes a memory bound problem (millions of in-flight batches, very large message_history), back the cache with RocksDB (Faust / Kafka Streams pattern). Adds a runtime dep on a Python rocksdb binding. Significant complexity. v2+ work.

### 17.7 Multi-partition aggregator topic vs single-aggregator-instance ownership (resolved)

Resolved in favour of consumer-group ownership: each worker owns a slice
of correlations via the standard partition-assignment model. No leader
election. Co-partitioning gives us the locality we need; see §7.1.

### 17.8 What does `fan_out_id` look like when the same agent inbound is processed twice in parallel?

Pathological: same `correlation_id`, same `frame_id` (since redelivery), two workers both decide to dispatch. The idempotent-dispatch check in §7.5 catches the second one — BUT the check is "state record exists." If both workers race on the write-through, both write `FanOutState` records. Since the key is `(correlation_id, fan_out_id)` and `fan_out_id` is deterministic from `frame_id`, both write the SAME key. Last-write-wins. Both then proceed to publish their own set of N tool Calls — duplicate dispatches.

**Resolution:** the second worker's publish is deduped by *the tool's idempotency on `(correlation_id, tool_call_id)`* — which is the existing calfkit contract. So tools run once. The aggregator dedups returns. Correctness is preserved.

The risk: tool publishes are not transactional with the state write; we cannot prevent duplicate dispatches in a true split-brain scenario. We rely on tool-side idempotency.

**Action:** document this contract clearly in the migration note. "Tools must be idempotent on `(correlation_id, tool_call_id)`" — this is already an implicit contract; we are making it explicit.

### 17.9 Should aggregator topics always be single-partition for simplicity? (resolved)

Resolved against single-partition: the shipped design is multi-partition
with `FanOutAggregatorPartitioner` keeping co-partitioning intact. See
§7.1 for the linearisation contract.

### 17.10 What if the agent's main topic has a different partitioner from `fanout-returns`? (resolved)

The shipped `FanOutAggregatorPartitioner` is installed broker-wide by
`Client.connect` (`calfkit/client/base.py::BaseClient.connect`) and
hashes plain keys with the same murmur2 as Kafka's default partitioner
— so co-location holds as long as users do not pass their own
`partitioner=` to the `Client.connect` kwargs (which the framework
explicitly rejects with a `ValueError`). A partition-count mismatch
between main and aggregator topics is caught by
`ensure_aggregator_topics`, which raises `AggregatorStateStoreError`
on validation failure.

### 17.11 Idle-timeout reaping (Future work — deferred)

Earlier drafts of this document proposed an `idle_timeout_seconds` knob
on `FanOutAggregator` plus a `FanOutTimeoutError` raised when a batch's
`last_updated_ms` ages past the threshold. Both were removed from v1:
the reaping mechanism needs a partition-scoped background task whose
lifecycle is intertwined with consumer-group rebalances, plus
deterministic test plumbing — non-trivial enough that shipping the
parameter without a working reaper would have been ship-dead-code.
`last_updated_ms` is still recorded on `FanOutState` for observability.

A proper implementation (partition-scoped periodic sweep, async task
lifecycle, test plumbing) is deferred to a follow-up; if and when it
is prioritised, a design doc will be added under `docs/` and linked
from `ROADMAP.md`.

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

### 18.3 Reference to shipped code

Key sites in `calfkit/`:

- `nodes/aggregator/aggregator.py::FanOutAggregator` — public class (overrides + `merge_error_policy` + `setup`).
- `nodes/aggregator/aggregator.py::MergeErrorPolicy` — `ABORT` / `RETRY` / `FALLBACK_TO_DEFAULT`.
- `nodes/aggregator/_runtime.py::_FanOutRuntime` — framework-managed runtime state behind `FanOutAggregator._runtime`.
- `nodes/aggregator/state.py::FanOutState` — wire format on `{node_id}.fanout-state`.
- `nodes/aggregator/state.py::AggregatorBatch` — immutable view passed to overrides.
- `nodes/aggregator/state.py::AggregatedReturn` — return shape of `merge` (with `degraded: bool` for FALLBACK_TO_DEFAULT-fallback signalling).
- `nodes/aggregator/_kafka_state_store.py::_KafkaStateStore` — durable store + partition-scoped cache + rehydration.
- `nodes/aggregator/_in_memory_store.py::_InFlightBatch` — frozen dataclass; ``received`` exposed as a ``Mapping`` (``MappingProxyType``); ``with_received`` returns a fresh instance with the new ``received`` / ``last_updated_ms``.
- `nodes/aggregator/_topic_admin.py::ensure_aggregator_topics` and `STATE_TOPIC_CONFIG` — topic provisioning and the 4-key compacted-topic config.
- `nodes/aggregator/_partitioner.py::FanOutAggregatorPartitioner` and `build_composite_key` / `parse_composite_key` / `has_composite_delimiter`.
- `nodes/aggregator/_rebalance.py::_StateStoreRebalanceListener` — rebalance hooks driving `rehydrate_partitions` / `evict_partitions`.
- `nodes/aggregator/errors.py::AggregatorMergeError` (`correlation_id` / `fan_out_id` attrs), `AggregatorStateStoreError` (`state_topic` attr).
- `nodes/agent.py::BaseAgentNodeDef.kafka_subscriptions` — registers the aggregator returns subscriber with `max_workers=1`.
- `nodes/agent.py::BaseAgentNodeDef._publish_parallel_with_aggregator` — the dispatch path with idempotent-dispatch branching (§7.5).
- `nodes/agent.py::BaseAgentNodeDef._aggregator_handler` — the returns handler.
- ``nodes/agent.py::BaseAgentNodeDef._ensure_aggregator_ready`` — strict validation that ``setup()`` has run; raises ``AggregatorStateStoreError`` pointing at ``calfkit.nodes.aggregator.testing.setup_for_tests`` when the runtime is missing. No lazy synthesis.
- `worker/worker.py::Worker._prepare_aggregators` and `Worker.run` — startup wiring.
- `client/base.py::BaseClient.connect` — installs `FanOutAggregatorPartitioner` broker-wide and captures `KafkaConfig`.
- `client/kafka_config.py::KafkaConfig` — bootstrap servers + client kwargs snapshot.
- ``_protocol.py::HDR_FANOUT_ID``, ``HDR_FRAME_ID``, ``HDR_DEGRADED_MERGE`` — added headers.
- ``nodes/aggregator/testing.py::InMemoryAggregator`` — in-memory harness; mirrors production ``_batch_view`` (deep-copy ``base_state``) and ``_run_merge`` (try/except around the ``FALLBACK_TO_DEFAULT`` default-merge fallback). ``persist_to_disk=True`` by default.
- ``nodes/aggregator/testing.py::RestartSimulatedError`` — raised by harness ``wait_for_completion`` / ``wait_for_partial_state`` waiters when ``simulate_restart()`` fires mid-await. Defined in ``testing.py`` (not ``errors.py``) so production code catching ``AggregatorError`` cannot accidentally swallow it.

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
