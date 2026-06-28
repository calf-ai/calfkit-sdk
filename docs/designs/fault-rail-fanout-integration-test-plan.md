# Fault Rail, Policy Seams & In-Node Fan-out — Real-Broker Integration Test Plan

**Status:** Plan · **Date:** 2026-06-18
**Targets:** the merged implementation of PR #247 (commit `04203fc`; HEAD `613ea02`).
**Companion:** `docs/designs/fault-rail-fanout-behavior-catalogue.md` (behavior IDs `FR-*`, `SE-*`, `FO-*`, `XC-*` referenced throughout). Source specs: `fault-rail-and-policy-seams-spec.md` (scenarios `S*`), `in-node-fanout-aggregation-spec.md`.

---

## 1. Goals, lane, and non-duplication

**Goal.** Prove the fault rail, the four policy seams, and the in-node durable fan-out fold behave correctly **over a real broker** — the lane that exercises real partitions, real consumer-group joins, real header filtering, real `KtablesFanoutBatchStore` materialization with `barrier()`, and real producer `acks=all`/idempotence. Today **every** seam, fault-escalation, auto-fault, decode-floor, oversized-fault, OPEN-abort and `calf.retry`-into-the-loop behavior is **offline-only**; this plan fills that gap.

**Lane.** All tests carry `pytestmark = pytest.mark.kafka`, live under `tests/integration/`, run via `make test-kafka` / `uv run --group integration pytest -m kafka`, and use the session-scoped `kafka_bootstrap` (testcontainers Redpanda or `CALF_TEST_KAFKA_BOOTSTRAP`) + function-scoped `topic_namespace` fixtures from `tests/integration/conftest.py`. Every worker passes `extra_subscribe_kwargs={"auto_offset_reset": "earliest"}` (load-bearing — a group join otherwise races and drops the publish addressing it).

**Do NOT duplicate** (already real-broker covered — `test_durable_fanout_agent_kafka.py`, `test_fanout_store_kafka.py`, `test_tool_node_roundtrip_kafka.py`, `test_mcp_roundtrip_kafka.py`): the durable fan-out happy arc + real store opening; graceful-rebalance continuation; store RYOW barrier / tombstone visibility / failed-outcome typed-fault round-trip; and all tool/MCP **happy-path** single-dispatch / fan-out / dup-call / sequential / multi-worker / multi-turn / arg-validation-to-model / include-pinning / `list_changed`.

**Do NOT test vapor** (deferred — see catalogue Appendix): `surface_to_model`, `self_retry_budget`/`seam_budgets`/`calf.agent.self_retry_exhausted`, provider `calf.model.context_window_exceeded`, client-side `NodeFaultError`, `ConsumerContext.fault`, MCP-`isError`-marking. These get **skipped/xfail stubs** (§7) that flip on when their PR lands.

---

## 2. Observability — three channels (standardize on these)

No typed fault reaches the client today (FR-29/FR-30). The plan uses three deterministic channels; **most fault/seam tests use Channel A + B together** (B asserts the in-process decision, A asserts the wire/headers).

### Channel A — the broadcast-mirror fault tap (NEW shared helper)

The faulting node's fault is mirrored on its `publish_topic` as an `Envelope` whose `reply` is a `FaultMessage`, with headers `x-calf-kind=fault` + `x-calf-error-type`. We add **one** reusable tap (no such helper exists in `tests/` today). Sketch — `tests/integration/_fault_tap.py`:

```python
# tests/integration/_fault_tap.py
from __future__ import annotations
import asyncio, uuid
from contextlib import asynccontextmanager
from aiokafka import AIOKafkaConsumer
from calfkit.models.envelope import Envelope
from calfkit.models.reply import FaultMessage, ReturnMessage
from calfkit._protocol import HDR_KIND, HDR_ERROR_TYPE, decode_header_str

class FaultTap:
    """Raw consumer on a topic, decoding calfkit Envelopes and exposing their replies.

    Subscribe BEFORE the publish under test; pass auto_offset_reset='earliest'."""
    def __init__(self, consumer: AIOKafkaConsumer):
        self._c = consumer

    async def next_envelope(self, *, timeout: float = 30.0) -> tuple[Envelope, dict[str, str | None]]:
        record = await asyncio.wait_for(self._c.getone(), timeout=timeout)
        env = Envelope.model_validate_json(record.value)
        headers = {k: decode_header_str(v) for k, v in record.headers}
        return env, headers

    async def next_fault(self, *, timeout: float = 30.0) -> tuple[FaultMessage, dict[str, str | None]]:
        """Drain until a FaultMessage envelope appears (skips return/call mirrors)."""
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            env, headers = await self.next_envelope(timeout=max(0.1, deadline - asyncio.get_event_loop().time()))
            if isinstance(env.reply, FaultMessage):
                return env.reply, headers
        raise AssertionError("no FaultMessage observed on the tapped topic")

@asynccontextmanager
async def fault_tap(bootstrap: str, topic: str):
    consumer = AIOKafkaConsumer(
        topic, bootstrap_servers=bootstrap,
        auto_offset_reset="earliest", group_id=f"fault-tap-{uuid.uuid4().hex[:8]}",
    )
    await consumer.start()
    try:
        yield FaultTap(consumer)
    finally:
        await consumer.stop()
```

Usage: give the node-under-test a `publish_topic`, start the tap on it **before** invoking, then assert:

```python
async with fault_tap(kafka_bootstrap, agent_pub) as tap:
    handle = await driver.start("go", agent_in)              # don't block on the (hanging) future
    fault, headers = await tap.next_fault(timeout=30)
    assert headers[HDR_KIND] == "fault"
    assert headers[HDR_ERROR_TYPE] == "calf.exception"
    assert fault.error.find("calf.exception") is not None
```

> Why a raw consumer and not a `@consumer` node: `ConsumerContext` has no `.fault` today (SE-27), so a sink node sees `output=None`. The raw tap reads `envelope.reply.error` directly — the only typed channel.

### Channel B — in-callback seam assertions (in-process)

Seams are developer-supplied callables that run inside the worker and receive the live `SeamContext`/`ErrorReport`. Register a seam that **records** what it saw (and optionally asserts), then assert in the test body after the roundtrip. This is the cleanest channel for seam-firing/ordering/context-content checks and **sidesteps the deferred client edge entirely**.

```python
# A recorder seam — capture-in-callback, assert-in-test-body (robust default).
class SeamProbe:
    def __init__(self): self.calls: list[dict] = []
    def on_callee_error(self, ctx, fault):
        self.calls.append({"kind": ctx.delivery_kind, "type": fault.error_type,
                           "tag": ctx.failing_call.tag if ctx.failing_call else None,
                           "depth": len(fault.frame_chain)})
        return None                                    # decline → escalate (or return a value to handle)

probe = SeamProbe()
agent = Agent(..., on_callee_error=probe.on_callee_error)
...
async with worker:
    handle = await driver.start("go", agent_in)
    await asyncio.wait_for(_settle(...), timeout=30)   # wait for the probe to fire / fault to mirror
assert probe.calls == [{"kind": "fault", "type": "calf.exception", "tag": "c1", "depth": 1}]
```

**Caveat (document in the helper):** a *raised* `AssertionError` inside a seam is a node-own accident — for `before_node`/`after_node` it routes to `on_node_error` → `calf.exception` (visible as a fault, not a test failure); for `on_callee_error` it is slot-scoped (resolves the slot failed). So **default to capture-then-assert.** Direct in-callback `assert`/`raise NodeFaultError(...)` is valid only for "must-fire / must-see-X" checks where you *want* the failure to surface as an observable fault (then assert that distinctive fault on Channel A). A handler that must *not* fire can assert by leaving its recorder empty.

### Channel C — client edge (current behavior) + caplog

- **Routed fault → `DeserializationError`:** `await handle.result(timeout=...)` raises `DeserializationError` (FaultMessage → empty parts → strict projection). Proves "didn't hang, didn't succeed" but carries no typed report — pair with Channel A for the `error_type`.
- **Hang-prone cases** (fire-and-forget fault, floored fault, undecodable reply): connect the driver with a **finite `reply_ttl`** and assert `ReplyExpiredError`, or wrap in `asyncio.wait_for`.
- **caplog (secondary):** floor/synthesis events log at ERROR (`calfkit.client.middleware`, `base.py` synthesis), escalation hops at WARNING. Over a real broker these are timing-sensitive (logged in the worker consume coroutine) — use only as a corroborating assertion, never the sole one.

---

## 3. Test fixtures & doubles to add

### 3.1 Example fault-emitting tools (`tests/integration/_fault_tools.py`)

```python
from calfkit.nodes import agent_tool
from calfkit.exceptions import NodeFaultError
from calfkit._vendor.pydantic_ai import ModelRetry   # exact import per tool.py

@agent_tool
def boom(x: int) -> int:                  # FR-2: generic raise → calf.exception
    raise ValueError("kaboom")

@agent_tool
def quota(x: int) -> int:                 # FR-3: deliberate verbatim fault (mint rule)
    raise NodeFaultError("billing.quota_exceeded", message="over quota",
                         retryable=False, details={"x": x})

@agent_tool
def needs_retry(x: int) -> int:           # XC-4: ModelRetry → calf.retry-marked return
    raise ModelRetry("provide a positive x")

@agent_tool
def huge(n: int) -> str:                  # FR-21: oversized result to force strip-and-retry
    return "z" * n                        # n sized > broker message.max.bytes at the fault hop

@agent_tool
def ok_a() -> str: return "a_result"      # happy siblings for fan-out mixes
@agent_tool
def ok_b() -> str: return "b_result"
```

A **gated** tool variant (reuse the durable-fanout suite's `_GATE` idiom) for deterministic ordering in rebalance / interleave stress tests.

### 3.2 FunctionModel scripts (extend `tests/integration/_roundtrip_helpers.py`)

Reuse `scripted_model([...])` (turn-1 calls, turn-2 finalize), `reactive_model(decide)`, `tool_returns`, `returns_by_call_id`, `retry_prompt_texts`. Add only if needed:
- a **fan-out script** = `scripted_model([ToolCallPart("boom",{"x":1},tool_call_id="c1"), ToolCallPart("ok_a",{},tool_call_id="c2")])` (≥2 calls → durable batch).
- a **retry-then-recover** reactive model: turn 1 calls `needs_retry`; on seeing the retry text, turn 2 calls `ok_a`; then finalize — proves `calf.retry` re-enters the loop (XC-4) over the wire.

### 3.3 Worker helper

Reuse the suites' `_worker(bootstrap, *, nodes)` (own `Client.connect`, `extra_subscribe_kwargs=_EARLIEST`). Each node-under-test gets an explicit `publish_topic` so Channel A can tap it. Tear down with `await driver.close()` + `await worker._client.close()`.

---

## 4. Test suites

Each row: **ID** · behavior(s) covered · setup · action · assertion / channel · maps-to scenario. `A`=mirror tap, `B`=in-callback, `C`=client-edge.

### Suite F — Fault escalation over the wire

| ID | Covers | Setup | Action → Assert |
|---|---|---|---|
| F-1 | FR-2, FR-11, FR-18 | agent(`tools=[boom]`, `model=scripted([boom c1])`, `publish_topic=pub`), no seams | `driver.start`; **A**: `next_fault` → `error_type=="calf.exception"`, `exception.type=="ValueError"`, headers `kind=fault`+`error-type`. **C**: `handle.result()` raises `NodeFaultError` (`e.report.exception.type=="ValueError"`). |
| F-2 | FR-3, FR-4 | agent(`tools=[quota]`), no seams | **A**: fault `error_type=="billing.quota_exceeded"`, `retryable is False`, `details["x"]==1`. Verbatim, not `calf.exception`. `S32`. |
| F-3 | FR-7, FR-8, FR-9 | deep chain A→B→C as 3 agents over 3 topics; C's tool `boom`; B has no `on_callee_error` (declines) | invoke A; **A** taps A's `publish_topic` → fault `error_type` identical to C's origin, `frame_chain` length ≥ 2, same `report_id` as the C-hop mirror. **B**: a recorder `on_callee_error` on A fires once with that fault. `S3`, `S24`. |
| F-4 | FR-12, FR-13 | fire-and-forget: `driver.send("go", agent_in)` (no reply_to) to an agent whose tool `boom`s; agent `publish_topic=pub` | **A**: fault mirrored on `pub`. **C**: no future exists (send returns a correlation id only); assert nothing hangs. `S10`, `S14`. |
| F-5 | FR-25, FR-16 (stray) | publish a hand-built `kind=fault`/`reply=None` envelope (kind/slot disagreement) onto a live agent's return topic | **B/caplog**: WARNING stray, the open invocation untouched; the agent's reply future still resolves normally for a concurrent valid call. `S29`. |

### Suite S — Policy-seam pipeline over the wire

| ID | Covers | Setup | Action → Assert |
|---|---|---|---|
| S-1 | SE-3, B1 `before_node` | agent with `before_node` returning a substitute string on ingress | invoke; **C**: `handle.result().output` is the substitute; **B**: a body-probe tool never ran. `S` (uniform contract). |
| S-2 | SE-21 input transform | agent `before_node` mutates `ctx.state` (rewrite prompt) + returns `None` | **B**: body sees the transformed input (recorder tool captures it). `S18`. |
| S-3 | B1 `after_node` replace | agent `after_node` returns `"redacted"` | **C**: output is `"redacted"`. |
| S-4 | SE-8, SE-11 | agent `before_node` raises `ValueError`; `on_node_error` returns a recovery value | **A**: no fault; **C**: output is the recovery value (flowed through `after_node`). `S8`. |
| S-5 | SE-10 decline-escalate | tool `boom`; agent `on_node_error` *handler* raises a non-NodeFaultError (declines) | **A**: original `calf.exception` escalates; `details["calf.seam_errors"]` notes the handler failure. `S9`. |
| S-6 | SE-7 mint bypass | tool `boom`; agent `on_node_error=[recover]` **and** the body raises `NodeFaultError("x.y")` | **A**: `x.y` escalates verbatim; the recovery seam never fired (recorder empty). `S32`. |
| S-7 | SE-9 slot-scoped raise | fan-out of 2 (`boom`,`ok_a`); agent `on_callee_error` raises a `NodeFaultError("seam.reject")` on the `boom` slot | **A**: at closure the group/flattened fault carries `seam.reject` chained to the inbound fault; `ok_a` recorded ok; no double reply. `S31`. |
| S-8 | SE-13/14/15/16 guards | agent `before_node` returns `True` / its `StateT` / `b"x"` / (separately) `after_node` returns a `Call` | each: **A**: `SeamContractError`-origin fault (`calf.exception`) escalates; body not silently skipped. `S41`, `S16`. |
| S-9 | SE-18, SE-19 | register a seam on a `@consumer`; register a wrong-arity seam | worker **startup raises** `RegistryConfigError` (assert at `async with worker:` entry). `S25`. |
| S-10 | SE-24, SE-22, SE-23 | recorder seams on all four positions | drive ingress + a callee fault; **B**: assert `delivery_kind`/`route`/`awaiting_reply`/`failing_call`/`exception` are populated exactly per position and cleared elsewhere. |

### Suite R — Auto-fault, decline, fire-and-forget

| ID | Covers | Setup | Action → Assert |
|---|---|---|---|
| R-1 | FR (auto-fault), §10 | a routed (`execute`) delivery whose only handler **declines** (all-declined) on a reply-owing node | **A**: `calf.delivery.rejected`, `details["reason"]=="all_declined"`. **C**: `DeserializationError` (didn't hang). `S15`. |
| R-2 | schema_rejected | a routed delivery whose matching handler rejects the body schema | **A**: `calf.delivery.rejected`, `reason=="schema_rejected"`. `S39`. |
| R-3 | fire-and-forget no-op | `send(...)` to a node whose handler declines (no reply owed) | **B/caplog**: DEBUG no-op, no fault, no publish on a callback rail. `S15`, `S28`. |

### Suite D — Decode floor

| ID | Covers | Setup | Action → Assert |
|---|---|---|---|
| D-1 | FR-26 | a raw `AIOKafkaConsumer`/producer publishes a **malformed body** (invalid JSON / wrong shape) to a live node's input topic | **caplog (ERROR, `calfkit...middleware`/node decode)**: `calf.delivery.undecodable` floored; the worker keeps consuming (a subsequent valid invocation still completes — the stronger assertion). `S30`. |
| D-2 | FR-27 (current) | malformed body on the **client reply topic**; driver with finite `reply_ttl` | **C**: the pending future does **not** resolve from the junk; it raises `ReplyExpiredError` (documents the current hang-until-TTL behavior). Flip to `NodeFaultError(calf.delivery.undecodable)` when the reception PR lands. |

### Suite O — Oversized fault strip-and-retry

| ID | Covers | Setup | Action → Assert |
|---|---|---|---|
| O-1 | FR-21 | tool raises a `NodeFaultError` whose `details` (or a fault group whose `causes`) serialize **> the broker's `message.max.bytes`** on the fault hop; node `publish_topic=pub` | **A**: a fault still arrives, stripped to minimal (`to_minimal()` — only `report_id`/`error_type`/`message`/`retryable`/origin ids; `causes`/`details`/`frame_chain` empty). Confirms the strip-and-retry-once path against a *real* `MessageSizeTooLargeError`, not the offline `_FailThenCaptureBroker`. |

> Sizing note: set a small broker/topic `max.message.bytes` on the tap/fault topic (or build a genuinely large `details` via `huge`) so the first publish trips the size limit but the minimal report fits. Document the chosen limit in the test.

### Suite X — Fan-out fault paths over the wire

| ID | Covers | Setup | Action → Assert |
|---|---|---|---|
| X-1 | FO-12, FO-14 (flatten) | agent fan-out of 3 (`boom`,`ok_a`,`ok_b`), no `on_callee_error` | real `KtablesFanoutBatchStore`; **A**: at closure a **flattened** fault (the bare `boom` `calf.exception`) with `details["calf.fanout_topology"].ok==2`, the two successes as `status=="ok"` slots; body never ran (recorder). `S4`. |
| X-2 | FO-12, FO-14 (group) | fan-out of 3, **two** tools `boom` | **A**: `calf.fault_group` with 2 `causes`; `report.find("calf.exception")` matches at depth; `failed==2`. `S5`, `S40`. |
| X-3 | FO-6, B1 `on_callee_error` resolve | fan-out of 3 (`boom`,`ok_a`,`ok_b`); agent `on_callee_error` returns a substitute string for `boom` | **C**: batch **completes** (`handle.result().output` is the finalized text); **B**: the model's turn-2 saw 3 results (one substituted). `S6`. |
| X-4 | FO-3, FO-15 dispatch-abort | fan-out where a sibling **publish fails** (point a sibling at an invalid topic / inject a broker error via a wrapper) **or** the store is made unavailable at OPEN | **A**: `calf.fanout.aborted`, `details["reason"]=="dispatch_failed"` (or `store_unavailable`), exactly one fault to the caller. |
| X-5 | FO-16 mid-fold abort | fan-out open on real store; force a node-own raise mid-fold (a seam recorder that raises on the 2nd sibling) | **A**: `calf.fanout.aborted` once; both tables tombstoned (assert via a follow-up `KafkaTable` read that `state[X]` is gone); late siblings stray-floor. |
| X-6 | FO-20, FO-21 | a sequential agent runs single calls before & after a fan-out turn; verify the post-fan-out single `Call` is unmarked and completes (not orphan-floored) | **C**: full completion; **B**: no stray/orphan floor logged. `S47`. |
| X-7 | FO-22 re-fan-out | model fans out, closes, then the resumed body fans out again on the same frame | real store; **C**: both batches complete; assert the second OPEN read fresh registration (no stale-tombstone hang). |

### Suite M — `calf.retry` marker round-trip into a real agent loop

| ID | Covers | Setup | Action → Assert |
|---|---|---|---|
| M-1 | XC-4 | `tools=[needs_retry, ok_a]`; **retry-then-recover** reactive model (§3.2) | **C**: completes; `retry_prompt_texts(history)` contains the rendered retry; the model's turn-2 `ok_a` result is present. Proves the `calf.retry` TextPart materialized as a `RetryPromptPart` and re-entered the loop over real Kafka. `S20`, `S36`. |
| M-2 | XC-5 | MCP toolbox tool returning `isError=True` (extend `_mcp_roundtrip_server.py`) | **C**: the `isError` result arrives as an **ordinary** model-visible return (transparent, **not** a fault, **not** `calf.retry`-marked) — assert `tool_returns(history)` carries it and no fault mirrors. Locks the B2 passthrough. |

### Suite Z — Stress / soak

| ID | Covers | Setup | Action → Assert |
|---|---|---|---|
| Z-1 | FO-1, FO-7, fold throughput | fan-out of **large N** (e.g. 16–32 tools, mix of `ok_*`); real store | completes once; **A**: no fault; assert the model's final turn saw exactly N results; assert exactly one re-entry (spy/store read shows one tombstone). |
| Z-2 | FO-7 idempotency under dup | fan-out of N; replay a sibling reply (duplicate `in_reply_to`) via a raw producer | batch still closes once; the duplicate is a stray (no double-resolve, no early closure). `S33`. |
| Z-3 | concurrency / serialization | launch **M concurrent invocations** (distinct correlation ids) at one agent, each fanning out; `max_workers=1` | all complete; no cross-batch bleed (each result set matches its own correlation); proves the serial fold under load. XC-1. |
| Z-4 | mixed success/fault batches | K concurrent fan-outs, half with a `boom` sibling | the faulting halves each escalate exactly one group/flattened fault (Channel A count == failing-batch count); the healthy halves complete. No strands, no doubles. |
| Z-5 | rapid sequential re-fan-out | one agent doing B back-to-back fan-out turns | each closes before the next opens; no tombstone races; B completions. FO-22. |
| Z-6 | large payload carriage | fan-out where siblings return large (but in-budget) results, plus one `huge`-driven oversized fault | healthy results carry; the oversized fault strips to minimal (O-1) without dropping. |

> Stress determinism: use gated tools (`_GATE`) to control sibling arrival ordering where an assertion depends on it; otherwise rely on `_wait(predicate, timeout, what)` polling (no bare sleeps). Keep N/M/K modest (CI budget) but > the offline lane's 2–3.

---

## 5. Determinism & hygiene rules (every test)

1. **`auto_offset_reset="earliest"`** on every worker and tap consumer.
2. **Namespace** all topics/groups off `topic_namespace` (per-test isolation); the fan-out tables are `calf.fanout.{agent_id}.{state,basestate}` so the agent id must be namespaced too.
3. **Subscribe the tap before the publish** under test (Channel A). Start the worker, `await` its readiness (resource phase opens the real store), then invoke.
4. **Never block on a hang-prone future** — use `driver.start(...)` + Channel A/B, or a finite `reply_ttl`, or `asyncio.wait_for`. A test that does `await driver.execute(...)` on a path that escalates a fault will **hang** until timeout (FR-30).
5. **No bare `sleep`** — poll with `_wait(predicate, timeout, what)`; gate ordering with `_GATE`.
6. **Capture-then-assert** for seam probes (Channel B); reserve direct in-callback `assert`/`raise` for must-fire/must-see checks paired with a Channel-A fault assertion.
7. **Teardown**: `await driver.close()` and `await worker._client.close()` (and `await tap...stop()` via the context manager). Drain in-flight folds before stopping a worker mid-batch.
8. **Mark** `pytestmark = [pytest.mark.kafka]`; set `models.ALLOW_MODEL_REQUESTS = True` only if a live model is used (these tests use `FunctionModel`, so it is not needed — keep them offline-model, real-broker).

---

## 6. Coverage traceability (gap → suite)

| Gap (offline-only today) | Behavior IDs | Suite/tests | Spec scenario |
|---|---|---|---|
| Body-raise escalation to a real caller + headers | FR-2, FR-11, FR-18, FR-29 | F-1 | S1 |
| Deliberate verbatim mint over the wire | FR-3, FR-4 | F-2 | S32 |
| Deep-chain identity-preserving escalation | FR-7..FR-10 | F-3 | S3, S24 |
| Fire-and-forget terminal fault | FR-12, FR-13 | F-4 | S10, S14 |
| Stray fault/return on a live node | FR-16, FR-25 | F-5 | S17, S29 |
| Seam pipeline (all four) through a live Worker | SE-3..SE-25 | S-1..S-10 | S8, S9, S16, S18, S31, S41, S44 |
| Registration validation at startup | SE-18, SE-19 | S-9 | S25 |
| `calf.delivery.rejected` auto-fault | §10, FR | R-1, R-2 | S15, S39 |
| Decode floor `calf.delivery.undecodable` | FR-26, FR-27 | D-1, D-2 | S30 |
| Oversized-fault strip-and-retry (real size limit) | FR-21 | O-1 | S42-adj |
| Fan-out fault group / flatten over the wire | FO-12, FO-14 | X-1, X-2 | S4, S5, S40 |
| `on_callee_error` slot-resolve in a real fan-out | FO-6 | X-3, S-7 | S6, S31 |
| OPEN-dispatch-abort + mid-fold abort | FO-3, FO-15, FO-16 | X-4, X-5 | — (in-node §4.4) |
| Unmarked continuation / re-fan-out | FO-20..FO-22 | X-6, X-7 | S47 |
| `calf.retry` round-trip into the loop | XC-4 | M-1 | S20, S36 |
| MCP `isError` transparent passthrough | XC-5 | M-2 | — (B2) |
| Fan-out under load / dup / concurrency | FO-1, FO-7, XC-1 | Z-1..Z-6 | S33 |

---

## 7. Deferred-behavior stubs (skip/xfail until the PR lands)

Add these now as `@pytest.mark.skip(reason="reception PR — FR-31")` / `xfail(strict=True)` so the suite documents the target and flips on automatically:

| Stub | Asserts (when live) | Unblocked by (filed issue) |
|---|---|---|
| `test_client_raises_node_fault_error_on_fault_reply` | `await handle.result()` raises `NodeFaultError`; `e.report.find(...)` branches | **#250** fault reception (FR-31) — `S23` |
| `test_consumer_tap_sees_fault` | a `@consumer` on a mirror reads `ctx.fault.error_type`, `ctx.delivery_kind=="fault"` | **#250** fault reception (SE-27) — `S25`, `S28` |
| `test_surface_to_model_bounds_then_escalates` | prebuilt resolves slots until budget, then declines → escalates | **#251** self-healing budgets (SE-28) — `S12`, `S35` |
| `test_self_retry_budget_exhausts` | one WARNING retry, then `calf.agent.self_retry_exhausted`; `budget=0` → `reason=self_retry_disabled` | **#251** self-healing budgets (FO-25) — `S37` |
| `test_context_window_classifies` | a provider context-window error → `calf.model.context_window_exceeded` | **#193** provider rider (FR-6) — `S13` |
| `test_mcp_iserror_marks_calf_retry` | MCP `isError` materializes a `RetryPromptPart` (Anthropic `is_error` fidelity) | follow-up MCP PR / B2 deferral (XC-5) |

> Until each lands, the corresponding **current-behavior** test in §4 (D-2 hang, F-1/X-1 `DeserializationError`, M-2 transparent passthrough) is the live assertion; the stub is the forward target.

---

## 8. Suggested file layout

```
tests/integration/
  _fault_tap.py            # NEW — Channel A shared helper (§2.A)
  _fault_tools.py          # NEW — boom/quota/needs_retry/huge/ok_* + gated variants (§3.1)
  _roundtrip_helpers.py    # extend with fan-out + retry-then-recover scripts (§3.2)
  test_fault_escalation_kafka.py     # Suite F
  test_seam_pipeline_kafka.py        # Suite S
  test_auto_fault_kafka.py           # Suite R
  test_decode_floor_kafka.py         # Suite D
  test_oversized_fault_kafka.py      # Suite O
  test_fanout_faults_kafka.py        # Suite X
  test_retry_marker_kafka.py         # Suite M
  test_fault_stress_kafka.py         # Suite Z
  test_deferred_reception_kafka.py   # Suite §7 stubs
```

Each module: `pytestmark = pytest.mark.kafka`; reuse `kafka_bootstrap`/`topic_namespace`, `_worker`, `_wait`, the new `_fault_tap`/`_fault_tools`. Build incrementally, TDD-style (`/test-driven-development`), one suite at a time; run `make fix && make check` before raising the PR.
