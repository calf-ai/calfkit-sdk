# In-Node Durable Fan-out Aggregation — Design Spec (v2)

**Status:** DRAFT — rounds 1-4 review **converged (2026-06-14)**: zero CRITICAL/MAJOR across the round-4 confirming pass (grounded in source); only optional polish remained, folded here. Ready for the implementation plan.
**Companion to:** `docs/designs/fault-rail-and-policy-seams-spec.md` (the batch *semantics* this makes durable: §6.4 seam firing, §6.7 strays, §6.8 pipeline, §6.9 materialization, §7 slots/closure/budgets, §4.2 `fanout_id`, §13 producer posture).
**Revises / supersedes:** `docs/designs/durable-fanout-aggregator-spec.md` (v1 — the separate worker-level aggregator loop + `calf.fanout.events`). v1 is retained for its rejected-alternatives record.
**Supersedes in code:** in-process fan-out aggregation (`BaseAgentNodeDef._pending_batches`, `PendingToolBatch`, `agent.py:55/102-162`).
**Dependency:** **ktables `barrier()` ships in `ktables` v0.2.0** (§5.1 pins its contract; §10 makes it a conformance gate); calfkit bumps the dep.
**Companion (future):** `docs/designs/concurrency-model.md` (the `concurrency=N` knob; deferred — §12).
**Successor issue:** umbrella **#222**; children **#218** (ack/redelivery → at-least-once), **#220** (timeouts/aged reclaim), #221 (DLT).

> **Read §12 (Decision log) first if you lack the discussion context** — it records *why* the sweep was deleted, why `ACK_FIRST` is kept, why the record accumulates, etc.

---

## 1. Motivation — what v2 changes, and the machinery it trades

Fan-out batch state ("I dispatched N siblings and await N replies, then aggregate and resume") lives in an in-process dict today (`agent.py:55`), locality-fragile (a rebalance/restart loses it, `agent.py:152-162`) and broken under multi-replica.

v1 solved it with a **separate worker-level aggregator** (a `fanout_id`-keyed `calf.fanout.events` topic + a consumer loop in every worker + a re-entry the aggregator published back). **v2 removes that aggregator and the events topic** and folds in the node, in the `_aggregate` stage the fault-rail pipeline (§6.8) already defines, backed by two compacted ktables. The justification is a verified transport fact:

> **Every publish is keyed by `correlation_id`** (`nodes/base.py:270/292/318/351`). **All replicas of a node share `group_id = node.name`** (`worker/worker.py:226`). Caller-capable nodes run **`max_workers=1`** (§2; fault-rail §4.1).

So every sibling reply of one batch lands on one partition of `{node}.private.return`, owned by exactly one replica, consumed serially — already the single, serial writer v1's events topic re-derived. v2 folds where the replies land.

**The closure is a self-published re-entry.** Once batch state is durable, the close can't resume in place: the completing fold holds one outcome and must rebuild the closure context from the durable tables regardless. A fresh delivery is the coherent home for that rebuild — exactly what fault-rail §7.7 binds the durable closure to (`kind=return`, `in_reply_to = fanout_id`; scenarios 43/47). v2 keeps that *mechanism* and deletes v1's *machinery for producing it* (the events topic, the aggregator loop). (In-process close was evaluated and rejected — §8/§12.)

**Honest accounting** (this is not "delete a topic, add nothing"):
- **Deletes:** `calf.fanout.events`, the per-worker aggregator loop, the per-sibling outcome→aggregator-consume hop, *and the ownership-gain sweep + per-node rebalance listener* (§6/§12 — recovery is the durable tables).
- **Adds (framework-internal):** the **`barrier()`** ktables primitive (v0.2.0 — §5.1); a **frame-preserving self-return publish path** for the re-entry (§3); a **stray-check exemption** for the self-addressed re-entry (§4.3); two compacted ktables; a **per-node-type `max_workers` pin** in registration (§2).

**Posture:** `ACK_FIRST` (at-most-once) is kept — *not* flipped to `REJECT_ON_ERROR` (§7/§12 — that's a framework-wide change deferred to #218). **`max_workers=1`** stands (§2). **Operational failures (crashes, ungraceful kills, rebalances) are out of scope** (§7/§12).

What this closes (as v1): lost batches across *graceful* rebalance/restart, multi-replica locality, the workflow-fork class. What it does **not** change: the per-hop at-most-once windows on a *crash* (§7), accepted as operational.

---

## 2. Architecture

No new transport topics, no new consumer loop, **no rebalance listener.** The node's existing `{node}.private.return` subscriber does dispatch, folds, the closure re-entry, and abort. Durability lives in two **node-scoped, framework-owned** compacted ktables; freshness in `barrier()`.

Per fan-out-capable node, the framework wires a **batch store** (four ktables objects — two whole-topic GlobalKTable readers + two writers — keyed by `fanout_id`):

| Component | Role |
|---|---|
| `KafkaTable` over `calf.fanout.{node_id}.state` | reader: registration + accumulating outcomes, read **globally** |
| `KafkaTableWriter` over the same topic | writer: registration at open, each fold, the tombstone at close |
| `KafkaTable` over `calf.fanout.{node_id}.basestate` | reader: the open-time snapshot, read **globally** |
| `KafkaTableWriter` over the same topic | writer: snapshot at open, tombstone at close |

The closure re-entry rides the existing `{node}.private.return` topic. The batch store is opened as a **node-owned `@resource` — one per hosted fan-out-capable node, registered on the node itself, landing in the node's *own* resource bag** — whose setup awaits `caught_up` before yielding (mirroring the `_capability_view_resource` lifecycle, `worker.py:173/372-375`, which runs in `on_startup` before serving; the engine enters node-owned resources there too). **Node-ownership is the correct scoping for a per-node resource:** the agent retrieves its store by a fixed local key, with no cross-node handle visibility and no `node_id`-embedded resource names (the duplicate-name trap a shared worker bag would otherwise force). *(This does not cost the future `concurrency=N` lever — there is one node object per process, so N in-process consumers share that node's single materialization regardless; node-ownership vs the worker bag is a factoring choice, not a sharing trade. The capability view uses the worker bag correctly because it is one genuinely worker-wide table.)* The per-fold `barrier()` (§5.1) is the correctness backstop regardless.

**`max_workers=1`, pinned structurally (not policed) — a small registration change.** Caller-capable nodes must be serial-per-consumer because FastStream's only concurrent modes are both unusable for the fold: `max_workers>1`+ACK_FIRST is a no-affinity in-process coroutine pool that races the await-spanning fold, and the partition-affine alternative is single-topic-only while our nodes are multi-topic (verified `kafka/subscriber/factory.py:102-117/167-170`, `mixins.py:46-87`). Today `worker.py:246` passes one worker-global `max_workers` to every subscriber; **this PR adds a per-node branch in `register_handlers`** — pass `max_workers=1` to a caller-capable node's `subscriber(...)` call (the kwarg is already per-call), leaving the worker's configured value for observers/consumers. This is the framework choosing the correct value per node type, **not** a registration guard policing a worker-global knob (document-don't-police). Scaling is by adding **group members** (replicas now; an in-process `concurrency=N` knob — which is exactly N such per-subscriber-`max_workers=1` registrations — is a future lever, `concurrency-model.md`, §12).

```
  agent replica D (dispatch)                 agent replica O (= D unless rebalanced; sole steady-state
  ── decides to fan out ──┐                  owner of this correlation's return partition; max_workers=1)
   1. snapshot State/stack │
   2. basestate.set(X,snap)│ await acks       ┌─ marked sibling reply on {node}.private.return ─
   3. state.set(X, expected)│ before siblings │   a. barrier(state); read state[X]   (degraded → block)
   4. publish N siblings ──┴──► tools ─reply──┤   b. stray check → floor / idempotent-ignore, OR
      (marker = X)                            │   c. stage-1 on_callee_error (per sibling) → handled?
                                              │   d. fold: state.set(X, +outcome{handled}); await ack
   X = fanout_id (node's own inbound          │   e. complete? no → no-reply mirror (_CONSUMED)
       frame_id; the batch key)               │              yes → self-publish re-entry (acks=all
   tables: calf.fanout.{node_id}.             │                    shared producer; kind=return,
   {state,basestate} — compacted,             │                    in_reply_to=X, key=correlation);
   node-scoped, read whole. barrier() from    │                    await ack; _CONSUMED
   ktables v0.2.0. No events topic, no        │
   aggregator loop, no rebalance listener.    └─ closure re-entry (in_reply_to==X — stray-exempt) ─
   Recovery on graceful rebalance = the new       f. stage-0: restore ctx.state/stack/deps from basestate[X]
   owner reads the durable tables and             g. barrier(state)+read; absent → already closed → abandon
   continues folding (§6).                        h. incomplete → spurious → WARN + ignore
                                                  i. ANY unhandled fault → tombstone → return _BatchFaulted
                                                     → escalate GROUP (✗ before_node ✗ body ✗ after_node
                                                       ✗ on_node_error)
                                                  j. all resolved → materialize → tombstone →
                                                     before_node → body(resume) → after_node → publish
```

- **`calf.fanout.{node_id}.state`** — compacted; `FanoutState` (registration `expected` + outcomes); read **globally**, written once at open + once per fold + tombstoned at close. **Mutable per fold ⇒ barrier-before-read (§5).**
- **`calf.fanout.{node_id}.basestate`** — compacted; `FanoutBaseState` (the snapshot); read **globally**, written **once** at open, tombstoned at close. **Write-once-immutable ⇒ barrier-on-miss only (§5).**

**Node-scoping — kept, with an honest trade.** A worker materializes only its hosted nodes' batches — the RAM win. The cost: each `KafkaTable` is a whole-topic GlobalKTable *replay*, so a worker hosting M fan-out nodes pays **2M whole-topic replays gating startup**. The future in-process **`concurrency=N`** knob (§12) relieves this — N consumers in one process **share one batch store** (two materializations per node, not 2N) rather than N replicas each replaying. Both tables are read whole ⇒ **zero partition invariants** (the deciding constraint, preserved).

---

## 3. Record protocol (both tables keyed by `fanout_id`)

v2 deletes v1's `FanoutAbort` (abort is an in-process escalation, §4.4) and `FanoutClosure` (the re-entry carries **no payload** — closure state is read from the tables).

```python
class SlotRef(BaseModel):
    frame_id: str                       # the sibling callee frame id the caller minted
    tag: str | None                     # the caller's domain token (tool_call_id)

class FanoutOpen(BaseModel):            # SMALL registration metadata (snapshot lives in basestate)
    kind: Literal["open"] = "open"
    fanout_id: str                      # = the node's own inbound frame_id; the batch key
    node_id: str
    publish_topic: str | None           # the node's broadcast topic — fault-mirroring (§7)
    expected: list[SlotRef]             # the full slot set, fixed at open (no dynamic fan-out)
    # NOTE: correlation_id REMOVED (round-3) — sweep-only. caller_callback REMOVED (round-4) — both
    #       abort paths reach the caller without it: dispatch-abort uses D's in-memory inbound frame,
    #       fold/close-abort reads basestate[X].snapshot.stack. A derivable denormalized field is a smell.

class FanoutOutcome(BaseModel):         # one resolved slot — a sub-value of FanoutState.outcomes
    slot: str                           # SlotRef.frame_id
    tag: str | None                     # tool_call_id — the §7.6 budget tally keys tool identity via
                                        # snapshot.state.tool_calls[tag].tool_name
    target_topic: str                   # fault-group per-slot topology metadata (§4.3); carried on
                                        # every outcome, consumed only on the fault arm
    handled: bool                       # True ⇒ on_callee_error returned a substitute (counts as that
                                        # tool's FAILURE for §7.6); False on a fault-free return (RESETS
                                        # the budget). A substitute and a real return both carry `parts`,
                                        # so the tally needs this flag (fault-rail CalleeResult.handled).
    parts: list[ContentPart] | None     # resolved (return, or handled substitute) — XOR fault
    fault: ErrorReport | None           # unhandled (slot failed) — the on_callee_error decline, recorded

class FanoutState(BaseModel):           # calf.fanout.{node_id}.state value — re-written per fold
    open: FanoutOpen
    outcomes: dict[str, FanoutOutcome]  # by slot frame_id — idempotent fold key

class FanoutBaseState(BaseModel):       # calf.fanout.{node_id}.basestate value — written ONCE
    fanout_id: str
    snapshot: EnvelopeSnapshot

class EnvelopeSnapshot(BaseModel):      # the closure's correctness contract
    state: State                        # conversation State AS OF fan-out (pre tool-results)
    stack: WorkflowState                # the call stack, captured PRE-marker-stamp, node's OWN fan-out
                                        # frame as the TOP frame, no sibling frames pushed
    deps: dict[str, Any]                # deps are WIRE DATA (SessionRunContext.deps, carried on every
                                        # hop, session_context.py:108) — NOT worker resources (a
                                        # PrivateAttr, never wire). The re-entry envelope clears
                                        # context.deps (parallel to context.state), so this snapshot
                                        # copy is the SOLE restore source at close (§4.3) — correct by
                                        # construction across a rebalance. resources are re-stamped at
                                        # the node from its local bag.
```

**The re-entry envelope — `_publish_reentry`, a bespoke self-return path.** The re-entry is *not* a frame-unwinding `ReturnCall` (no frame to pop — the node addresses itself, its fan-out frame stays current). Published on the **shared node producer (`acks=all` + idempotence per fault-rail §13 — §4.2)**: `internal_workflow_state` = the node's **current** stack (fan-out frame on top, so `in_reply_to == current_frame.frame_id` holds); `context.state` **and `context.deps` cleared** (rebuilt from `basestate` at stage 0 — §4.3); `reply = ReturnMessage(in_reply_to=fanout_id, tag=<fan-out frame's tag>, parts=[])`; header `x-calf-kind=return`; `key=correlation_id`. Direct point-to-point — **not broadcast-mirrored**.

Closed batches hold **no** record. The records carry **no `version` field**: cross-version skew is handled operationally (drain-before-deploy, §9) — ktables' decoder swallows any record it cannot decode (`kafka_table.py:437-443`), so a code-level version guard would do no work. *(The field was struck from the design; the merged records — `models/fanout.py` — omit it.)*

---

## 4. Node-side protocol

Fold and closure are **framework-final on `BaseNodeDef`** (the §6.8 `_aggregate` stage + a sealed close path); the agent overrides only `_resolve_slot` (materialization, fault-rail §6.9).

### 4.1 Dispatch (replica D, in this exact order — the causality precondition)

1. **Capture `EnvelopeSnapshot`** **pre-marker-stamp** (State + current stack with the node's own fan-out frame on top, no siblings pushed; **plus `deps`**). The closure continuation is unmarked by construction (scenario 47).
2. `basestate.set(fanout_id, FanoutBaseState(snapshot))` and `state.set(fanout_id, FanoutState(open=reg, outcomes={}))`; **await both acks before any sibling publish** (base-state first ⇒ *registration present ⟹ basestate present*).
3. Publish the N sibling `Call`s, marker (`fanout_id`) stamped on the node's own frame in each sibling copy.

A **single `Call` (batch-of-one) is not registered** — it's a stateless continuation (fault-rail §6.7); registration is for a true fan-out (N ≥ 2). *Dependency:* the `fanout_id`/`tag` frame fields the marker rides are fault-rail-introduced — a hard predecessor (§10).

### 4.2 Fold (replica O — the §6.8 stage-2 made durable)

Per marked sibling reply (`in_reply_to` = a sibling frame id, frame marked `fanout_id = X`):

1. **`barrier(state); read state[X]`** — fresh, incl. O's own prior folds (§5). **A barrier that can't confirm freshness** (table loading/degraded) **blocks** until it can; a persistently-degraded table is an operational failure (§7), not a floor.
2. **Stray check — precedes the seams**, on the confirmed-fresh read:
   - `state[X]` **absent** → **post-closure stray** → floor (ERROR + full report if a fault); no stage 1.
   - `slot ∉ open.expected` → **foreign stray** → floor; no stage 1.
   - `slot ∈ outcomes` already → **duplicate** → idempotent ignore; no stage 1.
   - else → a **live pending slot**: proceed.
3. **Stage 1 — `on_callee_error`** runs on the node, per sibling (fault-rail §7.1; slot-scoped raises, scenario 31): handler returns a value → `_SlotResolved(parts, handled=True)`; declines/raises → `_SlotFailed(report)`; a return sibling skips stage 1 → `_SlotResolved(parts, handled=False)`.
4. **Fold:** `state.set(X, …outcomes | {slot: FanoutOutcome(…, handled=…)})`; **await the ack.** Safe by single-writer (correlation-keying, §5) + barriered read.
5. **Complete?** `outcomes.keys() == {s.frame_id for s in expected}`:
   - **No** → no-reply mirror (`_CONSUMED`).
   - **Yes** → having awaited the completing fold's ack, **self-publish the closure re-entry** (§3 envelope; `key=correlation_id`) **on the shared node producer**, await its ack, return `_CONSUMED`. *Durability:* the fault rail's §13 producer posture (`acks=all` + idempotence — a build-order predecessor, §10) makes "await its ack" durable and makes transient failures retry; a *permanent* re-entry-publish failure is a close-side abort (§4.4).

### 4.3 The closure re-entry handler (replica O — one close path)

The closure is a self-published re-entry — the durable analog of fault-rail scenario 47, what §7.7 binds to. **Recognition precedes stray detection** (the exemption): `in_reply_to == fanout_id` resolves the node's **own** fan-out frame (no sibling slot equals it), so it's the closure, never a stray. *(A behavior change to the sealed `_stray_check` — §10.)*

**Stage 0 — rebuild the closure context.** After the standard `prepare_context` (which copies the empty wire `state`/`deps` and stamps the frame's `overrides`), **overwrite `ctx.state` from `basestate[X].snapshot`** (`barrier`-on-miss read) *before any seam runs*: `ctx.state` ← `snapshot.state`, stack ← `snapshot.stack`, `ctx.deps` ← `snapshot.deps`. **The snapshot's own `overrides` are authoritative** (re-applied to the restored state — `prepare_context`'s stamp ran against the soon-discarded empty state). This restore **precedes `before_node`** — seams never see the empty wire state. If `basestate[X]` is absent after a confirmed barrier (impossible by §4.1's basestate-first ordering, but the close reads it anyway) → **floor `calf.fanout.basestate_missing`**, never `AttributeError`.

**Guards** (`barrier(state); read state[X]`):
- `state[X]` **absent** after a confirmed barrier → **already closed** → **abandon** (defensive — under ACK_FIRST + no sweep there's one re-entry, but the guard is free since we read `state[X]` anyway). *`state` is the abandon gate (barriered); `basestate`-presence is never the sole gate.*
- `state[X]` present but `outcomes ≠ expected` → **spurious/early** → WARN + ignore.
- both present, complete → proceed.

**The three-way decision (the durable `_aggregate`) and the seam flow.** Partition outcomes into **resolved** (`parts`) and **unhandled** (`fault`). `on_callee_error` already ran per sibling during the folds; the re-entry is `kind=return`, so stage 1 does **not** re-run. A slot whose materialization fails at close (fault-rail §6.9 — a wire-unsafe substitute → `calf.slot.materialization_failed`) is marked failed and **routes the batch through the unhandled-fault arm** (the close stays deterministic, never hangs).

- **Any unhandled fault → the batch fails (fault-rail §7.3):**
  1. Build the **fault group** (`calf.fault_group`; `causes` = unhandled faults; **singletons flatten**; `details` = per-slot `{tag, target_topic, ok|failed}` + counts). Compute the `seam_budgets` tally (below).
  2. **Tombstone `state[X]` + `basestate[X]`** (await acks).
  3. **`return _BatchFaulted(group)` — returned, never raised** (fault-rail §6.8). `_publish_fault` pops the node's fan-out frame off the snapshot stack (the outbound fault carries the popped stack so the next hop's stateless escalation has a stack), re-stamps `FaultMessage(in_reply_to, tag)` for the caller's slot, mirrors on `publish_topic`. Carries the **snapshot context** (clean pre-fan-out `State`). **`before_node`/body/`after_node` never run; `on_node_error` never entered.**
- **All resolved → the batch succeeds:**
  1. **Materialize** each resolved slot into `snapshot.state` (fault-rail §6.9 — marker-aware; `_SlotFailed` never materializes). Compute the tally.
  2. **Tombstone `state[X]` + `basestate[X]`** (await acks).
  3. **`_BatchClosed`** → `before_node → body (resume model call) → after_node` → publish. The fan-out frame stays on top (the body resumes on it, unwinding it later via its own `ReturnCall`). `before_node` fires once, here (§6.4 / scenario 38 — *and* it fired on the ingress delivery that produced the fan-out). A raise in the body is the node's **own** work → `on_node_error`.

**The `seam_budgets` tally (fault-rail §7.6):** computed from the outcomes once, at the re-entry. Per slot's **tool identity** (`snapshot.state.tool_calls[outcome.tag].tool_name`; `tool_calls` is issue-time-immutable, so the snapshot read equals the live read): a **fault-free return** (`fault is None and handled is False`) **resets** that tool's budget to 0; a **handled substitute** and an **unhandled fault** both **count as failure**. When multiple slots map to one tool key, reset is success-dominant (any fault-free return resets). Folds return `_CONSUMED` and never tally.

**Tombstone-first** (before escalate/resume) — the at-most-once close discipline; a crash in the gap is an accepted operational strand (§7).

**Sequential re-fan-out is safe.** The close tombstones before the body runs; a resumed body re-fanning out (same frame ⇒ same `fanout_id`) opens a fresh batch over a tombstoned key; barrier-before-read reads the new registration, never the stale tombstone (writes temporally ordered on one key by the single owner, serial loop).

**Seam-flow summary** (fan-out of 3):
```
folds (per sibling):  fault → on_callee_error → handled → _SlotResolved(parts, handled=True)   ┐ recorded in
                                              → declined → _SlotFailed(report)                  ┘ state[X].outcomes
                      return → (no on_callee_error) → _SlotResolved(parts, handled=False)
closure (re-entry):   ANY unhandled fault → tombstone → return _BatchFaulted(group) → escalate up the rail
                         ✗ before_node ✗ body ✗ after_node ✗ on_node_error
                      all resolved → materialize → tombstone → _BatchClosed
                         ✓ before_node ✓ body(resume) ✓ after_node   (on_node_error only if body raises)
```
Satisfies fault-rail scenarios #4/#5 (unhandled → group, no body), #6 (handled substitute → success), #31 (slot-scoped seam raise), #38 (`before_node` at closure + ingress), #43/#47 (durable re-entry; unmarked continuation).

### 4.4 Abort (node-own fault, or a permanent re-entry-publish failure)

A node-own terminal fault while a batch is open can't be recovered (fault-rail §6.8 forbids mid-batch `on_node_error`). Not the §4.3 callee-fault-group; the node's *own* machinery failing. Fail to the caller exactly once (tombstone-first + single-writer ⇒ escalate-once):
- **At dispatch:** registration durable but the batch can't complete (a sibling publish failed, or D's own code raised after step 2) → D **tombstones both, then escalates** to the caller (using D's **in-memory inbound frame** — no record field needed), mirrored on `publish_topic`. Published siblings later stray-floor at O.
- **During a fold:** a *permanent* batch-store write failure (transient ⇒ block-and-retry; a degraded broker is operational, §7) → O reads the caller from `basestate[X].snapshot.stack`, tombstones both, escalates.
- **(round-4) A permanent re-entry-publish failure** (after a successful completing fold; the `acks=all`+idempotent producer has exhausted retries on a non-retriable error — not a crash) → the same close-side discipline: read the caller from `basestate[X].snapshot.stack`, **tombstone both, escalate once.** This keeps a fully-folded batch from leaking a complete-but-unclosed corpse that only #220 could reap.

A frameless bottom frame (`callback_topic = None`) ⇒ the escalation floors.

---

## 5. Freshness model — the core correctness argument

ktables is a `GlobalKTable`: reader (`KafkaTable._data`, updated only by `_run`'s `_apply`, `kafka_table.py:438/459`) and writer (`KafkaTableWriter`, its own producer, `:558`) are **decoupled — no read-your-own-writes.** Two consequences:

- **Single-writer-per-batch is a *precondition*, from transport, not the tables.** Compacted topics are LWW with no compare-and-swap. Correlation-keying + the shared group + `max_workers=1` give exactly one steady-state folder per batch. Tables give *availability* (recovery); transport gives *serialization*. Both required.
- **Mutable `state` ⇒ barrier-before-read; immutable `basestate` ⇒ barrier-on-miss.** Without RYOW, an owner reading its own not-yet-applied write gets a **stale hit** (present, older version) → drops an outcome → hang. A stale hit is *not* a miss, so barrier-on-miss can't catch it. `barrier()` before every `state` read gives RYOW. `basestate` is write-once: present ⇒ correct, only absence is stale.

### 5.1 The `barrier()` contract (ktables v0.2.0) — pinned, because the obvious implementations are wrong

Two verified hazards: (a) `consumer.position()` advances **inside** `getmany`'s fetch, *before* `_apply` writes `_data` (`kafka_table.py:456-459`), so a position-checked-mid-poll barrier returns "fresh" while serving stale; (b) a **per-record** applied-offset counter never reaches the high-water mark on a **compacted** topic, because the fetcher advances past compacted/control/hole offsets *without yielding a record* (`aiokafka/fetcher.py:266-273`) — so it would park forever. The contract:

1. Snapshot `target = end_offsets(assigned_tps)` — a **read-only** broker call (does not drain/advance position).
2. **ktables v0.2.0 adds a persisted applied-offset watermark field**, set to `consumer.position(tp)` **after each fully-drained apply-batch** (`kafka_table.py:459`), advanced **every `_run` iteration, ungated by `_caught_up`** (today's `position()` read at `:470` lives under the one-shot `_caught_up` latch and would freeze post-catch-up — this watermark must be ungated). It's both hole/compaction-safe (it *is* `position()`, which reaches log-end past holes) and RYOW-safe (captured only after every record in the batch is in `_data`). **Not** `position()` mid-poll, **not** a per-record counter.
3. `barrier()` awaits the watermark reaching `target` via an **`asyncio.Condition` notified each `_run` iteration** — the watermark advances every iteration, so a notify-per-iteration `Condition`, **not** the one-shot `asyncio.Event` that `wait_until_caught_up` uses (same `failed`-race discipline, repeating primitive). It **never** calls `getmany`/`getone` (that drains records away from `_run`), and never interleaves a `getmany` between the per-partition `position()` reads.
4. **Failure contract (as shipped in v0.2.0):** `barrier(timeout=None)` returns **`bool`** — `True` once the watermark reaches `target`, `False` when freshness can't be confirmed (timeout / reader death / stop / broker error); it raises `RuntimeError` only on lifecycle misuse (stopped / never started), **not** on reader death. The store therefore reads the reader's `status` after a `False`: `status == "failed"` (task died, `_data` frozen) is terminal → the fold/close **aborts** (`FanoutStoreUnavailableError`); any other `False` (`degraded`/`loading`/timeout) is transient → the caller **blocks** and retries until catch-up (or eviction). A barrier that can't confirm **never** floors, serves stale, or satisfies the close PROCEED guard. *(An earlier draft pinned `barrier()` to raise on a failed reader; the shipped API returns `bool` + exposes `status`, so the store status-checks — `_fanout_store.py` `_await_fresh`.)*

This is a reader-internals addition, not a pure consumer-surface helper. Tests must include a ktables-level RYOW + tombstone-visibility test and an injected-lag fold test (the synchronous `TestKafkaBroker` can't reproduce the lag — §10). **The ktables v0.2.0 `barrier()` PR is reviewed against *this contract* (§10) — the two rejected implementations (mid-poll `position()`, per-record counter) are stale-serving and park-forever respectively.**

### 5.2 Single-writer holds in steady state; a rebalance overlap is operational

Correlation-keying + shared group + `max_workers=1` give one folder per batch **in steady state**. A rebalance can briefly overlap two folders (the losing replica's in-flight handler isn't cancelled — `aiokafka` runs the revoke in a background task). The gaining owner's barrier-before-read picks up the losing owner's committed write in the common case (the rejoin/fetch/barrier sequence dwarfs a fold's ms write), and the fold is idempotent per slot (LWW on the same key absorbs a re-fold) — so the worst case is a *lost outcome* only if the losing write is delayed *past* the gaining barrier (a producer stall coinciding with a rebalance). That is **operational and out of scope** (§7/§12): a rare strand, never a fork, reclaimed by #220.

---

## 6. Recovery

**Graceful partition movement is recovered by the durable tables — no sweep.** On a graceful rebalance (deploy, scale), the batch's state is in Kafka, so the new owner reads `state[X]`/`basestate[X]` (barriered) and **continues folding the remaining siblings**; on completion it publishes the re-entry and closes. The durable re-entry message is processed by whoever owns the partition. **The in-scope safety rests on three mechanisms, not on "drain"** (round-4 correction):

- For the replica that is *leaving* (scale-down, its own deploy stop): FastStream drains its in-flight handler before it leaves.
- For an *incumbent that loses a partition on scale-up*: it is **not** drained (the revoke runs in aiokafka's coordinator background task with only a logging listener). Safety here rests on **durable-write-survives-revoke** (the `KafkaTableWriter` is an independent producer — it completes its fold write regardless of consumer ownership) **+ barrier-before-read** (the new owner reads fresh) **+ idempotent fold** (a re-folded sibling is absorbed) — the same machinery §5.2 invokes.

Either way no in-scope graceful interleaving loses or doubles a batch. The batch's durability across partition movement — the whole point of the design — *is* the recovery. No ownership-gain sweep, no rebalance listener.

**Crashes are out of scope (§7).** An ungraceful kill mid-handler under `ACK_FIRST` strands the in-flight hop (at-most-once); the batch's table corpse is reclaimed by #220's aged timeout. We do not design a sweep / redelivery path for it — see §12 for why (and why `REJECT_ON_ERROR`, which *would* redeliver, was deferred).

---

## 7. Operational contract & failure model

**Guarantees.** (1) **Durable batch state survives graceful partition movement** — a rebalanced batch resumes on the new owner from Kafka. (2) **At-most-once per hop** (`ACK_FIRST`): a hop's *observable effect* occurs zero or one time — **side effects are never duplicated.** (Under `ACK_FIRST` the consumer *position* advances at fetch, pre-handler, and the commit is asynchronous — autocommit + the pre-revoke/stop autocommit — so a hop killed before its position is committed is simply lost, not redelivered. The one place a *fold* re-runs is the §5.2 revoke overlap, where the idempotent per-slot LWW makes the re-execution produce no duplicate outcome and no downstream duplicate.)

**Accepted operational windows** (all on a *crash*/ungraceful kill or a producer stall coinciding with a rebalance — none caused by application code, since the fault-rail chokepoint catches every handler exception; each strands, never duplicates; bounded by the caller's `reply_ttl`; the table corpse reclaimed by **#220**):
- A hop killed before its position commits → that hop's work is lost → the workflow strands (the caller's `reply_ttl` surfaces it for application-level retry).
- The §5.2 revoke-window overlap (producer stall + rebalance) → a lost outcome → strand.
- A close killed between tombstone and resume/escalate → strand.

**The contract.** The framework provides graceful-rebalance durability + at-most-once, **not** exactly-once. Operational failures (OOM/`kill -9`/pod eviction/node drain/broker degradation/ungraceful rebalance) are the operator's domain — addressed by operational contracts (graceful rolling restarts, sized `reply_ttl`, broker health), not by application code (§12; no framework is bulletproof against arbitrary ops breakage). Because v1 is at-most-once there are **no duplicate side-effects**, so applications need not make tools idempotent for this design (that requirement arrives only with the future at-least-once flip, #218).

**No timeouts in v2.** Batch age is durably visible (the open record's existence); permanently-open corpses (a hung sibling; an orphan basestate from a crash between the two open writes; a close-side partial tombstone) accumulate until **#220** reclaims them. A persistently-degraded ktable blocks the affected node — correct behavior for a broker outage.

---

## 8. Considered alternatives

| Alternative | Disposition (see §12 for the running rationale) |
|---|---|
| **In-process close** | Rejected. Recovery already needs the durable-state rebuild on a graceful rebalance, so in-process close would be a *second* close path that must stay behaviorally identical — completion-as-event / one-consumer says one path. (Also: it'd swap `ctx` mid-handler and diverge from §7.7.) |
| **Per-slot keys** (`(fanout_id, slot)`, append-only) | Deferred lever (§11). Closes the revoke-window and drops O(N²)→O(N) bytes — but doesn't eliminate the barrier (completion still needs a fresh count), adds N+2 tombstones, and the revoke-window is operational anyway. The accumulating record wins at v2's small N. |
| **Global static-name tables** | First-class alternative (§11). Deployment-shaped: node-scoped wins RAM for few-nodes-per-worker; global wins consumer/topic count for many. The future `concurrency=N` knob narrows the gap (shared materialization). |
| **`REJECT_ON_ERROR` (at-least-once)** | **Deferred to #218** (§12). Its only payoff (crash-recovery → delete the sweep) is delivered anyway; it's a framework-wide posture flip; and it trades a crash-*strand* for a crash-*fork* on the non-idempotent resume. |
| **RYOW overlay now** | Deferred (§11). Removes the per-read barrier but reintroduces in-ktables write-tracking + rebalance cache-invalidation. |
| **v1 separate aggregator / co-location / scoped-changelog / Quix / transactions / external store** | Rejected (v1 §7 reasons unchanged). |

---

## 9. Provisioning, lifecycle & operations (framework-owned; document-don't-police)

**Which nodes.** Provision the two tables **only for fan-out-capable nodes** — `isinstance(node, BaseAgentNodeDef) and not node.sequential_only_mode` (a `sequential_only_mode=True` agent issues only single `Call`s, `agent.py:335`, and never registers a batch; tool/consumer nodes never fan out).

**Topics** (self-provisioned by each node's ktables `@resource` via `ensure_topic`, default `cleanup.policy=compact` — *not* the calfkit startup ensurer, which forces `topic_configs=None` for framework topics and so cannot create compacted topics):
- `calf.fanout.{node_id}.state` — `cleanup.policy=compact`; **`max.message.bytes` sized for the accumulated outcomes** — the final fold's record is ≈ N × (per-result `parts` or `ErrorReport`); size `N_max × max(max_tool_result_size, max_error_report_size)`. A fold exceeding it is a permanent write failure → §4.4 abort.
- `calf.fanout.{node_id}.basestate` — `cleanup.policy=compact`, `max.message.bytes` for a full conversation snapshot (the ceiling normal hops already need).

**`node_id`** must be Kafka-topic-safe (`[a-zA-Z0-9._-]`) — **validated on `BaseNodeSchema.__post_init__`** via `is_topic_safe` (it lives there, not `BaseNodeDef.__init__`, because `@dataclass` tool nodes bypass that `__init__` — `__post_init__` is the one hook every subclass runs; `_return_topic` already embeds `node_id` raw, `base.py`). Partition counts are irrelevant (read-whole) and **never policed**.

**Startup gate.** The batch store opens as a **node-owned `@resource` (one per hosted fan-out node, in the node's own bag)** whose setup awaits `caught_up` before yielding (`worker.py:372-375` runs resource setup in `on_startup`, before serving — node-owned resources are entered there too). The per-fold `barrier()` is the correctness backstop (it blocks, never floors a false-absent). Boot replay scales with the *retained* (un-tombstoned) record count per topic, so the drain-before-deploy contract below also bounds boot latency.

**Shutdown.** Graceful stop flushes in-flight folds before `KafkaTableWriter` stop. Open batches survive in the tables.

**Version skew / migration.** Mixed-version rolling deploys of these tables are **unsupported** (ktables' decoder swallows cross-version records); **drain-before-deploy.** Likewise the cutover from in-process `_pending_batches`: in-flight batches at the deploy boundary are lost — **drain-before-deploy** is the documented contract.

---

## 10. Sequencing & downstream

**Build order.** The fold reads `envelope.reply.error`, returns `_BatchFaulted`, routes on `x-calf-kind=fault`, the marker rides `CallFrame.fanout_id`/`tag`, **and the re-entry's durability depends on the shared producer being `acks=all`+idempotent** — all fault-rail-introduced (`MessageKind` is `Literal["call","return"]` today; `CallFrame.tag` dormant; the producer is `acks=1` today — verified). So:
1. **ktables v0.2.0** (`barrier()` per §5.1 + the conformance test as the acceptance gate) → dep bump.
2. **fault-rail wire model + the `fanout_id`/`tag` frame fields + the §13 `acks=all`/idempotent producer posture** — predecessor of, or merged with, the aggregation PR.
3. **In-node aggregation PR** (replaces `_pending_batches`; the fold *is* §6.8 `_aggregate`; the close *is* the self-published re-entry; the per-node tables + `_publish_reentry` + the `@resource` startup gate + the per-subscriber `max_workers=1` pin).
4. Fault-rail + seams PR(s).

No separate ack-policy PR (`ACK_FIRST` kept). `concurrency=N` is future (`concurrency-model.md`).

**Fault-rail spec edits — the complete list** (round-4: orphans + #43 rescope added). *Until these land, the fault-rail spec's fan-out language is stale (it still describes the v1 aggregator) and this spec is authoritative; the edits are a **blocking checklist for the aggregation PR**, not optional follow-up — the two specs are only jointly coherent once they're applied:*
- **front-matter** — **ADD** the in-node spec to "Companion docs" (that *line* lists only CONTEXT.md/ADRs/header-route — it doesn't reference any aggregator spec; the *body*'s aggregator references are covered by the bullets below).
- **§4.1 (line 107)** — "obsoleted … by the **durable fan-out aggregator** (§7.7 — direction confirmed, design under discussion)" + the co-location-machinery references → "the in-node durable fold (§7.7)"; strike "partition-parity probe / co-locating-assignor pin."
- **§4.2** — the marker routes a reply into the **in-node fold**; the re-entry is **self-published**; strike "post-closure stray detection lives at the aggregator … the node holds no batch state" AND the residual tokens in the same block ("re-homed to the durable aggregator," "stage 1 → outcome record").
- **§6.7** — node holds **durable-backed** batch state; strays floor **in-node**; remove the "must tolerate firing for later-floored outcomes" caveat; strike "the slot outcome is published to the aggregator."
- **§6.8** — `_aggregate` is the durable in-node fold; closure is a self-published re-entry; `_BatchFaulted` *returned* → escalate, bypassing `on_node_error`. Flip the code-sketch comments ("the AGGREGATOR escalates … the node must not also fault directly"; "strays floor at the AGGREGATOR") **and** note `_publish_abort`'s *semantics* change (publish-a-record-the-aggregator-reads → in-node tombstone-both-tables-and-escalate, §4.4). **Add the `_stray_check` closure-recognition arm** (`in_reply_to == current_frame.frame_id` ⇒ closure, stray-exempt) — a behavior change to the *sealed* `_stray_check` (reachable from its `(kind, envelope)` signature; no signature change).
- **§7 item 3** — "Partial-result *handling* belongs **at the aggregator**" → "in-node, per sibling."
- **§7.5/§7.7** — re-homed to the in-node fold + self-published re-entry; **recovery is the durable tables on a graceful rebalance, not a lookup-miss-refresh/sweep**; strike the deleted v1 vocabulary ("outcome records," "abort record," "events stream," "lookup-miss refresh rule," "marker cleared there"). `max_workers=1` stands.
- **§13(2)** — replace the v1 two-sub-window enum (and the dead "companion spec §6" pointer, and "applied to the aggregator's own hops") with §7's accepted-windows framing; the rebalance-strand closes via the durable tables; crashes = accepted at-most-once (ADR-0003).
- **§15 (migration table)** — the row's replacement points to **`in-node-fanout-aggregation-spec.md`** (swap the `durable-fanout-aggregator-spec.md` path); "ktables unchanged for v1" is now false (barrier() added); **`ACK_FIRST` unchanged.**
- **§17** — the round-narration at line 864 asserts the *entire v1-aggregator design* (events stream, `FanoutClosure`-via-DataPart, the sweep, global table, "~15-line barrier()") as the *resolution* — rewrite it to the in-node design (no events stream; the re-entry carries no payload; sweep deleted; two node-scoped tables; barrier() is a v0.2.0 method); reconcile the "co-location shipped by default" claim (856) with §7.7's "co-location rejected" (667), **and** annotate the surrounding 856 round-1 narration ("co-location posture shipped by default … rebalance window remains") as superseded by the in-node design — not only the headline.
- **Scenarios** — **#43** rewrite: rescope to **graceful rebalance** (ungraceful can strand, out of scope); recovery = the new owner reads the durable tables and continues (no "events partition," "lookup-miss refresh," "outcome record"); the post-closure duplicate **floors in-node**; drop the "companion spec §5" ref. **#47** "cleared `fanout_id`" → "unmarked by construction." **#46** "sub-aggregator callee" → "callee that itself fanned out." **#33 holds** (its §6.7/§7.5 refs are re-homed by those bullets — no "at the aggregator" text in #33 itself to strike). #4/#5/#6/#31/#42 hold.

---

## 11. Future levers (recorded, not built)

1. **`REJECT_ON_ERROR` / at-least-once (#218)** — crash-recovery via redelivery; requires an idempotency key on the resumed action framework-wide. The fan-out path is already redelivery-ready (idempotent fold + tombstone-first).
2. **Per-slot append-only keys** — closes the revoke-window, drops O(N²)→O(N); the large-N / large-result lever.
3. **Global static-name tables** (§8) — the many-nodes-per-worker alternative.
4. **`concurrency=N` in-process group members** (`concurrency-model.md`) — throughput per process with a shared batch store; relieves the node-scoped boot cost.
5. **RYOW overlay in ktables** — removes the per-read barrier; reintroduces cache-invalidation.
6. **`#220` timeouts + aged reclaim** — reclaim the corpse classes (§7) via durably-visible batch age.
7. **Claim-check basestate** — `(partition, offset)` pointers + seek-read at close if all-open-snapshots-in-RAM bites.

---

## 12. Decision log (why the design is shaped this way)

For reviewers without the discussion context. Each is a deliberate decision, not an omission.

1. **Fold in the node; delete the separate aggregator + events topic.** The return topic *already* serializes per-batch (correlation-keying + shared group + `max_workers=1`) — single-writer **by partition**; v1's events topic re-derived the same guarantee on a second topic + a per-worker loop. In-node is what the fault-rail §6.8 pipeline was written for. *Rejected:* the v1 separate aggregator (redundant serialization, extra hop/loss-window).

2. **Self-published re-entry close, not in-process resume.** Durability removed the in-memory `_pending_batches`; the completing fold no longer *holds* the closure context — it must rebuild it from the durable tables regardless. A fresh `kind=return` delivery (`in_reply_to=fanout_id`) is the coherent home — **completion-as-event, one consumer** — giving one coherent ctx per handler, unifying fan-out closure with the single-call continuation, and matching §7.7. *Rejected:* in-process close (a second close path + a mid-handler `ctx` swap — §8).

3. **Accumulating `FanoutState` record, not per-slot append-only keys.** Per-slot would close the revoke-window and drop O(N²)→O(N) bytes — but the revoke-window is operational (decision 7), N is small, and per-slot keys **don't eliminate the barrier** (completion still needs a fresh count) while adding N+2 tombstones and a table scan. Future lever for large-N (§11).

4. **Node-scoped tables, not global static-name.** RAM win + consistency with the `{node_id}.private.return` pattern. The cost (2M whole-topic replays gating startup for M hosted nodes) is relieved by the future in-process `concurrency=N` knob (shared materialization). Global stays a first-class alternative for many-nodes-per-worker (§8/§11).

5. **`ACK_FIRST` (at-most-once) kept; `REJECT_ON_ERROR` reversed → deferred to #218.** `REJECT_ON_ERROR`'s only payoff was crash-recovery → delete the sweep, but **the sweep deletes anyway** (decision 6). It's a framework-wide posture flip (not scoped), and it trades a crash-*strand* (recoverable, no duplicate side-effects) for a crash-*fork* (divergent double model-turn) on the non-idempotent resume. So at-least-once is a future direction, not v1.

6. **Ownership-gain sweep + per-node rebalance listener — deleted.** They existed to recover the **complete-but-untombstoned window** — *the window where a completing fold has written all outcomes but the re-entry handler hasn't yet tombstoned* — which only arises on a **crash** (a graceful shutdown drains the completing fold, and a graceful rebalance is recovered by the durable tables — decision 1, §6's durable-write-survives-revoke + barrier + idempotent-fold mechanism). Crashes are out of scope (decision 7), so the sweep recovers nothing in-scope. Deleting it removes a whole machinery class (the listener wiring, the barrier-in-`on_partitions_assigned` hazard, the degraded-scan, the single-replica gap).

7. **Operational failures (crashes, ungraceful kills, rebalances, broker degradation) are out of scope.** No *application code* can cause a crash — the fault-rail chokepoint catches every handler exception, so the only uncommitted-message cause is external infra. No framework is bulletproof against arbitrary ops breakage; robust systems ship *operational contracts* (graceful rolling restarts, sized `reply_ttl`) instead. So the design provides graceful-rebalance durability + at-most-once and **does not** add fences/sweeps/exactly-once for crash/rebalance edges (the close-fork, the §5.2 revoke overlap — all accepted, §7).

8. **`max_workers=1` pinned per-subscriber by the framework (a small registration change), not a guard.** FastStream's `max_workers>1` is either a no-affinity coroutine pool (races the await-spanning read-modify-write — even single-threaded, because the critical section spans `await`s) or a partition-affine subscriber that's single-topic-only (our nodes are multi-topic). So `register_handlers` passes `max_workers=1` to caller-capable nodes' `subscriber(...)` calls (a per-call kwarg) and the worker's value to observers — the framework choosing the right value per node type, **not** policing a worker-global knob (document-don't-police). Concurrency scales by group members (replicas; future `concurrency=N`, which is N such registrations).

9. **`barrier()` watermark = `position()` snapshotted after each apply-batch.** This is the classic **changelog-materialization read-your-own-writes** problem: Kafka Streams gets RYOW by co-locating a local state store with the processor; our `GlobalKTable` is store-less and global, so we re-derive RYOW per-read with `barrier()`. Single-writer (decision 1) gives serialization; the barrier gives freshness; both are required (§5). The two obvious watermark implementations are wrong — `position()` checked mid-poll is stale (advances before `_apply`), and a per-record applied counter never confirms on a compacted topic (the fetcher skips compacted/control/hole offsets without yielding). Ships in ktables v0.2.0 as a reader-internals addition (§5.1).
