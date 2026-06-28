# Fault Rail, Policy Seams & In-Node Fan-out — Behavior Catalogue

**Status:** Reference (living) · **Date:** 2026-06-18
**Grounds:** the *merged* implementation of PR #247 ("the fault rail: typed fault propagation, policy seams & in-node fan-out fold", commit `04203fc`; repo HEAD `613ea02`) — the only guaranteed source of truth — cross-referenced to the two design specs.
**Source specs:** `docs/designs/fault-rail-and-policy-seams-spec.md` (scenarios `S1`–`S47` referenced inline) · `docs/designs/in-node-fanout-aggregation-spec.md`.
**Companion:** `docs/designs/fault-rail-fanout-integration-test-plan.md` (the real-broker test plan derived from this catalogue).

> This document enumerates the behaviors the three features **should meet**. Each entry is tagged with its implementation status so the test plan can target what runs today and forward-declare what is deferred. Where the merged code diverges from the spec prose, the catalogue follows the **code** and flags the drift.

---

## 0. Legend & ground rules

| Tag | Meaning |
|---|---|
| ✅ **Implemented** | Behavior is present in merged code (PR #247). A test can assert it today. |
| ⏳ **Deferred** | Spec describes it; **no producer/handler in merged code.** A test would assert vapor. Belongs to a later PR (mostly the "reception PR" and the successor retry issue #222). |
| ⚠️ **Diverges** | Implemented, but the behavior differs from the spec prose. Follow the code. |

**The six deferred behaviors (do not test as if live):**
1. ⏳ `surface_to_model` prebuilt — does not exist in any module.
2. ⏳ `self_retry_budget` / `State.seam_budgets` / `calf.agent.self_retry_exhausted` / `details.reason="self_retry_disabled"` — none exist; self-retry is an *unbounded* `TailCall` loop.
3. ⏳ Provider classification of `calf.model.context_window_exceeded` — the `FaultTypes` constant exists but has **no producer**; a context-window error surfaces as generic `calf.exception`.
4. ⏳ Client-side `NodeFaultError` on a `kind=fault` reply — the reply dispatcher has no `x-calf-kind` branch.
5. ⏳ `ConsumerContext.fault` / `ConsumerContext.delivery_kind` — absent; a consumer tap sees `output=None`, not the fault.
6. ⏳ MCP `isError=True` → `calf.retry` marking — merged code passes `isError` results through **transparently/unmarked**.

**Vocabulary** (see the specs' §2 / CONTEXT.md): *fault* (terminal failure carried as a typed `ErrorReport` on the same rail its success would take) · *recoverable error* (model-bound, rendered to ordinary content at origin — `ModelRetry`, MCP `isError`) · *policy seam* (`before_node`/`after_node`/`on_node_error`/`on_callee_error`) · *escalation* (unwind one frame, repeat; never wrapped) · *fault group* (`calf.fault_group`, children in `causes`; singletons flatten) · *delivery kind* (`x-calf-kind ∈ {call, return, fault}`) · *reply slot* (`Envelope.reply: ReturnMessage | FaultMessage | None`).

**Observability note (load-bearing for the test plan):** today no typed fault reaches the client cleanly. The deterministic typed channels are **(a)** the broadcast mirror on a node's `publish_topic` (a `FaultMessage` envelope with `x-calf-kind=fault` + `x-calf-error-type` headers) and **(b)** in-process **seam-callback assertions/recording** (a registered seam receives the live `SeamContext`/`ErrorReport`). The client edge currently surfaces a routed fault only as a `DeserializationError`, or hangs (see §A7).

---

## Part A — The fault rail (error propagation)

### A1. Synthesis & classification at the chokepoint

| ID | Behavior | Status | Grounding / notes |
|---|---|---|---|
| FR-1 | A node body that returns normally publishes a `return` (no fault). | ✅ | `_publish_action` ReturnCall arm, `base.py:495-599`. |
| FR-2 | An **uncaught non-`NodeFaultError` exception** in a body → synthesized `ErrorReport(error_type="calf.exception")` with the exception class in `details["calf.exception_type"]` and a clamped message; **no exception escapes to FastStream** (P1 — no silent drop). | ✅ | `_handle_delivery` `except Exception`, `base.py:1633-1669`; `_fault_from_exception` / `ErrorReport.from_exception`, `error_report.py:326-365`. `S1`. |
| FR-3 | A **`raise NodeFaultError(...)`** anywhere (any seam, any body) converts **verbatim** — its `error_type`/`retryable`/`details` honored — and **bypasses `on_node_error`** (the mint rule). | ✅ | `except NodeFaultError as nfe` arm, `base.py:1630-1632`; `S32`. |
| FR-4 | Minting a `NodeFaultError("calf.something", ...)` (user-supplied reserved `calf.*` type, or a `calf.*` key in `details`, or non-JSON-serializable `details`) raises `ValueError` **at construction** (fail-fast at the keyboard). Framework `calf.*` types pass via the internal factory. | ✅ | `NodeFaultError.__init__`, `exceptions.py:64-86`; `S26`. |
| FR-5 | A `NodeFaultError` carrying an existing `ErrorReport` (receive/wrap arm) plus any of `message`/`retryable`/`details` raises `ValueError` ("mint-only"). | ✅ | `exceptions.py:58-63`. |
| FR-6 | Context-window provider error classifies to `calf.model.context_window_exceeded`. | ⏳ | Constant declared (`error_report.py:53`); **no producer** in `calfkit/providers/`. Today such an error → `calf.exception`. `S13`. |

### A2. Escalation & propagation identity

| ID | Behavior | Status | Grounding / notes |
|---|---|---|---|
| FR-7 | An unhandled fault **escalates one frame at a time** (P5): pop the node's own frame, re-address, publish to the next `callback_topic`. Stateless per hop. | ✅ | `_publish_fault` pops the snapshot stack and re-stamps, `base.py:660-770`. |
| FR-8 | **Escalation never wraps.** The same `ErrorReport` (identical `report_id`, `error_type`, `origin_*`, `frame_chain`, `causes`) travels untouched at every hop; only the addressing (`in_reply_to`/`tag` on the new `FaultMessage`) is re-stamped per ancestor. A declining ancestor is *propagation, not transformation*. | ✅ | `base.py:660-674` re-addresses the same `report`; `S3`, `S24`. |
| FR-9 | Deep chain A→B→C; C faults, B declines (`on_callee_error` returns `None`), A handles → A's seam receives the fault with the multi-frame `frame_chain` and an `error_type` identical to C's origin type. | ✅ | `frame_chain` populated at synthesis; `S3`. |
| FR-10 | `ErrorReport.walk()` / `find(error_type)` traverse nested fault groups so an `error_type` check matches at **any composition depth** (cycle-guarded, iterative). | ✅ | `error_report.py:209-239`; `S40`, `S46`. |

### A3. The P2 rails — a fault takes its success's path

| ID | Behavior | Status | Grounding / notes |
|---|---|---|---|
| FR-11 | A node owing a point-to-point return (`callback_topic` set) faults to that `callback_topic` (`kind=fault`, `reply=FaultMessage`). | ✅ | `_publish_fault`, `base.py:660-706`. |
| FR-12 | A node that broadcasts also **mirrors the fault** on its `publish_topic` (`kind=fault` + `x-calf-error-type`), so any consumer can tap it. Fires even for fire-and-forget terminals. | ✅ | `_fault_response` mirror, `base.py:772-784`; `worker.py:285-286` wires the `@publisher`; `S10`. |
| FR-13 | **Fire-and-forget** terminal fault (bottom frame `callback_topic=None`) → ERROR log + broadcast mirror only; **no callback publish**; the client is unaffected. | ✅ | `base.py:688-698`; `S10`, `S14`. |

### A4. Wire-model invariants

| ID | Behavior | Status | Grounding / notes |
|---|---|---|---|
| FR-14 | The reply is **per-delivery** (`Envelope.reply` stamped fresh or cleared each hop) — kills the stale-output-slot class. A no-reply hop clears the inbound reply rather than re-broadcasting it. | ✅ | `_no_reply_mirror` clears `envelope.reply`, `base.py:487-493`; `S22`. |
| FR-15 | `x-calf-kind` classification is **total** (`_classify`): missing header ⇒ `call`; recognized values map through; **unknown value ⇒ `None` ⇒ ERROR-log + ignore** (`_floor_unknown_kind`). | ✅ | `base.py:1048-1068`; `S11`. |
| FR-16 | **Kind ↔ slot-shape disagreement** (e.g. `kind=fault` with `reply=None`, or `kind=return` with a `FaultMessage`) ⇒ **stray** handling (WARNING + ignore for returns; ERROR + floor for readable faults) — **never** the node-own-failure path. A junk message on the return inbox must not fault a live invocation. | ✅ | `_stray_check`, `base.py:1070-1086`; `S17`, `S29`. |
| FR-17 | Fault/return deliveries carry no `x-calf-route` and **structurally cannot** enter the route CoR or `'*'` — classification precedes routing. | ✅ | `_execute` routes `kind in {return,fault}` through `_aggregate` before the body; `S11`. |
| FR-18 | Outbound fault headers = emitter headers + `x-calf-kind=fault` + `x-calf-error-type=<error_type>`; the broadcast mirror carries them too (filterable at the broker without deserialization). | ✅ | `_headers`, `base.py:478-485`; fault add at `base.py:705,783`. |

### A5. `ErrorReport` totality & carriage budget

| ID | Behavior | Status | Grounding / notes |
|---|---|---|---|
| FR-19 | `ErrorReport.build_safe(...)` and `from_exception(...)` **never raise** (the error path must not re-open the drop hole): clamping validators, defensive `str()`, last-resort scalar-only fallback. | ✅ | `error_report.py:255-365`. |
| FR-20 | Carriage clamps: `message ≤ 2000` chars (coerce-then-clamp, never reject); `causes` depth ≤ 8 / total ≤ 64; `frame_chain` ≤ 64 (head+tail elision); `details ≤ 16 KB` serialized; elision breadcrumb under `details["calf.elided"]`. Caller-supplied `calf.*` keys in `details` are stripped. | ✅ | `error_report.py:27-35`, `_bound_details`, `285`. |
| FR-21 | **Oversized fault** (publish hits `MessageSizeTooLargeError`) → strip to `to_minimal()` (identity only) and **retry once**; second failure floors. An oversized fault must not become a new silent-drop class. | ✅ | `_publish_fault` size arm, `base.py:718-759`. `S42`-adjacent. |
| FR-22 | `report_id` is a stable UUID7 minted at synthesis — the dedup key across hops/mirrors. | ✅ | `error_report.py:150`. |
| FR-23 | `FrameRef` is topology-only (`frame_id`, `target_topic`); it **excludes** payloads/overrides — faults carry no user payload by construction (leak posture). | ✅ | `error_report.py:108-119`. |

### A6. Stray, stage-0, and decode-floor

| ID | Behavior | Status | Grounding / notes |
|---|---|---|---|
| FR-24 | A **stage-0** failure (classification/context-build/stray-check raises) runs **no seams** (no context yet): on `call`-kind ingress it faults the caller where the stack is readable; on `return`/`fault`-kind it **floors only** (junk must not fault a live invocation). | ✅ | `_floor_stage0`, `base.py:1595-1606`, `796-797`; `S29`. |
| FR-25 | A **stray return** (no pending slot) → WARNING + ignore, batch state untouched; a **stray fault** → ERROR with full `ErrorReport` JSON + broadcast mirror where `publish_topic` exists. | ✅ | `_floor_stray`; `S17`, `S33`. |
| FR-26 | An envelope that **fails pydantic decode** never reaches a handler; the decode shim floors `calf.delivery.undecodable` and **re-raises** (suppressing would push an empty body through the `@publisher`). At nodes routing is impossible (address is inside the unreadable body) — floor-only is the ceiling. | ✅ | client `DecodeFloorMiddleware`, `client/middleware.py:35-77`; `S30`. |
| FR-27 | At the **client** reply subscriber, an undecodable reply floors + re-raises **like a node** → the pending future is *not* failed → hangs under `reply_ttl=None`. (The spec's "fail the future" is the deferred reception PR.) | ⚠️/⏳ | `client/middleware.py`; `S30` correction. Test must use finite `reply_ttl`. |

### A7. The client edge

| ID | Behavior | Status | Grounding / notes |
|---|---|---|---|
| FR-28 | `execute(...)` / `start(...)` / `send(...)` / `connect(...)` surface; `execute` = `start` then `await handle.result(timeout)`. `reply_ttl` default `None`; finite `reply_ttl` → `ReplyExpiredError` on expiry. `send(...)` registers no future (fire-and-forget; optional `reply_to` for a *different* address). | ✅ | `client/client.py`, `client/base.py:106-115`, `reply_dispatcher.py`. |
| FR-29 | A `kind=fault` reply routed to a **registered future** is `set_result`'d like a success; with `strict=True` the projection of an empty `FaultMessage.parts` raises **`DeserializationError`** at the caller. There is **no `x-calf-kind` branch** and **no `NodeFaultError`** at the client. | ⚠️ (current) | `reply_dispatcher.py:64-100`; `node_result.py:247-287`; `S23`/`S30` corrections. |
| FR-30 | A `kind=fault` reply that does **not** route back to a future (fire-and-forget / floored / undecodable) leaves the future **hanging** until `reply_ttl`. | ⚠️ (current) | Test must set finite `reply_ttl` or `asyncio.wait_for`. |
| FR-31 | Future target: the reply dispatcher branches on `x-calf-kind` — `fault` fails the future with `NodeFaultError(report)`; `return` resolves from `reply.parts`; `find()` lets the client branch on slotted reports. | ⏳ | The "reception PR". `S23`. |

---

## Part B — Policy seams

### B1. The uniform contract (P4): *whatever a seam returns becomes the output of the thing it guards; `None` means you weren't there*

| Seam | Guards | Return a value → | Return `None` → | Status |
|---|---|---|---|---|
| `before_node` | the node body | node output; **body never runs** (mock/cache/refusal) | body runs | ✅ |
| `after_node` | the produced output | **replaces** the output (values only) | output kept | ✅ |
| `on_node_error` | the node's own failure | the node output (**recovered**) | fault synthesizes & escalates | ✅ |
| `on_callee_error` | one callee's failed result | the callee's output (**slot resolves**, values only) | unhandled → escalation (after batch closure for fan-outs) | ✅ |

`SE-1` ✅ — `is None` / `is not None` everywhere (no truthiness ambiguity). `_seams.py`, `run_chain` first-non-`None` wins.
`SE-2` ✅ — Chains: each seam accepts a single callable **or a list**; runs in registration order; constructor entries precede decorator entries; first non-`None` stops the chain. `base.py:270-311,365-387`; `S38`/chain-order trap.

### B2. `before_node` firing rule

| ID | Behavior | Status | Notes |
|---|---|---|---|
| SE-3 | `before_node` fires on **every inbound delivery on which the body would run**: ingress, and single-`Call` returns. | ✅ | `_execute` stage 3, `base.py:1461-1468`. |
| SE-4 | In a fan-out, mid-batch sibling arrivals are slot-keeping (body doesn't run) → `before_node` does **not** fire. It fires **once at batch closure**, post-aggregation, with all results assembled — *and* it fired on the ingress delivery that opened the fan-out (two firings/invocation, one per body run). | ✅ | `_close_fanout_batch` → `_BatchClosed` → before_node; `S38`. |
| SE-5 | A batch closing with ≥1 unhandled fault **escalates instead of running the body** — `before_node`, body, and `after_node` are all skipped. | ✅ | `_aggregate` returns `_BatchFaulted` → `_execute` returns immediately, `base.py:1447-1456`. |
| SE-6 | A `before_node` handler that is ingress-shaped (auth/cache/mock) **must discriminate on `ctx.delivery_kind`** (and note `ctx.route is None` off-ingress); a cache short-circuit on a *continuation* delivery would replace the in-flight workflow with the cached terminal. The agent's self-retry `TailCall` arrives `kind=call` on the private inbox, so `delivery_kind=="call"` alone admits it. | ✅ (contract) | `S45`. Caveat the handler author owns. |

### B3. Seam failure semantics

| ID | Behavior | Status | Notes |
|---|---|---|---|
| SE-7 | The **mint rule** is absolute: `raise NodeFaultError(...)` in any seam/body bypasses `on_node_error` and converts verbatim (incl. inside a recovery-path `after_node`). | ✅ | `base.py:1630-1632`; recovery-path local `except NodeFaultError`; `S19`, `S32`. |
| SE-8 | A **non-`NodeFaultError`** raise inside `before_node`/`after_node` is a node-own accident → routed to `on_node_error`, with the original inbound fault (if any) chained in `causes` via `_SeamAccidentError`. | ✅ | `_SeamAccidentError`, `base.py:171-185,1637-1640`; `S8`. |
| SE-9 | A raise inside **`on_callee_error` is slot-scoped, never node-own**: the slot resolves *failed* carrying the raised error (a `NodeFaultError` honored verbatim; else `calf.exception`) chained to the inbound fault; siblings continue; closure escalates. No double reply. | ✅ | `_resolve_callee` except arm, `base.py:1013-1032`; `S31`. |
| SE-10 | A **non-`NodeFaultError`** raise inside an `on_node_error` handler = *that handler declines* (logged, noted under `details["calf.seam_errors"]`); the chain continues; all-declined → the **original** fault escalates (no regress). A `NodeFaultError` there = mint → stop chain, convert verbatim, original chained via `causes` (`_Minted`). `CancelledError` propagates. | ✅ | `run_chain_guarded`, `_seams.py:69-117`; `S9`. |
| SE-11 | `on_node_error` recovery value flows through `after_node` (ADK parity). A **non-`NodeFaultError`** raise during recovery processing is terminal (single-shot), chaining the original as `cause`. | ✅ | `base.py:1658-1669`; `S19`. |
| SE-12 | A seam can never produce *nothing*: substitute / decline (`None`) / raise. (`Silent` is removed.) | ✅ | §10 of the spec. |

### B4. Return-value guards (teach at coercion)

| ID | Behavior | Status | Notes |
|---|---|---|---|
| SE-13 | A seam returning a **`bool`** → `SeamContractError` ("a bool is never a node output…"). `bool` checked **before** `int` (bool subclasses int). | ✅ | `_coerce_output`, `base.py:859-916`; `S41`. |
| SE-14 | A seam returning the node's **`StateT`** or the **`SeamContext`** itself → `SeamContractError` ("mutate `ctx.state` in place and return `None`"). | ✅ | `S41`. |
| SE-15 | A seam returning **`bytes`** → `SeamContractError` (no parts arm). A seam returning the route-CoR sentinel `Next` → rejected (not a seam gesture). | ✅ | `base.py:859-916`. |
| SE-16 | `after_node` returning an **action** (Call/TailCall/ReturnCall) → `SeamContractError` ("after_node returns values, not actions") → `on_node_error`. `before_node`/`on_node_error` may return a `NodeResult` action. | ✅ | `_apply_after`, `base.py:959-976`; `S16`. |
| SE-17 | An agent seam substitute **in output position** (`before_node` short-circuit, `on_node_error` recovery, `after_node` replacement) is validated against the agent's declared `final_output_type` at coercion — but only when that type is **schematizable** (exotic `OutputSpec` skips, lenient). An `on_callee_error` substitute (callee-output, slot position) is **exempt**. | ✅ | `S44` + PR-6 correction; `_coerce_output` output-type check. |

### B5. Registration validation (startup, never mid-message)

| ID | Behavior | Status | Notes |
|---|---|---|---|
| SE-18 | Registering any seam on an **observer (consumer)** raises `RegistryConfigError` at startup ("consumers receive faults as `ctx.fault`… seams exist only on nodes that call"). Observer check runs **first**. | ✅ | `_register_seam`, `base.py:338-363`; `S25`/§6.6. |
| SE-19 | Wrong-**arity** seam → `RegistryConfigError` at startup (`before_node`=1 arg; others=2). Non-callable → `RegistryConfigError`. | ✅ | `_SEAM_ARITY`, `base.py:75,355-362`. |
| SE-20 | Constructor seam params exist on `BaseNodeDef.__init__` and `Agent.__init__`. **Tool nodes (`@dataclass`) and `MCPToolbox` have no seam constructor params** — seams attach only via the instance decorator (`@node.on_callee_error`), which every node type supports via the lazy `_chains` property. | ⚠️ | `tool.py:23-28` dataclass bypasses base `__init__`; `base.py:313-326`. |

### B6. `SeamContext` capability surface

| ID | Behavior | Status | Notes |
|---|---|---|---|
| SE-21 | Seams receive `SeamContext` (capability-scoped), **not** `SessionRunContext`. `ctx.state` is the **same mutable object** the body reads — `before_node` mutate-in-place + return `None` is the input-transform channel. | ✅ | `seam_context.py:60-100`; `base.py:441`; `S18`. |
| SE-22 | `ctx.failing_call: CalleeResult | None` is set **only during `on_callee_error`** (the transport view: `tag`/`target_topic`/`frame_id`/`fault`); cleared in `finally`. | ✅ | `base.py:1030`, `seam_context.py:91`. |
| SE-23 | `ctx.exception: BaseException | None` is set **only during `on_node_error`** (the live in-process exception; never serialized; cleared post-recovery). | ✅ | `base.py:1648,1658`. |
| SE-24 | `ctx.awaiting_reply` is `True` iff this node is the addressee of a reply-owing frame (the hard-vs-soft-reject discriminator). `ctx.delivery_kind ∈ {call,return,fault}`; `ctx.route` is the inbound route key (call-kind ingress only, else `None`). `ctx.callee_results` lists resolved slots in dispatch order (`[]` on ingress). | ✅ | `seam_context.py:81-93`. |
| SE-25 | `CalleeResult` fields: `frame_id`, `tag`, `target_topic: str | None` (None for a single non-fan-out call), `parts`, `value` (auto-extracted: DataPart→TextPart→None), `fault`, `handled`. | ✅ | `seam_context.py:27-57`. |

### B7. The observer surface

| ID | Behavior | Status | Notes |
|---|---|---|---|
| SE-26 | Consumers have **no seams** and **never produce on the workflow rails** (no point-to-point publish, no auto-fault; `awaiting_reply ≡ False`). A consumer's own `consume_fn` raise floor-logs at ERROR. | ✅ | `consumer.py`; `S21`, `S28`. |
| SE-27 | A `kind=fault` delivery reaches a consumer's body, but the fault payload is **invisible** — projects to `output=None`/`output_parts=[]`; there is **no `ctx.fault`** and **no `ctx.delivery_kind`** to read. | ⏳ (reception) | `consumer_context.py:20-101`; `S25`/`S28` corrections. |

### B8. Deferred seam-adjacent behaviors

| ID | Behavior | Status | Notes |
|---|---|---|---|
| SE-28 | `surface_to_model(max_failures, only_types, exclude_types)` prebuilt: converts a fault to a model-visible result, per-tool budget, declines on exhaustion. | ⏳ | **Does not exist.** Write the equivalent as a user-authored `on_callee_error` to exercise the slot-resolve path; do not import a prebuilt. `S12`, `S35`. |

---

## Part C — In-node durable fan-out aggregation

### C1. Dispatch / OPEN (replica D, causal order)

| ID | Behavior | Status | Notes |
|---|---|---|---|
| FO-1 | An agent model turn emitting **≥2 tool calls** returns a `list[Call]`; the handler recognizes it (`_is_fanout_capable and isinstance(output,list) and len≥2 and all Call`) and opens a durable batch. A **single** call (or `sequential_only_mode`) takes the plain `Call` path — never a durable batch (`FanoutOpen.expected` has `min_length=2`). | ✅ | `agent.py:464-495`, `base.py:1722-1730`, `fanout.py:52`. |
| FO-2 | OPEN order: capture `EnvelopeSnapshot` **pre-marker-stamp** (State + current stack, node's own frame on top, no siblings pushed, + `deps`); `await store.open` writes **basestate then state** (acks awaited) **before any sibling publish**; then publish N siblings with `mark_fanout()` stamping `fanout_id = frame_id` on each sibling stack copy. | ✅ | `_handle_fanout_open`, `base.py:1132-1183`. |
| FO-3 | The whole OPEN block is guarded: any failure (sibling publish, store write, missing store) → `abort_batch` (if resolved) + `_fanout_abort_report(REASON_DISPATCH_FAILED)` → fault to the caller. | ✅ | `base.py:1169-1182`; the §4.4 dispatch-abort arm. |

### C2. Fold (replica O — durable stage-2)

| ID | Behavior | Status | Notes |
|---|---|---|---|
| FO-4 | Per marked sibling reply: **`barrier(state); read state[X]`** for read-your-own-writes freshness; a barrier that can't confirm **blocks** (transient) or **aborts** (`status=="failed"` → `FanoutStoreUnavailableError`). | ✅ | `KtablesFanoutBatchStore._await_fresh`, `_fanout_store.py:306-314`. |
| FO-5 | **Stray check precedes the seams** (on the fresh read): `state[X]` absent → post-closure stray (floor); slot ∉ `expected` → foreign stray (floor); slot ∈ `outcomes` → duplicate (idempotent ignore); else live → proceed. | ✅ | `classify_sibling`, `_fanout_store.py:161-181`; `S33`. |
| FO-6 | **Stage-1 `on_callee_error` runs per sibling** during the fold: handler returns a value → `_SlotResolved(parts, handled=True)`; declines/raises → `_SlotFailed(report)`; a fault-free return skips stage 1 → `_SlotResolved(parts, handled=False)`. | ✅ | `_fold_sibling_reply`, `base.py:1223-1254`. |
| FO-7 | **Fold is idempotent per slot frame id** (LWW on `state[X].outcomes`); producer-retry duplicates neither double-resolve nor double-close. | ✅ | `fold`, `_fanout_store.py:325-334`; §5.2. |
| FO-8 | Not complete → no-reply mirror (`_CONSUMED`). Complete → having awaited the completing fold's ack, **self-publish the closure re-entry** on the shared `acks=all`+idempotent producer, await its ack, return `_CONSUMED`. | ✅ | `_record_and_maybe_close`; `base.py`. |

### C3. Closure re-entry (replica O — one close path)

| ID | Behavior | Status | Notes |
|---|---|---|---|
| FO-9 | The closure is a **self-published `kind=return` re-entry** with `in_reply_to = fanout_id` and **no payload** (state cleared, rebuilt from tables). Recognition is in a dedicated `_classify_fanout` (`in_reply_to == current_frame.frame_id` ⇒ reentry; a sibling's `in_reply_to` is a callee frame id ⇒ sibling). It is stray-exempt **by routing** — it passes the sealed `_stray_check` (a shape-valid return) and is then classified as a re-entry. | ✅ | `_classify_fanout`, `base.py:1114-1130`; `_publish_reentry`, `base.py:601-624`; in-node §4.3. |
| FO-10 | **Stage-0 rebuild**: `_restore_from_snapshot` overwrites `ctx.state`/`ctx.deps`/stack from `basestate[X].snapshot` **before any seam runs**; resources re-stamped from the local bag (not snapshotted). `basestate[X]` absent → floor `calf.fanout.aborted` (`reason=basestate_missing`), never `AttributeError`. | ✅ | `_restore_from_snapshot`, `base.py:1397-1416`. |
| FO-11 | Close guards (barriered): `state[X]` absent → **abandon** (defensive — one re-entry under ACK_FIRST); present but `outcomes ≠ expected` → **spurious** → WARN + park; complete → proceed. | ✅ | `close_batch`, `_fanout_store.py:202-231`. |

### C4. Three-way close decision

| ID | Behavior | Status | Notes |
|---|---|---|---|
| FO-12 | **Any unhandled fault → batch fails**: build the fault group, **tombstone state+basestate first**, `return _BatchFaulted(group)` (returned, never raised) → `_publish_fault` pops the fan-out frame, re-stamps for the caller, mirrors on `publish_topic`. `before_node`/body/`after_node`/`on_node_error` never run. | ✅ | `_close_fanout_batch`, `base.py:1291-1325`; `S4`, `S5`. |
| FO-13 | **All resolved → batch succeeds**: materialize each resolved slot into `snapshot.state` (marker-aware; `_SlotFailed` never materializes), tombstone first, `_BatchClosed` → `before_node → body (resume model call) → after_node` → publish. A body raise here is the node's **own** work → `on_node_error`. | ✅ | `base.py:1291-1325`; `S6`. |
| FO-14 | **Fault group shape:** exactly one unhandled fault **flattens** to the bare child (identity preserved; topology copied onto `details["calf.fanout_topology"]`); **2+** compose `calf.fault_group` carrying children in `causes`. Topology = `{slots:[{tag,target_topic,status}], ok, failed}` nested under `details["calf.fanout_topology"]` (⚠️ not at `details` root). Partial successes are **topology metadata only** — values never carried (leak posture). | ⚠️ | `_build_fault_group`, `base.py:1374-1395`; `S4`, `S40`. |

### C5. Abort paths

| ID | Behavior | Status | Notes |
|---|---|---|---|
| FO-15 | `calf.fanout.aborted` with `details["reason"] ∈ {dispatch_failed, store_unavailable, basestate_missing, reentry_failed}`. The caller is sourced from the **inbound delivery's pre-mutation stack snapshot**, *not* basestate (basestate is `None` in the `basestate_missing` case). | ✅ | `_fanout_abort_report`, `base.py:1327-1337`; reasons `error_report.py:65-69`. |
| FO-16 | Mid-fold node-own terminal fault (stage-5 unrecovered, or publish failure) while a batch is open → recovery is **forbidden** mid-batch; tombstone both + escalate once. A permanent re-entry-publish failure → close-side `reentry_failed` abort. | ✅ | `_in_batch_work` arm, `base.py:1641-1647`; in-node §4.4. |
| FO-17 | **Residual (accepted):** under a **dead store** (terminal reader death) the abort's tombstone write also fails, so each still-pending sibling independently aborts+escalates → the caller may receive up to **N** `calf.fanout.aborted` faults for one batch (a duplicate/over-delivery, not a loss; v1 holds zero in-process dedup state). | ✅ (by design) | in-node §4.4 / §13(6). |

### C6. Durability & recovery

| ID | Behavior | Status | Notes |
|---|---|---|---|
| FO-18 | Batch state **survives graceful partition movement**: the new owner reads `state[X]`/`basestate[X]` (barriered) and continues folding; the durable re-entry is processed by whoever owns the partition. Safety rests on durable-write-survives-revoke + barrier-before-read + idempotent fold — **no sweep, no rebalance listener**. | ✅ | in-node §6; kafka test `test_graceful_rebalance_continues_batch_on_new_owner`. `S43`. |
| FO-19 | Posture: **at-most-once** (`ACK_FIRST` kept), **`max_workers=1`** pinned per-subscriber for caller-capable nodes. No duplicate side-effects ⇒ tools need not be idempotent for this design. Crashes/ungraceful kills/broker degradation are **out of scope** (operator's domain; bounded by `reply_ttl`, reclaimed by #220). | ✅ | in-node §2, §7. |

### C7. Continuation & re-fan-out edges

| ID | Behavior | Status | Notes |
|---|---|---|---|
| FO-20 | A `return`/`fault` arriving where **no batch is registered** is a **stateless continuation**, validated against the durable stack — no in-process state required. Keeps sequential agents and every mid-chain escalation hop stateless. A batch-of-one is never registered. | ✅ | `_aggregate` `None` arm, `base.py:1185-1221`; §6.7. |
| FO-21 | After a successful fan-out closes, the agent's **next single `Call`** is **unmarked by construction** (the closure re-entry was assembled from the pre-stamp snapshot — no clearing step) → body-dispatches as an ordinary stateless continuation, not orphan-floored. | ✅ | `S47` + correction. |
| FO-22 | **Sequential re-fan-out is safe**: close tombstones before the body runs; a resumed body re-fanning out (same frame ⇒ same `fanout_id`) opens a fresh batch over a tombstoned key; barrier-before-read reads the new registration, never the stale tombstone. | ✅ | in-node §4.3. |
| FO-23 | A `TailCall`'s replacement frame **preserves `frame_id`/`tag`/`overrides`**, clears `payload` and `fanout_id` — the same pending call retargeted; a fresh id would orphan the caller's slot. | ✅ | `_publish_action` TailCall arm, `base.py:572-585`; `S34`. |

### C8. Agent self-retry

| ID | Behavior | Status | Notes |
|---|---|---|---|
| FO-24 | When **every** tool call in a turn is invalid (unknown tool / malformed args / validator raise → all become `RetryPromptPart` results) the agent emits a `TailCall` self-retry on its own private inbox (`kind=call`), re-prompting the model. | ✅ | `agent.py:456-460`. `S27`, `S37`. |
| FO-25 | The self-retry loop is **bounded** by `self_retry_budget` (default 1); exhaustion → `calf.agent.self_retry_exhausted`; `self_retry_budget=0` → immediate fault `reason=self_retry_disabled`. | ⏳ | **Unimplemented.** Merged code has an **unbounded** loop, no budget, no exhaustion fault. `S37`. |

---

## Part D — Cross-cutting / "other" behaviors

| ID | Behavior | Status | Notes |
|---|---|---|---|
| XC-1 | Caller-capable nodes require **`max_workers=1`**, set per-subscriber by `register_handlers` (the framework choosing the right value per node type, not policing a worker knob). The serialization is load-bearing for the fold and the seam/`State` ordering. | ✅ | in-node §2, §12(8); worker registration. |
| XC-2 | The shared producer defaults to **`acks=all` + `enable_idempotence=True`** — hardens every rail; makes "await the ack" durable and the re-entry retry-safe. | ✅ | §13 producer posture. |
| XC-3 | Structured logging: synthesis = ERROR (with traceback, at origin); each escalation hop = WARNING (`error_type`, origin, remaining depth); seam handling = INFO. The escalation logs carry the "why didn't my handler fire" teaching load. | ✅ | `base.py:650-657,709-716`; `_seams.py` INFO. `S9`. |
| XC-4 | `ModelRetry` raised by a tool → caught at origin, rendered to a **`calf.retry`-marked `TextPart`** (raw message, no suffix), shipped as an **ordinary return** (not a fault). The agent's `_resolve_slot` materializes a `RetryPromptPart` (Anthropic `is_error=True` fidelity). | ✅ | `tool.py:116-121`, `payload.py:47-53`, `agent.py:131-193`; `S20`, `S36`. |
| XC-5 | MCP **transport/session failure** → fault rail (`calf.exception`); **`isError=True`** result → **transparent passthrough** as an ordinary `ReturnCall` (NOT marked `calf.retry`). | ⚠️ | `mcp_toolbox.py:243-266` (B2 decision). Spec's `calf.retry`-for-MCP is deferred. |
| XC-6 | `surface_to_model` per-tool budget tally in `State.seam_budgets` at every slot finalization (closure + the single continuation); reset on a tool's fault-free return; key = tool identity not topic. | ⏳ | **Unimplemented** (no `seam_budgets` field). `S35`. |
| XC-7 | `reply_ttl` stays `None` by default; with the rail in place faults *arrive* instead of hanging — except where reception is deferred (FR-27/FR-30), where a finite `reply_ttl` is the liveness backstop. | ⚠️ | Until the reception PR, finite `reply_ttl` is **required** to observe routed-fault/undecodable cases without hanging. |

---

## Appendix — Deferred-behavior register (forward test targets)

When these land, promote the tagged behaviors from ⏳ to ✅ and activate the corresponding stubbed tests (see the test plan's §Deferred).

| Deferred item | Unblocks behaviors | Filed issue |
|---|---|---|
| Client reply-dispatcher `x-calf-kind` branch + `NodeFaultError` | FR-31; `S23` fault-half; `S30` client-half | **#250** (fault reception) |
| `ConsumerContext.fault` / `.delivery_kind` | SE-27; `S25`, `S28` fault-tap | **#250** (fault reception) |
| `surface_to_model` prebuilt + `State.seam_budgets` tally | SE-28, XC-6; `S12`, `S35` | **#251** (agent self-healing budgets) |
| `self_retry_budget` + `calf.agent.self_retry_exhausted` | FO-25; `S37` exhaustion arm | **#251** (agent self-healing budgets) |
| Provider classification `calf.model.context_window_exceeded` | FR-6; `S13` | **#193** (provider rider) |
| MCP `isError` → `calf.retry` marking | XC-5 marked-arm | follow-up MCP PR (B2 deferral) |

---

## Appendix — Spec ↔ code drift (follow the code)

These are the points where the *prose* of the two specs is ahead of the *merged code*. The catalogue above already follows the code; listed here so the divergence is recorded (not as an instruction to edit the specs — that's a separate call).

1. `surface_to_model`, `self_retry_budget`/`seam_budgets`, the provider classifier, and the client-side `NodeFaultError` are **described as present** in the fault-rail spec body but are **deferred/absent** (the spec's own PR-6 CORRECTION banners flag most of them).
2. The fault-group topology lives under `details["calf.fanout_topology"]`, **not** at the `details` root as the §7 prose reads.
3. MCP `isError=True` is **transparent/unmarked** (B2), not `calf.retry`-marked.
4. The fan-out aggregation language in the **fault-rail** spec still references the deleted v1 separate aggregator throughout §4/§6/§7/§13/§17 and scenarios #43/#46/#47; the authoritative current design is the **in-node** spec. (The fault-rail spec carries a blocking edit-checklist for this in the in-node spec's §10.)
