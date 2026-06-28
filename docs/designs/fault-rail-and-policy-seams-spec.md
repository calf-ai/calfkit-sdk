# Fault Rail & Policy Seams — Design Spec

**Status:** DRAFT — under review (Ryan)
**Date:** 2026-06-10 · **Revised 2026-06-11:** carriage model replaced (§4 — per-delivery reply slot + `x-calf-kind`; see ADR-0006) and the conversion layer collapsed (§6.9); decisions from the 2026-06-11 session folded throughout
**Supersedes:** the error-handling portion of issue #193's original framing; `gate()`; the `Silent` action (§10); the `FailedToolCall` → `ToolExecutionError` terminal path; the tool-result blob-write protocol and `State.final_output_parts` (results-in-`State` carriage, §4/§15)
**Out of scope:** retry/redelivery & ack-policy work — **minted 2026-06-12 as umbrella #222 (children #218 ack/redelivery, #219 node-local retry, #220 batch timeouts, #221 DLT)**; every "successor issue" reference in this spec means #222. Note #143 itself is the error-*propagation* issue this spec implements and closes (§15). Also out of scope: a middleware-shaped extension family (none planned — §14), per-type handler sugar
**Companion docs:** `CONTEXT.md` (vocabulary), ADR-0003 (propagation), ADR-0004 (seam surface), `docs/designs/header-route-dispatch-spec.md` (the dispatch machinery this builds on), `docs/designs/in-node-fanout-aggregation-spec.md` (the authoritative in-node fan-out fold this spec's fan-out language is superseded by — PR-6 / 2026-06-17)

> **⚠️ OUTDATED (PR-6 / 2026-06-17):** the fan-out / aggregation passages throughout this spec describe the deleted v1 *separate* fan-out aggregator (a worker-level consumer loop + a `calf.fanout.events` topic + outcome/abort records). That design was replaced by an **in-node durable fold** — aggregation happens inside the node's own `_aggregate` stage over two node-scoped compacted ktables, and closure is a self-published re-entry. The authoritative current doc is `in-node-fanout-aggregation-spec.md`; PR-6 implements it. Affected sections, each annotated below: §4.1, §4.2 (`fanout_id`), §6.7, §6.8 (code-sketch comments), §7 items 3/5/7, §7.7, §13(2), §15 (migration row), §17 (round-narration), and scenarios #43/#46/#47. Add this in-node spec to the Companion-docs line.

All items are confirmed decisions with their rationale; the [PROPOSED] mechanism is retired (§17). Round 1 of the adversarial review (2026-06-11, five lenses) has been applied throughout — its decisions are marked "(confirmed 2026-06-11, review round 1)".

---

## 1. Motivation

calfkit today **silently drops** any uncaught exception in a node handler. `BaseNodeDef.handler` has no top-level catch; the exception escapes to FastStream under `ACK_FIRST` (offset already committed), so the message is gone — no reply, no redelivery, no record. The originating caller either hangs forever (`reply_ttl` defaults to `None`) or gets a contentless `ReplyExpiredError`. The only structured error that crosses a hop is `FailedToolCall` (tool → agent), and even that path terminates by raising `ToolExecutionError` inside the agent — which is then itself dropped.

Issue #193 (typed `ModelContextWindowExceededError`) exposed this: a typed exception raised inside a worker process helps no remote consumer. The real feature is the rail the type rides on.

This spec defines:

1. **The fault rail** — how a terminal failure becomes a typed wire value and travels the workflow.
2. **The policy seams** — the four developer-facing callbacks on `BaseNodeDef` (`before_node`, `after_node`, `on_node_error`, `on_callee_error`) through which developers alter behavior, including all error handling. Gates are retired.
3. **The classification rider** — producing the first typed fault (`context window exceeded`) at calfkit's provider layer.

### Goals

- No uncaught exception is ever silently dropped (the **no-silent-drop invariant**).
- Error handling lives where the remediation knowledge lives: the caller — while still giving nodes an edge seam for failures in framework-authored internals (model calls, MCP sessions) the developer cannot `try/except`.
- One uniform, learnable contract across all seams; day-one developers write **nothing** and get sane behavior.
- DX deliberately familiar to Google ADK users (verified against `google-adk` 2.2.0, which independently converged on escalate-by-default with caller-side error callbacks).

### Non-goals

- Automatic retry/redelivery. `ACK_FIRST` stays; `ErrorReport.retryable` is **advisory metadata**, not framework-enforced behavior. (Deferred to the successor retry/redelivery issue; #143 is implemented by this spec.)
- The cross-cutting middleware family (`around_invoke`/`around_publish`). See §14 taxonomy.
- A dead-letter **topic** infrastructure (needs its own design). The v1 floor is the broadcast rail + structured logging — confirmed; see §13.

---

## 2. Vocabulary

Canonical terms are defined in `CONTEXT.md` (Routing + Error handling sections). Summary:

| Term | Meaning |
|---|---|
| **Fault** | Terminal failure, carried as a typed value on the same rail its success would have taken. Never dropped. |
| **Recoverable error** | Model-bound failure, rendered to text *at origin* and carried as ordinary reply content (`ModelRetry`, MCP `isError=True`). Not a fault — and not a distinct wire concept (§4.5). *(⚠️ PR-6 / 2026-06-17: MCP `isError=True` passes through **transparently/unmarked** in the temporary PR-6 implementation — `calf.retry` marking for it is deferred to a follow-up PR; see the §9 CORRECTION.)* |
| **Policy seam** | One of `before_node`, `after_node`, `on_node_error`, `on_callee_error` — inherited by every *caller-capable* node type; consumers/observers have none (§6.6). |
| **Escalation** | Default for an unhandled fault: unwind one frame, repeat. Never absorbed — and never wrapped by a declining ancestor (§4.4). |
| **Fault group** | The fault minted when a fan-out batch closes with unhandled sibling faults (`error_type="calf.fault_group"`, children in `causes`). Itself a fault; singletons flatten (§4.4). Not every fault with `causes` is a group — conversion and recovery-failure also chain causes. |
| **Fault stream** | The always-on observability floor (broadcast rail + logs). |
| **Rail** | Callback rail (call-stack unwind, point-to-point) or broadcast rail (`publish_topic`, forward). |
| **Delivery kind** | `x-calf-kind: call \| return \| fault` — framework-stamped, producer-side fact classifying every delivery; drives receiver-side pipeline classification (§4.1). |
| **Reply (slot/message)** | The envelope's per-delivery response carriage: `ReturnMessage` (content parts) or `FaultMessage` (`ErrorReport`), answering at most one frame via `in_reply_to` + `tag` (`in_reply_to=None` only on frameless floor faults — §4.2/§13). |
| **Tag** | Opaque caller-set correlation token on a `Call`, echoed verbatim on its reply (the agent sets `tool_call_id`). Transport metadata, never content (§4.2). |

---

## 3. Design principles (all confirmed)

- **P1 — No silent drops.** Every inbound message produces exactly one of: a routed success, a routed typed fault, a floor-logged typed fault, or — only where no reply is owed — a deliberate no-op (§10). Never silence toward a waiting caller. Enforced at the chokepoint (`BaseNodeDef.handler`) for every delivery that decodes, and at the **pre-handler floor** for deliveries that don't (§6.7 — an envelope that fails validation never reaches any handler; FastStream logs-and-swallows it, so the framework must catch it below the handler with a `calf.delivery.undecodable` floor event; routing is impossible there because the return address is inside the unreadable body — floor-only is the ceiling, stated honestly).
- **P2 — A fault travels the same rail its success result would have.** A node that would `ReturnCall` to a `callback_topic` faults to that `callback_topic`; a node that would broadcast faults to its `publish_topic`; fire-and-forget (`callback_topic=None` at the bottom frame) goes to the floor. No separate error-topic topology. (ADR-0003.)
- **P3 — Classify by audience.** Recoverable → the model (rendered to content at origin and carried as an ordinary return — §4.5). Terminal → the caller/consumer (the fault rail). The split is by *who can act*, not by node type.
- **P4 — Uniform seam contract.** Returning `None` from any seam is a no-op (behavior continues as if the seam weren't there). Returning a value is the explicit opt-in: *the value becomes the output of the thing that seam guards.* One sentence, no per-seam memorization. (ADR-0004.)
- **P5 — Escalate, never absorb.** An unhandled fault bubbles one frame at a time until a seam handles it or it reaches the workflow's reply address. The lazy path is loud-and-safe: forgetting to return from a seam means *decline*, which means *escalate* — never silent corruption.

---

## 4. Wire model (confirmed 2026-06-11 — replaces the first draft's routed-payload carriage; ADR-0006)

The delivery model has three building blocks: **headers** classify the delivery (transport metadata), the **call frame** carries the request side (durable for the whole pending call), and the **reply slot** carries the response side (exactly one delivery). Four invariants govern all carriage:

- **I1 — Correlation is transport's job, never content's.** `frame_id` and `tag` are minted by the caller's framework, carried on the frame, and echoed on the reply. No content schema carries correlation — no `origin_payload`, no callee writing results into the caller's `State` by convention.

> **⚠️ CORRECTION (PR-6 / 2026-06-17):** I2 below says the value→parts coercion has "two call sites"; the implementation calls the generic `_coerce_to_parts` at **three** sites — the publish chokepoint (`_publish_action`), handled-substitute slot materialization (stage-1 `_resolve_callee`), **and `_output_view`** (which coerces the output to parts to project the typed view `after_node` inspects). The §4.5 / §6.3 "two call sites" mentions are off by that `_output_view` site.

- **I2 — Content is parts.** Everything a node returns crosses the wire as `list[ContentPart]` (the existing `TextPart | FilePart | DataPart | ToolCallPart` vocabulary — nothing added). One generic coercion function in (value → parts; two call sites: the publish chokepoint for the node's own output, §4.5, and slot materialization for handled substitutes, §6.9), one generic extraction out (the existing `_extract_output` machinery, repointed at `reply.parts`).
- **I3 — The reply is per-delivery.** A pure function of the publish that produced it, stamped fresh or cleared on every hop. The request's lifetime is the frame (pushed at `Call`, popped at return); the reply's lifetime is one delivery. This kills the stale-output-slot class of bugs (`final_output_parts` showing turn N−1's output on mid-loop hops).
- **I4 — `State` is a record, not an interface.** It still travels in `context`, but no node reads another node's result out of it. `State.tool_results` becomes the agent's private conversation-assembly bookkeeping (§8).

### 4.1 Delivery kinds

```python
# calfkit/_protocol.py
HDR_KIND = "x-calf-kind"        # framework-stamped on every publish; never user-set
KindValue = Literal["call", "return", "fault"]
```

- **`call`** — "do work": `Call`, `TailCall`, client sends. The only kind that may carry the user routing header `x-calf-route` (whose existing contract — ingress-only, user feature — is now preserved rather than overloaded).
- **`return`** — "your call resolved": `ReturnCall` publishes on the callback rail.
- **`fault`** — "your call failed": fault publishes (origin or escalation hop), accompanied by **`x-calf-error-type: <error_type>`** for broker-level filtering without deserialization.

The kind is a **producer-side fact about the message, never about the receiver** (confirmed): on a broadcast medium any consumer may tap any topic — a consumer tapped into a tool's *input* topic receives `kind=call` messages, truthfully, because it is observing calls. Observer-vs-actor is a property of the node type, not the wire (§6.7). A fourth `event` kind for broadcast mirrors was considered and **rejected** for exactly this reason: a header cannot encode a receiver's relationship to a broadcast message, and once the observer role lives in the node type the extra value does no work. The broadcast mirror (the `Response` body republished on `publish_topic`): reply-producing hops mirror their kind; no-reply hops mirror `kind=call` with the reply cleared — §4.2's table is normative.

Receiver-side classification trusts the header. The kind ↔ reply-slot-shape agreement (`call` ⇔ `reply=None`, `return` ⇔ `ReturnMessage`, `fault` ⇔ `FaultMessage`) is a construction invariant for calfkit producers — both are stamped by the single publish chokepoint (`_publish_action`), the way an HTTP `Content-Type` agrees with its body because one writer produces both. Foreign producers cannot be bound by it, so **`_classify` is total** (confirmed 2026-06-11, review round 1):

- **Missing header ⇒ `call`.** The public inbox is writable by anyone (raw-producer integration is a designed-for audience — whose contract also includes keying ingress by correlation id, §7.7), and headerless ingress is the raw-producer norm — `call` is the only safe reading. Scope honestly: this is compatibility for raw *ingress on public inboxes*; mixed-version rolling deploys are not supported (a pre-migration *return* arriving at a migrated node classifies as `call` and its blob carriage is gone — the hard-break policy applies).
- **Unknown value ⇒ ERROR-log + ignore** (forward-compat rule, the `error_type` tolerance applied to `kind`): a node must not execute work it cannot classify; if the envelope decoded and its reply slot carries a readable `FaultMessage`, the report is floor-logged in full before ignoring.
- **Kind ↔ slot disagreement ⇒ stray handling** (WARNING + ignore; floor the report if fault content is readable) — **never the node-own-failure path.** A junk message on the return inbox must not fault a live invocation (without this rule, `reply.error` raising `AttributeError` inside stage 1 would escalate the node's whole pending batch).
- Classification, context construction, and the reply-slot shape check all run **inside** the chokepoint's fault boundary (§6.8) — a raise during stage 0 is handled *below the seams*: on `call`-kind ingress it faults to the caller where the stack is readable (floors otherwise); on `return`/`fault`-kind deliveries it **floors only** — junk on the return inbox must never fault the node's own live invocation. Never an escaped exception.

**Transport-level vs in-band multiplexing (confirmed 2026-06-11; single-entrypoint merge explored and REJECTED 2026-06-12 — decision record below).** The governing principle: **a topic multiplexes transport properties — audience/ACL, QoS class, ordering-coupling; a header multiplexes semantics within one audience.** Shared topic ⟺ shared transport properties, not shared "information." calfkit's topology splits exactly on those boundaries:

- **Public inbox** (`subscribe_topics`) — the node's API: writable by anyone, the work queue for *new* invocations. **Shareable by design**: multiple caller-capable nodes may independently listen on one ingress topic (each with its own group — fan-out of work/events to several agents), because replies never travel through it.
- **Private continuation inbox** (`{node_id}.private.return`) — continuations of *in-flight* invocations: callee returns, callee faults, and the node's own self-retry `TailCall`, all `x-calf-kind`-discriminated. Two guarantees hold **by construction, not convention** (made explicit 2026-06-12 — the rejected merge would have demoted both to deployment contracts): (1) **uniqueness** — the topic name is *derived from* `node_id` (#141), so no two nodes can share a continuation inbox and no foreign node can ever consume this node's continuations (the identity-blind-fork class is structurally impossible); (2) **config ownership** — it is a `framework=True` topic, never touched by user `topic_configs`, so the compaction/retention drop hazards cannot be configured into existence.
- **Broadcast** (`publish_topic`) — the observer audience.

A **per-node error topic** (peeling `fault` off the continuation inbox) was considered and rejected: returns and faults differ on *no* transport property — they are the two outcomes of one pending obligation, discharged by the same callee to the same waiter. Splitting them (a) forfeits the lock-free serialization that one correlation-keyed topic gives per-workflow processing; (b) cannot retire the kind header anyway (the client reply topic awaits both outcomes of one correlation; broadcast mirrors stay mixed-kind); (c) the legitimate ops wants — a fault firehose, long fault retention (per-topic-only in Kafka, which has no broker-side header filtering) — belong to the observability plane: the §13 fault-stream/DLT topic, where topic separation genuinely pays. Industry "error queues" (MassTransit, NServiceBus) are dead-letter parking for failed *consumption*, not an error-*reply* lane; both frameworks return failure results on the reply address.

> **⚠️ OUTDATED (PR-6 / 2026-06-17):** the paragraph below ends by saying the locality machinery is "obsoleted the right way: by the **durable fan-out aggregator** (§7.7 — direction confirmed, design under discussion)" + the "partition-parity probe / co-locating-assignor pin" references. That v1 separate aggregator is deleted — see `in-node-fanout-aggregation-spec.md` §7.7. The machinery is now obsoleted by the in-node durable fold (no parity probe, no co-locating assignor); `max_workers=1` still stands but for serialization, not co-location.

**The single-entrypoint merge — considered and rejected (Ryan, 2026-06-12).** Collapsing each node to one topic for all kinds was explored in full (folded, delta-reviewed, then reverted same day; §17 records the episode). It deleted real machinery — the partition-parity probe, a co-locating-assignor pin, a rolling migration — and gained per-workflow total ordering. It was rejected on two grounds the delta review made concrete: **it forbids shared-ingress listening** (a caller-capable node's entrypoint would double as its reply address, so two agents sharing a topic would consume each other's continuations — an identity-blind workflow fork), and **it demoted the two by-construction guarantees above to deployment conventions** whose violations are silent forks and silent drops. The locality machinery the merge would have deleted is instead obsoleted the right way: by the **durable fan-out aggregator** (§7.7 — direction confirmed, design under discussion). Consciously retained with the split: the intended callees-only trust posture (per-topic ACL-able later) and the completion-priority consumption option. And topics remain **addresses, not classifiers**: the continuation inbox legitimately carries `kind=call` (the self-retry) and the client reply topic carries `return` + `fault` — which is why `_classify` reads the header, never the topic.

> **⚠️ CORRECTION (PR-6 / 2026-06-17):** the installed FastStream is **0.7.1**, not 0.6.6 — any "0.6.6" reference below (and the decode-floor mechanism) is stale and was re-verified on 0.7.1.

**Serialization precondition** (confirmed 2026-06-11, review round 1): the lock-free per-workflow serialization above holds because the node's single FastStream subscriber consumes its assigned partitions through one serial loop. `Worker(max_workers>1)` switches FastStream to a concurrent subscriber with **no partition/key affinity** — same-partition messages then process concurrently and the batch bookkeeping (§7) races. Caller-capable nodes therefore **require `max_workers=1`, enforced at registration**, until partition-affine concurrency exists; the invariant is load-bearing, not incidental. (FastStream 0.6.6's partition-affine concurrent subscriber requires non-ACK_FIRST *and* truncates the subscription to a single topic — unusable for calfkit's multi-topic subscribers as shipped; assess future options against those facts.)

### 4.2 The reply slot

```python
class Envelope(BaseModel):
    context: SessionRunContext              # the traveling session record — unchanged
    internal_workflow_state: WorkflowState  # the durable call stack — unchanged
    reply: ReturnMessage | FaultMessage | None = None   # NEW — per-delivery response carriage


class _ReplyBase(BaseModel):
    in_reply_to: str | None     # frame_id of the frame this reply answers, echoed by the
                                # framework at unwind. None only on frameless floor faults (§13).
    tag: str | None             # echo of CallFrame.tag — the caller's opaque correlation
                                # token (the agent sets tool_call_id); never interpreted.

class ReturnMessage(_ReplyBase):
    kind: Literal["return"] = "return"      # union discriminator
    parts: list[ContentPart]                # the output, in the existing content-part vocabulary

class FaultMessage(_ReplyBase):
    kind: Literal["fault"] = "fault"
    error: ErrorReport
```

The request side gains only the correlation token; the actions grow matching fields:

> **⚠️ OUTDATED (PR-6 / 2026-06-17):** the `fanout_id` field comment in the code block below describes the deleted v1 *separate* fan-out aggregator — "re-homed to the durable aggregator," "the marker's one job: route marked sibling replies into the aggregation protocol (stage 1 → outcome record, companion spec §4)," and "post-closure stray detection lives at the AGGREGATOR (lookup miss + refresh rule), not the node." Aggregation now folds **in-node** — see `in-node-fanout-aggregation-spec.md` §4.2/§4.3. The marker routes a marked sibling reply into the node's own `_aggregate` fold (stage 1 → an outcome in the node's `state` ktable, not an aggregator outcome record), and post-closure strays floor **in-node** (the node holds durable-backed batch state). The "unmarked by construction" lifecycle for the closure continuation is unchanged.

```python
@dataclass(frozen=True)
class CallFrame:
    target_topic: str
    callback_topic: str | None
    frame_id: str                       # framework-minted UUID7 — unchanged
    overrides: OverridesState | None
    payload: Any = None                 # the CALL'S INPUT — written once at Call time, durable
                                        # for the pending call and readable on every delivery of
                                        # the invocation (exposed as SeamContext.payload; today's
                                        # agent run() reads it only via run/handler-unification).
    tag: str | None = None              # NEW — caller-set correlation token, echoed on the reply
    fanout_id: str | None = None        # NEW (review round 2; re-homed to the durable aggregator
                                        # 2026-06-12) — stamped on the CALLER'S OWN frame in each
                                        # SIBLING stack copy at fan-out (the batch key = the
                                        # caller's inbound frame_id), and ONLY there: the closure
                                        # re-entry is assembled from the pre-stamp envelope
                                        # snapshot, so the continuation is unmarked BY CONSTRUCTION
                                        # (no clearing step exists; the next single call is
                                        # automatically stateless — scenario 47). The marker's one
                                        # job: route marked sibling replies into the aggregation
                                        # protocol (stage 1 → outcome record, companion spec §4)
                                        # instead of the stateless-continuation path. Post-closure
                                        # stray detection lives at the AGGREGATOR (lookup miss +
                                        # refresh rule), not the node — the node holds no batch
                                        # state to check against. A single Call / batch-of-one
                                        # never stamps; unmarked frames (single calls, escalation
                                        # hops) stay fully stateless. TailCall never carries it.


Call(target_topic, state, *, route=None, body=None, tag=None)   # grows tag
ReturnCall(state, value: Any = None)    # grows value — the node's output, EXPLICIT in the action
                                        # instead of side-written into State (final_output_parts
                                        # and the tool_results blob-write protocol both retire)
```

**Why the request rides the frame but the reply rides the envelope** (asymmetry confirmed deliberate, 2026-06-11): placement follows lifetime — the calling-convention pattern (arguments live in the activation record for the activation's lifetime; return values ride the transfer and are consumed at the return site; no machine stores return values in frames). A pending call's input must survive the callee's entire delivery subtree; a reply exists for exactly one hop. Structurally the reply *cannot* ride the stack: the only frame it belongs to is popped by the very publish that carries it. Forcing it in anyway means one of three patches — clobbering the caller's frame payload (the first draft's rejected dead-payload maneuver), a pseudo-frame (the stack stops meaning "pending calls, exactly"), or symmetrizing the other way with a per-delivery request slot (multi-delivery invocations lose their input after ingress and copy it into `State` — blob-stuffing again). Protocol precedent: a JSON-RPC response never mirrors the request's placement; it is a smaller object referencing the request by `id` — exactly `in_reply_to`.

**Why `in_reply_to` and `tag` both exist:** `in_reply_to` is the *framework's* slot key — framework-minted, unique by construction; batch accounting keys on it (§7). `tag` is the *caller's* domain key (the agent's `tool_call_id`), echoed so slot materialization needs no in-process correlation map — the reply is self-describing even for a single non-batch call. Consequence, accepted: `tool_call_id` appears twice on a tool-call message — in `ToolCallRef` (content: part of the tool's input, surfaced as `ToolContext.tool_call_id`) and as the frame's `tag` (transport correlation). Same datum, two roles, two layers; the alternative — the callee copying caller correlation into its reply content — is the convention-coupling this model removes.

**The stamping schema** — every publish, one writer (`_publish_action`):

| Publish | `x-calf-kind` | `reply` | other headers |
|---|---|---|---|
| `Call` / client send | `call` | `None` | `x-calf-route` if the caller set one |
| `TailCall` | `call` | `None` | — |
| `ReturnCall` → `callback_topic` | `return` | `ReturnMessage(in_reply_to=popped.frame_id, tag=popped.tag, parts=coerced value)` | — |
| fault → `callback_topic` (origin or escalation hop) | `fault` | `FaultMessage(in_reply_to=popped.frame_id, tag=popped.tag, error=report)` | `x-calf-error-type` |
| broadcast mirror of a reply-producing hop (`Response` → `publish_topic`) | same kind as the hop | mirror of that hop's reply | `x-calf-error-type` on fault mirrors |
| broadcast mirror of a **no-reply hop** (`Call`/fan-out/`TailCall`/mid-batch `_CONSUMED`/declined no-op) | `call` | **`None` — cleared** | — |
| fire-and-forget terminal fault | no callback publish — mirror + log only (§13) | `FaultMessage` on the mirror | `x-calf-error-type` |

Three rules accompany the table (confirmed 2026-06-11, review rounds 1–2): **a fault publish carries the inbound envelope's `context` unchanged** — handler mutations live on the working copy and die with the faulted turn (an agent's half-committed turn, e.g. a dangling tool-call response under an exhausted self-retry, must not ride out to mirrors and resumption; framework diagnostics in `details` are curated summaries — tool names, error kinds — never raw frame inputs or LLM-emitted args, which keeps §4.3's leak posture intact); **a node that produced no reply this hop mirrors no reply** — the mirror of a mid-batch `_CONSUMED` delivery clears the *inbound sibling's* reply rather than re-broadcasting it under this node's emitter headers (re-broadcasting would leak a foreign output to this node's observers, the same bug class I3 kills); and **`TailCall`'s replacement frame preserves `frame_id`, `tag`, and `overrides`** — it is the *same pending call retargeted*, and a freshly minted id would orphan the caller's slot (the eventual reply's `in_reply_to` must match the id the caller registered at fan-out). Today's `invoke_frame` mints a new UUID7 and drops overrides on the TailCall arm; both change.

### 4.3 `ErrorReport`

A plain pydantic `BaseModel` (not `CompactBaseModel` — all fields always serialize; no `exclude_unset` gotchas on a model whose absence/presence is load-bearing). Lives in `calfkit/models/`.

```python
class FrameRef(BaseModel):
    """Topology-only breadcrumb of one call frame. Deliberately excludes
    input payloads/overrides — frame inputs may carry user data; shipping
    them in every fault is a leak vector. (Confirmed topology-only, 2026-06-11:
    with origin_payload gone, faults carry NO user payloads by construction —
    the first draft's open redaction-rule question dissolved.)"""
    frame_id: str
    target_topic: str

class ErrorReport(BaseModel):
    report_id: str                       # framework-minted UUID7 at synthesis — stable across hops
                                         # and mirrors; the dedup key for ops taps and the future DLT
    error_type: str                      # dotted code, e.g. "calf.model.context_window_exceeded"
    message: str = ""                    # human summary, clamped to 2000 chars (a BEFORE-mode
                                         # clamping validator — never a rejecting constraint, which
                                         # would poison inbound decode; FailedToolCall precedent)
    retryable: bool = False              # ADVISORY — consumers may act on it; framework does not
    origin_node_id: str | None = None
    origin_frame_id: str | None = None
    frame_chain: list[FrameRef] = []     # full chain topology at fault time (confirmed: propagate
                                         # the whole chain — subject to the carriage budget below)
    details: dict[str, Any] = {}         # open, JSON-serializable extension slot
    causes: list[ErrorReport] = []       # recursive — non-empty ⇒ composed of other faults (§4.4)

    @classmethod
    def build_safe(cls, ...) -> "ErrorReport": ...
```

**Carriage budget** (confirmed 2026-06-11, review rounds 1–2 — totality at *construction* is not enough; the report must also be total at *carriage*): the binding constraint is a **total serialized-report cap of 256 KB** — chosen so a maximal in-budget report fits Kafka's default 1 MB `message.max.bytes` in typical envelopes (the per-field caps alone could compose to >1 MB; `State` itself is unbounded, so the *total* guarantee is the strip-and-retry-floor chain below, not the cap). Beneath it: `causes` bounded (depth ≤ 8 — the root report is level 1, so ≤ 7 nested levels below it — ≤ 64 reports total; excess elided with `details["calf.elided"] = n`), `frame_chain` bounded (≤ 64 frames, head+tail elision), `details` size-clamped (≤ 16 KB serialized) and eagerly `to_json`-checked at `NodeFaultError` mint so an unserializable user `details` fails at the keyboard, not on the error path. If a fault publish still fails on size, the framework strips to a minimal report (`report_id`, `error_type`, `message`, `retryable`, origin ids — no `causes`/`details`/`frame_chain`) and retries once before flooring — an oversized fault must not become a new silent-drop class.

- **`error_type` is an open string code, not a closed enum** (confirmed, prior session): grounded in protocol convention — extensible/domain-specific error sets use string codes (OAuth2, Stripe, RFC 9457); closed integer/enum codes are for small universal sets (HTTP, gRPC). calfkit ships known values as constants; consumers must tolerate unknown codes.
- **Naming scheme (confirmed 2026-06-11):** dotted lowercase, framework-minted types under the reserved `calf.` prefix (`calf.model.context_window_exceeded`, `calf.fault_group`, `calf.exception`, `calf.delivery.rejected`, `calf.delivery.undecodable`, `calf.slot.materialization_failed`, `calf.agent.self_retry_exhausted`). User-minted types are free-form outside `calf.`, **enforced at mint**: `NodeFaultError` rejects a user-supplied type under `calf.` with an in-process `ValueError` (fail-fast at the keyboard; the `Call(route=...)` constructor-validation precedent) so consumers branching on `calf.*` can trust the namespace — the framework mints its own `calf.*` reports through an internal factory, never through the public `NodeFaultError` constructor, which is how scenario 26's asymmetry is implemented. Known types ship as a `FaultTypes` constants module, alongside constants for the load-bearing `details` keys (`details.reason` values, the exception-class key, `calf.elided`) so consumers never typo magic strings. (Reserving the `calf.` prefix for *error types* survives the death of the reserved *route* prefix — different namespace, no conflation.) **Additive (2026-06-26, client caller-surface reception arm):** `calf.delivery.malformed` — a reply that **decoded into a valid `Envelope`** but is structurally invalid as a terminal, because its `x-calf-kind` header and `Envelope.reply` slot disagree (`kind=return` with an absent or `FaultMessage` slot, or `kind=fault` with a `ReturnMessage`). Distinct from `calf.delivery.undecodable` — that is an *unreadable body*; here the bytes decode fine but the producer's classification and payload contradict each other. Minted (via the internal `build_safe` factory, never the public `NodeFaultError` ctor) by the **client hub's receive arm** when it cannot materialize a demux'd terminal as its declared kind, so a waiting `result()` raises `NodeFaultError` instead of hanging (caller-surface spec §5.1). On a *node* the same kind↔slot disagreement is a **stray** (§4.1: WARNING + ignore, never the node-own-failure path); the client edge differs deliberately because it has already demux'd a single waiting run by `correlation_id` and must not strand it.
- **Total construction:** `build_safe` mirrors the discipline `FailedToolCall.build_safe` established — the error path must never itself raise (clamping validators, defensive `str()` of exception args). A fault that throws while being built re-opens the silent-drop hole this feature closes. The cause-tree bounding is **iterative and cycle-guarded** (an `id()`-visited set), so a deeply-nested fault group (§4.4) cannot `RecursionError` and a malformed cyclic `causes` cannot loop — either would otherwise degrade the precise elision breadcrumb to a bare `fallback`.
- **Exception identity does not cross the wire.** No serialized exception objects, no class-name dispatch (the `ToolExecutionError.__reduce__` gymnastics are the cautionary tale). The string code is the contract. (ADK contrast: ADK passes live `Exception` objects — valid in-process, impossible here.)
- **`origin_payload` is deleted** (was [PROPOSED] in the first draft): slot identity is transport's job (I1) — `in_reply_to`/`tag` replace it. Faults no longer carry the faulted invocation's input at all, which also shrinks the §4.3 leak surface to `frame_chain` alone.

### 4.4 Nesting and propagation identity (confirmed 2026-06-11)

`causes` makes the report **recursively nestable**, and nesting is produced at exactly three places — all *transformations*:

1. **Batch closure** → one fault group (`error_type="calf.fault_group"`) carrying the unhandled sibling faults in `causes` — `ExceptionGroup` semantics, which modern Python developers know from `asyncio.TaskGroup`. The group is itself a fault: one carrier slot, uniform escalation, a catch-all seam iterates `causes` (or uses `report.walk()`/`report.find(error_type)` — shipped traversal helpers, because groups compose and naive `error_type ==` checks must not silently stop matching the day an agent fans out). **Singleton groups flatten** to the bare child fault (confirmed) — and flattening copies the batch topology metadata (§7) onto the flattened child's `details`, so partial-success visibility survives the flatten (closure is a transformation point; enrichment there does not violate escalation identity).
2. **Deliberate conversion** — a seam or node catches a fault and faults with its *own* (e.g. raising `NodeFaultError` while handling): a new report with the original in `causes` — the `raise ... from` analog.
3. **Recovery-then-failure** — §6.8's single-shot rule: the second error chains the original report as a cause.

**Escalation never wraps.** An ancestor whose `on_callee_error` declines (returns `None`) is *propagation, not transformation* — the Python/Java model: an exception unwinding ten frames is the same object the whole way up; the traceback grows, identity is preserved, and wrapping happens only at an explicit `raise ... from`. Per-hop wrapping would misrepresent a decline as a new failure (the ancestor didn't fail — it wasn't there, per the uniform contract), grow the payload with stack depth, and — decisively — break the slotted-report contract everywhere: `fault.error_type == X` must hold at any depth, in any seam and at the client, without unwrap loops. What changes per hop is only **addressing**: the framework re-stamps `FaultMessage(in_reply_to=..., tag=...)` for each ancestor's pending slot while the `ErrorReport` inside travels untouched. The full trace the final receiver needs is already present: `error_type` (what failed) + `frame_chain` (where, through whom — the traceback analog; the escalation path is this chain in reverse) + `causes` (what it was made of) + the per-hop WARNING logs (§13).

### 4.5 Reply content typing: schema-on-read (confirmed 2026-06-11)

Requests are typed at the **write site**: the callee owns and declares its input schema (`@handler(schema=...)`), validated at its door. Correct, because a request body has exactly one interpreter — the callee's handler code. Replies are typed at the **read site**: the wire carries the generic, discriminated, renderable part vocabulary, and the party that knows the expected type recovers it there (`output_type` → `TypeAdapter` projection — the existing machinery behind `NodeResult` and `ConsumerContext`, now also behind `_output_view` and `CalleeResult`). Correct, because a reply has many generic interpreters that *cannot* know the callee's concrete type — the calling agent's materializer (rendering *any* callee's output into a model-visible message), the catch-all seams, sinks, the fault stream, the client — and topic addressing has no compile-time caller→callee linkage (§6.3 caveat). **Ownership follows the interpreter: one specific interpreter → schema-on-write; many generic interpreters → schema-on-read.**

At the carrier altitude the model is symmetric: the framework owns both carriers (`frame.payload: Any`; the parts vocabulary), endpoints own both contents (input schema declared at the callee's door; output semantics plus the caller's read-side projection). Precedent: JSON-RPC (the protocol owns the response frame; the method owns `result`'s shape; the caller interprets it), MCP tools (JSON-Schema'd arguments in, generic `content` blocks out — the exact split), every LLM tool-calling API, and A2A's Part vocabulary (which `ContentPart` mirrors). The gRPC-style alternative — write-site response schemas per node — was **rejected**: catch-all surfaces go blind on opaque typed blobs, the model boundary needs a universal renderable vocabulary anyway (without it, every caller needs per-callee renderers — the N-codecs disease this revision eliminates), and the static guarantee is unredeemable over topic strings.

**The generic coercion (value → parts)** — one implementation, two call sites (I2: publish chokepoint + slot materialization): `str` → `TextPart`; `list[ContentPart]` → passthrough (the agent's preamble case); `None` → `[]` (extracted back as `None` by the lenient materialization read); anything else JSON-serializable → `DataPart` — preserving `tool.py`'s eager `pydantic_core.to_json` wire-safety check (a non-serializable value faults via `on_node_error` instead of killing the publish mid-handler).

> **⚠️ CORRECTION (PR-6 / 2026-06-17):** the `calf.retry` carriage shipped as "option 1" — the wire carries the **raw** message text plus a `{calf.retry: True}` metadata marker, and the agent re-renders `RetryPromptPart` on materialization. The "error message plus the standard fix-and-retry suffix, the same rendering `RetryPromptPart.model_response()` produces" wording below is superseded: pre-rendering the suffix at origin would **double** it, since every provider already calls `model_response()` when it materializes the `RetryPromptPart`. The marker + provider-fidelity rationale (Anthropic `is_error`) is otherwise intact.

**`ModelRetry` is rendered text plus a retry marker** (confirmed, Ryan; marker added 2026-06-11, review round 1): a tool's `ModelRetry` is caught in the tool body and rendered *at origin* into self-contained text — the error message plus the standard fix-and-retry suffix, the same rendering `RetryPromptPart.model_response()` produces — and ships as an ordinary `ReturnMessage` with `TextPart(text=..., metadata={"calf.retry": True})`. The marker exists because the original "no wire-observable work" claim was verified **provider-asymmetric**: on OpenAI a tool-bound retry prompt and a `ToolReturn` map to the same message shape, but the Anthropic provider maps `ToolReturnPart` → `tool_result(is_error=False)` and a tool-bound `RetryPromptPart` → `tool_result(is_error=True)` — text-only would silently drop the error flag Anthropic models are steered by. The agent's materializer (§6.9) honors the marker by materializing `RetryPromptPart` instead of `ToolReturn` — full provider fidelity — and the marker doubles as the retry-counting hook the successor retry issue needs. "Recoverable error" still isn't a protocol concept — **the wire knows exactly two reply outcomes: `return` and `fault`**; the marker is one documented metadata convention on ordinary content, ignorable by every non-agent consumer (they see plain text). Rendering at origin keeps the reply self-contained for *any* caller, mirroring how MCP clients treat `isError=True` results. *(⚠️ PR-6 / 2026-06-17: the marker is applied only to a tool-raised `ModelRetry`. MCP `isError=True` is passed through **transparently/unmarked** in the temporary PR-6 implementation — its `calf.retry` marking is deferred to a follow-up PR; see the §9 CORRECTION.)*

### 4.6 Decision record — carriage

| Option | Verdict |
|---|---|
| Slot in `State` (early lean) | Rejected: `State` is model-facing — projected into LLM context, deserialized as `NodeResult`. Control-plane faults in it can leak to the model. Protocol survey: no protocol buries the error inside the success payload (JSON-RPC `result` XOR `error`; GraphQL `data` + `errors` siblings; gRPC trailers). |
| Frame-payload reuse + reserved route values (`x-calf-route: calf.return`/`calf.fault`) — the first draft's chosen design | **Superseded (2026-06-11).** It required four patch rules: the dead-payload frame overwrite, overloading the route header against its documented ingress-only contract (`_protocol.py`), `origin_payload` as content-borne correlation, and the load-bearing "fault must never match `'*'`" rule. Needed patch rules = wrong factoring; all four are structurally unnecessary under the reply slot. |
| **Per-delivery `Envelope.reply` + `x-calf-kind` + frame `tag` echo (chosen)** | Correlation in transport (JSON-RPC `id` / AMQP `correlation_id` precedent); content as parts (I2); delivery classification fully decoupled from the user routing feature; returns and faults symmetric (`result` XOR `error`); per-delivery reply kills the stale-output-slot bug class; and any `ReturnCall`-ing node becomes a protocol-correct callee — agent-as-callee and custom nodes compose without adapters (a prerequisite the agent-discovery work otherwise had to build as an "ask adapter"). |

---

## 5. The fault lifecycle, end to end

```
                                                   ┌─ recovered (a handler returned a value) ─→ normal output rail
raise in node work ──→ ErrorReport synthesized ──→ on_node_error chain
                       (NodeFaultError converts        │ unhandled (all declined)
                        verbatim and BYPASSES          │
                        on_node_error — §6.5)          ▼
                              fault takes the node's success rail (P2):
                ┌─────────────────────────────────┼──────────────────────────────┐
        ReturnCall-bound                    broadcast-bound                fire-and-forget
   (frame has callback_topic)            (static publish_topic)        (bottom callback=None)
                │                                 │                              │
   publish to callback_topic            publish to publish_topic          floor (§13)
   kind=fault, reply=FaultMessage
                │
        arrives at the caller
                │
        on_callee_error chain ──→ handled (returned value) ─→ slot resolves as callee output
                │ unhandled
                │
        in a fan-out batch? ──→ mark slot failed; batch continues (aggregate-all);
                │  no             at closure all unhandled faults escalate as one fault
                │                 group (singletons flatten, §4.4; skips before_node,
                │                 the body, and after_node — nothing to guard)
                ▼
        unwind own frame, publish fault to next callback_topic   ← escalation hop
                │  ... repeats frame-by-frame (P5), stateless per hop (§6.7) ...
                ▼
        bottom of the stack = the workflow's reply address:
          • client reply future  → raises NodeFaultError(report)        (§11)
          • consumer-node sink   → consume body, surfaced as ctx.fault  (§6.6)
          • callback_topic=None  → floor (§13)
```

Escalation is **frame-by-frame in v1** (confirmed): semantically, the fault visits each ancestor's `on_callee_error`; mechanically each hop is one Kafka delivery. Since seam registration is static per node class, a later optimization can stamp "intercepts faults" on each `CallFrame` at `Call` time and route in one hop to the nearest interested frame — same observable semantics, fewer hops/loss-windows. **Explicitly deferred** (ADR-0003).

A fault is **also** mirrored on the broadcast rail at each producing node whose `publish_topic` exists (the fault-stream floor, §13), so ops can tap faults with an ordinary consumer even when every seam declines.

---

## 6. Policy seams

### 6.1 Surface

Four seams on `BaseNodeDef`; **every caller-capable node type inherits all four** (agent, tool, MCP toolbox, custom). Consumers/observers have none — `ConsumerContext` is their entire surface, and registering a seam on one raises at startup (§6.6; confirmed 2026-06-11, review round 1). Mirrors ADK's seam family (`before/after_agent`, `on_model_error`, `on_tool_error`) reshaped to calfkit's node altitude.

```python
async def handle_overflow(ctx: SeamContext[State], fault: ErrorReport) -> str | None:
    if fault.error_type == FaultTypes.MODEL_CONTEXT_WINDOW_EXCEEDED:
        return "research failed: context overflow — retry with a smaller scope"
        # → resolves the slot as the callee's output; at batch closure the agent's
        #   model sees it and re-issues the call (the agentic retry).
    return None                                  # decline → next handler in the chain
    # NB: unbounded by itself — the bounded equivalent is
    # surface_to_model(max_failures=3, only_types=[FaultTypes.MODEL_CONTEXT_WINDOW_EXCEEDED]).
    # `==` is safe HERE (slot level: singleton flattening preserves identity); at the
    # client use report.find() — groups compose (§4.4, §11).

node = SomeNodeDef(
    node_id="orchestrator",
    ...,
    before_node=[auth_check],                  # constructor: list of callables, or single callable
    on_callee_error=[handle_overflow,          # SPECIFIC handlers BEFORE catch-all converters:
                     surface_to_model(max_failures=3)],   # surface_to_model returns non-None for
)                                              # every in-budget fault — anything after it is dead code
```

**Chain-order trap, stated plainly:** first-non-`None` wins, and constructor entries precede decorator entries — so a catch-all prebuilt registered in the constructor makes a later-decorated specific handler unreachable. Register specific handlers first (one constructor list, as above), or scope the prebuilt (`surface_to_model(only_types=...)`/`exclude_types=...`). The decorator form (`@node.on_callee_error`) appends to the same chain and returns the function unchanged (the `gate()` precedent), so decorated handlers stay directly unit-testable. Framework prebuilts carry a totality marker (`only_types`/`exclude_types` both unset ⇒ total): registering anything *after* a marked-total handler in the same chain WARNs at startup — dead chain entries are detectable, so they are detected (the §6.6 registration-validation principle, applied uniformly).

- **Catch-all, not per-type** (confirmed): one seam per cross-section; type branching happens *inside* the handler body. Per-type registration was rejected as too low-level for day one — it demands taxonomy knowledge before the first handler and makes the catch-all the awkward case. Per-type dispatch can arrive later as sugar without redesign (the `x-calf-error-type` header already carries the key).
- **Values-only seams** (confirmed): `on_callee_error` and `after_node` return *values*, never actions. For `on_callee_error`, a direct framework-level re-`Call` of the failed callee is deferred — it requires batch-slot re-binding machinery that is undesigned, and ADK's `on_tool_error` carries the same restriction (its retry plugin retries *through the model*, not by re-executing). `before_node` and `on_node_error` (the boundary seams) accept actions (redirects via `Call`, `TailCall` self-retry). **There is no silent-drop gesture anywhere** (confirmed 2026-06-11, §10): a seam substitutes an output, declines with `None`, or raises — nothing else.
- **Chains:** each seam accepts a single callable or a list; the list runs in order; **first non-`None` return wins** and stops the chain (ADK semantics). Sync or async callables.
- **Registration (locked): instance-level only in v1** — constructor parameter and instance decorator, both appending to the same chain in registration order. Class-level method decoration (for node-type authors) and worker/app-level plugin layers (ADK's plugin tier) are explicit future options, not v1.
- **Seam handlers receive `SeamContext`, not `SessionRunContext`** (confirmed, Ryan's requirement): a capability-scoped view (§6.3) — ADK's `CallbackContext`-vs-`InvocationContext` split is the precedent; `ConsumerContext` is the in-house idiom it mirrors.
- **No subclass alias names** (confirmed): `before_model` etc. are *not* aliases for `before_node` — in ADK those are genuinely different seams (per-LLM-request vs per-invocation), and an agent invocation that is mid-aggregation makes no model call at all. Inner seams may be added later as *separate* seams if demand appears; the names stay reserved, not spent.

### 6.2 The uniform contract (P4)

> **Whatever a seam returns becomes the output of the thing that seam guards. `None` means you weren't there.**

| Seam | Guards | Fires when | Return a value → | Return `None` → |
|---|---|---|---|---|
| `before_node` | the node's body | the body would run (see §6.4) | the node's output; **body never runs** (mock / cache / refusal) | body runs |
| `after_node` | the node's produced output | an output was produced (body, short-circuit, or recovery) | **replaces** the output | output kept |
| `on_node_error` | the node's own failure | the node's own work raised uncaught | the node's output (**recovered**) | fault synthesizes and escalates |
| `on_callee_error` | one callee's failed result | a fault arrives from a node this node called | the callee's output (**slot resolves**, handled) | unhandled → escalation (after batch closure for fan-outs) |

The per-seam meanings (skip / replace / recover / resolve) follow from *position*; nothing is memorized per seam. This deliberately fixes ADK's two documented irregularities: return-value polymorphism (skip vs replace vs recover varying by seam name) and the truthiness-vs-`is not None` predicate split. calfkit uses **`is None` / `is not None`** everywhere, full stop.

**Teaching guards at coercion** (confirmed 2026-06-11, review round 1 — each closes a migration trap that would otherwise corrupt silently): a seam returning a **`bool`** is rejected with `SeamContractError` ("a bool is never a node output; return `None` to proceed, substitute a real output or raise to reject") — the predictable port of a gate's `return True` must not short-circuit every happy path with `True` as the node's output (note: the guard checks `bool` *before* `int`, since `bool` subclasses `int`). A seam returning the node's **`StateT` or the `SeamContext` itself** is rejected the same way ("mutate `ctx.state` in place and return `None`") — the reflexive functional idiom must not serialize the whole session record, history included, out to the caller as the node's "answer". A **`bytes`** return is rejected the same way — no parts arm exists for it (§6.3/§4.5). These guards fault loudly per P1; they never pass garbage downstream.

### 6.3 Seam handler interfaces, the seam context, and return-value interpretation

**Typed interfaces.** `BaseNodeDef` is `Generic[StateT, OutputT]`; each caller-capable node type pins `OutputT` so handler authors get concrete signatures (the agent reuses the existing `AgentOutputT` from `calfkit/_types.py`). The callable aliases are named `*Seam` per the glossary ("hook" stays reserved for lifecycle hooks):

```python
MaybeAwaitable: TypeAlias = R | Awaitable[R]                  # handlers may be sync or async
Serializable:   TypeAlias = (str | int | float                # bool and bytes EXCLUDED (§6.2 guard;
                             | dict[str, Any] | list[Any] | BaseModel)   # bytes has no parts arm)

BeforeNodeSeam:    (ctx: SeamContext[StateT])                -> MaybeAwaitable[Serializable | NodeResult[StateT] | None]
AfterNodeSeam:     (ctx: SeamContext[StateT], output: OutputT) -> MaybeAwaitable[Serializable | None]   # values only
OnNodeErrorSeam:   (ctx: SeamContext[StateT], fault: ErrorReport) -> MaybeAwaitable[Serializable | NodeResult[StateT] | None]
OnCalleeErrorSeam: (ctx: SeamContext[StateT], fault: ErrorReport) -> MaybeAwaitable[Serializable | None]   # values only

class AgentNodeDef(BaseNodeDef[State, AgentOutputT]): ...     # final_output_type-typed view
class ToolNodeDef(BaseNodeDef[State, ToolOutput]): ...        # ToolOutput = Any wire-serializable
# ConsumerNode: no seams (§6.6) — it is not a BaseNodeDef[..., OutputT] participant in this table
```

Static-typing caveat (discussed, accepted): a node's generics type its *own* seam handlers, not its callees' — callees are addressed by topic string, so there is no compile-time caller→callee output-type linkage (`CalleeResult.value` is `Any`). A future `Call[OutT]`/typed-stub feature could add it; out of scope.

**`SeamContext` (confirmed, Ryan's requirement).** Seam handlers do not receive `SessionRunContext` — exposing the internal context (transport stamping, `_frame_id`, resource plumbing) invites workflow-breaking mutation. `SeamContext` is a capability-scoped façade in the `ConsumerContext` idiom (precedent: ADK callbacks receive `CallbackContext`, never the full `InvocationContext`):

> **⚠️ CORRECTION (PR-6 / 2026-06-17):** in the `CalleeResult` model below, `target_topic` is implemented as `str | None` (`None` for a single-call reply), not the bare `str` shown.

```python
class SeamContext(Generic[StateT]):
    state: StateT                          # MUTABLE — the official input-transform channel.
                                           # Mid-batch caveat: on sibling deliveries this is the
                                           # sibling's traveling copy; only framework-computed fields
                                           # (slot results; the §7.6 budget tally) survive closure.
    deps: Mapping[str, Any]                # read-only
    resources: Mapping[str, Any]           # read-only (auth clients etc.)
    payload: Any | None                    # read-only inbound frame payload (tool: its ToolCallRef)
    node_id: str
    correlation_id: str
    emitter_node_id: str | None            # x-calf-emitter (mirrors ConsumerContext's field name)
    route: str | None                      # inbound route key (call-kind ingress only)
    delivery_kind: KindValue               # call | return | fault — distinguishes ingress firings
                                           # from continuation/closure firings (§6.4)
    awaiting_reply: bool                   # True iff THIS node is the addressee of a reply-owing
                                           # frame — the §10 hard-vs-soft-reject discriminator
    callee_results: list[CalleeResult]     # all resolved slots, dispatch order; [] on ingress
    failing_call: CalleeResult | None      # during on_callee_error ONLY: the transport view of the
                                           # call whose fault is being handled (tag/target_topic/
                                           # frame_id/fault set; handled pending). I1-compatible:
                                           # the tag stays out of ErrorReport; it is shown to the
                                           # one party deciding the slot's fate. None elsewhere.
    exception: BaseException | None        # during on_node_error ONLY: the live in-process
                                           # exception (ADK parity; retry policy needs exc state).
                                           # Never serialized; the report stays the wire contract.
    @property
    def callee_result(self) -> CalleeResult | None: ...   # convenience: the single-call case
    # NOT exposed: workflow state / call stack, frame mutation, transport stamping

class CalleeResult(BaseModel):
    frame_id: str                          # the reply's in_reply_to — the framework slot key
    tag: str | None                        # the caller's correlation token, echoed back (§4.2)
    target_topic: str
    parts: list[ContentPart] | None        # raw reply content (returns / handled substitutes)
    value: Any | None                      # convenience view of parts (confirmed 2026-06-11):
                                           # auto-extracted — DataPart.data first, TextPart.text
                                           # fallback, empty parts → None; raw parts kept alongside
    fault: ErrorReport | None              # set when this slot faulted (handled or not)
    handled: bool                          # True if on_callee_error resolved it
```

One context type for all four seams in v1; per-seam capability narrowing (e.g. read-only state on `after_node`) is a compatible later refinement.

**Return-value interpretation — two tiers (confirmed):**

1. **Plain serializable** — the 80% gesture, playing into the "a node is a unit that returns text data" mental model. Coercion is **generic for every caller-capable node type** (revised 2026-06-11): the value becomes the node's output via `ReturnCall(state, value=...)` and the one value→parts coercion (§4.5) — agent, tool (no requirement to match the tool's declared schema, confirmed — ADK parity), and custom `BaseNodeDef` subclasses alike, with one exception, precisely scoped (review round 2): **an agent's seam substitute in *output position* — `before_node` short-circuit, `on_node_error` recovery, `after_node` replacement — is validated against its declared `final_output_type` at coercion**; a structured-output agent's contract is machine-projected by its callers, and a type-breaking substitute must fail at the seam that broke it, not as a `DeserializationError` in a different process. `on_callee_error` substitutes are **exempt**: they are *callee-output* substitutes materialized at the slot (§6.9), not the agent's own output — `surface_to_model`'s failure strings must never be validated against the agent's output type (scenario 44). (Consumers have no seams at all, §6.6, which also retires the first draft's per-type coercion carve-outs and the false "a consumer's ReturnCall has no callback" claim — a tapped consumer's envelope can carry live frames; observers simply never produce.)
2. **Typed framework objects** — the low-level opt-in, `isinstance`-checked **before** the serializable interpretation: a `NodeResult` action from a *boundary* seam (`before_node`, `on_node_error`) executes as the node's action; future policy objects (`Retry(...)`, `Transform(...)`) slot in here without breaking tier 1. The *output* seams (`after_node`, `on_callee_error`) are values-only (§6.1).

**Input transformation** (e.g. rewriting the prompt in an agent's `before_node`) is done by **mutating `ctx.state` in place and returning `None`** — the mutation channel ADK developers already use (`llm_request` mutation). Ryan's earlier per-node-type proposal (agent `before_node` return = LLM input; tool `before_node` return = output) was withdrawn in favor of this uniform rule (confirmed).

**Ownership model — conversions vs transitions (confirmed; restated post-collapse):** the base owns every *transition* (stage ordering, escalation, batch closure, publish rails — §6.8, framework-final) **and** the generic *conversions* (one value→parts coercion with two call sites, one parts→value projection — §4.5/I2). A node type may override only slot materialization, and exactly one does: the agent's `_resolve_slot` (§6.9) — the SDK's single per-type codec. The conversion *trigger points* are fixed (body output, `before_node` short-circuit, `on_node_error` recovery, `after_node` replacement, slot materialization — distinct from I2's two *execution* sites), which is the single-writer property that keeps seam handling free of per-seam special cases.

### 6.4 `before_node` firing rule (confirmed, Ryan's refinement)

`before_node` fires on **every inbound delivery on which the body would run**, with full gate parity for admission control. The refinement: in a fan-out, mid-batch sibling arrivals are framework slot-keeping — the body doesn't run, so `before_node` doesn't fire. It fires **once, at batch closure, post-aggregation**, with `ctx` containing all assembled results, immediately before the body processes them. This is not an exception to the contract; it *follows* from "guards the body."

Consequences (explicit):
- Non-batch deliveries (ingress, single-`Call` returns): `before_node` per delivery, as gates today.
- A batch that closes with unhandled faults **escalates instead of running the body — `before_node` is skipped** (nothing it guards will run), and so are the body and `after_node`. Confirmed: the error path takes precedence over the guard.
- Mid-batch sibling deliveries are not individually gateable. Their fault arm IS individually visible (`on_callee_error` runs per arriving sibling fault, §7).
- **The firing shape differs from ADK's `before_agent`** (once per invocation) — a `before_node` handler also fires on continuation deliveries and at batch closure. Handlers that are ingress-shaped (auth, cache, mock) must discriminate on `ctx.delivery_kind` (and note `ctx.route` is `None` off-ingress): a cache handler that short-circuits on a *continuation* delivery replaces the in-flight workflow's continuation with the cached terminal answer and discards the batch's work. One documented edge: the agent's self-retry continuation arrives as `kind=call` on the private continuation inbox (§4.1), so `delivery_kind == "call"` alone admits it — handlers needing strict ingress should also check `ctx.route is not None` or treat the trap as acceptable for their use. The seam docstring carries both traps with the cache counter-example (review rounds 1–2).
- **Gate-parity scope:** `before_node` gives full gate parity for *admission control on reply-owing work*. A gate that *filtered* fire-and-forget traffic (drop most, pass some) has no seam equivalent — `None` proceeds, a substitute fabricates an output, a raise floors a fault per message; that pattern migrates to route-handler decline (all-declined f-a-f = DEBUG no-op) or a consumer, stated in §15 (review round 2).

### 6.5 Seam failure semantics (closes ADK's coverage gap; revised 2026-06-11, review round 1)

- **The mint rule, first:** a `raise NodeFaultError(...)` anywhere — any seam, any body — **bypasses `on_node_error` entirely** and converts verbatim at synthesis (§6.7). It is not an accident in need of recovery; it is the node's deliberate error decision (the §11 mint gesture). Without this rule a generic `on_node_error` recovery can convert a deliberate typed fault into a *success* — the swallow trap, demonstrated in review.
- A non-`NodeFaultError` raise **inside `before_node`/`after_node`** is a node-own accident → routed to `on_node_error` (the original inbound fault, if any, attached in `causes`). In ADK, a raise in `before_tool` bypasses `on_tool_error` entirely and is terminal — we deliberately do not import that.
- A raise **inside `on_callee_error` is slot-scoped, never a node-own failure**: the slot being handled resolves as *failed*, carrying the raised error (a `NodeFaultError` honored verbatim — the per-slot transformation gesture; anything else wrapped as `calf.exception`) chained to the inbound fault via `causes`; the batch continues and escalates at closure per §7. Routing it to `on_node_error` was incoherent mid-batch (one sibling's seam bug would mint a whole-invocation outcome while siblings are in flight — double replies, leaked batches) and is the Python analog done right: a raise in an `except` block propagates *outward* (up the batch/escalation axis), not sideways into a sibling handler.
- An *accidental* raise **inside an `on_node_error` handler** means *that handler declines* — logged, noted on the report — and the chain continues to the next registered handler (a deliberate deviation from pluggy's propagate-on-raise, pinned); when all decline, the *original* fault escalates. No infinite regress, and a second registered recovery still gets its chance. A **`NodeFaultError`** raised inside the chain is the mint gesture, not an accident: the chain stops and the minted fault converts verbatim with the original chained via `causes` (§6.8's `_Minted`).
- A seam can never produce *nothing* — the contract is total: substitute a value, decline with `None`, or raise (→ fault rail). `Silent` is removed from the vocabulary outright (§10), so the industry-standard hook off-ramps (substitute-or-raise — ADK callbacks, Flask `before_request`/`abort()`, Rails halt-by-render/`throw :abort`, Spring `preHandle` with the response written) are the only off-ramps.
- **Seam liveness is an explicit non-goal (v1):** a handler that never returns blocks the consume loop until the broker's `max.poll.interval.ms` evicts the consumer — deadline enforcement belongs to the successor retry/liveness issue; stated so it is a recorded acceptance, not an oversight.

### 6.6 The observer surface (confirmed 2026-06-11; hardened in review round 1)

Consumers are **observers**: they make no `Call`s and they own their whole body (`consume_fn` is entirely user-authored — the §9 rationale for seams, "developers lack authorship inside framework bodies," does not apply to them; their guard is an early return, their error handling is `try/except`). Therefore:

- **Consumers have no policy seams.** Registering any seam on an observer raises at startup with a redirect ("consumers receive faults as `ctx.fault` in the consume body; seams exist only on nodes that call"). This replaces the earlier inert-`on_callee_error` shape: silently accepting registration that can never fire violates the registration-time-validation principle, and the "loose name" question dissolves with the surface.
- **`ConsumerContext` is the entire observer surface**, extended with `fault: ErrorReport | None` (projected from a `kind=fault` delivery's reply; `output=None` there) and `delivery_kind: KindValue` (so a tap on a mixed-kind topic can tell an empty return from a call hop; the documented sink idiom checks `ctx.fault` *before* the `output is None` early-return). Every kind reaches the consume body as an observation — addressed reply-topic subscriptions and broadcast mirrors alike.
> **⚠️ CORRECTION (PR-6 / 2026-06-17):** `ConsumerContext.fault` and `ConsumerContext.delivery_kind` are part of the **deferred reception PR** and are **absent in PR-6** (the shipped `ConsumerContext` has only `output`/`state`/`correlation_id`/`output_parts`/`emitter_node_id`/`emitter_node_kind`/`deps`/`resources`). In PR-6 a `kind=fault` delivery still reaches the consume body, but the fault content is invisible — it projects to `output=None`/`output_parts=[]`, with no `fault` field to read and no `delivery_kind` discriminator. The fault-tap idiom (`if ctx.fault:`) lands with the deferred reception PR. (Matches the deferred-reception scope; consistent with the deferred annotations elsewhere in this spec.)
- **Observers never produce on the workflow's rails** (review round 1 — this closes a forgery hole): a tapped consumer's envelope can carry *live* frames (a tool-input tap carries the real caller's `callback_topic`), so the observer rule extends to the producing side — a consumer never unwinds an observed frame, never publishes point-to-point, and never auto-faults (`awaiting_reply ≡ False` on observers; "reply-owing" means *this node is the frame's addressee*, §10). A consumer's own `consume_fn` raise is floor-logged (ERROR) — today's blanket swallow in `consumer.py` is deleted (§15) — and since `ConsumerNode` has no `publish_topic`, consumer-origin faults are log-only floors by construction (§13).

This is what makes the §13 fault stream literally "tappable by any ordinary consumer": a fault tap is a plain `@consumer` with an `if ctx.fault:` branch. A sink that ignores `ctx.fault` loses nothing — floor logging already happened at the producing hops. The slotted-report uniformity holds with three role-native surfaces: seams receive it as the `fault` argument, the client as `NodeFaultError.report`, sinks as `ctx.fault`.

### 6.7 Internal dispatch mechanics (revised 2026-06-11, review round 1)

- **The pre-handler floor.** An envelope that fails pydantic validation never reaches any handler — FastStream captures the parsing error, logs generically, and the ack-first offset means the message is gone. The framework therefore installs a broker-level decode shim (the verified mechanism: body decode and `Envelope` validation both run lazily inside the consume scope a middleware wraps — calfkit's `ContextInjectionMiddleware` is the in-house precedent): a typed `calf.delivery.undecodable` floor event with the raw-message metadata. Three pinned constraints (review round 2): the shim **re-raises after flooring** (suppressing would push an empty body through the attached `@publisher` — the known FastStream empty-payload trap); it hooks **both** the consume scope and the after-processing path (parser-stage failures bypass the former); and at nodes routing is impossible — the return address is inside the unreadable body — so floor-only is the ceiling, stated honestly (P1). The client's reply subscriber goes one better: it fails the pending future (§11).
> **⚠️ CORRECTION (PR-6 / 2026-06-17):** the client-edge "goes one better — fails the pending future" behavior is **DEFERRED to the reception PR** and is **NOT in PR-6**. PR-6 floors + re-raises at the client reply subscriber **exactly like a node** (the `DecodeFloorMiddleware` floors `calf.delivery.undecodable` and re-raises), so the re-raise never reaches the reply dispatcher and the pending future is neither resolved nor failed — an undecodable **reply** hangs the caller until `reply_ttl` (which defaults to `None` ⇒ hangs indefinitely). **Floor-only is the ceiling at the client too in PR-6.** (Consistent with the deferred-reception scope; the code-side docstrings already say so.)
- The chokepoint (`BaseNodeDef.handler`) implements P1 for everything that decodes: **the fault boundary encloses the entire pipeline** — context construction and classification (stage 0), the seam stages, the body, *and the terminal publish* (§6.8). Nothing escapes to FastStream.
- **Classification reads `x-calf-kind`** (§4.1; total contract there — missing ⇒ `call`, unknown ⇒ ERROR + ignore, kind/slot disagreement ⇒ stray). Fault and return deliveries carry no `x-calf-route`, so they *structurally cannot* reach routed handlers or the `'*'` catch-all — classification precedes routing, and only `call`-kind deliveries enter the route CoR **on caller-capable nodes**; on observers every kind reaches the consume body (§6.6).
> **⚠️ OUTDATED (PR-6 / 2026-06-17):** the second half of the bullet below (the "**The `fanout_id` marker completes it**" sentence onward) says the slot outcome "is published to the aggregator" and that post-closure strays "are detected and floored **at the aggregator** (lookup miss + the refresh rule — companion spec §5)," with the node holding "zero batch state" and its `on_callee_error` handlers tolerating "firing for outcomes that are subsequently floored." That is the deleted v1 separate aggregator — see `in-node-fanout-aggregation-spec.md` §6.7/§4.3. The node now holds **durable-backed** batch state: the marker routes a sibling reply into the node's own `_aggregate` fold (the outcome lands in the node's `state` ktable), and the stray check runs **before** stage 1, so foreign/dup/post-closure siblings floor **in-node** and never run `on_callee_error` (no must-tolerate-later-floored caveat). The stateless-continuation / unmarked-closure mechanics are unchanged.

- **Continuations are stateless by default — the batch-of-one rule.** Slots are registered only for `list[Call]` fan-outs (§7). A `return`/`fault` arriving where **no batch is registered** for the inbound frame is a *stateless continuation*, validated against the envelope's durable stack (after the callee's pop, the top frame is this node's own invocation by construction): a return proceeds to the body (today's sequential path, rebalance-tolerant by construction); an unhandled fault escalates — pop, re-stamp, publish — with **no in-process state required**. This is what keeps sequential agents working and every mid-chain escalation hop stateless; a batch-of-one is never registered (registering one would make in-process state load-bearing for all calls — a durability regression — and contradict the self-describing-reply property of §4.2). **The `fanout_id` marker completes it** (review round 2; re-homed to the durable aggregator 2026-06-12): a fan-out sibling's reply is *not* a stateless continuation — its top frame carries the durable marker (§4.2), which routes it into the aggregation protocol: stage 1 runs on the node (per-sibling seams preserved), the slot outcome is published to the aggregator, and the body never runs on a sibling delivery. Post-closure strays are detected and floored **at the aggregator** (lookup miss + the refresh rule — companion spec §5; the node, holding zero batch state, cannot and does not make that call — its `on_callee_error` handlers must tolerate firing for outcomes that are subsequently floored). Without the marker, a sibling reply would pass the stack validation and body-dispatch as a single-call continuation — duplicate execution forking the workflow. Closure arrives as the re-entry (`kind=return`, `in_reply_to = fanout_id` — the node's own frame id, which no sibling slot ever equals), built from the pre-stamp snapshot and therefore unmarked, making the next single call automatically stateless.
- **Stray detection precedes the seams.** Where a batch *is* registered, "pending slot" means an *unresolved* slot of the open batch; a `return`/`fault` whose `in_reply_to` matches none — foreign, duplicate (producer retries can duplicate; a resolved slot is no longer pending), or post-rebalance straggler — is a **stray**: detected at slot lookup *before* `on_callee_error` runs (seam side effects and budgets must not fire for faults that were never yours, and a "handled" value with no slot to resolve would be a new silent swallow). A stray **return** is WARNING + ignore; a stray **fault** takes the floor — ERROR with the full `ErrorReport` JSON + broadcast mirror where a `publish_topic` exists — never a bare warning (P1's floor arm survives lost batches).
- Exception → `ErrorReport` mapping: a raised `NodeFaultError` (see §11 — one class for minting and receiving) converts verbatim (its `error_type`, `retryable`, `details` honored) and **bypasses `on_node_error`** (the mint rule, §6.5). Any other exception maps to `error_type="calf.exception"` with the exception class name and clamped message in `details`, plus the live exception exposed in-process as `ctx.exception` for the recovery seam (§6.3). Provider-classified exceptions (§12) carry their own `error_type`.

### 6.8 Implementation architecture: the staged pipeline (locked)

Internal structure per the canonical hook-host pattern — **closed skeleton on the base + hooks as registered data + narrow designated extension points** (Flask `full_dispatch_request`; ADK `BaseAgent.run_async` + canonical resolvers; Rails `run_callbacks`; pluggy `_multicall`):

- **Stage order is a framework constant, not a route-string convention.** The wire carries only the *delivery kind* (`x-calf-kind: call | return | fault` — a sender-side fact, §4.1); the receiver-side pipeline orders the stages. Encoding stages into compound route keys was considered and rejected: senders can't know the receiver's pipeline, pattern specificity can't express "before the body, for every key", and `'*'` is already owned by `run()`. (An earlier intermediate — reserved `calf.*` values on `x-calf-route` — was also rejected: it overloaded the user routing feature with framework classification, §4.6.)
- The chain-runner (`run_chain`: in-order, sync-or-async, first non-`None` wins, `None` = decline — `Next` is route-CoR vocabulary, not a seam gesture) is a **pure module-level function** (`calfkit/nodes/_seams.py`, no transport imports) — pluggy's `_multicall(firstresult=True)` semantics for the first-result rule. `run_chain_guarded` exists only for `on_node_error`, with two deliberate behaviors: an *accidental* raise in one handler is logged, noted on the report, and treated as *that handler's* decline — the chain continues; all-declined ⇒ the ORIGINAL fault escalates (no regress; §6.5); a **`NodeFaultError`** raised in a handler stops the chain and converts verbatim with the original report chained via `causes` (the mint rule must work inside the error seam too — returned to the skeleton as `_Minted`).
- Seam chains are **normalized and validated at registration time** (callable + arity checks; observer rejection §6.6; startup failures, never mid-message).
- Batch outcomes are an explicit closed vocabulary — `_BatchOpen | _BatchClosed | _BatchFaulted(report)` — and `_BatchFaulted` is **returned, not raised**, so a callee's failure can never trip the node's *own* error seam. Slot outcomes share one vocabulary: `_SlotResolved(parts)` (a return, or a handled substitute coerced to parts at materialization — I2's second call site) and `_SlotFailed(report)`.

> **⚠️ OUTDATED (PR-6 / 2026-06-17):** several comments in the `handler`/`_execute` code sketch below describe the deleted v1 *separate* fan-out aggregator — "Post-closure batch strays floor at the AGGREGATOR, not here"; the `_publish_abort` arms saying "the AGGREGATOR escalates to the caller exactly once (companion spec §5) — the node must not also fault directly"; and "published siblings' replies stray-floor at the aggregator." Aggregation is now the durable **in-node** fold — see `in-node-fanout-aggregation-spec.md` §4.4 (abort) and §6.7. `_publish_abort`'s *semantics* change: instead of publishing a record the aggregator reads, it **tombstones both node-scoped ktables and escalates the node-own fault to the caller in-node, exactly once** (`abort` source = the INBOUND delivery's stack — see CORRECTION below). Post-closure / published-sibling strays floor **in-node** (at the durable store's fold). Closure recognition is implemented in a **dedicated `_classify_fanout` method** (`in_reply_to == current_frame.frame_id` ⇒ the self-published closure re-entry → routed as a re-entry), **NOT as an arm on the sealed `_stray_check`** (which is unchanged): the self-published closure re-entry is a shape-valid `return` + `ReturnMessage`, so it simply **passes** `_stray_check`'s kind↔slot-shape agreement test and is then classified as a re-entry by `_classify_fanout` — stray-exempt by **routing**, not by a new check.

> **⚠️ CORRECTION (PR-6 / 2026-06-17):** the in-node abort's "escalates … exactly once" invariant holds **only under a *writable* store**. If the durable store is **unavailable** (terminal reader death — the §5.1 abort trigger), the tombstone write itself fails, so each still-pending sibling arriving during the outage independently aborts + escalates → the caller may receive up to **one fault per pending sibling**. Accepted operational residual (v1 = at-most-once; the in-node design holds zero in-process batch state, so there is no cheap in-process dedup). See `in-node-fanout-aggregation-spec.md` §4.4 and §13 below (residual-loss-windows).

> **⚠️ CORRECTION (PR-6 / 2026-06-17):** the abort caller-source is the **INBOUND delivery's stack, NOT basestate** — basestate is `None` precisely in the `basestate_missing` abort, so reading the caller from basestate would fail in the one case it must not. See `in-node-fanout-aggregation-spec.md` §4.4.

> **⚠️ CORRECTION (PR-6 / 2026-06-17):** there is **no `_in_batch_work` arm at the publish-guard** shown below — it is provably unreachable, because `_publish_action` is only ever reached with an *unmarked* frame (a marked sibling delivery returns `_CONSUMED` before the publish guard). The `_in_batch_work` arm at the boundary stage-5 `on_node_error` (the `except Exception` arm) **is** present.

> **⚠️ CORRECTION (PR-6 / 2026-06-17):** new fault vocabulary was implemented for the in-node abort path: `FaultTypes.FANOUT_ABORTED = "calf.fanout.aborted"` (with `details.reason ∈ {store_unavailable, basestate_missing, reentry_failed, dispatch_failed}`) and `FaultTypes.FANOUT_TOPOLOGY = "calf.fanout_topology"`.

> **⚠️ CORRECTION (PR-6 / 2026-06-17):** the recovery-value-processing arm below (the `try: output = await self._apply_after(...)` under `except Exception as exc2:`, the "SINGLE-SHOT" wrap) is reconciled with §6.5's **absolute** mint rule. The bare `except Exception as exc2:` shown here would otherwise swallow a `NodeFaultError` raised by a *recovery-path* `after_node` handler into `calf.exception` (discarding the minted `error_type`/`retryable`/`details` — the exact swallow trap §6.5 exists to kill). The implementation honors "a `raise NodeFaultError(...)` anywhere — any seam, any body — converts verbatim" **here too**: a `NodeFaultError` raised while processing the recovered value converts **verbatim** (a local `except NodeFaultError as nfe: return await self._publish_fault(nfe.report, snapshot, ...)` arm *before* the `except Exception as exc2:`). The single-shot "a raise during recovery processing is terminal, chain the original" discipline applies **only** to a non-`NodeFaultError` raise — that one chains the original report via `causes`. (§6.5 governs; the sketch's bare-`except` is the pre-correction shorthand.)

```python
async def handler(self, envelope, correlation_id, headers, broker) -> Response:   # CLOSED
    # ── stage-0 guard: no user code may run before a context exists ──────────
    try:
        kind = self._classify(headers, envelope.reply)    # total (§4.1): missing→CALL; unknown→
                                                          # ERROR + ignore (returns the cleared
                                                          # no-reply mirror); slot disagreement→stray
        ctx  = self._build_seam_context(envelope, headers, kind, ...)
        stray = self._stray_check(kind, envelope)         # BEFORE stage 1 (§6.7): foreign/duplicate/
        if stray:                                         # kind-disagreement replies never run seams;
            return self._floor_stray(stray, envelope)     # log-only-guarded. (Post-closure batch
                                                          # strays floor at the AGGREGATOR, not here)
    except Exception:                                     # stage-0 failure: NO seams (no context yet;
        return self._floor_stage0(envelope, headers)      # ctx may be unbound) — call-kind ingress:
                                                          # fault the caller where the stack is readable;
                                                          # return/fault-kind: FLOOR ONLY (§4.1 — junk
                                                          # must not fault a live invocation)
    snapshot = self._stack_snapshot(envelope)             # pre-mutation caller address — every
                                                          # _publish_fault below addresses THIS
    try:
        output = await self._execute(ctx, kind, envelope)            # stages 1–4 + 6
    except NodeFaultError as nfe:                         # mint rule (§6.5): deliberate fault,
        return await self._publish_fault(nfe.report, snapshot, ...)  # BYPASSES on_node_error
    except Exception as exc:                              # ── stage 5: on_node_error ──
        report = ErrorReport.from_exception(exc, node=self, ctx=ctx)  # total construction
        if self._in_batch_work(ctx):                      # whole-invocation recovery is FORBIDDEN
            return await self._publish_abort(ctx, report) # mid-batch (§7.5): recovery chain SKIPPED;
                                                          # the AGGREGATOR escalates to the caller
                                                          # exactly once (companion spec §5) — the
                                                          # node must not also fault directly
        recovery = await run_chain_guarded(self._chains[ON_NODE_ERROR], ctx, report)
        if isinstance(recovery, _Minted):                 # NodeFaultError raised INSIDE the chain:
            return await self._publish_fault(recovery.report, snapshot, ...)   # convert verbatim,
        if recovery is None:                              # original chained via causes (§6.5)
            return await self._publish_fault(report, snapshot, ...)   # P2 rail
        try:   # recovered output still passes after_node (ADK parity); SINGLE-SHOT —
               # a raise during recovery processing is terminal, no second recovery
            output = await self._apply_after(ctx, self._interpret(ctx, recovery))
        except Exception as exc2:
            return await self._publish_fault(ErrorReport.from_exception(exc2, cause=report), snapshot, ...)
    if output is _CONSUMED:                               # batch open: output owed by pending sub-calls
        return Response(self._mirror(envelope, kind=CALL, reply=None), ...)   # §4.2: no-reply hop
    if output is _DECLINED:                               # all-declined terminal: nothing pending
        if ctx.awaiting_reply:                            # no-output on a reply-owing delivery →
            report = self._mint(FaultTypes.DELIVERY_REJECTED, ...)   # §10: auto-fault,
            return await self._publish_fault(report, snapshot, ...)  # caller never hangs
        return Response(self._mirror(envelope, kind=CALL, reply=None), ...)  # f-a-f no-op (DEBUG)
    if isinstance(output, _BatchFaulted):                 # returned, never raised
        return await self._publish_fault(output.report, snapshot, ...)
    # ── publish guard: transport failures NEVER re-enter on_node_error ───────
    try:
        body = await self._publish_action(output, envelope, correlation_id, broker)
    except Exception as exc:                              # broker/size failure on the success rail:
        report = ErrorReport.from_exception(exc, node=self, ctx=ctx)
        if self._in_batch_work(ctx):                      # mid-fan-out partial publish: the OPEN
            return await self._publish_abort(ctx, report) # record precedes siblings, so the batch is
                                                          # durably registered → abort; the aggregator
                                                          # escalates once; published siblings' replies
                                                          # stray-floor at the aggregator (§7.5)
        return await self._publish_fault(report, snapshot, ...)   # non-batch publishes fault the
                                                          # caller directly, addressed by the
                                                          # pre-mutation snapshot
    return Response(body, headers=self._emitter_headers() | self._kind_headers(output))  # §4.2 mirror rows

async def _execute(self, ctx, kind, envelope):
    # ConsumerNode (observer, §6.6): none of this — no seams, no caller stages, no auto-fault;
    # every kind reaches the consume body as an observation. This is the caller-capable path.
    if kind is FAULT:                                     # ── stage 1: on_callee_error ──
        report = envelope.reply.error                     # §4.2 carriage (FaultMessage)
        ctx.failing_call = self._slot_view(envelope.reply, report)    # §6.3: tag/topic/frame shown
        try:
            handled = await run_chain(self._chains[ON_CALLEE_ERROR], ctx, report)
            outcome = (_SlotResolved(self._coerce_parts(handled))     # I2 second call site — substitute
                       if handled is not None else _SlotFailed(report))   # COERCION failures are
        except Exception as exc:                          # slot-scoped too (§6.5/§6.9): the slot fails
            outcome = _SlotFailed(self._chain_seam_error(exc, report))    # with the error chained;
        finally:                                          # NEVER a node-own failure
            ctx.failing_call = None
        self._resolve_slot(ctx, outcome)                  # idempotent (§7.5); an internal materialization
        kind = RETURN                                     # failure resolves _SlotFailed(
                                                          #   calf.slot.materialization_failed) — §6.9
    if kind is RETURN:                                    # ── stage 2: aggregation ──
        match self._aggregate(ctx, envelope.reply):       # framework-final; consults the stage-1 slot
            case _BatchOpen():           return _CONSUMED          # outcome on fault deliveries; batch
                                                                   # registered → slot accounting (§7);
            case _BatchFaulted() as f:   return f                  # stateless continuation (§6.7):
            case _BatchClosed():         pass                      # tally (§7.6 — BOTH finalizations,
                                                                   # continuation included), then
                                                                   # return→body / fault→escalate
    pre = await run_chain(self._chains[BEFORE_NODE], ctx)  # ── stage 3: before_node ──
    if pre is not None:
        return await self._apply_after(ctx, self._interpret(ctx, pre))
    result = await self._dispatch_routed(ctx, route, payload, ...)  # ── stage 4: the body —
    if result is None:                                              #    today's CoR, untouched
        return _DECLINED                                   # all-declined: §10 totality rule applies
    return await self._apply_after(ctx, result)

async def _apply_after(self, ctx, output: NodeResult) -> NodeResult:   # ── stage 6 ──
    view = self._output_view(ctx, output)
    if view is None:
        return output                                      # non-terminal: nothing to guard
    post = await run_chain(self._chains[AFTER_NODE], ctx, view)
    if post is None:
        return output
    if isinstance(post, _ACTION_TYPES):
        raise SeamContractError("after_node returns values, not actions")   # → stage 5
    return self._coerce_output(ctx, post)

def _interpret(self, ctx, value) -> NodeResult:            # §6.3 two-tier rule
    return value if isinstance(value, _ACTION_TYPES) else self._coerce_output(ctx, value)
```

Framework-final (sealed): `handler`, `_execute`, `_apply_after`, `_classify`, `_stray_check`, `_floor_stray`, `_aggregate`, `_publish_fault`, `run_chain`. Designated extension points (the only overrides; Template Method confined to framework-internal use): `run()`/`@handler` routes (the body — exists today) and slot materialization (`_resolve_slot`, §6.9 — the only conversion override; `_coerce_output`/`_output_view` are sealed generics). A publish failure inside `_publish_fault` itself can only be log-floored — you cannot publish your way out of a dead broker — and the floor/mirror paths are themselves log-only-guarded: a failure while flooring never enters the node-own fault path (a junk delivery must not fault a live foreign invocation twice over).

### 6.9 Conversion functions (revised 2026-06-11 — the reply slot collapses the trio)

The first draft needed three conversions *per node type*. Under the §4 carriage they keep their names and call sites but become **framework-generic with exactly one per-type override in the entire SDK** (the agent's slot materialization — the genuinely irreducible conversion, because only the agent must render callee output into a model conversation):

| Conversion | Direction | Base implementation (generic) | Overrides |
|---|---|---|---|
| `_coerce_output(ctx, value) -> NodeResult` | seam value → output action | `ReturnCall(state=ctx.state, value=value)`; value→parts happens once at the publish chokepoint (§4.5) | none |
| `_output_view(ctx, output) -> OutputT \| None` | output action → typed view for `after_node` | extract `OutputT` from the `ReturnCall`'s value via the generic projection (`_extract_output`); `None` for non-terminal actions (`Call`/`TailCall`) | none |
| `_resolve_slot(ctx, outcome) -> None` | reply → pending slot + `ctx.callee_results` | append `CalleeResult` built from `in_reply_to`/`tag`/parts/fault | **agent only** (below) |

```python
# ── AgentNodeDef — the one irreducible per-type conversion ───────────────────
def _resolve_slot(self, ctx, outcome):             # IDEMPOTENT per in_reply_to (§7): a duplicate
    super()._resolve_slot(ctx, outcome)            # of a resolved slot never reaches here (stray)
    tool_call_id = outcome.tag                     # transport echo (§4.2) — no origin_payload,
    match outcome:                                 # no in-process correlation map
        case _SlotResolved(parts=p) if _is_retry(p):      # TextPart marked calf.retry (§4.5):
            ctx.state.add_tool_result(tool_call_id,        # materialize the RETRY type — preserves
                RetryPromptPart(content=_text(p), tool_call_id=tool_call_id))  # Anthropic is_error
        case _SlotResolved(parts=p):               # a return, or a handled substitute
            value = extract_lenient(p)             # parts → value; [] → None (§4.5)
            tool_return = ToolReturn(return_value=value, metadata={"tool_call_id": tool_call_id})
            pydantic_core.to_json(tool_return)     # eager wire-safety on SUBSTITUTES too — a seam
            ctx.state.add_tool_result(tool_call_id, tool_return)   # value must fail here, not at the
                                                   # next publish (the agent's PRIVATE bookkeeping, I4)
        case _SlotFailed():
            pass                                   # framework batch bookkeeping only (§7): unhandled
                                                   # faults escalate at closure and never reach the
                                                   # model — ErrorReport does NOT enter tool_results
    # A reply that resolves a pending frame but defeats materialization (missing tag where
    # the domain needs one — impossible from a calfkit producer; or a wire-unsafe substitute)
    # marks the slot failed with error_type="calf.slot.materialization_failed", ERROR logged:
    # the batch still closes deterministically; it never hangs.
```

Body changes per node type (the producers of `ReturnCall.value`):

```python
# ── ToolNodeDef.run — the blob-write protocol is deleted ─────────────────────
result = await self._tool.function_schema.call(payload.args, tool_call_ctx)
return ReturnCall[State](state=ctx.state, value=result)        # wire-safety checked at coercion
# ModelRetry: caught in the body, rendered AT ORIGIN to self-contained marked text (§4.5):
#   return ReturnCall[State](state=ctx.state,
#       value=[TextPart(text=render_retry_text(e), metadata={"calf.retry": True})])
# generic `except Exception → FailedToolCall` (tool.py:140-158): DELETED — an uncaught
# exception escapes to the chokepoint, on_node_error gets its edge chance, the fault
# rail carries the ErrorReport to the agent.

# ── AgentNodeDef.run terminal arm ────────────────────────────────────────────
return ReturnCall[State](state=ctx.state, value=parts)         # the preamble case passes
# list[ContentPart] through coercion untouched (§4.5); the final_output_parts write
# (agent.py:529-543) is DELETED — the slot is retired (§15).

# ── Custom BaseNodeDef subclasses ────────────────────────────────────────────
# Nothing. The generic defaults cover them — custom nodes are free (§6.3 tier 1).
# ── ConsumerNode ─────────────────────────────────────────────────────────────
# Nothing here either, for the opposite reason: no seams, no conversions, no rails (§6.6).
# Its consume_fn blanket catch (consumer.py:99-106) is DELETED — a consumer's own raise
# now reaches the chokepoint and floor-logs (ERROR) instead of vanishing at logger.exception.
```

Migration note baked into the above: the agent's `FailedToolCall` scan + `raise ToolExecutionError` (agent.py:285-329) is **deleted** — its replacement is stage 1 + `_resolve_slot` + batch-closure group escalation. The agent's aggregation stops scanning sibling `State` copies for results (`agent.py:127-137`); it resolves slots from arriving replies against its own batch state. `RetryPromptPart` no longer crosses the wire (§4.5) but **stays in the agent's in-process vocabulary** for its two in-process writers: its own validation writes (invalid tool name / malformed args / validator raises — agent.py:392-482, unchanged) and the materialization of `calf.retry`-marked returns (above).

---

## 7. Fan-out: aggregation and faults (confirmed)

Per arriving sibling delivery in a batch (`list[Call]` fan-out — the batch is keyed by the node's own inbound `frame_id`, exactly as today; its slots are keyed by the callee `frame_id`s the framework minted at fan-out, resolved by each reply's `in_reply_to`; `tag` carries the caller's domain key, §4.2):

1. **Fault delivery → `on_callee_error` runs immediately, per sibling, before aggregation.** Handled (value returned) → the slot resolves with that value as the callee's output; aggregation proceeds normally. Unhandled (`None`) → the slot is marked failed; aggregation **continues**.
2. **Aggregate-all, never fail-fast** (ADR-0003): Kafka has no cancellation — the siblings already consumed their messages and will complete and publish regardless, so fail-fast saves no work; it only saves waiting, while creating a straggler-to-dead-batch problem, discarding partial successes, and reporting only the first of possibly several faults. The batch always closes deterministically. Aggregation is **framework-final on `BaseNodeDef`** (locked): `list[Call]` is every node's vocabulary, so the bookkeeping is generic and sealed — node types customize only slot materialization (`_resolve_slot`, §6.9). The in-process `_pending_batches` durability hole (a rebalance mid-batch loses the batch) is pre-existing and explicitly NOT fixed by this design.
> **⚠️ OUTDATED (PR-6 / 2026-06-17):** item 3 below says "Partial-result *handling* belongs **at the aggregator**, where the power already lives." That references the deleted v1 separate aggregator — see `in-node-fanout-aggregation-spec.md` §4.3. Partial-result handling now lives **in-node, per sibling** (per-sibling `on_callee_error` during the folds, `surface_to_model` at the node). The fault-group topology-metadata-only rule is unchanged.

> **⚠️ CORRECTION (PR-6 / 2026-06-17):** the implemented closing fault-group topology is `details = {slots: [{tag, target_topic, status}], ok, failed}` (per-slot objects under a `slots` list, plus `ok`/`failed` counts) rather than the per-slot `{tag, target_topic, ok|failed}` + counts shape stated below (subject to review).

3. **At closure with ≥1 unhandled fault:** the batch fails — all unhandled faults escalate as a single **fault group** up the node's own rail (skipping `before_node`/body/`after_node`, §6.4). Sibling successes are not vaporized: partial-result *representation* is **topology metadata only** (confirmed 2026-06-11) — the group's `details` carries per-slot `{tag, target_topic, ok|failed}` plus counts, never the success values themselves (lean faults; carrying values would re-open the §4.3 leak posture). Partial-result *handling* belongs at the aggregator, where the power already lives (per-sibling `on_callee_error`, `surface_to_model`). The traveling `State` record *may* incidentally contain partial materializations (under §4.2's inbound-context rule, the escalating fault carries the last sibling's inbound copy, not the assembled closure state) — the record, not the interface (I4), and not API. Slot-mapping failures no longer threaten a hang (the first draft's unmappable-`origin_payload` rule is retired): a reply whose `in_reply_to` matches no pending slot is a stray — return: WARNING + ignore; fault: ERROR + floor (§6.7) — batch untouched, and a matched slot that defeats materialization marks itself failed with ERROR logged (§6.9), so the batch always closes deterministically.
4. A hung sibling hangs the batch — exactly as it does for successes today. Bounded waits are the successor retry/liveness issue's concern, not this design's.
> **⚠️ OUTDATED (PR-6 / 2026-06-17):** item 5 below describes the deleted v1 separate aggregator — "duplicate outcome records are absorbed at the aggregator's fold," "publishes an **abort record**; the aggregator — the batch's single writer — escalates the fault to the caller **exactly once** and tombstones the batch; later siblings stray-floor at the aggregator (post-closure/post-abort lookup miss, after the refresh rule — companion spec §5)." Aggregation now folds **in-node** — see `in-node-fanout-aggregation-spec.md` §4.4/§6.7. The fold is idempotent per slot frame id in the node's own `state` ktable; a node-own mid-batch fault tombstones both node-scoped ktables and escalates **in-node** exactly once (no abort record); later siblings stray-floor **in-node**; recovery across a graceful rebalance is the durable tables, not a lookup-miss refresh rule. The "at most one outcome per frame, across rebalances" guarantee holds.

5. **Batch integrity rules** (confirmed 2026-06-11, rounds 1–2; mechanics re-homed to the durable aggregator 2026-06-12): slot folding is **idempotent per slot frame id** (duplicate outcome records are absorbed at the aggregator's fold), so producer-retry duplicates can neither double-resolve nor double-close. A node-own terminal fault while the batch is open (stage-5 unrecovered, or a publish failure) publishes an **abort record**; the aggregator — the batch's single writer — escalates the fault to the caller **exactly once** and tombstones the batch; later siblings stray-floor at the aggregator (post-closure/post-abort lookup miss, after the refresh rule — companion spec §5). **The caller hears at most one outcome per frame, in steady state and across rebalances** (the rebalance strand itself closes — batch state is durable; "zero outcomes" remains possible only through the system-wide per-hop crash windows, §13). Whole-invocation `on_node_error` recovery is **forbidden while slots are outstanding** — the recovery chain is skipped entirely mid-batch (the invocation's output is owed by pending sub-calls; §6.8 sketch).
6. **`State.seam_budgets` is a framework-computed tally at every slot-outcome finalization, not a sibling-copy merge** (revised in review round 2 — the max-wins merge was unimplementable: no reset writer, and a same-closure reset would be erased by the merge; writer extended in round 3 — closure-only left sequential agents with *no writer at all*, unbounding `surface_to_model` on the default path): the framework computes each **per-tool** budget key from the **slot outcomes** — any success slot for a key's tool resets it to 0 (success = a *fault-free* return; a handled substitute still counts as that tool's failure — else `surface_to_model` could never exhaust); otherwise it increments by that finalization's failures — at **batch closure** *and* at **the single stateless continuation** (after stage 1, before the body). Same rule, one writer, race-free under the `max_workers=1` invariant. Mid-batch seam reads see the pre-batch value; nothing is merged from sibling copies. (The `calf.self_retry` key is not slot-based and has its own writer, §8.) General rule, stated in §6.3: mid-batch `ctx.state` mutations persist only through framework-computed fields (slot results, the budget tally); everything else on a sibling copy is discarded at closure.
> **⚠️ OUTDATED (PR-6 / 2026-06-17):** the entire item 7 below describes the deleted v1 *separate* fan-out aggregator — the `calf.fanout.events` events stream, the compacted global state table the aggregator consumed, outcome records, abort records, the re-entry delivery the aggregator published, and "the node holds zero in-process batch state." That whole design was replaced by the **in-node durable fold** — see `in-node-fanout-aggregation-spec.md` (the authoritative successor; §2 architecture, §3 records, §4 protocol, §7.7-binding). There is no events stream and no aggregator loop: aggregation folds in the node's own `_aggregate` stage over two **node-scoped** compacted ktables (`calf.fanout.{node_id}.state` + `.basestate`), the marker routes marked sibling replies into that fold (per-sibling seam timing preserved exactly), node-own mid-batch faults tombstone both tables and escalate **in-node** exactly once, and closure is a **self-published** re-entry (`kind=return`, `in_reply_to = fanout_id`, carrying no payload — closure state is read from the tables). The rebalance-strand-closes and `max_workers=1`-stands conclusions hold; the durable-fanout-aggregator-spec.md path is superseded.

7. **Aggregation durability — RESOLVED (2026-06-12): the durable fan-out aggregator** (`docs/designs/durable-fanout-aggregator-spec.md`, the companion spec this section binds to). Co-location machinery was **rejected** (deployment constraints and Kafka-internals workarounds to protect process RAM); batch state instead lives in two framework-owned topics — an events stream (key = `fanout_id`; single-writer-per-batch via Kafka's own ordering) and a compacted state table read globally (**zero partition invariants anywhere** — confirmed by Ryan as the deciding requirement; a partitioned/scoped table is the recorded future scale lever, companion spec §8). Consequences for this spec: **the node holds zero in-process batch state** — `_pending_batches` and in-memory tombstones are deleted; the `fanout_id` frame marker routes marked sibling replies to stage 1 + an outcome-record publish (per-sibling seam timing preserved exactly — option (i), confirmed), node-own faults mid-batch publish an **abort record** (the aggregator escalates to the caller exactly once), and closure arrives as a **re-entry delivery** (`kind=return`, `in_reply_to = fanout_id` — resolving the node's own fan-out frame; marker cleared there, §4.2 lifecycle) that runs materialization, the budget tally, and the closure stages as specced. The rebalance-window strand of §13(2) **closes** (batch state survives partition movement); multi-replica fan-out works with no topology constraints. The `max_workers=1` invariant (§4.1) **stands** (resolved at the aggregator review): its rationale shifts from batch-dict races to seam/`State` serialization and the serial per-partition processing the stage machinery assumes — and the aggregator's own subscriber is pinned serial for the same reason (companion spec §5).

---

## 8. Agent node specifics

- **Default: escalate** (confirmed). An agent with no registered `on_callee_error` propagates a sub-call's terminal fault up its own rail. This is uniform with every other node (one rule: unhandled bubbles) and preserves the vendored pydantic_ai semantic — `ModelRetry` goes to the model, an unexpected tool exception fails the run — except "fails the run" now means a typed escalating fault instead of a dropped message. ADK 2.2.0 verified to ship the same default.
- **The recoverable *semantics* are untouched; the carriage migrated (§4.5):** `ModelRetry` and MCP `isError=True` remain model-visible failures the agent's model sees and adapts to (the model is the audience, P3) — but they now arrive as rendered text in ordinary returns rather than as typed `RetryPromptPart` blob-writes. The agent's *own* in-process retry writes (invalid tool call, malformed args) keep using `RetryPromptPart` directly — no wire involved. *(⚠️ PR-6 / 2026-06-17: only `ModelRetry` carries the `calf.retry` marker that materializes a `RetryPromptPart`; MCP `isError=True` passes through **transparently/unmarked** as an ordinary return in the temporary PR-6 implementation — `calf.retry` marking for it is deferred to a follow-up PR; see the §9 CORRECTION.)*
- **Prebuilt show-to-model opt-in** (confirmed; modeled on ADK's `ReflectAndRetryToolPlugin`):

  ```python
  agent = AgentNodeDef(..., on_callee_error=[surface_to_model(max_failures=3)])
  ```

  Converts an inbound fault into a model-visible tool-failure result (the slot resolves; at batch closure the model sees the mix and adapts), tracks **consecutive failures against a budget**, and **declines (escalates) on exhaustion** — bounded, then bubbles; self-healing is opt-in and visible, never a silent default. **Budget placement (confirmed 2026-06-11):** the counter rides a small dedicated framework-owned `State` field — `State.seam_budgets: dict[str, int]` — wire-carried, so a rebalance/restart cannot silently refill the budget (ADK counts in-process because it *is* in-process), computed by the framework from slot outcomes at every slot finalization — batch closure *and* the single stateless continuation (§7.6 — reset on a key's success, incremented by its failures; no sibling-copy merging; sequential agents are bounded too). Deliberately **not** a namespaced `State.metadata` key — `metadata` is application-owned, and the framework reserving slices of the user's field is the patch-rule smell. **Budget key = tool identity, not topic** (review round 1): an MCP toolbox fronts N tools on one dispatch topic, so a topic key would pool all N into one counter *and* let any sibling tool's success refill it — the key is the failing call's tool (`ctx.failing_call.tag → state.tool_calls[tag].tool_name` on agents; `target_topic` fallback elsewhere), reset on *that tool's* success. The signature is `surface_to_model(max_failures=3, only_types=None, exclude_types=None)` — `max_failures` because the prebuilt counts faults shown to the model (whether a retry happens is the model's choice), and the scope parameters keep a catch-all prebuilt from shadowing specific handlers registered after it (§6.1 chain-order trap).
- **The all-invalid self-retry loop is bounded, default budget 1** (confirmed, Ryan, 2026-06-11; revised from 0 in review round 2): this budget governs the *LLM-loop* retry — re-prompting the model with its validation failures after a turn whose tool calls were all invalid (the `TailCall` self-retry, scenario 27) — **not** node-edge re-execution, which remains successor-issue territory. The unbounded loop is closed; the default of **1** gives the model one visible, WARNING-logged fix-it turn — parity with vendored pydantic_ai (`retries=1`) and ADK, and the right day-one experience for the single most common model flub (one malformed tool call). Exhaustion faults `calf.agent.self_retry_exhausted` with the validation failures summarized in `details` (tool names + error kinds — never raw LLM-emitted args, §4.2); at an explicit `self_retry_budget=0`, the immediate fault carries `details.reason="self_retry_disabled"` (nothing was exhausted). Counted in `State.seam_budgets["calf.self_retry"]` (wire-carried; reset on any valid dispatch or terminal). Accepted asymmetry, recorded honestly: a model alternating one-valid/one-invalid calls resets the counter each turn and is *not* bounded by this budget — that pattern surfaces through `surface_to_model`'s per-tool budget and the model's own behavior, not this knob.
- **`FailedToolCall` and `ToolExecutionError` retire.** The tool node's internal generic-exception catch is replaced by the fault rail (the chokepoint synthesizes the `ErrorReport`; the rail carries it to the agent's `on_callee_error`); the agent's `raise ToolExecutionError` dead-end is deleted. Migration in §15.
- **`CalfToolResult` becomes agent-internal** (revised 2026-06-11): batch slot accounting is framework bookkeeping keyed by `frame_id` (§7), and unhandled faults escalate *before* the body — so `ErrorReport` never enters `State.tool_results` at all, and nothing wire-borne lands there except materialized `ToolReturn`s (§6.9). The union shrinks to `ToolReturn | ModelRetry | RetryPromptPart`, where the retry types come from the agent's own in-process writes: validation failures *and* the materialization of `calf.retry`-marked returns (§6.9). `State.tool_results` is the agent's private conversation-assembly record (I4), no longer an inter-node protocol.

---

## 9. Tool node & MCP toolbox specifics

- A tool function's uncaught exception → `on_node_error` chain (the dev can recover at the edge with a substitute result) → unhandled → fault to the calling agent. The existing `try/except` in `tool.py` collapses into this uniform path.
- `ModelRetry` raised by a tool keeps its current meaning (recoverable, model-bound) — it is *not* routed to `on_node_error`. Carriage changed (§4.5): the tool body renders it to self-contained text at origin and returns it as an ordinary `ReturnMessage` `TextPart`; `RetryPromptPart` no longer crosses the wire.
- MCP toolbox: transport/session failures (terminal) → fault rail; `isError=True` tool results (domain errors, model-visible by MCP convention) → ordinary return content **carrying the same `calf.retry` marker as `ModelRetry`** (§4.5 — the marker's rationale, Anthropic's `tool_result(is_error=True)` fidelity, applies identically to MCP domain errors; the result's content blocks are already parts-shaped, MCP's own precedent).

> **⚠️ CORRECTION (PR-6 / 2026-06-17):** the temporary PR-6 implementation passes MCP `isError=True` results through **TRANSPARENTLY** — the result rides the reply slot as an ordinary successful `return` (the agent/model sees it exactly as today) and is **NOT** marked `calf.retry`. Only a tool-raised `ModelRetry` gets the `calf.retry` marker (`tool.py`); the MCP chokepoint (`calfkit/mcp/mcp_toolbox.py` — the `isError` path returns `ReturnCall(value=tool_result)`, see its inline B2 comment) applies no marker. The `calf.retry`-marked design described in the bullet above — proper Anthropic `is_error=True` fidelity for MCP domain errors — is **DEFERRED to a follow-up PR**: later we will mark `isError` for Anthropic `is_error` fidelity; for now the transparent pass-through stands. (This is the locked PR-6 decision B2.) The bullet's deferred-target prose is retained, not deleted.
- **Why `on_node_error` exists at all** (confirmed; reverses an earlier recommendation): developers lack authorship inside agent and MCP-toolbox node bodies — there is no user code in which to `try/except` the model call or the MCP session. Without an edge seam, their only handling point is across a network hop. The edge seam also seeds future node-local retry (the successor retry issue): retrying at the edge keeps the stack intact and costs zero extra hops (ADK 2.x node `RetryConfig` is precedent). The contract keeps P1 intact: recovery must produce an output; `None` produces a fault; *nothing* is impossible.

---

## 10. Gate retirement & `Silent` removal (confirmed 2026-06-11 — hard break, no deprecation period)

`gate()` and the `gates=` constructor parameter are **deleted**, and so is the **`Silent` action** (Ryan's call). The grounding: in every mature hook-host framework, user code throws the happy path off through exactly **two sanctioned gestures — substitute an output, or raise** — never a token meaning "produce nothing." ADK callbacks short-circuit *with the returned value as output*; Flask `before_request` returns a response or `abort()` raises; Rails halts by rendering (or `throw :abort` at the model tier); Spring's `preHandle → false` requires the interceptor to have *written the response itself* — substitution in disguise, with silent-`false` documented as the hung-request bug pattern. "Consume and do nothing" is legitimate only where nobody is owed a reply (stream operators like Kafka Streams `filter()`, bus middleware dedup) — and calfkit's *route-handler* decline (all-declined fire-and-forget = DEBUG no-op, §6.4/§15) already covers that case — the seam's `None` proceeds and cannot drop. Today's `Silent` is half-deprecated by its own implementation: every use logs WARNING and still broadcasts the unchanged inbound envelope (`base.py:319-327`).

| Gate behavior today | Seam equivalent |
|---|---|
| return `True` → proceed | return `None` |
| return `False` → skip body, envelope unchanged | **hard reject:** raise `NodeFaultError(...)` → caller receives a typed fault; **soft reject:** return a substitute output |
| gate raises → treated as reject (swallowed!) | seam raise → `on_node_error` → fault (visible) |
| boolean only; cannot substitute output | return a value → short-circuit with output |
| reply-owing rejection strands the caller (TODO #201) | **impossible by construction** — see below |

**#201 closes by construction, not by configuration.** With `Silent` gone, the only no-output terminals on a reply-owing delivery are *accidents* (schema rejection, all handlers declined) — and accidents are loud: the framework auto-faults them with `error_type="calf.delivery.rejected"` (`details.reason ∈ {"schema_rejected", "all_declined"}` — shipped as constants, §4.3; plumbing note: the route dispatcher reports *why* it declined — today it just falls through — so the discriminator has a writer). **"Reply-owing" means this node is the addressee of the top frame and that frame's `callback_topic` is set** — exposed as `ctx.awaiting_reply` (§6.3), the hard-vs-soft-reject discriminator; observers are never reply-owing (`awaiting_reply ≡ False`, §6.6 — a tapping consumer must not inject faults into traffic it merely observes). P1's "never nothing" becomes true by vocabulary: a seam or body can substitute, decline, or raise — it cannot strand a caller. The first draft's [PROPOSED] WARNING-vs-auto-fault default is retired unasked: it was a policy knob for a gesture that no longer exists. On fire-and-forget traffic, an all-declined delivery remains a no-op (DEBUG log — the stream-filter case, correctly scoped to where no reply is owed). The agent's mid-batch "consume, output comes later" was never user-facing `Silent` semantics — it is the pipeline's internal `_CONSUMED` outcome (§6.8), where the reply obligation is owed by the still-pending sub-calls.

---

## 11. The client edge (confirmed)

One exception class, symmetric for minting and receiving — **`NodeFaultError`**:

```python
# receiving (client):
try:
    result = await handle.result()
except NodeFaultError as e:
    if e.report.find(FaultTypes.MODEL_CONTEXT_WINDOW_EXCEEDED):   # find(), not == — the client
                                                                  # sees groups of unknown depth (§4.4)
        ...                      # branch on the slotted report — same gesture as the seams
    e.report.causes              # fault group children, if any

# minting (any node/tool code) — deliberate typed faulting:
raise NodeFaultError("billing.quota_exceeded", message="...", retryable=False, details={...})
```

- The reply dispatcher branches on **`x-calf-kind`**: `fault` → deserialize `reply.error` and fail the future with `NodeFaultError(report)`; `return` → resolve the future, with `NodeResult` projecting its output from **`reply.parts`** (the same `_extract_output` machinery, repointed — `state.final_output_parts` is retired, §15); unknown/missing kind on the reply topic → ERROR + ignore (§4.1's total contract). Grounding note: PR #215 (the `send`/`start`/`execute` surface) merged 2026-06-11 — this section binds to the renamed surface; a fault addressed to a caller-chosen `reply_to` topic has no dispatcher future and lands in the §6.6 sink path instead.
- **Dispatcher floor duties** (confirmed 2026-06-11, review rounds 1–2): any `kind=fault` that cannot be delivered to a live future — unknown correlation, future already done (a fault losing the race with TTL eviction), client restarted (fresh group, `auto_offset_reset="latest"` skips parked replies — stated honestly) — is **ERROR-logged with the full `ErrorReport` JSON**, never dropped at DEBUG; late *returns* keep the quiet DEBUG drop. The reply subscriber gets the pre-handler decode shim (§6.7), and at the client it does more than floor: `correlation_id` rides transport headers and survives an undecodable body, so the shim **fails the pending future** with `NodeFaultError(calf.delivery.undecodable)` — "routing is impossible" is true at nodes (the address is inside the body) but false at the client edge, and a loud log alone would still hang the caller forever under the `reply_ttl=None` default.
> **⚠️ CORRECTION (PR-6 / 2026-06-17):** this client-edge fail-the-pending-future-on-undecodable behavior is **DEFERRED to the reception PR** and is **NOT in PR-6**. PR-6's `DecodeFloorMiddleware` floors `calf.delivery.undecodable` and **re-raises at the client reply subscriber exactly like a node** — it does *not* fail the future, so an undecodable reply hangs the caller under the `reply_ttl=None` default (the very hang this bullet warns of is the *current* PR-6 behavior, not yet closed). Floor-only is the ceiling at the client too in PR-6. (The mechanism is feasible — the address survives in headers — it is simply not built yet; the code-side docstrings already note the deferral.)
- Naming note: `NodeResult` is used with two meanings in the codebase — the *action union* (`models/actions.py`) and the *client result-projection class* (`models/node_result.py`). The collision is pre-existing; this spec uses both per context, and the implementation should rename one (the projection class is the natural candidate, e.g. `InvocationResult`) — flagged, not designed here.
- **No error_type→exception-class registry** (confirmed): every traffic touchpoint — seams, sinks, client — presents the same slotted `ErrorReport` interface; a registry of typed exception classes at the client only would make one surface inconsistent with all others. (#193's `except ModelContextWindowExceededError:` becomes `except NodeFaultError` + a type check; the typed class may still exist *in-process* at the provider layer, §12, but it is not the cross-process contract.)
- A fault arriving for a `send(...)`-with-no-`reply_to` (fire-and-forget) correlation is, by definition, undeliverable to the client — there is no future. Floor only. Matches the documented fire-and-forget contract (caller forfeits delivery).

---

## 12. Classification rider: `calf.model.context_window_exceeded` (#193)

The first production fault type, landing with (or immediately after) the rail:

- **The classification lives in calfkit's provider layer** (`calfkit/providers/pydantic_ai/` — the `OpenAIModelClient`/`AnthropicModelClient` subclasses the agent loop polymorphically calls), **not** in `_vendor/`. `_vendor` is read-only upstream code: excluded from ruff/mypy/coverage, every patch is a re-applied liability on each pydantic_ai bump. The provider wrappers are the calfkit-owned, checked seam that already owns provider-specific request mapping.
- Mechanism: wrap `request()`/`request_stream()`; on `ModelHTTPError`-shaped failures, run per-provider matchers (OpenAI `code="context_length_exceeded"` etc.; Anthropic body patterns); on match, raise the in-process typed exception carrying `error_type="calf.model.context_window_exceeded"`, `retryable=False` — which the chokepoint converts verbatim to the wire fault (§6.7).
- Classification low, policy high: the provider layer names the failure; remediation (compact-and-retry, fallback model, surrender) belongs to whichever ancestor's `on_callee_error` chooses to act — or the client.
- Per-provider matcher details (confirmed 2026-06-11): enumerated at implementation against current SDK error shapes (verify against installed `openai`/`anthropic` SDKs, not training data).

---

## 13. Observability & the floor

- **Fault stream:** every node that produces or escalates a fault also returns the fault-bearing envelope as its FastStream `Response` body, so the worker's `@publisher` broadcasts it on `publish_topic` where one exists — tappable by any ordinary consumer node. No new primitive (confirmed: fault streams are the floor/ops layer, not the handling API).
- **Structured logging:** one log line per fault event — synthesis (ERROR, with traceback, at origin), each escalation hop (WARNING, with `error_type`, origin, remaining stack depth), seam handling (INFO). The escalation logs carry the teaching load for "why didn't my handler fire."
- **Fire-and-forget / frameless terminals:** no rail exists → ERROR log at origin with the full `ErrorReport` JSON + broadcast where a `publish_topic` exists. A dedicated dead-letter **topic** stays deferred (confirmed 2026-06-11): #180's `StartupTopicEnsurer` now gives a future DLT its provisioning path, but it still deserves its own design (retention, keying, replay; successor-retry-issue-adjacent).
- Headers (`x-calf-kind`, `x-calf-error-type`) make faults filterable at the broker level without deserialization. Implementation note: the broadcast `Response` today carries only emitter headers (`base.py:453`) — the mirror must also stamp the kind/error-type headers (§4.2's stamping table) for the broadcast rail to be filterable as claimed here. `ErrorReport.report_id` (§4.3) is the dedup key: identity-preserving escalation mirrors one logical fault on up to stack-depth `publish_topic`s; ops taps dedup on it.
- **Producer posture (confirmed 2026-06-11, review round 1):** calfkit's shared producer defaults to **`acks=all` + `enable_idempotence=True`** (aiokafka defaults are `acks=1`, no idempotence — under which a "successful" fault publish can vanish on leader failover and producer retries can duplicate deliveries). One configuration line; it hardens every rail, and the fault rail is the strongest argument calfkit has had for it. `reply_ttl` stays `None` by default — with the rail in place faults *arrive* instead of hanging; the TTL is an opt-in liveness backstop.
> **⚠️ OUTDATED (PR-6 / 2026-06-17):** window (2) below says the rebalance/multi-replica window is "**closed** by the durable aggregator (§7.7)" with the residual being "the generic per-hop crash class of (1) applied to the aggregator's own hops (outcome-consumed-then-died-before-fold; tombstone-then-died-before-re-entry — companion spec §6)." The separate aggregator is deleted — see `in-node-fanout-aggregation-spec.md` §7. The window is still closed (batch state survives graceful partition movement), but by the **in-node** durable tables; the residual crash windows are the node's own hops (fold-then-died-before-ack; tombstone-then-died-before-re-entry), per in-node §7 — not "the aggregator's own hops" and not "companion spec §6."

- **Residual loss/hang windows, enumerated honestly** (inputs to the successor retry/redelivery issue): (1) crash after ack-first commit, before the hop's publish — per-hop, at-most-once, accepted (ADR-0003); (2) ~~batch lost to rebalance/multi-replica split~~ — **closed** by the durable aggregator (§7.7): batch state survives partition movement and any worker can own any batch; what remains of this window is only the generic per-hop crash class of (1) applied to the aggregator's own hops (outcome-consumed-then-died-before-fold; tombstone-then-died-before-re-entry — companion spec §6); (3) an escalation hop addressed to a decommissioned/renamed node's callback topic parks the fault on a consumerless topic — Kafka accepts the publish, nothing logs at the parking site, the producing hop's log is the only trace; (4) a fault stripped-and-refloored after an oversized publish (§4.3) reaches ops but not the caller; (5) client-side: TTL eviction racing a late fault, and restart skipping parked replies (§11); (6) **a fan-out abort under an *unavailable* durable store escalates once *per pending sibling*** — when the store is dead (terminal reader death) the abort's tombstone write also fails, so each still-pending sibling arriving during the outage independently aborts + escalates `calf.fanout.aborted`; the §4.4 "escalate-once" invariant holds only under a *writable* store. Note this residual is a **duplicate / over-delivery** of the abort escalation (up to N `calf.fanout.aborted` faults for one batch), NOT a loss like (1)/(3) — it is the accepted consequence of the at-most-once design posture carrying no exactly-once dedup state (the in-node design holds zero in-process batch state, so there is no cheap in-process dedup; in-node-fanout-aggregation-spec.md §4.4). Every window is logged at its producing edge; none is silent; none is new to this design except as named machinery replacing formerly-invisible drops.
- **Floor coverage note:** a node with `publish_topic=None` mirrors nothing — its floor is log-only; `ConsumerNode` cannot even declare a `publish_topic`, so consumer-origin faults (a sink's own `consume_fn` raise, §6.6) are log-only by construction. "Tappable by any ordinary consumer" describes nodes that broadcast, not a universal guarantee.

---

## 14. Extension taxonomy: one family (revised 2026-06-11)

The policy seams are **the** extension surface of this design — registry/chain-shaped, run *in* the flow, return outputs, make business decisions. Members: `before_node`, `after_node`, `on_node_error`, `on_callee_error`. (Gates were a degenerate policy seam; retired.)

**No second, middleware-shaped family is planned** (confirmed, Ryan 2026-06-11). The first draft framed a deferred "cross-cutting" family (`around_invoke`/`around_publish`/`on_event`), inherited from `calfkit-v1-design.md` G5/§12; that framing is withdrawn from this spec — it presupposed an implementation that is not committed. Transport-altitude cross-cutting (tracing, metrics, log enrichment) already has a home *underneath* calfkit: FastStream's own broker/subscriber middlewares wrap the whole handler today, with no calfkit API required. Framework precedent supports the single-family posture: where mature frameworks ship both patterns (ASP.NET middleware + MVC filters, Spring `Filter` + `HandlerInterceptor` + AOP, Flask/Rails hooks over WSGI/Rack), it is because they own *both* altitudes as public API — calfkit's transport altitude belongs to FastStream.

Conditional constraints, recorded so they bind any future proposal (a guard, not a plan): if a wrap-shaped family is ever introduced, it must (a) wrap fault dispatch identically to normal dispatch — no ADK-style unprotected paths — and (b) obey §10's totality rule: a wrapper may not swallow a reply-owing delivery without producing or raising. Whether `calfkit-v1-design.md` G5/§12 should itself be revised is out of this spec's scope.

---

## 15. Breaking changes & migration

Pre-1.0 hard breaks, no compat shims (project policy):

| Removed | Replaced by |
|---|---|
| `gate()` / `gates=` | `before_node` (§10 table) |
| `FailedToolCall` marker (terminal path) | `ErrorReport` on the fault rail |
| `ToolExecutionError` raise in the agent | agent `on_callee_error` → escalation |
| tool node's internal generic-`except` → marker | chokepoint floor + `on_node_error` |
| silent drop of uncaught handler exceptions | P1 invariant (this is a behavior change consumers may observe: previously-hanging callers now receive faults) |
| `State.final_output_parts` (+ its `project_output(state, …)` read path) | `Envelope.reply.parts`; the same projection machinery repointed (`NodeResult`, `ConsumerContext`) — retired outright, no record kept (confirmed) |
| the tool-result blob-write protocol (`state.tool_results` as inter-node carriage) | `ReturnCall(value=…)` → `ReturnMessage.parts`; the agent materializes via the `tag` echo (§6.9) |
| `RetryPromptPart` over the wire | retry text rendered at origin, shipped as an ordinary return (§4.5) |
| `CalfToolResult` as a wire union (incl. `FailedToolCall`) | agent-internal union `ToolReturn \| ModelRetry \| RetryPromptPart` (§8); `FailedToolCall` deleted |
| `Silent` action (incl. seam-level silent reject; agent mid-batch `return Silent()`, agent.py:274) | removed (§10) — substitute, decline (`None`), or raise; fire-and-forget no-output = decline; mid-batch = internal `_CONSUMED`; reply-owing no-output terminals auto-fault `calf.delivery.rejected` |
| consumer `consume_fn` blanket catch (consumer.py:99-106) | deleted — a consumer's own raise floor-logs at ERROR (§6.6); consumers also lose seam registration entirely (startup error) |
| aiokafka producer defaults (`acks=1`, no idempotence) | `acks=all` + `enable_idempotence=True` (§13) — an ops-observable behavior change (latency/throughput trade for delivery hardening) |
| TailCall frame minting (fresh `frame_id`, dropped `overrides`) | replacement frame preserves `frame_id`/`tag`/`overrides`; `payload=None` — TailCall carries no body by design, the traveling `State` is its input (§4.2) |
| in-process `_pending_batches` fan-out aggregation (locality-fragile under >1 replica) + in-memory batch tombstones | the durable fan-out aggregator (`docs/designs/durable-fanout-aggregator-spec.md` — own spec + own PR, sequenced before this spec's fan-out machinery; ktables unchanged for v1); the split topology and `_return_topic` wiring are RETAINED (the 2026-06-12 single-entrypoint merge was explored and rejected, §4.1) |
> **⚠️ OUTDATED (PR-6 / 2026-06-17):** the replacement cell above points to `durable-fanout-aggregator-spec.md` and asserts "ktables unchanged for v1." Both are stale — the replacement is the **in-node durable fold**, `in-node-fanout-aggregation-spec.md` (the separate aggregator was deleted), and **ktables is NOT unchanged**: it gains a `barrier()` primitive in v0.2.0 (in-node §5.1/§10), which calfkit depends on. The split topology / `_return_topic` retention and `ACK_FIRST`-unchanged points hold.
| filtering gates on fire-and-forget traffic | route-handler decline (all-declined f-a-f = DEBUG no-op) or a consumer — NOT `before_node`, which cannot drop (§6.4/§10) |

Dependencies & sequencing: the dead-actions cleanup (`Reply`/`Delegate`/`Sequential`/`Emit`/`Parallel` pruned) landed as PR #217; this spec targets the cleaned vocabulary and then removes `Silent` itself (§10), leaving `NodeResult = Call | list[Call] | ReturnCall | TailCall` (+ dispatcher-internal `Next`). PR #215 (`send`/`start`/`execute` + caller-chosen `reply_to`) **merged 2026-06-11** — implementation rebases onto it; §11 binds to the renamed surface. Issue bookkeeping (review round 1): **this spec implements and closes #143** (worker-side error propagation to the original caller — its scope is this deliverable); the *deferred* retry/redelivery/ack-policy/bounded-wait work needs a **freshly minted successor issue**, which all "deferred" references in this spec point at. Decomposition option for implementation planning: the §4 reply-slot model for *returns* is separable as its own refactor PR (no user-visible behavior change) between the cleanup and the fault rail, shrinking the rail+seams PR — decide at planning time (split-orthogonal-concerns rule).

## 16. Acceptance scenarios (seed list for TDD)

1. Tool raises generic exception; agent has no seams → client future raises `NodeFaultError` with `error_type="calf.exception"`, origin breadcrumb intact; nothing hangs.
2. Same, agent registers `on_callee_error` returning a string → model sees it as the tool result; workflow completes normally.
3. Deep chain A→B→C; C faults; B declines; A handles → A's seam receives the fault with two-frame `frame_chain` and an `error_type` identical to C's origin type (identity-preserved escalation, §4.4); B's hop logged.
4. Fan-out of 3; one sibling faults unhandled → batch closes after all 3; singleton-flattened fault escalates carrying the batch topology metadata in its `details` (§4.4 flattening rule) — the 2 successes recorded there as `ok` slots.
5. Fan-out of 3; two fault unhandled → `calf.fault_group` with 2 `causes`.
6. Fan-out of 3; one faults, seam returns substitute → batch completes, model sees 3 results.
7. `before_node` raises `NodeFaultError` on a reply-owing delivery → the caller receives the typed fault (hard reject); a reply-owing delivery where every handler declines auto-faults `calf.delivery.rejected` — the caller never hangs (#201 closed by construction, §10).
8. `before_node` raises → `on_node_error` fires; recovery value becomes node output; `after_node` sees it.
9. `on_node_error` raises → original fault escalates, seam failure noted in `details`.
10. Fire-and-forget terminal faults → ERROR log + broadcast; no callback publish; client unaffected.
11. Fault/return deliveries carry no `x-calf-route` and never enter the route CoR on any caller-capable node — classification precedes routing (§6.7); on a consumer they reach the consume body as observations (§6.6), never as routed work.
12. `surface_to_model` exhausts its budget across process restarts (budget in `State`) → declines → escalates.
13. Context-window-exceeded from each supported provider classifies to `calf.model.context_window_exceeded` (provider-layer tests, no `_vendor` edits).
14. Client `send(...)` with no `reply_to` + callee fault → floor only; no dispatcher entry leaks.
15. All-declined **fire-and-forget** delivery → `before_node` fired, `after_node` did NOT (no output), no-op + DEBUG log (the stream-filter case); the same outcome on a **reply-owing** delivery → auto-fault `calf.delivery.rejected` with `details.reason="all_declined"` (§10).
16. `after_node` returns an action → `SeamContractError` → `on_node_error` → fault. A serializable returned from a custom `BaseNodeDef` seam coerces generically and returns on the node's rail (custom nodes are free, §6.3).
17. A stray **return** (no pending slot) at an agent → WARNING + ignore, batch state untouched; a stray **fault** → ERROR with the full `ErrorReport` JSON + broadcast mirror where `publish_topic` exists — never a bare warning (§6.7); the same deliveries at a consumer are observed normally (§6.6).
18. `before_node` mutates `ctx.state` and returns `None` → body runs with the transformed input (the official transform channel); returning the mutated state itself is not required.
19. `on_node_error` recovery value flows through `after_node`; a **non-`NodeFaultError`** `after_node` raise during recovery processing is terminal (single-shot), chaining the original fault as `cause` — but a `NodeFaultError` raised there converts **verbatim** (the absolute mint rule, §6.5 / the §6.8 M1 CORRECTION), **not** chaining the original.
20. Tool raises `ModelRetry` inside a parallel batch → the rendered retry text crosses as an ordinary return; the model sees it as the tool result; the batch completes (regression guard for the de-blobbed aggregation, §4.5/§6.9).
21. A consumer tapped into a tool's *input* topic receives `kind=call` deliveries → body runs with `output=None`; no aggregation attempted (kind is a producer-side fact; observer role is the node's, §4.1/§6.7).
22. A terminal return's broadcast mirror carries `kind=return` + the reply → `ConsumerContext.output` projects from `reply.parts`; a mid-loop mirror (`kind=call`, `reply=None`) projects `None` — the stale-output-slot bug class is gone (I3).
23. Client send stamps `kind=call`; the reply dispatcher branches on the kind header — `fault` fails the future with `NodeFaultError(report)`, `return` resolves it with output projected from `reply.parts` (§11).
> **⚠️ CORRECTION (PR-6 / 2026-06-17):** the `fault → fail the future with NodeFaultError(report)` half of scenario #23 is part of the **deferred reception PR** and is **absent in PR-6** — the reply dispatcher has no `x-calf-kind` branch yet (it unconditionally resolves the future), and an undecodable/fault reply is floored + re-raised at the client subscriber like a node, so a fault reply hangs the caller under the `reply_ttl=None` default. The `return` half (resolve the future from `reply.parts`) is present. See the §6.7 / §11 / scenario #30 corrections.
24. Escalation hops re-stamp `in_reply_to`/`tag` per ancestor while the `ErrorReport` (`error_type`, origin ids, `frame_chain`, `causes`) is identical at every hop (§4.4).
> **⚠️ CORRECTION (PR-6 / 2026-06-17):** scenarios #25 and #28 both reference `ctx.fault` (populated, or a tapped fault delivery). `ConsumerContext.fault` and `ConsumerContext.delivery_kind` are part of the **deferred reception PR** and are **absent in PR-6** — see the §6.6 correction. In PR-6 a `kind=fault` delivery reaches `consume_fn` with `output=None`/`output_parts=[]` but **no `ctx.fault`** to read; the rest of each scenario (no `on_callee_error` on a consumer; observers never produce / no callback-rail publish / no auto-fault; own raise floor-logs at ERROR) holds.

25. A `kind=fault` delivery at a consumer reaches `consume_fn` with `ctx.fault` populated and `output=None`; `on_callee_error` never fires on a consumer (§6.6).
26. A user-minted `NodeFaultError("calf.anything", ...)` raises `ValueError` at construction; framework-minted `calf.*` types pass (§4.3).
27. With self-retry budget available (the default permits one, §8), the agent's all-invalid `TailCall` self-retry arrives on its own private continuation inbox stamped `kind=call` → classified by header, not topic (§4.1): guard + body run a fresh model turn; the caller's return address is preserved on the inherited frame.
28. A consumer tapped into reply-owing traffic (live frames in the envelope) declines/raises/returns → **no publish on the callback rail, no auto-fault, ever** (observers never produce, §6.6); its own raise floor-logs at ERROR.
29. A delivery with `kind=fault` but `reply=None` (kind/slot disagreement) at a mid-batch agent → stray handling, WARNING + ignore; the open batch is untouched (§4.1) — never the node-own-failure path.
> **⚠️ CORRECTION (PR-6 / 2026-06-17):** the second clause of scenario #30 ("the same at the client's reply subscriber does not strand the future silently") describes the client-edge fail-the-future, which is **DEFERRED to the reception PR** and is **NOT in PR-6** — see the §6.7/§11 corrections. In PR-6 the client reply subscriber floors `calf.delivery.undecodable` and re-raises like a node, so an undecodable **reply** *does* strand the pending future (hangs under `reply_ttl=None`). The first clause (the node-side pre-handler floor event) holds.

30. An envelope that fails pydantic validation at decode → pre-handler floor event `calf.delivery.undecodable` (§6.7); the same at the client's reply subscriber does not strand the future silently (§11).
31. A raise inside `on_callee_error` mid-batch → that slot resolves failed with the seam's error chained to the inbound fault; siblings continue; closure escalates; no node-own fault, no double reply (§6.5).
32. `raise NodeFaultError(...)` from a tool body with a registered catch-all `on_node_error` → the recovery seam never fires; the minted fault escalates verbatim (the mint rule, §6.5).
33. A duplicate sibling reply (same `in_reply_to`, slot already resolved) → stray; no double-resolution, no early closure, no seam side effects (§6.7/§7.5).
34. B `TailCall`s C inside A's fan-out → C's reply carries the frame id A minted; A's slot resolves (TailCall preserves `frame_id`/`tag`, §4.2).
35. `surface_to_model`'s per-tool budget tallies at slot finalization (§7.6): consecutive closures/continuations with the same tool's failures exhaust it → declines → escalates; a different tool's success does not refill it (§8 keying); a *sequential* agent's repeated tool failures exhaust it too (the continuation writer).
36. Tool raises `ModelRetry` against an Anthropic-backed agent → the marked retry text materializes as `RetryPromptPart` → the provider sends `tool_result(is_error=True)` (§4.5 marker; provider fidelity preserved).
37. Model emits one all-invalid turn under the default budget (1) → one WARNING-logged self-retry; the model's corrected turn proceeds. A second consecutive all-invalid turn → `calf.agent.self_retry_exhausted` (details = tool names + error kinds). With explicit `self_retry_budget=0` → immediate fault, `details.reason="self_retry_disabled"`. No livelock in any configuration (§8).
38. Fan-out completes fully → `before_node` fires at batch closure with all results assembled in `ctx` (§6.4) and did NOT fire on mid-batch sibling arrivals (it also fired on the ingress delivery that produced the fan-out — two firings over the invocation, one per body run).
39. A reply-owing delivery whose only matching handler rejects the body schema → auto-fault `calf.delivery.rejected` with `details.reason="schema_rejected"` (§10).
40. A fault group's `details` carries per-slot `{tag, target_topic, ok|failed}` topology; `report.walk()`/`find()` traverse nested groups so an `error_type` check matches at any composition depth (§4.4/§7).
41. A migrated gate returning `True` from `before_node` → `SeamContractError` teaching error → fault; the body is not skipped silently and `True` never becomes the node's output (§6.2 guards; same for `StateT`/`SeamContext`/`bytes` returns).
42. A broker failure during the terminal `ReturnCall` publish → fault synthesized on the pre-mutation stack snapshot, addressed to the *caller* (never back to the node itself via a phantom callee frame); mid-fan-out partial publish → batch tombstoned, exactly one fault to the caller, late siblings floor as strays (§6.8).
> **⚠️ OUTDATED (PR-6 / 2026-06-17):** scenario 43 below references the deleted v1 aggregator — "the batch's events partition," the "lookup-miss refresh rule," "publishes an outcome record that stray-floors at the aggregator," and "companion spec §5." Rescoped under the in-node design — see `in-node-fanout-aggregation-spec.md` §6 (recovery) / §6.7: the scenario is **graceful rebalance** (ungraceful can strand, out of scope); the new owner reads the durable **node-scoped tables** and continues folding (no events partition, no lookup-miss refresh); a post-closure duplicate sibling reply floors **in-node**. The "completes, no strand, no duplicate, exactly one re-entry" outcome holds.

43. Agent fans out on member M; a rebalance moves the batch's events partition mid-flight → the new owner resumes from the durable state (lookup-miss refresh rule), late siblings fold normally, the batch **completes** — no strand, no duplicate execution, exactly one re-entry (§7.5/§7.7; companion spec §5). A *post-closure* duplicate sibling reply still publishes an outcome record that stray-floors at the aggregator.
> **⚠️ CORRECTION (PR-6 / 2026-06-17):** the output-position seam-substitute validation in scenario 44 applies **only when the agent's declared output type is schematizable**. An exotic/unschematizable `OutputSpec` skips the seam-side check (lenient) — the substitute is not validated at the seam in that case.

44. Structured-output agent (`final_output_type=ResearchReport`) with `surface_to_model` → a surfaced failure string resolves the slot without output-type validation (slot position, exempt); the same agent's `after_node` returning `"redacted"` fails at the seam with a typed fault (output position, validated) (§6.3).
45. `before_node` cache handler keyed on ingress fires on a *sequential continuation* delivery only if it ignores `ctx.delivery_kind` — the documented discriminator prevents replacing an in-flight continuation with a cached terminal (§6.4).
> **⚠️ OUTDATED (PR-6 / 2026-06-17):** "sub-aggregator callee" below is v1 vocabulary — there is no separate aggregator. Read it as "a callee that itself fanned out" (and escalated its own closing fault group). See `in-node-fanout-aggregation-spec.md`.

46. An `on_callee_error` handler receives a fault group escalated by a sub-aggregator callee → `fault.find(...)` matches the nested type at depth; the handler's substitute resolves the single slot (§4.4/§6.2).
> **⚠️ CORRECTION (PR-6 / 2026-06-17):** the closure continuation is **unmarked by construction**, not "cleared." The closure re-entry is assembled from the pre-marker-stamp envelope snapshot, so there is no clearing step — the next single `Call` is automatically stateless. See `in-node-fanout-aggregation-spec.md` §4.1 (scenario 47).

47. After a successful fan-out closes, the agent's next **single** `Call`'s reply is unmarked (the closure cleared `fanout_id`, §4.2) and body-dispatches as an ordinary stateless continuation — it is NOT orphan-floored.

## 17. PROPOSED-items index (for review)

Locked since the first draft (no longer proposed): values-only `on_callee_error` and `after_node`; instance-only seam registration; aggregation framework-final on `BaseNodeDef`; `SeamContext` (capability-scoped seam-handler context) + `CalleeResult`; the staged-pipeline internal architecture (§6.8) and the conversions-vs-transitions ownership model (§6.3/§6.9).

**Locked in the 2026-06-11 revision** (ADR-0006): the §4 carriage model wholesale — per-delivery `Envelope.reply` (`ReturnMessage | FaultMessage`), `x-calf-kind ∈ {call, return, fault}` (a fourth `event` kind was considered and rejected, §4.1), `CallFrame.tag` + `in_reply_to` echo, `ReturnCall(value=…)`; schema-on-read reply typing (§4.5); `ModelRetry` rendered to text at origin — no new part kind, no wire-level "recoverable" concept; `RetryPromptPart` survives in-process only (§4.5); identity-preserving escalation — declining never wraps (§4.4); `final_output_parts` retired; the conversion-trio collapse with agent `_resolve_slot` as the SDK's only per-type override (§6.9); `CalfToolResult` agent-internal (§8); stray-reply handling (§6.7). **Retired as structurally unnecessary**: the reserved `calf.` route prefix, the never-match-`'*'` rule, `origin_payload`, the frame-payload overwrite, the consumer/custom coercion carve-outs, and the unmappable-slot escalation rule.

**Settled in the 2026-06-11 review pass** (were open items #1–#6, #8, #9): `FrameRef` topology-only — redaction question dissolved (§4.3); `error_type` scheme with mint-time `calf.` enforcement + `FaultTypes` constants (§4.3); `CalleeResult.value` auto-extraction with raw `parts` alongside (§6.3); sink faults reach the consume body as `ConsumerContext.fault` — `on_callee_error` inert on consumers, loose-name question dissolved (§6.6); partial results = topology metadata only in the group's `details` (§7); `surface_to_model` budget in dedicated `State.seam_budgets` (§8); provider matchers enumerated at implementation (§12); DLT deferral (§13).

**No open items remain** (2026-06-11). The last one — the reply-owing rejection default (#201) — was resolved by **removing `Silent` from the action vocabulary** (confirmed, §10): with substitute / decline (`None`) / raise as the only gestures, a no-output reply-owing terminal auto-faults `calf.delivery.rejected` *by construction*, and the WARNING-vs-auto-fault policy knob dissolved with the gesture it existed to govern. Also locked in the same pass: §14 reframed to a **single extension family** — no middleware-shaped second family is planned; conditional guard-constraints recorded for any future proposal.

> **⚠️ OUTDATED (PR-6 / 2026-06-17):** the round-1 narration below ends with "the **co-location posture shipped by default** (§7.7 — client keying + partition parity + co-locating assignor; steady-state multi-replica fan-out works, rebalance window remains)." That co-location design was rejected and then superseded by the **in-node** durable fold — see `in-node-fanout-aggregation-spec.md` (and §7.7 above, "co-location rejected"). There is no client keying / partition parity / co-locating assignor and no residual rebalance window; multi-replica fan-out works because batch state is durable in node-scoped ktables.

**Review round 1 applied (2026-06-11; five adversarial lenses — distributed-systems, DX, coherence, code-grounding, P1 failure-hunt).** The locked architecture survived intact; the round's findings were folded as: the fault boundary widened to enclose stage 0 and the terminal publish, plus the pre-handler decode floor (§6.7/§6.8); the total `_classify` contract (§4.1); the stateless batch-of-one/continuation rule and stray-before-seams ordering with stray-fault flooring (§6.7); batch tombstoning, slot idempotency, the `seam_budgets` merge (superseded round 2: closure tally; round 3: every-finalization tally), and the horizontal-scale honesty paragraph (§7); the `max_workers=1` registration invariant (§4.1); slot-scoped `on_callee_error` raises + the `NodeFaultError` mint bypass (Ryan, §6.5); **consumers have no seams** — `ConsumerContext` is the whole observer surface, and observers never produce (Ryan, §6.6); the `calf.retry` metadata marker restoring provider fidelity (Ryan, §4.5 — the text-only equivalence claim was verified false on Anthropic); `acks=all` + idempotence producer defaults (Ryan, §13); `ErrorReport` carriage budgets + `report_id` (§4.3); the seam-context additions (`failing_call`, `awaiting_reply`, `delivery_kind`, `exception` — §6.3); coercion teaching guards (§6.2); the fixed flagship example + chain-order trap + `surface_to_model(max_failures, only_types, exclude_types)` keyed by tool (§6.1/§8); the bounded self-retry loop (§8 — default 0 at the time, **superseded round 2: default 1**); `report.walk()/find()` (§4.4); TailCall frame-identity preservation (§4.2); client-edge floor duties (§11); mirror rows for no-reply hops (§4.2); #143 re-pointed as implemented-by-this-spec (§15); the **co-location posture shipped by default** (§7.7 — client keying + partition parity + co-locating assignor; steady-state multi-replica fan-out works, rebalance window remains); and ~25 coherence repairs throughout.

> **⚠️ CORRECTION (PR-6 / 2026-06-17):** the round-2 narration below records "**the client decode shim fails the pending future**" as a folded decision. That client-edge fail-the-future is **DEFERRED to the reception PR** and is **NOT in PR-6** — see the §6.7/§11/scenario-#30 corrections. PR-6 floors + re-raises at the client reply subscriber exactly like a node (floor-only is the ceiling there too); an undecodable reply currently hangs the caller under `reply_ttl=None`.

**Review round 2 applied (2026-06-11; four reports — distributed-systems, DX, coherence, combined grounding+P1).** Every round-1 fold was verified present and implementable against the installed stack (the decode shim has a confirmed FastStream middleware mechanism; `RangePartitionAssignor` ships in aiokafka 0.13 with the FastStream knob exposed; client keying is a one-argument change; all code citations re-verified). Round 2's findings, folded: **the durable `fanout_id` frame marker** (Ryan — closes the round's one structural hole: lost-batch siblings passed the stateless-continuation validation and would have body-dispatched as duplicates, forking the workflow; now they orphan-floor, §4.2/§6.7/§7.5); **self-retry default revised 0 → 1** (Ryan — vendored pydantic_ai `retries=1`/ADK parity; budget-0's `details.reason="self_retry_disabled"`; the one-valid/one-invalid alternation recorded as an accepted gap, §8); the §6.8 sketch restructured to actually encode its claims (stage-0 guard with no-seams flooring, pre-mutation stack snapshot, a dedicated publish guard that never re-enters `on_node_error`, mid-batch recovery chain skipped, `_Minted` for `NodeFaultError` inside `run_chain_guarded`, slot-scoped coercion failures in stage 1); §7.7 legs hardened (Range only — sticky verified non-co-locating; unconditional read-only partition-parity probe at startup; raw-producer keying line + fan-out partition tripwire; `[Range, RoundRobin]` rolling-deploy advertisement); `seam_budgets` redefined as a closure-computed tally (the merge had no reset writer); the §6.3 output-type validation scoped to output-position substitutes (slot substitutes exempt — `surface_to_model` works on structured agents); the client decode shim fails the pending future; a 256 KB total report cap; fault publishes carry the inbound context unchanged; floor paths log-only-guarded; §4.1 compat claim scoped (no mixed-version rolling deploys); and ~20 sweep repairs (scenario 17/27/37/38 corrected, scenarios 43–46 added, `emit_to_node`→`send`, ADR-0003's #143 line, `max_attempts`→`max_failures`, hook→seam-handler, CONTEXT.md lag).

**Review round 3 applied (2026-06-11; two delta-focused reports — distributed-systems verification + coherence sweep). Zero architectural findings; the locked design held for the third consecutive round.** Both reviewers converged on the same two real gaps — both sequential-side consequences of round-2's parallel-side fixes, both folded: the **`fanout_id` lifecycle** (the marker had no clearing rule, so the first single call after a successful fan-out would have orphan-floored its own legitimate reply — now: stamped at batch registration only, cleared from the node's own frame at closure, in-flight sibling copies keep their stamps, TailCall clears; scenario 47) and the **`seam_budgets` writer** (closure-only meant sequential agents never incremented, unbounding `surface_to_model` on the default path — now: tally at every slot finalization, closure *and* the single stateless continuation; scenario 35 rewritten off the retired merge language). Also folded: the stage-0 disposition unified (§4.1 = §6.8: call-kind ingress faults-where-readable, reply-kind junk floors only — never faults a live invocation); the §6.2 `bytes` guard landed (was cited but unstated); ADR-0004's stale `#143`; the parity probe's pinned read-only mechanism (`MetadataRequest(allow_auto_topic_creation=False)` — aiokafka's public `describe_topics` auto-creates); the `[Range, RoundRobin]` dual-join rebalance-churn caveat (verified 0.13.0) weighting atomic restart; murmur2 named in the raw-producer contract + tripwire; `_aggregate` slot-outcome consultation noted in the sketch; "at most one outcome" quantifier; per-tool scoping of §7.6 vs the self-retry key's own writer; scenarios 43–46 re-ordered after 42 (renderer numbering); and five wording repairs (§4.2 "accompany", §4.3 cap honesty, §7(3) group-state, §10 route-decline naming, §6.9 second writer). **Review round 4 (2026-06-11; micro-verification): CONVERGED.** Both round-3 MAJOR fixes verified by adversarial tracing — all four `fanout_id` lifecycle sequences hold (registration-only stamping; own-frame clear at closure; persistent sibling copies; TailCall clear), all three `seam_budgets` traces hold (sequential bounded via the continuation writer; race-free mid-batch reads; per-tool reset isolation) — with every artifact agreeing and zero CRITICAL/MAJOR remaining. Two residual MINOR wording touches folded (the §7.6 success-definition parenthetical; the §6.8 stage-2 comment). **The review-until-convergence requirement is satisfied: four rounds, eleven reviewer reports, the locked architecture unchanged since round 1.**

**Post-convergence topology episode (2026-06-12): the single-entrypoint merge — explored, delta-reviewed, REJECTED.** Prompted by the parity-probe remediation question, the call/reply topic split was provisionally collapsed to one entrypoint topic per node (deleting the parity probe, the `RangePartitionAssignor` pin, and the rolling migration). A same-day focused delta review verified the merged mechanics sound but surfaced the decisive costs as two by-construction→by-convention degradations: entrypoint uniqueness (a chosen string instead of a `node_id`-derived name — violation = identity-blind workflow fork) and topic-config ownership (ops-configured instead of `framework=True` — a compacted correlation-keyed entrypoint silently deletes unconsumed sibling replies). Ryan rejected the merge on those grounds plus the loss of **shared-ingress listening** (multiple agents independently consuming one work topic — supported under the split because replies route home). **Reverted in full**: §4.1 restores the two-topic block with the by-construction guarantees now stated explicitly; the rejection is recorded in §4.1's decision record and ADR-0006. Survivors of the episode (topology-agnostic, verified during it): the `fanout_id` marker and all batch-integrity rules, the kind classification, the entire wire model.

> **⚠️ OUTDATED (PR-6 / 2026-06-17):** this entire closing narration asserts the deleted v1 *separate* fan-out aggregator as the resolution — the events stream, the **global** state table, `FanoutClosure`-via-`DataPart`, the aggregator escalating via abort records, the ownership-gain sweep, the "~15-line `barrier()`" framing, and "post-closure strays floor at the aggregator." It was replaced by the **in-node durable fold** — see `in-node-fanout-aggregation-spec.md` (the authoritative successor; the `durable-fanout-aggregator-spec.md` companion is retained only for its rejected-alternatives record). What is now true: no events stream and the re-entry carries **no payload** (closure state is read from the tables); the sweep is **deleted**; state lives in **two node-scoped** compacted ktables (not one global table); `barrier()` ships as a ktables **v0.2.0 method**; and post-closure strays floor **in-node**. The reviewed-to-convergence decision trail otherwise stands.

**The open item RESOLVED (2026-06-12): the durable fan-out aggregator** — designed through extended discussion and folded as the companion spec `docs/designs/durable-fanout-aggregator-spec.md`; this spec's §6.8 batch arms, §7.5, §7.7, §13(2), §15, and scenario 43 now bind to it. The decision trail, recorded: co-location machinery rejected (deployment constraints + internals workarounds); replies flow to the node first so the per-sibling seam timing is preserved exactly — option (i), Ryan; state layer = **global table** (zero partition invariants anywhere — Ryan's deciding requirement; ktables as published, no library changes), with the **partitioned/scoped table recorded as the future scale lever** if traffic ever demands it (static `partitions=` flag + replace-on-rebalance + the equal-counts two-integer check; thin-records/claim-check as the orthogonal lever — companion spec §8); open record carries the envelope snapshot once; abort records replace in-memory tombstones (the aggregator escalates exactly once); tombstone-before-re-entry (at-most-once, consistent system-wide); re-entry = `kind=return` with `in_reply_to = fanout_id` (no new kinds, no union changes); no timeouts in v1; version field from day one. Considered and rejected with reasons (companion spec §7): scoped+equal-counts, scoped+explicit-placement, single-topic replay, the two-stage pipeline, **Quix Streams** (reviewed against its docs and PyPI 2026-06-12 — windowed-only aggregations, deprecated `reduce`, changelog topics that import the same equal-counts invariant, blocking librdkafka runtime with no asyncio, native deps), Kafka transactions, external stores. The rebalance-strand window closes; the node becomes fully stateless for batches; `max_workers=1` is re-examined at the aggregator review. **The focused review round over the companion spec ran same-day** (one distributed-systems reviewer; 14 findings, 2 critical, all folded): the lookup-miss refresh rule was unimplementable on ktables-as-published — the catch-up gate is a one-shot boot latch — so **ktables gains a ~15-line `barrier()` method** (end-offsets-now freshness barrier; the only library change, `partitions=` stays future); the abort arm now **tombstones first** like close (escalate-then-tombstone left a zombie-close window — two outcomes for one frame); the re-entry payload got its wire shape (`FanoutClosure` via `DataPart` — correct under §4.5's own ownership-follows-the-interpreter principle: one framework interpreter); the OPEN is **acked before any sibling is sent** (makes the causality claim airtight) and an OPEN-publish failure faults the caller directly (no batch exists to abort); both framework topics' partition counts must not change post-creation — **a documented operational contract, not an app-code check** (Ryan: deployment config is ops' domain; zero invariants *between* topics, one documented contract per topic — growth mid-flight would remap live batches); re-entry publish failure falls back to a caller fault (`calf.fanout.reentry_failed` — covers the deterministic oversized-closure case); the marker simplification landed here too (§4.2: snapshot captured pre-stamp ⇒ the closure continuation is unmarked by construction, no clearing step; post-closure strays floor at the aggregator, and node-side `on_callee_error` handlers must tolerate firing for later-floored outcomes); the residual crash windows are enumerated in full with the permanent-open-corpse artifact class owned honestly (ownership-gain sweep recorded as a lever); `max_workers=1` resolved as standing. **Next: implementation planning** (worktree confirmation; §15 sequencing — ktables `barrier()` + aggregator PR first; mint the successor retry/redelivery issue).
