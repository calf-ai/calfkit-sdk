# Intermediate Step Streaming — Design Decisions

> **Status: IMPLEMENTED** (design converged over four adversarial review rounds; the implementation plan
> had two plan-review rounds folded, then shipped as increments A–F). This document pins the **as-built**
> design for streaming mid-turn (per-hop) **steps** back to a caller. The load-bearing decisions are
> recorded in ADR-0025 (separate `StepMessage` wire type + `x-calf-wire`), ADR-0026 (the unified, non-frozen
> step-event type), and ADR-0027 (happy-path-only, best-effort emission).
>
> **Parent:** [`client-caller-surface-spec.md`](./client-caller-surface-spec.md) — this realises that
> spec's deferred feature **§9.1 "Worker-side intermediate event emission."** Where this design changes
> a parent-spec contract, it is called out inline (the parent must be updated to match).
>
> **What it is, in one line:** every happy-path agent/tool hop publishes a small, side-effect-free
> `step` message — a batch of typed events (assistant text, tool calls, tool results, handoffs) — to the
> run's inbox, which `handle.stream()` and `events()` surface for a live UI view of the task's progress.

---

## 1. Current reality (the problem)

A run is silent on the caller's inbox until its terminal, by construction: one delivery = one model
step, multi-turn is Kafka re-entry, and the only thing published to the caller is the terminal
`ReturnMessage`/`FaultMessage` (`nodes/base.py:562-590`/`688-798`). Intermediate assistant text is
persisted to `message_history` but never selected for the wire (the terminal is built from
`result.output`, `nodes/agent.py:805-817`; the wire `ContentPart` has no thinking type, `models/payload.py:37`).
The client is closed-once and terminal-only (`client/hub.py:62-69`), the `RunEvent` union is the
closed terminal pair (`client/events.py:58-60`), and the `_RunChannel` intermediate buffer was never
built. This feature adds emission and reception.

---

## 2. Confirmed design

### 2.1 Multi-producer, happy-path only

The run's observable trace is the **action graph**, and steps are authored by whichever node took the
hop. **Every node that takes a happy-path action publishes the step(s) it authored:**

| Producer | Emits |
| --- | --- |
| Agent node | `AgentMessageEvent` (preamble), `ToolCallEvent` (per requested call), `HandoffEvent`, and `ToolResultEvent` — both for an **agent-rejected invalid call** (`is_error=True`, draft-authored) and when its own `ReturnCall` is inner-frame (a consulted peer answering) |
| Tool node | `ToolResultEvent` (always — success or `is_error=True` for a `ModelRetry`; never a run root, §3.2) |

**Steps are a quality-of-life, happy-path observation channel — not business logic.** They are emitted at
the single **disposition chokepoint** — `_handle_delivery` at `nodes/base.py`, where the hop's
`output` is known before it branches to `_handle_fanout_open` (`:1790`) or `_publish_action` (`:1803`), so
fan-out and non-fan-out hops project uniformly (§2.5). Infrastructure failures (a tool crash, an escalating
fault, a declined/aborted delivery, re-entry) are **framework concerns** — they propagate to the caller as
the terminal `RunFailed` via the existing fault rail, and are **never** emitted as steps. So `project_steps`
is not called at `_publish_fault` / abort / decline / re-entry — only at the happy-path chokepoint.

### 2.2 What is and isn't surfaced (model mistakes)

A **calfkit-caught** invalid call is surfaced — the dispatch loop rejects an unknown tool
(`nodes/agent.py:651`), malformed args (`:676`), or a failed validator (`:702`) **within the same hop**,
so it batches into that hop's `step` message as a `ToolCallEvent` + a `ToolResultEvent(is_error=True)` carrying
the retry/error content (no bespoke `ToolFailed`/`InvalidToolCalls` type — a rejected call is just a tool
call whose result is an error; §3.2). The all-invalid self-retry `TailCall` (`:748`) is therefore
surfaced, not suppressed.

**Out of scope (documented, deliberate):** pydantic-ai's **internal** retries are **not** surfaced. A
hallucinated tool name is classified `unknown` and auto-retried *inside* one model run (`:600`), never
reaching `tool_results`; we do not dig those out of `result.new_messages()` for v1. Sourcing the full
internal model-turn history is a possible future enhancement, explicitly deferred.

### 2.3 Transport — emit to the root inbox at the base-node seam

At the disposition chokepoint (§2.5, `base.py`), `base.py` publishes the hop's step batch to the
**root** frame's `callback_topic` (the bottom of `envelope.internal_workflow_state.call_stack`, set by the
client at `client/caller.py:349`), keyed by `correlation_id`. Every publish **on the reply rail** is
already `key=correlation_id.encode()`, so steps co-partition with the terminal. Emission is
**unconditional** — there is no client-vs-node "root" gate (see §2.4 for why node topics are safe).
The step publishes **before** the action branch, so a step precedes its action on the wire. **[Decision —
owner: the step may publish first.]** A rare action-publish failure or fan-out-OPEN abort can therefore
leave a phantom step (e.g. a `ToolCallEvent`) ahead of the resulting `RunFailed` — accepted as within the
best-effort / close-on-terminal contract (§3.3), not designed around.

### 2.4 A `step` is a distinct message type, not an `Envelope` reply

**[Decision — hard cutover, no rolling-deploy compatibility]** A step is a **side-effect-free
notification**, not a request/reply outcome — so it is its **own** top-level wire model, **not** a
member of `Envelope.reply` (which carries `return`/`fault` business outcomes). This:

- **Eliminates the envelope-body leak by construction** — a `StepMessage` carries only step data
  (correlation, hop identity, events), never the `Envelope`'s `state`/`deps`. New fields are added to
  the schema as needed.
- **Routed by a positive wire-schema header filter, verified pre-body-deserialization (0.7.1).** A
  subscriber's `filter` runs in `is_suitable` after the header-bearing parser but *before* the body is
  decoded into the handler's type (`_internal/endpoint/subscriber/call_item.py:104-127` — the decoder is
  *set, not invoked*, before `await self.filter(message)`), so a header filter rejects a `step` body
  **without** triggering `Envelope` validation — no decode-floor false-fault (the original C1). A
  dedicated, always-stamped `x-calf-wire ∈ {envelope, step}` header carries the wire-schema (leaving
  `x-calf-kind` as the business kind), matched **positively**: the `Envelope` handler filters
  `x-calf-wire == "envelope"`, a `step` handler `== "step"`. Positive equality (not a negative
  `!= "step"`) means a future third wire type never falls through to the `Envelope` decoder. Filters
  attach on the subscriber object — `sub = broker.subscriber(topic); @sub(filter=…)` — not a
  `broker.subscriber(filter=…)` kwarg.
- **An unmatched message is dropped, the consumer survives, and FastStream logs it — relied on (no
  step-drop handler).** If no filter matches, `process_message` raises `SubscriberNotFound`
  (`usecase.py:398`), `consume()` swallows it (`except Exception: pass`, `:318`) so the consumer survives
  (verified: driving the production `consume()` with an unmatched record returned without raising, and
  subsequent matching messages still routed), **and** `CriticalLogMiddleware` — always wired into every
  subscriber (`usecase.py:409/420`) — logs the `SubscriberNotFound` at **ERROR with traceback**
  (`middlewares/logging.py:83-89`, since it is not an `IgnoredException`), via the broker's `logger_state`
  (active in a Worker app; it was silent on the bare *client* broker tested — but the client always
  matches, since it carries the `step` handler). **[Decision — owner: rely on FastStream's ERROR.]** So a
  node carries **only** its `Envelope` handler: a `step` landing on a node topic → `SubscriberNotFound` →
  swallowed + ERROR-logged. No `step`-drop handler is added on nodes (the ERROR-per-dropped-step is the
  accepted, intended signal). **Accepted consequence:** the parent's blessed `@consumer`-draining-a-named-
  inbox pattern (parent §4.6) — if that inbox is a run's root callback — receives every step and produces
  one ERROR-with-traceback *per step, per hop, all depths* (the consumer never sees the steps). This
  ERROR-storm is **accepted** (owner); revisit only if a real deployment finds it intolerable.
- **v1 wiring is minimal — no handler registry.** The wire value is a `WIRE` ClassVar on each model
  (`Envelope.WIRE = "envelope"`, `StepMessage.WIRE = "step"`) — the single source for both the inbound
  filter and the outbound stamp, so they can't drift. A node has one business entrypoint today (`handler`,
  framework-fixed and `Envelope`-typed, `nodes/base.py:1616`); the worker registers it with
  `filter=wire_filter(Envelope)` — **one kwarg, no node-side step handler** (an unmatched `step` →
  `SubscriberNotFound` → FastStream ERROR-logs + drops, the chosen behavior, `worker/worker.py:400`). The
  client hub uses **one** groupless subscriber (`client/hub.py:184`) with **two filtered call-items** — its
  real `step` handler (`_on_step`) + the `envelope` handler — the only `step` *consumer* in v1.
- **Extension point (deferred, named — not built in v1):** when a node first needs to *consume* steps, a
  `@wire_entrypoint(Model)` marker + a `WireEntrypointMixin` registry — a faithful, **additive** parallel
  to the existing `@handler`/`RegistryMixin` idiom (`_registry.py`: a marker attribute collected per
  subclass in `__init_subclass__`, but keyed by wire-model instead of route, on a *different* marker attr
  so the two registries never collide) — turns that one filtered call into a
  `for method, model in node.wire_entrypoints: subscriber(method, filter=wire_filter(model))` loop.
  Building it now would be a one-entry registry (a node that consumes steps doesn't exist yet), so it is
  deferred per "final implementation or defer."

**Produce side — the stamp must be universal (or runs hang).** Because the filter is strict positive
(`== "envelope"`, no absent-fallback by decision), the consume-side filter only works if **every**
message carries `x-calf-wire`. An unstamped message misses the filter → dropped (this is the explicit
intent for foreign producers; calfkit's own publishes must all stamp). The stamp centralizes to exactly
**two sites** (verified): `_headers()` (`nodes/base.py:494`) — the sole header builder for every node-rail
publish (Call / Return / TailCall / fault / fan-out OPEN siblings / re-entry / the broadcast mirror) — and
the client ingress header dict (`client/caller.py:354`). The step publish stamps its own `"step"`. The
value comes from each model's `WIRE` ClassVar (single source for stamp + filter) — concretely **`Envelope.WIRE`
at `_headers`** (which takes no `msg` arg — it only ever serves `Envelope` publishes, so it references the
constant, not `type(msg).WIRE`) and **`StepMessage.WIRE` at the step publish**. The new
`x-calf-wire ∈ {envelope, step}` header constant + `WireKind` literal live beside `HDR_KIND`/`MessageKind`
(`_protocol.py`); `MessageKind` (the business kind) and `Envelope.reply` are **unchanged**.

### 2.5 Emission mechanism — action-coupled projection + `StepDraft`

A polymorphic `project_steps(output, ctx, frame) -> list[StepEvent]`, called **once per hop** at the
**disposition chokepoint** — `_handle_delivery` at `nodes/base.py`, where `output` (the
`NodeResult`) is fully determined, *before* it branches to `_handle_fanout_open` (`:1790`) **or**
`_publish_action` (`:1803`). Both publish paths flow through this one point, so coupling projection here
covers fan-out and non-fan-out hops uniformly (the round-2 hole was projecting only inside
`_publish_action`, which an N-way fan-out / any `isolate_state` consult skips entirely). It is **not** a
free-form `ctx.emit()` (which would forfeit the once-per-hop guarantee and the single-publish-seam
invariant; intra-hop progress is out of scope — whole-turn grain only). **The whole projection + step
publish is wrapped in its own `try/except` that logs-and-drops and then *falls through* to the real
action publish (§2.9).** This is load-bearing: the disposition chokepoint runs **outside** the outer publish guard (which
wraps only the `_publish_action` call at `:1803`), so an *unguarded* projection raise would **escape to
FastStream** and — since ACK_FIRST has already acked the inbound — the real action would never publish and
**the run would hang** (worse than a fault). The own `try/except` must therefore both swallow the error
*and* not short-circuit the action that follows.

The agent's authored events for the hop — the residual not derivable from `output`/`ctx`/`frame` — ride a
transient `PrivateAttr` slot mirroring the existing `_reply`/`_frame_id` family
(`models/session_context.py:179-185`). **The draft is typed to the step-event kinds (a discriminated
union on `kind`), not generic `ContentPart`s** — the node says explicitly *what* it is surfacing, the
types match the wire `StepEvent`s the caller deserialises, and structured events (`HandoffEvent`'s
`target`/`reason`) have a natural home:

```python
_step_draft: list[StepEvent] | None = PrivateAttr(default=None)   # off-wire; per-hop; framework-written
# the hop's authored events; their identity stays unset here and is back-filled caller-side by StepMessage's validator (§3.1)
```

- **Off-wire, per-hop, framework-internal.** `PrivateAttr` → never serialised; rebuilt each Kafka
  re-entry; written by the framework's agent body in `run()`, read-and-cleared at the chokepoint. The
  context object `run()` mutates **is** the one that reaches the chokepoint (the deep copy in
  `prepare_context` happens *before* `run()`), so the draft reaches it.
- **The agent authors its own events directly.** `run()` stashes typed `AgentMessageEvent` (preamble),
  `ToolCallEvent` (the model's requested calls — sourced from the model emission, **not** the dispatch
  action, since an invalid call is never dispatched and `sequential_only_mode` dispatches one-per-hop
  while the model requested them together), a `ToolResultEvent(is_error=True)` for **only** the calls it
  rejected *this* hop (right there in the dispatch loop — so there is no `tool_results` accumulator scan
  and no re-emit of prior hops' failures), and `HandoffEvent` (from the model's `HandoffRequest`,
  emitted for an online *or* offline target — §3.2). `project_steps(agent)` returns the draft; a
  `ToolResultEvent` from a tool node / consulted peer is **not** in the draft — it is action-derived at the
  seam (an inner-frame `ReturnCall`, §3.2). So `ToolResultEvent` has two producers (draft for a pre-dispatch
  rejection, action for a real return), one type.
- **Pre-model re-dispatch hops emit nothing** (`:556-576`: no model run → no draft → no step), keeping
  *one model hop = one batch* and avoiding double-emit. Per-hop *metadata* (token usage, latency) is not
  smuggled into events; if ever wanted it is a separate, named slot.
- **An empty projection publishes nothing.** If `project_steps` returns `[]` (a pre-model re-dispatch
  hop, or a depth-1 terminal `ReturnCall`, §3.3), the chokepoint publishes **no** `StepMessage` — so
  routing hops and the terminal hop never flood the inbox with empty steps.

### 2.6 Per-hop batching — one hop, one `step` message

A hop's events ship as **one** `StepMessage` carrying `events: list[StepEvent]`, never N messages. The
hop is the natural **atomic unit** (a model turn emits preamble + all tool calls simultaneously), so
batching has **no latency cost** and is chosen for **atomicity** (the client never sees half a hop) and
semantic cohesion — *not* throughput (the Kafka producer already coalesces sends). A hop batch is bounded
by the model's output, and the large payloads (`ToolResultEvent` full values) are their own single-event hops;
an oversized or otherwise-failed step publish is simply logged and dropped (§2.9), never faulting the run.

### 2.7 `depth` on every event; all depths stream

Hops at **all depths** stream to the original caller — a sub-agent's or peer's full internal trace is
**wanted** (a live view of the whole task's progress, §4). Every `StepEvent` carries `depth` so the
caller can orient. **`depth` is measured from the inbound call-stack snapshot, before the action mutates
it** (`base.py` holds the pre-action snapshot ~`:1666`) — a single rule for every action type, not a
per-action `±1` (a `Call` pushes, a `ReturnCall` pops, a `TailCall` is depth-neutral, so measuring
post-mutation would be wrong per-case).

### 2.8 Reception buffering — consume-once queue + cached terminal

`_RunChannel` keeps intermediate and terminal state **separate**, because they have different access
semantics:

- **Intermediates → an unbounded, consume-once queue with its OWN wake signal.** Push is non-blocking (so
  the hub never stalls) and is a **no-op once `_closed`** (a post-terminal reordered/duplicate step is
  dropped, not appended — §3.3). A `stream()` drains the queue — buffered backlog first (a late `stream()`
  gets the events that already occurred), then live, **once**. Memory drains as consumed. (A
  held-but-undrained handle, e.g. `execute()`/`result()`-only, accumulates the queue until the
  terminal/GC — accepted here; no per-run drop policy, and the resulting memory cost is accepted in §2.10.)
- **Terminal → a cached slot, idempotent.** `result()` reads it repeatably (works even after `stream()`
  already yielded it); `stream()` reads (does not consume) it as its terminal-bearing last element. The
  existing close-once/"first wins" dedup stays on the terminal slot only.

**Wake model (the load-bearing part — pin it positively, not via one negative guard-rail).** The two
storages have two **separate** signals, both `asyncio.Event`s (cancel-safe — the same reason the terminal
signal is an `Event` not a `Future`, `hub.py:49-55`):
- **terminal / close** (`push_terminal`, `close_with`, `fail_run`) sets **both** the terminal signal **and**
  the queue signal — so a `stream()` parked on the empty queue wakes on normal completion *and* on an
  error-close (`aclose()`→`ClientClosedError`, decode-floor→`fail_run`), drains, then yields/raises the
  cached terminal/close-error **as its last act**.
- **intermediate push** sets **only** the queue signal (never the terminal signal — that would spuriously
  wake a parked `result()`).
- `result()` parks on the terminal signal only; `stream()` drain-loop must keep **no `await` between the
  empty-check and the signal-clear** (the lost-wakeup invariant the firehose relies on, `events.py:127-131`).

Intermediates flow through a **dedicated `_on_step` handler → `push_intermediate`** (the §2.4 step
subscriber naturally gives one), **never** through `_dispatch`/`push` (terminal/return/fault only).
`stream()` is an **`async with`-scoped single iterator** so an early `break` releases cleanly (no
`_stream_active` leak / spurious "one live stream" `RuntimeError`).

**Do-not-reuse (all terminal-by-construction):** the terminal signal; the terminal-minting `_slot_anomaly`
(`hub.py:233-244`, would close a live run on a malformed step); **`fail_run` / the undecodable sink** (a
malformed/schema-skewed *step* must NOT fault the run — kept away from the topic-keyed sink by the
**lenient per-call step decoder** that swallows the decode error so the floor never sees a raise, §3.4);
and the no-handle `_dispatch` `else` (a `send()` run's steps drop **silently**, still tee'd to the firehose).

**Parent-spec changes (complete list):** §3.1 ("`stream()` yields exactly one element — the terminal" →
yields intermediates then terminal); §3.3 (the `RunEvent` "future shape" comment → shipped); §4.4
("result-then-stream yields just the terminal; intermediates not replayed" → "a late `stream()` also
drains the buffered intermediates, consume-once"; "result() alone *discards* intermediates" → *retains*
until terminal/GC); §4.5 (the blanket "per-run channel is dedup'd" → terminal dedup'd, intermediate queue
not); §5.3's single-channel description + its "small, finite event count" sizing rationale (now two
storages, queue can hold the full all-depths trace); §5.5 (terminal-slot dedup vs the no-dedup
intermediate queue); §9.1 (this realises the deferred feature — its "per-run overflow policy" prerequisite
is consciously **not** taken, §2.8/§2.10). Per the line-9 discipline, the parent must be updated to match.

### 2.9 Delivery semantics — best-effort, lossy, no dedup

Steps are **best-effort and at-most-once.** Under ACK_FIRST a worker that acks then crashes before the
step publish loses that step permanently — **accepted.** There is **no dedup machinery** (no broker
redelivery to dedup; a rare hub re-fetch dup is fine for a QoL firehose); hop-identity fields exist for
*correlation*, not dedup.

**Publish-failure handling (never faults the run):** the **entire** step emission — projection
(`project_steps` + the `_step_draft` read/clear) *and* the publish — is wrapped in one `try/except` at the
chokepoint (§2.5); on **any** failure (a projection bug, an oversized step, a transient broker error) it
**logs and moves on**. No retry (the strip-and-retry idea was dropped — best-effort observation does not
warrant it, and a stripped event couldn't satisfy its own schema). A failed observation must never affect
the run outcome; the wrap is required because the chokepoint is **outside** the outer publish
guard (which wraps only `:1803`), so an unguarded raise would escape to FastStream and **hang** the run
(§2.5). The wrap must fall through to the real action publish, never short-circuit it.

### 2.10 Performance — accepted, no mitigation

The extra publish per hop, the single-partition-per-run hotspot, and the single groupless hub consumer
are **accepted**; we do not design around them. Intermediate streaming targets moderate-hop interactive
runs.

---

## 3. Wire & event model

### 3.1 `StepMessage` (the wire body for `x-calf-wire == "step"`)

A new top-level model (not under `Envelope.reply`). Hop-level identity sits **once** on the message;
the events ride bare on the wire and pick up identity caller-side:

```
StepMessage:
  correlation_id: str
  emitter: str            # the producing node's name
  depth: int              # §2.7
  frame_id: str
  events: list[StepEvent] # the hop's batch (§2.6)

  @model_validator(mode="after")    # the "blessed factory": stamps identity onto each event
  def _stamp_identity(self): for e in self.events: e.correlation_id/.depth/.frame_id/.emitter = self.<...>
```

**One event type, identity excluded-from-wire (empirically validated).** Each `StepEvent` carries its
caller-facing identity (`correlation_id`/`depth`/`frame_id`/`emitter`) as `Field(default=None,
exclude=True)` — a real, readable public field that **does not ride the wire** (nested `exclude=True` is
dropped from `model_dump`, the same idiom as `ToolBinding.validator`, `tool_dispatch.py:48`). So identity
is serialized **once** on the `StepMessage`, never per event. Two blessed construction paths control the
fields: **node-side**, `run()` authors an event with event-specific fields only (identity unset, never
read in the draft/wire roles); **caller-side**, the `StepMessage` `model_validator(mode="after")`
back-fills each event's identity from the message — so unpack is just `StepMessage.model_validate_json(...)`
and events emerge identity-stamped (no separate type, no copy-construct). *(Confirmed: a wire round-trip
drops nested identity, the validator back-fills it on decode, and the discriminated union resolves
correctly.)*

> **Load-bearing constraint (verified): `StepEvent` models MUST be mutable (non-frozen).** The
> `model_validator` stamps identity by **in-place assignment** (`e.correlation_id = …`). This deliberately
> **breaks from** the codebase's prevailing frozen-value-object convention — the very exemplars cited here
> (`ToolBinding` is `frozen=True`; the sibling terminals `RunCompleted`/`RunFailed` are `@dataclass(frozen=True)`).
> A frozen `StepEvent` would make the back-fill raise `ValidationError("Instance is frozen")` **during
> decode** → the lenient step decoder (§3.4) swallows it → **every step is silently dropped, forever**. So
> the `StepEvent` models are non-frozen by design; state it where they're defined so a faithful
> implementer doesn't "fix" them to frozen.

### 3.2 `StepEvent` — a closed, discriminated union (`kind` literal)

> The `kind` literal values are a separate union from `ContentPart`'s `kind` (no collision), but pick
> **distinct** values for readability — e.g. `"tool_call"` for `ToolCallEvent`, not `"tool"` (which is
> `ContentPart`'s `ToolCallPart.kind`). `BaseNodeDef.project_steps` returns `[]` by default (a plain custom
> node emits no steps); `BaseAgentNodeDef`/`BaseToolNodeDef` override it.

| Event | Producer | Carries | Source |
| --- | --- | --- | --- |
| `AgentMessageEvent` | agent | preamble `parts` | `_step_draft` — the **`TextPart` content of the hop's model output** (`result.new_messages()`, the final `ModelResponse` only), excluding thinking/tool-call/file parts; empty ⇒ no `AgentMessageEvent` (see below) |
| `ToolCallEvent` | agent | `tool_call_id`, name, args | `_step_draft` — every call the model requested this hop (from the model emission, **not** the dispatch action) |
| `ToolResultEvent` | tool node, consulted peer (agent), **or** agent (pre-dispatch rejection) | `tool_call_id`, name, **result `parts`**, **`is_error: bool`** | **two producers, one type:** action-derived at the seam (an inner-frame `ReturnCall`, keyed by the frame `tag`; `_coerce_to_parts`), **or** draft-authored by the agent for a rejected invalid call (`is_error=True`, retry content). `is_error` is **derived once at its producer** (see below). |
| `HandoffEvent` | agent | `target` + `reason` | `_step_draft` — stashed from the model's `HandoffRequest.name`/`.message` (`peers/handoff.py:69-70`) |
| `AgentThinkingEvent` | agent | thinking `parts` | **defined, not emitted in v1** (§5) |

- **There is no `ToolFailed` type — a rejected/retried call is a `ToolResultEvent(is_error=True)`.** A tool's
  `ModelRetry`, an agent's pre-dispatch rejection (invalid name/args/validator), and a normal success are
  all "a tool call produced a result" — the only difference (where it was caught) is an implementation
  detail that must not leak into the type. So one type, with `is_error` distinguishing them. (A *hard*
  tool failure that escalates the rail → terminal `RunFailed`, never a step.)
- **`is_error` is a compute-once denormalisation, not a second source of truth.** For an action-derived
  `ToolResultEvent` (tool node / peer) the seam **coerces first, then checks** — `parts = _coerce_to_parts(value);
  is_error = is_retry(parts)` (`payload.py:80-93`) — and reuses `parts` for the event. Order matters: at the
  chokepoint `output.value` is the **raw** return for a tool success (a `str`/`dict`/model, not parts), and
  `is_retry(raw_scalar)` **raises `AttributeError`** (verified); coercing first avoids it (and a missed coerce
  would, via the §2.5 guard, silently suppress the `ToolResultEvent` for every scalar-returning success). For the
  agent's pre-dispatch rejection `run()` sets `is_error=True` directly (it *is* the producer). The
  `calf.retry` marker stays on the model-facing content (provider `is_error` fidelity); `is_error` is its
  caller-facing summary on the observation event.
- **The agent's preamble (`AgentMessageEvent`) needs a new extractor.** The existing `structured_output_preamble`
  (`nodes/_projection.py:100-120`) returns text **only** on the structured *terminal* hop (a `final_result*`
  output-tool call) — which emits no step (§3.3). For a tool-dispatching / handoff hop the model emits plain
  text + ordinary tool calls, so a new extractor is required: concat the `TextPart` text of the hop's
  `result.new_messages()`, the final `ModelResponse` only, **excluding** thinking/tool-call/file parts; if empty, author
  no `AgentMessageEvent`. (This is the marquee event — the impl plan pins the multi-`ModelResponse` edge.) Unlike
  `structured_output_preamble`, this extractor needs **no `has_final_result` guard**: the structured-output-as-
  text case (native/prompted mode, where the `TextPart` *is* the JSON answer) cannot coincide with a
  step-emitting hop — a handoff forces a multi-member output union that pydantic-ai bars from native/prompted
  ("must be the only output type"), and an un-handed-off structured answer is produced only on the depth-1
  terminal hop, which emits no step. (Revisit if native/prompted is ever enabled on a non-terminal hop.)
- **A draft `ToolResultEvent.parts` must serialise non-`str` rejection content.** A `RetryPromptPart.content` is a
  `str` for the unknown-tool / malformed-args / validator-raise arms (`agent.py:656/688/726`) but a
  `list[dict]` for the schema-`ValidationError` arm (`agent.py:703/713`, `e.errors(...)`). `run()`'s draft
  builder must render both into a `TextPart` (optionally `calf.retry`-marked), not assume `str`.
- **Inner-vs-root is stack depth — the *inbound (pre-action) snapshot* depth, the same measure as the
  §2.7 `depth` field.** A `message_agent` consult is a tool call from the model's POV, so the peer agent's
  inner-frame `ReturnCall` is a `ToolResultEvent` keyed by the frame `tag`. The discriminator is **inbound depth
  `== 1` ⇒ the run's terminal (no step); `> 1` ⇒ `ToolResultEvent`** (the client pushes exactly one root frame,
  `caller.py:349`, so the top-level agent runs at inbound depth 1; a tool/peer answering it runs at depth
  ≥ 2). Measure it on the **pre-action** stack, *not* post-pop (a `ReturnCall` pops first, so post-pop the
  terminal is depth 0 and an inner return is depth 1 — measuring post-pop inverts the classification).
  Do **not** use `callback_topic is None` (the client always sets the root's callback to its inbox) or
  `tag is None` (a custom node may `Call` without a tag) — both misclassify. **A tool node always emits
  `ToolResultEvent`** (never a run root — directly-called-tool out of scope); only the **agent's** `ReturnCall`
  needs the depth test.
- **The agent authors its draft events directly, so the two `TailCall`-to-self shapes need no inference:**
  an all-invalid retry stashed `ToolCallEvent`+`ToolResultEvent(is_error=True)`, a stale-handoff retry (offline
  target, `agent.py:399`) stashed `HandoffEvent` — `run()` knows which it produced. A handoff to an
  offline target **still emits** `HandoffEvent` (a happy-path action that raised nothing); the impl
  must not special-case it away.
- `tool_call_id` threads `ToolCallEvent` ↔ `ToolResultEvent` (it rides the `Call`'s `ToolCallRef` out and the
  frame `tag` back). Note a consulted-peer `ToolResultEvent.name` is the peer agent's name, while the
  paired `ToolCallEvent.name` is `message_agent` — they pair by `tool_call_id`, not name.
- **`ToolCallEvent.args` accepts the raw model emission** (`str | dict | None`), not a parsed dict — the
  malformed-args rejection (`agent.py:676`, where `args_as_dict()` *raised*) must still surface a
  `ToolCallEvent` (+ its `is_error=True` `ToolResultEvent`), and there is no parsed dict to carry in that case.

### 3.3 Ordering

Steps and the terminal co-partition by `correlation_id`. In practice they arrive in causal order: a
terminal-bearing hop runs a full model turn (seconds) after the last inner step, so the step is on the
partition long before the terminal exists. The hard guarantee is **client-side close-on-terminal** — a
pathologically reordered late step simply no-ops on the closed channel (a dropped intermediate, never an
out-of-order terminal). We accept the rare reorder and do not design for it.

**The agent's terminal hop emits no step.** On the final-answer hop (a depth-1 `ReturnCall`, §3.2) `run()`
stashes no draft: the preamble already rides the terminal's parts, and a step published *after* the
terminal (the chokepoint runs before the terminal publish, but the close-on-terminal would drop it anyway)
would be a guaranteed-dropped, content-duplicating event. So `project_steps` returns `[]` for a depth-1
`ReturnCall`.

### 3.4 Caller-side reception

- The inbox carries **two filtered call-items on ONE groupless subscriber** (not two `broker.subscriber`
  calls — that would create two consumers fetching every reply twice): a `step` handler
  (`@sub(filter=…=="step")`) and the existing `envelope` handler (`=="envelope"`), §2.4.
- The dedicated **`_on_step`** handler (NOT `_on_reply`/`_dispatch`) receives the decoded `StepMessage`
  (whose `model_validator` has already back-filled each event's identity, §3.1), and `push_intermediate`s
  **each** event onto the `_RunChannel` consume-once queue (§2.8); it also `_tee`s each to the firehose.
  **One unpack point feeds both surfaces** — `events()` streams the same individual events.
- **A malformed/schema-skewed step must NOT fault the run — via a lenient per-call decoder (empirically
  verified against the real `DecodeFloorMiddleware`).** The step call-item is registered with a custom
  `@sub(filter=…=="step", decoder=lenient_step_decoder)`; the decoder validates `StepMessage` and, on
  `ValidationError`/`JSONDecodeError`/`UnicodeDecodeError`, **swallows and returns `None`** — and `_on_step`
  is typed `async def _on_step(step: StepMessage | None)` (drops on `None`, logs). **Load-bearing constraint
  (verified):** the sentinel MUST be `None` (or the handler must take the raw message and decode manually),
  and the handler MUST be typed `StepMessage | None`, **not** `StepMessage`. If `_on_step` is typed
  `StepMessage` (the natural mirror of `_handle_reply(envelope: Envelope)`), FastDepends `apply_types`
  **re-validates the sentinel against `StepMessage` and raises a *second* `ValidationError`** at handler-call
  time — which `DecodeFloorMiddleware.consume_scope` (`middleware.py:91`) then catches → `fail_run` → the run
  is faulted (the exact C1 failure). With the `None` sentinel + `StepMessage | None` signature, the decode no
  longer raises, `call_next` returns normally, and the floor (`middleware.py:84-117`, which only re-routes a
  *raised* decode error) **never sees it**. The per-call `decoder=` is FastStream's first-class slot
  (`sub.__call__`), so the generic floor stays untouched and the decode-failure policy lives on the handler
  that owns it (lenient for the observation rail; strict on the envelope handler, where a malformed *reply*
  still floors → `fail_run`). *(Confirmed empirically: `None` sentinel + `StepMessage | None` → 0 floor-sink
  hits + clean drop; a `StepMessage`-typed handler → second `ValidationError` → floor → fault.)*
- Surfaced types are the **individual** widened `RunEvent` members (`AgentMessageEvent`, `ToolCallEvent`,
  `ToolResultEvent` (with `is_error`), `HandoffEvent` + terminals `RunCompleted`/`RunFailed`). The batch
  never surfaces as a public object; hop grouping is reconstructable from each event's `frame_id`/`depth`.
  Steps surface their **raw** `parts` — they are **not** `output_type`-coerced (that scoping is the
  terminal `result()` only); a preamble or tool result is not the run's typed answer.
- **No-handle steps are firehose-visible.** A step for a run with **no live handle** — a `send()` run
  (which registers none) or a GC'd handle — is dropped from the per-run path **silently** (no log line,
  unlike a no-handle *terminal*), but is **still `_tee`d to the firehose**. So `send()` runs are
  observable via `events()`, just not via a per-run `stream()`.
- Widen `RunEvent` (`events.py:58-60`); the dead `terminal_only` filter (`:138-139`) goes live.
- Firehose drop-oldest is **per event** (a lagging `events()` observer may see a partial hop —
  consistent with its best-effort contract); the held-handle `stream()` is the lossless path.

---

## 4. Security / trust posture

Streaming **all depths** to the original caller is a **deliberate goal**, not a tolerated leak: the
point is a live, end-to-end view of the task's progress, including the activity of consulted peers and
sub-agents. Consequence to **document** (not police — deployment is ops' domain): the run's inbox
therefore carries the full transitive trace (tool results, reasoning, handoffs) of everything done on
the run's behalf, so inbox read-access is as sensitive as the run's full content. Cross-trust-boundary
confinement, if ever needed, is a future depth/redaction knob — not v1.

---

## 5. Thinking vs preamble — pattern documented, activation deferred

Thinking is distinct at the source (pydantic-ai `ThinkingPart`) but absent from the wire `ContentPart`
(`payload.py:37`). The agreed approach (do **not** widen the wire union) mirrors the `calf.retry` marker
idiom (`payload.py:80-89`): map a `ThinkingPart` → a `TextPart` with a `calf.thinking` marker; preamble
stays unmarked; `project_steps` splits marked → `AgentThinkingEvent`, unmarked → `AgentMessageEvent`.
Observation-only and lossy (text for display, not the provider signature — the authoritative thinking
stays in internal history for round-trips).

**v1:** the `AgentThinkingEvent` type is **defined but not emitted**, ready to turn on later (matches the
codebase's forward-compat scaffolding). **No knob is defined now**; the default-on-vs-opt-in choice is
made when thinking is activated.

---

## 6. Decisions — all resolved

| Topic | Resolution |
| --- | --- |
| Wire shape | Separate `StepMessage` model, **not** on `Envelope.reply`; routed by a new always-stamped `x-calf-wire ∈ {envelope, step}` header; **strict positive** filters (no absent-fallback); hard cutover |
| Produce-side stamp | Universal — stamped at `_headers()` (`base.py:494`) + the client ingress (`caller.py:354`) + the step publish; unstamped ⇒ dropped (intended for foreign producers) |
| Node-topic safety | Steps emit unconditionally; no emission gate. Nodes carry **only** their envelope handler; an unmatched `step` → `SubscriberNotFound` → swallowed + **FastStream ERROR-logs** (relied on; no step-drop handler) |
| Projection seam | The **disposition chokepoint** `_handle_delivery` (covers `_handle_fanout_open` AND `_publish_action`), wrapped in its own `try/except` |
| `StepDraft` | **Typed** `list[StepEvent]` (discriminated on `kind`), not generic `ContentPart`s; agent authors its events directly |
| Buffering | Consume-once queue (intermediates, own `Event` signal) + cached terminal; positive wake model (terminal/close sets both signals); `async with` stream scope |
| `depth` | Pre-action call-stack snapshot |
| Ordering | Causal-gap + close-on-terminal; rare reorder accepted; terminal hop emits no step |
| `ToolResultEvent` | One type for success *and* error (`is_error: bool`, derived once at its producer); **no `ToolFailed`**. Any inner-frame `ReturnCall` (covers `message_agent` peers) **or** agent pre-dispatch rejection; inner-vs-root = **inbound (pre-action) depth==1 ⇒ terminal, >1 ⇒ ToolResultEvent**; tool hard-faults → terminal |
| Fault/abort/decline seams | Not steps — framework concerns → terminal `RunFailed` |
| Model mistakes | calfkit-caught only; pydantic-ai internal retries out of scope |
| Delivery | Best-effort, at-most-once, lossy; no dedup; ack-then-crash loss accepted |
| Publish failure | Whole projection+publish wrapped; **log and drop, no retry**; never faults the run |
| Depth/trust | All depths stream (a goal); documented, not policed |
| Performance | Doubled publishes / hotspot / single consumer accepted; no mitigation |
| Thinking | `AgentThinkingEvent` defined-not-emitted; `calf.thinking` marker pattern documented |

No open decisions remain (round-2 review folded). Next: implementation plan + adversarial plan review.

---

## 7. Implementation seams (reference)

- Emission: the **disposition chokepoint** in `_handle_delivery` (`nodes/base.py`, where `output` is
  known before the `_handle_fanout_open` / `_publish_action` branch) — project once from `(output, ctx,
  frame)`, wrapped in its own `try/except`; root inbox = `call_stack[0].callback_topic`; depth from the
  pre-action snapshot (`~:1666`).
- `project_steps`: polymorphic on `BaseNodeDef` / `BaseAgentNodeDef` / `BaseToolNodeDef`. The agent stashes
  typed `StepEvent`s in `_step_draft` during `run()`; the chokepoint returns them + the action-derived
  `ToolResultEvent` (inner-frame `ReturnCall`, depth>1).
- `StepDraft`: `models/session_context.py` — `_step_draft: list[StepEvent] | None` PrivateAttr (`:179-185`).
- **New `models/step.py`** holds `StepMessage` (its `model_validator(mode="after")` back-fills event
  identity) + the closed `StepEvent` union (`AgentMessageEvent`/`ToolCallEvent`/`ToolResultEvent`/`HandoffEvent`, + deferred
  `AgentThinkingEvent`), discriminated on `kind`, identity fields `Field(exclude=True)`. It lives in `models/`
  (not `client/events.py`) because the **node side** publishes `StepMessage` and `nodes/`→`models/` is the
  only allowed direction; `client/events.py` **imports** the `StepEvent` types to widen `RunEvent`, and the
  public `calfkit` surface re-exports them — the same "public wire type defined in `models/`, re-exported"
  pattern as `ContentPart` (`models/payload.py`). So a single unified type (wire + public) is the trade for
  this placement, deliberately (§3.1/§3.2). New always-stamped `x-calf-wire` header + `WireKind` literal +
  `HDR_WIRE` constant in `_protocol.py`; stamp at `_headers()` (`base.py:494`) + `caller.py:354` + the step
  publish; `MessageKind` and `Envelope.reply` unchanged.
- v1 filter wiring: `WIRE` ClassVar on `Envelope`/`StepMessage`; worker adds `filter=wire_filter(Envelope)`
  to `subscriber(node.handler, …)` (`worker/worker.py:400`) — **no node-side step handler** (unmatched
  `step` → `SubscriberNotFound` → FastStream ERROR-logs + drops); client hub = **one** groupless subscriber
  (`hub.py:184`) with two filtered call-items (`envelope` + `step`). `@wire_entrypoint`/`WireEntrypointMixin`
  registry **deferred** (additive parallel to `@handler`/`RegistryMixin`, `_registry.py`).
- Tool/peer producer: `nodes/tool.py:123-170`; `ToolCallRef` (`models/tool_dispatch.py:164-188`).
- Thinking: `models/payload.py:37` (`ContentPart`), `:80-89` (`calf.retry` idiom).
- Reception: `client/hub.py` — a dedicated `_on_step` → `push_intermediate` (NOT `_on_reply`/`_dispatch`);
  `_RunChannel` two-storage (queue + own `Event` signal; cached terminal); terminal/close sets both
  signals; `async with` stream scope; no-handle silent drop + firehose tee; do **not** reuse
  `_slot_anomaly` / `await_terminal` / the `fail_run` undecodable sink for steps. `client/events.py`
  (`RunEvent` union `:58-60`, `terminal_only` `:138-139`).
- Parent spec to update (see §2.8 for the complete list): §3.1, §3.3, §4.4, §4.5, §5.3, §5.5, §9.1.
