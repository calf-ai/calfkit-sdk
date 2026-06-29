# Intermediate Step Streaming — Implementation Plan

> **Status: READY TO BUILD** (revised after a 3-agent plan review — 2 CRITICAL build-order errors + MAJOR
> under-specs folded). Realises the converged [`intermediate-step-streaming-spec.md`](./intermediate-step-streaming-spec.md)
> (4 spec review rounds; zero open design decisions). This plan does not re-decide anything — it sequences
> the build, names every file/change/test, and front-loads the silent-failure constraints.
>
> **Delivery:** ONE cohesive PR, **test-first** (`/test-driven-development`), in a **new git worktree off
> `main`**. `make fix && make check` green at every commit.
>
> **Line numbers** are re-pinned to current `main`. The `_handle_delivery` chokepoint cluster is **exact**:
> insertion point ≈ `base.py:1782`, fan-out OPEN `:1793`, publish guard `:1805` / call `:1806`, snapshot
> `:1669`; the rejection arms `agent.py:651/676/702/719`. The node business entrypoint `handler` is
> `base.py:1619` (def) / `:1621` (`envelope` param) — anywhere this plan or the spec writes `:1616`, read
> `:1619`. Anchors are semantic; re-confirm during the build.

---

## 0. Test-lane reality (corrected)

`TestKafkaBroker` **does** honor `filter=` / `decoder=` / broker middleware — its `publish()` calls
`process_message` directly (`faststream/kafka/testing.py`), so it runs `is_suitable` (the filter) and the
`DecodeFloorMiddleware`. `tests/test_decode_floor.py` already exercises the floor under `TestKafkaBroker`.
So **most new seams are offline-testable** (filter-routing, filter-before-decode, the lenient-decoder-vs-floor,
two-call-items-one-consumer, the projection units, the channel units, model round-trips).

The **one** behavior `TestKafkaBroker` cannot reproduce is **`SubscriberNotFound` survival** — its `publish()`
bypasses the real consumer's `consume()` swallow (`usecase.py:318`), so an unmatched message raises out of
`publish()` instead of being swallowed-and-logged-and-survived. That case + the full real-broker e2e are the
**kafka lane**; everything else is offline.

**Consequence (CRITICAL, the round-1 plan-review catch; scope widened in round-2):** the moment a node/inbox
handler gains `filter=wire_filter(Envelope)`, **every** test that **hand-rolls an unstamped publish** into a
(soon-to-be-)filtered subscriber breaks — on **both** lanes (offline `TestKafkaBroker` raises
`SubscriberNotFound` out of `publish`; the **kafka lane** silently never delivers, then asserts). So increment
B must **also stamp the test fixtures**, auditing the **whole `tests/` tree across both lanes** — *not* just the
26 `TestKafkaBroker` files (round-2 found a kafka-lane offender outside that set). Confirmed offenders:

- **offline:** `tests/test_caller_surface_hub.py` (incl. the undecodable-floor publish at `:382` — see C),
  `tests/test_caller_surface_client.py`, `tests/test_consumer.py`, `tests/test_client_author.py:50`,
  `tests/test_lifecycle_e2e.py::_publish` — all publish `headers=_headers("return")` / raw headers with no
  `x-calf-wire`.
- **kafka lane:** `tests/integration/test_topic_provisioning.py:312` — `client.broker.publish(envelope,
  topic=inbox, …)` (no `headers=`) into a worker-registered consumer; after C its `filter` rejects the
  unstamped envelope → the round-trip never delivers → `assert received == ["ping"]` fails.

Production-path round-trips (real Worker+Client) stay green automatically because B stamps them.

---

## 1. Increments (each test-first)

### A — Wire models + protocol  *(inert; offline)*

- **New `calfkit/models/step.py`:**
  - `StepMessage(BaseModel)` — `correlation_id`, `emitter`, `depth`, `frame_id`, `events: list[StepEvent]`,
    `WIRE: ClassVar = "step"`, `@model_validator(mode="after")` back-filling each event's identity (§3.1).
  - `_StepEventBase` — identity `correlation_id`/`depth`/`frame_id`/`emitter` as `Field(default=None,
    exclude=True)`. **NON-frozen `model_config`** (the validator mutates in place; pydantic-v2 allows
    attribute assignment by default — a frozen model would raise on the back-fill → all steps dropped).
  - Concrete events, discriminated on `kind` with **distinct** literals (`"agent_message"`, `"tool_call"`
    — *not* `"tool"`, `ContentPart`'s value — `"tool_result"`, `"handoff"`, `"agent_thinking"`):
    `AgentMessage(parts)`, `ToolCall(tool_call_id, name, args: str | dict | None)`,
    `ToolResult(tool_call_id, name, parts, is_error: bool = False)`, `Handoff(target, reason)`,
    `AgentThinking(parts)` *(defined now, never emitted in v1 — F)*. `StepEvent = Annotated[… , Field(discriminator="kind")]`.
- **`calfkit/_protocol.py`:** `HDR_WIRE = "x-calf-wire"`, `WireKind = Literal["envelope","step"]`,
  `wire_filter(model)` → `decode_header_str(m.headers.get(HDR_WIRE)) == model.WIRE`.
- **`models/envelope.py`:** `Envelope.WIRE: ClassVar = "envelope"` (add `from typing import ClassVar` — not
  currently imported there).
- **Tests (offline):** wire round-trip drops nested identity / keeps message-level; decode back-fills + the
  discriminated union resolves; **mutability regression** (`e.correlation_id = "x"` must succeed); `ToolResult`
  `is_error` default; `ToolCall.args` accepts `str`/`dict`/`None`; `kind` literals distinct from `ContentPart`.

### B — Produce-side stamp + test-fixture stamp  *(no behaviour change; offline)*

- Stamp `HDR_WIRE: Envelope.WIRE` (the **constant** — `_headers()` has no `msg` arg) in `_headers()`
  (`base.py:494`, the sole node-rail header builder — covers Call/Return/TailCall/fault/fan-out-OPEN-siblings/
  re-entry/broadcast-mirror) and the client ingress dict (`caller.py:354`). (Verified airtight: every
  node-rail `publish` routes through `_headers()`; the only other site is `caller.py:354`.)
- **Stamp every hand-rolled test fixture** that publishes into a (soon-to-be-)filtered subscriber — the full
  cross-lane offender list in §0 (offline: `test_caller_surface_hub.py` incl. `:382`,
  `test_caller_surface_client.py`, `test_consumer.py`, `test_client_author.py:50`,
  `test_lifecycle_e2e.py::_publish`; kafka lane: `test_topic_provisioning.py:312`). Audit the whole `tests/`
  tree, both lanes.
- **Tests:** node-rail publishes + client ingress carry `x-calf-wire="envelope"`; existing suite green (no
  test asserts an exact header set — verified — so the stamp is purely additive).

### C — Consume-side filter: the CUTOVER (envelope filter only)  *(offline + 1 kafka-lane)*

- `worker/worker.py:400`: `subscriber(node.handler, filter=wire_filter(Envelope))` (the broadcast mirror
  `publisher(...)(handler)` still wraps it). **No node-side step handler** (an unmatched step →
  `SubscriberNotFound`, swallowed + FastStream ERROR-logs — the accepted drop signal, §2.4).
- `client/hub.py:184`: add `filter=wire_filter(Envelope)` to the existing `_handle_reply` call-item.
  **Do NOT register the step call-item here** — `_on_step`/`push_intermediate`/the two-storage channel are E;
  referencing them now would `AttributeError` at `connect()`. (This is the cutover; it is a **hard break**,
  no rolling-deploy compat, §2.4.)
- **Tests:** offline (`TestKafkaBroker`) — an `envelope`-stamped reply routes to `_handle_reply`; the floor
  still fires on a malformed-**but-stamped** envelope. **NOTE (round-2):** the filter runs *before* decode, so
  an unstamped undecodable body is now *filtered, not floored* — the floor fixture
  (`test_caller_surface_hub.py:382`) must carry `x-calf-wire="envelope"` (stamped in B) for the body to reach
  the decoder. **Kafka lane** — an unmatched message (e.g. an unstamped/`step`) on a node topic →
  `SubscriberNotFound`, consumer **survives** + ERROR-logged, subsequent envelopes route.

### D — Emission: projection + draft + chokepoint publish  *(offline units)*

- `models/session_context.py:179-185`: add `_step_draft: list[StepEvent] | None = PrivateAttr(default=None)`
  (off-wire, per-hop, framework-written; read-and-cleared at the chokepoint). Verified the same `ctx` object
  `run()` mutates reaches the chokepoint (deep copy in `prepare_context:425` is *before* `run()`).
- **Prerequisite — `Stack` accessors** (`models/session_context.py`, the `Stack` at `:18-38`): add `__len__`
  (`return len(self._internal_list)`) **and** a `root` **`@property`** (`return self._internal_list[0]` — the
  client's bottom/root frame; guard the empty case as `peek` does). `Stack` today exposes only
  `push`/`pop`/`peek`/`is_empty`, so the bare `len(snapshot.call_stack)` / `call_stack[0]` the depth logic
  needs would `TypeError` → under the §2.5 guard that **silently drops every step** (the round-2 catch).
  Unit-test both. (`snapshot` is a `WorkflowState` deep-copy, `base.py:654-657`; `snapshot.call_stack` is the `Stack`.)
- **The chokepoint owns depth** (`_handle_delivery`, ≈`base.py:1782`, after the `_CONSUMED`/`_Declined`/
  `_BatchFaulted` early-returns, before the `_handle_fanout_open`/`_publish_action` branch). It:
  1. computes `inbound_depth = len(snapshot.call_stack)` from the pre-action `snapshot` (≈`:1669`);
  2. **terminal gate** — if `output` is a `ReturnCall` and `inbound_depth == 1` → emit no step (the run's
     final answer; a tool node is never depth-1, directly-called-tool out of scope);
  3. else calls `events = self.project_steps(output, ctx, frame)`, and if non-empty publishes a `StepMessage`
     to the **root** frame's `callback_topic` (`snapshot.call_stack.root.callback_topic`) with
     `depth=inbound_depth`, `emitter=self.node_id`, `frame_id=frame.frame_id`, `events=events`, headers
     **`HDR_WIRE: StepMessage.WIRE` + `HDR_EMITTER`/`HDR_EMITTER_KIND`** (use the `WIRE` ClassVar, **not** the
     literal `"step"`; **not** `_headers()` — that stamps `wire="envelope"` + `HDR_KIND`, and a step has no
     business kind), `key=correlation_id.encode()` (co-partition with the terminal — every reply-rail publish
     keys on `correlation_id.encode()`, e.g. `base.py:535/559/650`);
  4. all of 1–3 wrapped in a `try/except` that **logs-drops AND falls through** to the action publish (the
     chokepoint is OUTSIDE the outer publish guard → an unguarded raise escapes → run hangs). So
     `project_steps(output, ctx, frame)` stays 3-arg — the depth discriminator and the `depth` stamp live at
     the chokepoint, which has the snapshot.
- **`project_steps`** (polymorphic): `BaseNodeDef` → `[]`. `BaseToolNodeDef` (`nodes/tool.py`): an inner-frame
  `ReturnCall` → `ToolResult(tool_call_id=frame.tag, name=self.node_id, parts=p, is_error=is_retry(p))` where
  `p = _coerce_to_parts(output.value)` (**coerce FIRST** — `is_retry(raw_scalar)` `AttributeError`s).
  `BaseAgentNodeDef` (`nodes/agent.py`): an inner-frame `ReturnCall` (peer consult, depth>1) → the same
  `ToolResult` (name = the peer's `self.node_id`; pairs by `tool_call_id`, not name); otherwise
  (`Call`/`list[Call]`/`TailCall`) → `ctx._step_draft` (which is `None` ⇒ `[]` on a pre-model re-dispatch hop
  — `agent.py:556-576` runs no model → no draft → no step, the double-emit guard). **Imports:** the overrides
  live in `tool.py`/`agent.py`, so add `_coerce_to_parts` + `is_retry` to `tool.py` and `_coerce_to_parts` to
  `agent.py` (`agent.py` already imports `is_retry`; `_coerce_to_parts` being imported in `base.py:29` does
  **not** cover the override sites).
- **Agent `run()` authors the draft** (`nodes/agent.py`):
  - `AgentMessage` preamble — **new extractor**, the **final `ModelResponse` only** (cite the precedent
    `nodes/_projection.py:114` `final_resp = next((m for m in reversed(new_messages) if isinstance(m, ModelResponse)))`):
    concat its `TextPart` text (excludes thinking/tool-call/file by keeping only `TextPart`); empty ⇒ no
    `AgentMessage`. *Final-response-only is load-bearing* — concatenating all `ModelResponse`s would surface
    §2.2-out-of-scope internal-retry preamble.
    **Breadcrumb (round-2 — also add as a code comment):** unlike the sibling `structured_output_preamble`
    (`nodes/_projection.py:100`), this extractor needs **no `has_final_result` guard**. The
    structured-output-as-text case (native/prompted mode, where the model's `TextPart` *is* the JSON answer)
    cannot coincide with a step-emitting hop: (1) a handoff forces a multi-member output union, which
    pydantic-ai **bars** from native/prompted (`_vendor/pydantic_ai/_output.py:253-254` / `:279-280` — the
    `len(outputs) > 1` guard + "must be the only output type" raise), so a handoff hop is never
    native/prompted; (2) with no handoff, the structured answer is
    produced only on the depth-1 terminal `ReturnCall`, which emits **no step**. Revisit this guard if
    native/prompted output is ever enabled on a non-terminal step-emitting hop.
  - `ToolCall` per `result.output.calls` (raw `args`, `agent.py:639`) — model emission, not the action.
  - `ToolResult(is_error=True)` for each call it rejects this hop — **all four arms**: `:651` unknown tool,
    `:676` malformed args, `:702` `ValidationError` (content = `e.errors()` is a **`list[dict]`** at `:703/713`
    → render to a `TextPart`, not assume `str`), and **`:719`** validator-raised-non-`ValidationError`
    (`:726`, `str` content). Optionally `calf.retry`-marked.
  - `Handoff(target, reason)` from `HandoffRequest.name`/`.message` (`_dispatch_handoff`, both `:385` online
    and `:399` offline self-retry).
- **Tests (offline, `test_project_steps.py`):** each event type from `(output, ctx, draft)`; **`BaseNodeDef.project_steps`
  → `[]`** (a plain custom node emits nothing); the **`Stack.__len__`/`root` accessors** (and that
  `len()`/root read correctly off a snapshot); the preamble
  extractor (final-response-only, text-only, empty→skip; **a multi-`ModelResponse`/internal-retry hop surfaces
  no internal preamble**); `is_error` coerce-first (a scalar success → `False`, no `AttributeError`; a
  `ModelRetry` → `True`); all four rejection arms author `ToolCall`+`ToolResult(is_error=True)`; the
  **chokepoint terminal gate** (depth-1 `ReturnCall` → no step) and **empty projection → no publish**; the
  **pre-model re-dispatch hop emits nothing** (`draft is None`); the **fault/decline/abort/re-entry hops emit
  no step** (structural — the early-returns precede the chokepoint; a regression test so a future refactor
  can't silently emit on them); the **chokepoint guard** (a `project_steps`/publish raise logs-drops AND the
  action still publishes — run does not hang).

### E — Client reception: step call-item + two-storage channel + `RunEvent`  *(offline channel units + kafka-lane e2e)*

- **`lenient_step_decoder`** (in `client/hub.py`, beside `_on_step`): decode `StepMessage`; on
  `ValidationError`/`JSONDecodeError`/`UnicodeDecodeError` return **`None`**.
- **`_on_step(step: StepMessage | None)`** (typed `| None`, **NOT** `StepMessage` — a `StepMessage`-typed
  handler makes `apply_types` re-validate the `None` sentinel → 2nd `ValidationError` → floor → `fail_run`):
  drops on `None`; else `push_intermediate`s each event onto the channel queue AND `_tee`s each to the
  firehose; a **no-handle** step drops **silently** (no WARN) but is still tee'd. Do **not** reuse
  `_slot_anomaly` / the `fail_run` sink / the no-handle `_dispatch` WARN for steps.
- **Register the step call-item** on the one inbox subscriber (the second filtered call-item, §3.4):
  `@sub(filter=wire_filter(StepMessage), decoder=lenient_step_decoder) → _on_step`. (Preserve the existing
  subscriber's `group_id=None, auto_offset_reset="latest"`; still ONE consumer, two call-items.)
- **`_RunChannel` two-storage:** add the intermediate **queue** (unbounded, consume-once) + its **own
  `asyncio.Event`** signal, alongside the terminal slot + signal. Wake model: terminal/close
  (`push` (today's terminal push, `hub.py:62`) / `close_with` / the `_Hub.fail_run` path) set **both**
  signals; **intermediate push sets only the queue signal**; **intermediate push no-ops once `_closed`**.
  `result()` waits the terminal signal only; `stream()` is an **`async with`-scoped** single iterator (keep
  the existing single-live-`stream()` `RuntimeError` guard, `hub.py:140-141`) that drains the queue — **no
  `await` between the empty-check and the signal-clear** — then yields the cached terminal as its last act.
- **`client/events.py:58`:** widen `RunEvent` with a **runtime** `from calfkit.models.step import AgentMessage,
  ToolCall, ToolResult, Handoff` (verified no import cycle: `models/step → models/payload`, neither imports
  `client/`). The dead `terminal_only` filter (`:138`) goes live. **Re-export** the step types from the public
  `calfkit` surface (`__init__.py` + `__all__`) — an explicit step, landing here.
- **Steps surface RAW `parts`** — they are **not** `output_type`-coerced (that scoping is the terminal
  `result()` only, via `render_parts_as_text`). The separate `_on_step` path structurally avoids it; add a
  test asserting a `stream()` `AgentMessage` carries raw parts even when `output_type=str`.
- **Tests:** offline (`test_run_channel.py`) — the five parent-§4.4 coexistence cases; the wake model (parked
  `stream()` wakes on terminal AND error-close, no hang; **and an intermediate push does NOT wake a parked
  `result()`** — the terminal signal stays unset); `_closed`-drop of a post-terminal step; late-`stream()`
  replays the buffered backlog; `async with` releases on early `break` (no `_stream_active` leak); steps raw
  (not `output_type`-coerced); **a no-handle step drops silently but is still tee'd to the firehose** (offline
  unit, not only the kafka e2e below); **public re-export importability** (`from calfkit import AgentMessage,
  ToolCall, ToolResult, Handoff` succeeds + present in `__all__`). **Migrate existing `stream()` call sites**
  to the `async with`-scoped form — `test_caller_surface_hub.py:561/571/582` (`async for … stream()`) and
  `:591/594` (`anext(stream())`) change consumption form; assertions stay valid. **Kafka lane (e2e — spans
  D+E):** a real run (agent → 2 parallel tools, one `ModelRetry`, then final answer) → `stream()` yields the
  steps + terminal in order with correct `is_error`; **plus an all-depths case** (agent consults a
  `message_agent` peer → the peer's inner trace + its reply as a `ToolResult` reach the original caller);
  `events()` firehose tees; a `send()` run's steps are firehose-visible only.

### F — `AgentThinking` defined, not emitted
Defined in A. Confirm `project_steps` never produces it in v1; the `calf.thinking` marker mapping is **not**
wired (§4/§5). Test: the type decodes; v1 never emits it.

### G — Docs + ADR
- **Parent-spec edits** (`client-caller-surface-spec.md`, per spec §2.8/§7): §3.1/§3.3/§4.4/§4.5/§5.3/§5.5/§9.1.
  §3.3 needs the **old→new event rename** (`ToolCalled`/`HandoffOccurred` → `AgentMessage`/`ToolCall`/`Handoff`)
  **and** to **add `ToolResult`** (the `:377-380` comment + the `RunEvent` line) — not just a "future shape →
  shipped" flip. Keep `RunRejected` (the deferred *approval* feature, also in that comment) **still deferred** —
  don't ship it with these. §9.1's "per-run channel overflow policy" prerequisite is consciously **not** taken (unbounded
  consume-once queue; the memory cost is accepted, spec §2.8/§2.10) — say so when flipping the bullet.
- **User docs** (`/diataxis-docs-writer`): how-to for `stream()`/`events()` step consumption + the event
  taxonomy (incl. `is_error`); README/`api.md` for the widened `RunEvent`. **Document the §4 trust
  consequence** — the run's inbox carries the full transitive trace, so inbox read-access is as sensitive as
  the run's full content (document, not police).
- **ADR(s):** separate `StepMessage` wire type + the `x-calf-wire` discriminator (not on `Envelope.reply`);
  the unified wire+public `StepEvent` with `exclude=True` identity; happy-path-only / best-effort-at-most-once
  steps. (Scope/count per `grill-with-docs/ADR-FORMAT.md`.)

---

## 2. Load-bearing constraints checklist (silent-failure guards — each gets a regression test)

- [ ] **`StepEvent` non-frozen** (frozen → validator raises on decode → lenient decoder swallows → all steps dropped).
- [ ] **Lenient decoder sentinel = `None` + `_on_step(step: StepMessage | None)`** (`StepMessage`-typed → 2nd `ValidationError` → `fail_run`).
- [ ] **Universal stamp** (`_headers` + `caller.py:354`; **+ all hand-rolled test fixtures**) — a miss hangs that run.
- [ ] **`is_error = is_retry(_coerce_to_parts(value))`** — coerce **first**.
- [ ] **`Stack.__len__` + `root` accessor added** (`session_context.py`) — else `len(snapshot.call_stack)` / root read `TypeError` under the guard → all steps silently dropped.
- [ ] **depth via the chokepoint snapshot; `ReturnCall` + inbound-depth==1 ⇒ terminal (no step)**; `project_steps` stays depth-agnostic.
- [ ] **Chokepoint guard wraps project+publish AND falls through to the action**.
- [ ] **Preamble = final `ModelResponse` only** (else surfaces §2.2-out-of-scope internal retries).
- [ ] **Wake model:** terminal/close set **both** signals; **intermediate push sets only the queue signal**; **no-op once `_closed`**; queue signal is an `asyncio.Event`; **no `await` between empty-check and signal-clear**.
- [ ] **Don't reuse `_slot_anomaly` / `fail_run` sink / no-handle WARN** for steps.
- [ ] **Empty projection → no `StepMessage`**; **pre-model re-dispatch hop emits nothing**; **fault/decline/abort/re-entry emit no step**.
- [ ] **All four rejection arms** (`agent.py:651/676/702/719`) author `ToolResult(is_error=True)`; **non-`str` (`list[dict]`) content rendered to a `TextPart`**.
- [ ] **Step publish uses its own header dict** (`HDR_WIRE: StepMessage.WIRE` — the ClassVar, **not** the literal `"step"` — + `HDR_EMITTER`/`HDR_EMITTER_KIND`, **no** `HDR_KIND`) — **not** `_headers()`; **`key=correlation_id.encode()`**.
- [ ] **`ToolCall.kind="tool_call"`** (≠ `ContentPart` `"tool"`); **`BaseNodeDef.project_steps` → `[]`**; **steps surface raw `parts`** (not `output_type`-coerced); **public re-export** lands.

---

## 3. Test strategy

- **Offline lane** (`TestKafkaBroker` / pydantic / FunctionModel): models round-trip + mutability; filter
  routing + filter-before-decode + lenient-decoder-vs-real-floor + two-call-items/one-consumer; all
  `project_steps` units + preamble extractor + `is_error` + the chokepoint gates; the `_RunChannel` channel
  units; `lenient_step_decoder` pure.
- **Kafka lane** (real broker — only what `TestKafkaBroker` can't): `SubscriberNotFound`-survival; the full
  emission→reception e2e (incl. the all-depths `message_agent` peer-consult `ToolResult`).
- 100% coverage on new code (`/pytest-coverage`).

---

## 4. Sequencing & gating

- Commit order **A → B (+ test-fixture stamps) → C (envelope-filter cutover ONLY) → D (emission) → E
  (reception + step call-item + e2e) → F → G**. Green at every commit = `make fix && make check`
  (lint/format/type, `Makefile:31`) **and** `make test` (offline suite) — **plus `make test-kafka`** for the
  kafka-lane commits (C, E). `make check` alone is **not** the test suite.
- **B before C** (stamp + fixtures before filter). **C is the atomic hard cutover.** **The step call-item +
  `_on_step` + the two-storage channel live in E, never C.** The e2e spans D+E (lands with E); between D and E
  commits, emitted steps hit the inbox with no step handler → harmless `SubscriberNotFound` ERROR-log
  (outcome-safe, not asserted until E).
- Final gate: `review-until-convergence` (`/pr-review-toolkit:review-pr`) + `/simplify` before merge.

## 5. Done criteria

`make fix && make check` (lint/format/type) + `make test` + `make test-kafka` green · 100% coverage on new
code · every constraints box ticked with a test · parent-spec edits + ADR(s) + user docs landed · final
adversarial PR review converged to zero critical/major · worktree/branch ready to merge.

---

## 6. Wire/surface split — increment H (round-1 PR-review fold, in #302)

> **Status: data model FINALIZED** (decisions locked with Ryan: `*Step` wire names · **parallel**
> hierarchies, not subclasses · folded into **PR #302**). Supersedes the unified-type design of
> **ADR-0026** (type-design MAJOR: the unified `… | None`-identity, non-frozen, in-place-stamped type
> made the public surface dishonest and aliased a shared-mutable object across observers). This increment
> replaces it with two **frozen** families mapped once.

### 6.1 The two families (finalized `models/step.py`)

Two **parallel** (non-subclass) hierarchies, both `frozen=True`. A surface `*Event` is therefore **not**
assignable where a wire `*Step` is expected — empirically verified **both** at compile time (mypy rejects a
`*Event` in `list[StepEvent]`) **and** at runtime (a live `*Event` instance is rejected by the discriminated
union). So a surfaced event can never ride the wire and identity stays strictly once-on-the-message. (Precise
claim: *mypy-rejected + live-instance-rejected* — a surface event *dumped to a dict* would coerce to a `*Step`
with identity keys ignored, but the producer only ever authors `*Step` directly, so that path is unreachable.)

**WIRE / DRAFT — `*Step` (frozen, NO identity).** Authored by the node side; serialized verbatim inside
`StepMessage`; identity rides once on the message, never per event.

```python
class _StepBase(BaseModel):
    model_config = ConfigDict(frozen=True)            # no identity fields at all

class AgentMessageStep(_StepBase):
    kind: Literal["agent_message"] = "agent_message"; parts: list[ContentPart]
class ToolCallStep(_StepBase):
    kind: Literal["tool_call"] = "tool_call"; tool_call_id: str; name: str; args: str | dict[str, Any] | None = None
class ToolResultStep(_StepBase):
    kind: Literal["tool_result"] = "tool_result"; tool_call_id: str; name: str; parts: list[ContentPart]; is_error: bool = False
class HandoffStep(_StepBase):
    kind: Literal["handoff"] = "handoff"; target: str; reason: str
class AgentThinkingStep(_StepBase):
    kind: Literal["agent_thinking"] = "agent_thinking"; parts: list[ContentPart]   # decode-complete; never emitted in v1

StepEvent = Annotated[AgentMessageStep | ToolCallStep | ToolResultStep | HandoffStep | AgentThinkingStep, Discriminator("kind")]

class StepMessage(BaseModel):
    model_config = ConfigDict(frozen=True)
    WIRE: ClassVar[str] = "step"
    correlation_id: str; emitter: str; depth: int; frame_id: str
    events: list[StepEvent]
    # NO model_validator — wire events carry no identity, so there is nothing to back-fill.
```

**SURFACE — `*Event` (frozen, identity REQUIRED).** The public `RunEvent` members the caller observes;
identity is **always** present (stamped once in `_on_step`). Names unchanged → zero consumer churn.

```python
class _RunStepEventBase(BaseModel):
    model_config = ConfigDict(frozen=True)
    correlation_id: str; depth: int; frame_id: str; emitter: str   # all non-null

class AgentMessageEvent(_RunStepEventBase):
    kind: Literal["agent_message"] = "agent_message"; parts: list[ContentPart]
class ToolCallEvent(_RunStepEventBase):
    kind: Literal["tool_call"] = "tool_call"; tool_call_id: str; name: str; args: str | dict[str, Any] | None = None
class ToolResultEvent(_RunStepEventBase):
    kind: Literal["tool_result"] = "tool_result"; tool_call_id: str; name: str; parts: list[ContentPart]; is_error: bool = False
class HandoffEvent(_RunStepEventBase):
    kind: Literal["handoff"] = "handoff"; target: str; reason: str
class AgentThinkingEvent(_RunStepEventBase):
    kind: Literal["agent_thinking"] = "agent_thinking"; parts: list[ContentPart]   # defined-not-emitted (§5); not in RunEvent
```

The payload fields (`tool_call_id`/`name`/`args`/`parts`/`is_error`/`target`/`reason`) are **declared in
both families** — deliberate, ~5 short lines duplicated. A shared mixin was rejected: it adds a base per
event for less clarity, and subclassing was rejected because it makes a surface event *is-a* wire event
(re-opening the leak the split closes).

### 6.2 The mapping seam (`client/hub.py`)

The in-place validator back-fill is replaced by one explicit, kind-dispatched constructor. `_on_step`
filters the wire `AgentThinkingStep` (defined-not-emitted) **before** mapping, so the surface
`AgentThinkingEvent` is never produced in v1 (matching today):

```python
_SURFACE_BY_KIND: dict[str, type[_RunStepEventBase]] = {
    "agent_message": AgentMessageEvent, "tool_call": ToolCallEvent,
    "tool_result": ToolResultEvent, "handoff": HandoffEvent,
    # "agent_thinking" deliberately absent — filtered before mapping (defined-not-emitted, §5)
}

def _to_surface(step: StepEvent, msg: StepMessage) -> _RunStepEventBase:
    cls = _SURFACE_BY_KIND[step.kind]
    return cls(**{f: getattr(step, f) for f in type(step).model_fields}, correlation_id=msg.correlation_id,
               depth=msg.depth, frame_id=msg.frame_id, emitter=msg.emitter)   # type(step).model_fields, NOT instance (pydantic ≥2.11 deprecation)

# _on_step (preserve the EXISTING no-handle guard — a send() run has no handle: drop from the per-run
# path silently but STILL tee to the firehose, spec §3.4):
#   handle = self._runs.get(step.correlation_id)
#   for s in step.events:
#       if s.kind not in _SURFACE_BY_KIND: continue          # AgentThinkingStep — defined-not-emitted
#       event = _to_surface(s, step)
#       if handle is not None: handle._channel.push_intermediate(event)
#       self._tee(event)
```

(Field values are passed **live**, not via `model_dump()`, so nested `ContentPart`s are not needlessly
re-serialized; the surface model still validates on construction. `kind` rides in the splat and matches the
target's `Literal` default.)

### 6.3 Object-creation paths — before → after
| Site | Now (unified, ADR-0026) | After (split) |
| --- | --- | --- |
| Author (`agent.py` ×6, `tool.py` ×1; `_step_draft`/`project_steps` return) | construct `*Event` (identity unset) into `list[StepEvent]` | construct `*Step` into `list[StepEvent]` (wire union) |
| Batch (`base.py` chokepoint) | `StepMessage(events=[*Event])`; validator stamps on decode | `StepMessage(events=[*Step])`; no validator |
| Decode (client ingress) | `model_validate_json` → in-place stamp | `model_validate_json` → plain (no identity on wire) |
| Surface (`hub._on_step`) | push the *same* non-frozen `*Event` to queue + every firehose outlet | build one frozen `*Event` via `_to_surface`, push/tee it |
| `RunEvent` / re-exports (`events.py`, `__init__.py`, `client/__init__.py`) | the 4 `*Event` | unchanged (same names, frozen now) |

### 6.4 Churn surface (all in #302)
*(Production construction counts verified exact in review: `agent.py` ×6, `tool.py` ×1; `base.py`/`session_context.py`/`_protocol.py` need no construction change — `base.py:1801` builds `StepMessage(events=step_events)` from the `project_steps` return and names no event type; `StepEvent`/`_step_draft`/`project_steps -> list[StepEvent]` stay correct since `StepEvent` remains the wire-union name. `StepEvent` is **not** publicly re-exported and stays wire-internal; the surface `*Event` names stay public+unchanged, so `__init__.py`/`client/__init__.py`/`events.py` have **nothing to re-point** beyond the `models.step` import line.)*

- `models/step.py` — the §6.1 rewrite (drops the validator, `exclude=True`, `default=None`, `frozen=False`).
- `nodes/agent.py` (×6) + `nodes/tool.py` (×1) — authoring constructs `*Step`; `_step_draft` typed `list[StepEvent]` (wire).
- `client/hub.py` — `_to_surface` + `_SURFACE_BY_KIND`; `_on_step` maps wire→surface (filter on `AgentThinkingStep`), **keeping the no-handle guard** (§6.2).
- **Tests** (the full set — review found the original list under-scoped):
  - *Producer-side draft* `*Event` → `*Step`: `test_project_steps.py` (the arm/chokepoint/scalar/`message_agent` draft checks), `test_handoff_dispatch.py` (`TestHandoffAuthorsStepEvent`, the `isinstance(e, HandoffEvent)` over `_step_draft` → `HandoffStep`), `test_co_tenant_tool_isolation.py` (the unknown-tool-arm draft check).
  - *Wire-side `StepMessage(events=[…])` builders* `*Event` → `*Step`: `test_wire_filter_cutover.py` (`_step()` + the thinking-event helper) and `test_caller_surface_hub.py:471`. **Semantic break (not a rename):** `test_wire_filter_cutover.py:107` `assert list(outlet._buffer) == step.events` fails post-split (the firehose now holds mapped **surface** `*Event`, `step.events` holds **wire** `*Step`) — rewrite to compare mapped fields, not `== step.events`.
  - *Reception-side surface builders* now need identity: `test_run_channel.py` `_step()` (`:28-29`) + `:127` build `AgentMessageEvent(parts=…)` with **no** identity → add `correlation_id/depth/frame_id/emitter` (surface `*Event` is now identity-required).
  - `test_step_models.py` rewrites: drop mutability/back-fill; **add** a `_to_surface` mapping test, "surface event cannot construct without identity", "both families frozen", and a **mechanical parity test** (§6.5 H1) guarding the parallel-hierarchy drift hazard.
- **Docs** (widen from the original "§3.1/§3.2"): **ADR-0026** rewritten to record the split; **spec §2.5 / §3.1 / §3.2 / §3.4 / §7 + the status header (`:6-7`)** updated — scrub every "non-frozen / `exclude=True` / blessed-factory / `model_validator` back-fill / in-place" claim, describe the two frozen families + the `_on_step` mapping. Also scrub the stale "validator/back-fill" wording in `session_context.py:201` (the `_step_draft` comment) and `client/events.py:3-5` (module docstring). ADR-0025/0027 + `docs/api.md`/`docs/client-features.md` carry no stale design prose (surface names + "carries hop identity" both still true) — **no edit**.

### 6.5 Build order (TDD, within #302)
H1 `models/step.py` two families + `StepEvent` wire union (+ `test_step_models.py` rewrite: frozen both
sides, surface-needs-identity, wire-has-no-identity, union resolves, wire round-trip byte-identical to today,
**+ the mechanical parity test**: for each kind `set(WireCls.model_fields) | {"correlation_id","depth","frame_id","emitter"} == set(SurfaceCls.model_fields)`, and `set(_SURFACE_BY_KIND) == {every wire kind except agent_thinking}` — converts the parallel-hierarchy payload-drift + forgotten-allowlist-entry silent-drop hazards into a test-time failure) → H2 `_to_surface`/`_SURFACE_BY_KIND` + `_on_step` rewrite (channel/firehose
tests still green; thinking filtered; no-handle guard kept) → H3 flip authoring in `agent.py`/`tool.py` to
`*Step` + retype `_step_draft` (the §4 draft tests + the §6.4 wire-side builders flip) → H4 re-point the
`models.step` import in `events.py`/re-exports (names unchanged) → H5 ADR-0026 rewrite + spec §2.5/§3.1/§3.2/§3.4/§7
+ header + the stale `session_context.py`/`events.py` docstrings. `make fix && make check` + offline suite
green at each step.

> **Plan-internal supersession (review M4):** §1.A and the §2 checklist still describe the *old* unified
> design — `Field(default=None, exclude=True)`, **NON-frozen** `model_config`, the "mutability regression"
> box, and the bare `AgentMessage`/`ToolCall` names. Those are **superseded by §6** for #302: the events are
> now **frozen** (both families), identity is required on the surface / absent on the wire, and the validator
> back-fill is replaced by `_to_surface`. Read §1.A/§2 as the *as-originally-shipped* record; build to §6.
