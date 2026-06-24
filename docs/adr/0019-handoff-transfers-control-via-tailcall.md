---
status: accepted
---

# Handoff transfers conversation control via a structured-output union member that emits a TailCall

Agent-to-agent messaging (ADR-0015) is request/reply: the caller consults a peer and
keeps control. But a common multi-agent pattern is the opposite — an agent should
*transfer* the conversation entirely (a triage agent routes to a specialist; an agent
finishes its part and hands the rest off). That is a different control flow: the
handing agent relinquishes, the peer continues the *full* conversation, and the
peer's answer goes to the handing agent's *original* caller, not back to the handing
agent.

We add **handoff** as a **structured-output option**, `HandoffRequest(name, message)`,
configured by a `Handoff(...)` handle in the unified `peers=` surface (alongside
`Messaging(...)`; ADR-0015's surface migrates to these capability handles). The model
produces a `HandoffRequest` as its turn's **output** — *not* by calling a tool — and
calfkit discriminates by `isinstance`. It reuses the same discovery substrate as
messaging — the `calf.agents` view and `derive_input_topic(name)` (ADR-0017) — so
resolving a peer is identical; only the control primitive and state handling differ.

Handoff is a thin addition built on two things:

- **A bare structured-output union member (not an output tool).** When a `Handoff`
  handle is present, `HandoffRequest` is added to `output_type` as a *bare* model —
  `output_type = [final_output_type, HandoffRequest, DeferredToolRequests]` — built
  per run via a **memoized `create_model`** (`__base__=HandoffRequest` so `isinstance`
  discrimination survives the per-turn rebuild; `__doc__` = the freshly rendered
  directory; `name: Literal[<live agents>]`; `message` is `Field(min_length=1)`). Producing
  the output **ends the run**, so handoff is **terminal**; *exclusivity* (not running a
  sibling tool call) is **mode-dependent, not by construction** — in tool mode
  `end_strategy='early'` (calfkit's default, never overridden) stubs siblings once a final
  result is set, and in native/prompted mode pydantic-ai prioritizes a co-emitted tool call
  over the handoff text (**deferring**, never corrupting, the handoff). **Why a bare model and not a
  `ToolOutput`:** a `ToolOutput` forces the whole schema into `tool` mode with
  `allow_text_output=False`, which Anthropic rejects with extended thinking
  (`tool_choice=required` is incompatible with thinking). Two *bare* union members keep
  `allow_text_output=True` — a **structured** `final_output_type` builds an
  `AutoOutputSchema` (`auto` mode, identical to a no-handoff structured agent); a **`str`**
  type builds a `ToolOutputSchema` (`tool` mode) but with the text path still allowed — so
  in either case `tool_choice` stays `auto`, not `required`. So handoff is **thinking-safe
  on every provider** (verified by building the schemas against pydantic-ai 1.47.0 and
  1.107.0), and an invalid `name` is auto-retried by the `Literal` + pydantic-ai's
  output-retry rather than by calfkit code.

- **An empty live directory omits the member, with an ephemeral instruction in its place.**
  The `Handoff` handle gates the capability, but the per-turn `name: Literal[<live agents>]`
  can only be built with ≥1 live in-scope peer — a `Literal` has no empty form (an empty
  `Literal` raises `AssertionError` at pydantic schema-build — `TypeAdapter(Literal[()])` —
  though the type expression itself constructs fine; `typing.Never` has no pydantic-core
  schema; a failing-validator stand-in still surfaces to the model as a callable option and
  faults on retry-exhaustion — so a "never-valid type" is the wrong tool: never offer the model
  an action it can only fail). So when no in-scope peer is live, `HandoffRequest` is **omitted**
  (the per-turn builder returns the base `output_type`), and an **ephemeral instruction** is
  concatenated with any caller `temp_instructions` into the per-run `instructions=` (a
  `None`-filtered list — never substituting for `temp_instructions`) — *"You cannot currently
  hand off this conversation or task to another agent, as no other agents are online."* — to
  keep the dormant capability legible. Instructions are request-time text (a `ModelRequest.instructions`
  field, not a conversational part), recomputed each run and invisible to POV projection, so
  this self-heals the moment a peer comes online. This is a routine, self-healing runtime
  state (curated peers all offline; `discover` with only-self / pre-advert startup / a degraded
  view), not an error.

- **`TailCall`.** On a valid handoff, calfkit reads `name`/`message` from the
  `HandoffRequest` (to build the `TailCall`), **persists A's output to the conversation
  like any structured output**, then returns a `TailCall` carrying the current
  conversation. (When the peer continues, POV projection renders A's output cross-agent
  as a JSON string attributed to A, so the peer reads the handoff `message` from A's
  *actual* output — truthful and correctly attributed, no synthesized note. This relies
  on the `_surface` `final_result`-*prefix* match (**landed, #276** — `_is_output_tool`):
  in tool mode the union renames the output tool `final_result_HandoffRequest`, which the
  pre-#276 exact-`final_result` renderer dropped. See spec §5.3/§17.) `TailCall` pops-and-re-pushes the *same* frame retargeted to the peer's input
  topic, preserving `frame_id`/`tag`/**`callback_topic`** (and the `caller_node_id`)
  (`base.py:573`) — "the tailcallee inherits the callback commitment." So the peer
  inherits the original caller's return address and the full conversation, continues,
  and returns to the original caller; the handing agent drops out. The stack depth is
  unchanged. **The caller's per-run overrides are cleared on handoff** so B uses its own
  tools/model (these are the *caller's* per-invocation `tool_overrides`/`model_settings`,
  set on the client call — not "A's"). Both channels must be nulled: the agent reads
  `state.overrides`, but `prepare_context` re-applies `frame.overrides` onto it at B's
  start, so `run()` nulls `state.overrides` on the carried State **and** sets an opt-in
  **`clear_overrides=True`** flag on the handoff `TailCall`, which `_publish_action` honors
  via `replace(frame, …, overrides=None)`. The flag defaults `False`, so the generic
  `TailCall` keeps **preserving** overrides — correct for the staleness self-retry, which
  `TailCall`s to *self* and keeps the caller's surface; clearing is thus handoff-only, and
  is non-breaking (an in-process `NodeResult` action field, not a wire model).

A bad target is handled in two layers. An **invalid name** (hallucinated /
out-of-scope / self) is rejected by the per-turn `Literal` and auto-retried by
pydantic-ai's output-retry — no calfkit code (exhausting it surfaces as pydantic-ai's
`UnexpectedModelBehavior`, on the fault rail). The **render→dispatch staleness race** (a
name live at render that went offline before dispatch) is the only case calfkit
handles: a **thin, self-contained liveness re-check** in the **final-output branch**
(where `result.output` is the `HandoffRequest`) — if the target is gone, calfkit does not
relinquish but **self-retries with feedback** (`TailCall`s to itself, re-rendering a fresh
`Literal` minus the offline agent), mirroring the *pattern* of the all-invalid self-retry
(a different code path, the deferred-tools branch). The guard is encapsulated as a
**drop-in** so it does not leak into the surrounding dispatch code. This self-retry is
**unbounded in v1** — pydantic-ai's retry budget resets on each Kafka re-entry and there is
no calfkit-level counter that survives it, so a re-render→still-stale loop is the same
unbounded class as a handoff ring; a re-entry-surviving counter is the **#251 backstop**.

Considered and rejected:

- **Handoff as an output *tool*** (`ToolOutput(HandoffRequest, name="handoff")`) — the
  obvious way to get a terminal, model-named tool, and it gives terminality/exclusivity
  via `end_strategy='early'` for free. But it forces the output schema into `tool` mode,
  which **crashes a structured-output agent on Anthropic with extended thinking** (the
  forced `tool_choice` is incompatible with thinking) and renames the structured tool
  `final_result_*` (breaking name-keyed projection). The bare-union form avoids both
  while remaining terminal (exclusivity is mode-dependent either way — see above), so it
  supersedes this. (This was the design through review round 2; the regime hazard — "C1" —
  drove the change.)
- **Handoff as an external tool** (like `message_agent`) — then calfkit would have to
  implement terminality and exclusivity itself; a terminal output already does both.
- **A separate top-level config knob** — the unified `peers=[Messaging, Handoff]`
  surface (capability = type, independent per-capability scope) is cleaner and reuses
  the directory/resolution machinery.
- **A handoff-loop guard** — `TailCall` *replaces* the frame, so the call stack never
  grows; the messaging cycle guard (ADR-0016), which keys on accumulated ancestor
  frames, is structurally blind to an A→B→A handoff ring. Bounding it needs a separate
  hop counter, deferred to #251. v1 ships handoff loops **knowingly unbounded** (the
  model/developer's responsibility), documented as an operational hazard.
- **Callee-side opt-in** — a peer is handed control without opting in (caller-side
  `Handoff` scope is the v1 boundary); admission deferred to #231. Handoff adds no new
  injection surface (the peer continues the real conversation, not an injected message).

Consequences: chained handoffs (A→B→C) and a handoff *inside* a peer-message both
compose transparently via frame chaining — the last agent answers the original caller,
and a handoff inside a `message_agent` call folds the final agent's answer into the
messaging caller's tool result. The peer sees the full conversation (it is *becoming*
the agent). Full design in `docs/designs/agent-mesh-spec.md` §5.3.
