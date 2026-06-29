---
status: proposed
---

# Step events are two frozen families — a wire `*Step` (no identity) and a surface `*Event` (identity required) — mapped once on receive

Each [step event](../designs/intermediate-step-streaming-spec.md) needs, per hop, a typed batch on the wire
and the same events surfaced to the caller with hop identity (`correlation_id` / `depth` / `frame_id` /
`emitter`). That identity is the same for every event in a hop and already sits on the enclosing
`StepMessage`; the node side *publishes* an event while the client side *consumes* it. We model this as
**two parallel, frozen families** defined in `models/step.py`:

- a **wire / draft** family `*Step` (`AgentMessageStep` / `ToolCallStep` / `ToolResultStep` / `HandoffStep` /
  `AgentThinkingStep`), the `kind`-discriminated union `StepEvent`, carried by a frozen `StepMessage`. The
  wire events carry **no identity** — it rides **once** on the message;
- a **surface** family `*Event` (`AgentMessageEvent` / … — the public `RunEvent` members, union
  `RunStepEvent`), with **required, non-null** identity.

On receive, `client.hub._to_surface` maps each wire `*Step` plus the message's identity into a frozen
surface `*Event` (kind-dispatched via `_SURFACE_BY_KIND`), once per event at the single `_on_step` unpack
point; the surface types re-export from `calfkit`. The two families are **separate** (not subclasses), so a
surface `*Event` is not assignable where a wire `*Step` is expected — a surfaced event with identity can
never ride the wire (verified: rejected by the discriminated `StepMessage.events` at runtime, and by mypy at
compile time). So "identity rides once on the message" and "the public surface always has identity" are both
**expressed in the types**, not enforced by runtime discipline.

## Considered options

- **One unified type for wire + surface** (the original v1 design): identity fields
  `Field(default=None, exclude=True)`, the type **non-frozen**, and `StepMessage`'s `model_validator`
  back-filling identity by in-place assignment on decode. Rejected: the public surface is then dishonest
  (`correlation_id: str | None` for a value never null on that surface), the single event object is mutable
  and **aliased** across the per-run queue and every firehose outlet, and the non-frozen requirement breaks
  the codebase's frozen-value-object convention (`ToolBinding`, `RunCompleted`/`RunFailed` are frozen). The
  split dissolves all three at the cost of one small mapping function and a few duplicated field declarations.
- **Surface subclasses wire** (to DRY the payload fields). Rejected: a surface event would then be *is-a*
  wire event, type-checking into `StepMessage.events` and re-opening the identity-on-the-wire leak the split
  exists to close. Parallel hierarchies keep them disjoint.

## Consequences

- The wire JSON is **byte-identical** to the superseded unified design (which already excluded per-event
  identity via `exclude=True`), so this is a pure in-memory refactor with no wire-compatibility impact.
- The payload fields (`tool_call_id` / `name` / `args` / `parts` / `is_error` / `target` / `reason`) are
  declared in **both** families (~5 short lines). The type system does not enforce their lockstep, so a
  parity test (`set(WireCls.model_fields) | identity == set(SurfaceCls.model_fields)`, and `_SURFACE_BY_KIND`
  covers every emitted kind) converts that drift hazard into a test-time failure.
- `RunStepEvent` is the **single source of truth** for the surfaced step events: `client.events.RunEvent`
  composes it with the terminals, and `_to_surface` / `_SURFACE_BY_KIND` map into it — adding a future
  surface event touches one union.
- `AgentThinkingStep` stays in the wire union (the decoder must resolve every `kind`, e.g. a foreign
  producer's), but `AgentThinkingEvent` is **defined-not-emitted** (§5) — absent from `_SURFACE_BY_KIND`,
  `RunStepEvent`, and the re-exports — so `_on_step` filters it and it is never surfaced.
