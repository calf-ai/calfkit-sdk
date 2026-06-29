---
status: proposed
---

# A step event is one (non-frozen) type for both the wire and the public surface, with excluded-from-wire identity back-filled by `StepMessage`'s validator

Each [step event](../designs/intermediate-step-streaming-spec.md) needs caller-facing hop identity
(`correlation_id` / `depth` / `frame_id` / `emitter`), but that identity is the same for every event in a
hop's batch and already sits on the enclosing `StepMessage`; and the node side *publishes* an event while
the client side *consumes* it. We chose **one** type per event, used on both the wire and the public
`RunEvent` surface (defined in `models/step.py`, re-exported from `calfkit`). Its identity fields are real,
readable public fields declared `Field(default=None, exclude=True)` — so they ride **once** on the
`StepMessage` and are dropped from each event's own `model_dump` — and `StepMessage`'s
`@model_validator(mode="after")` back-fills them onto each event on decode (the "blessed factory": a plain
`model_validate_json` yields identity-stamped events). The event models are deliberately **non-frozen**.

One unified type (not separate wire + public types) is the right trade because `models/step.py` is importable
by both `nodes/` (which publishes `StepMessage`) and `client/events.py` (which widens `RunEvent`) — the same
"public wire type defined in `models/`, re-exported" shape as `ContentPart`. `exclude=True` serializes
identity once on the message rather than on every event (the same idiom as `ToolBinding`'s excluded
validator). **Non-frozen is load-bearing:** the validator stamps by in-place assignment, so a frozen model
would raise `ValidationError("Instance is frozen")` *during decode* → the lenient step decoder swallows it →
**every step is silently dropped forever.** This deliberately breaks the codebase's frozen-value-object
convention (the very exemplars cited here — `ToolBinding`, the terminals `RunCompleted`/`RunFailed` — are
frozen), so it is recorded here, and at the type, so a future engineer does not "fix" it to `frozen=True`.

The concrete events are named with an `*Event` suffix — `AgentMessageEvent`, `ToolCallEvent`,
`ToolResultEvent`, `HandoffEvent`, `AgentThinkingEvent` (the wire `kind` literals stay bare:
`agent_message` / `tool_call` / `tool_result` / `handoff` / `agent_thinking`). The suffix matches
pydantic-ai's `*Event` streaming-event convention, **frees the public `calfkit.Handoff` name** for the
shipped peer-capability handle (a bare `Handoff` event would clash), and clears the soft `ToolCall*` /
`ToolReturn*` clashes.

## Considered options

- **A separate wire type plus a separate public type, copy-constructed on decode.** Rejected: a second type
  and a per-event copy step for no benefit; `exclude=True` gives one type that serializes identity once and
  decodes already-stamped.
- **Frozen events** (matching the codebase convention). Rejected: the in-place back-fill cannot run, and the
  failure is silent — the lenient decoder turns a frozen-raise into "all steps dropped."

## Consequences

- The `StepEvent` models must stay mutable; the validator back-fill is the only blessed way to stamp
  identity (node-side authoring leaves identity unset and never reads it in the draft/wire roles).
- `AgentThinkingEvent` is **defined but not emitted in v1** — it is therefore *not* in the `RunEvent` union
  and *not* top-level re-exported (it lives at `calfkit.models.step`), honoring the "ship a type only once
  the fabric emits it" rule.
