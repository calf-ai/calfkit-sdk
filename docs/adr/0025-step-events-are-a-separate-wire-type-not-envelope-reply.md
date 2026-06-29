---
status: proposed
---

# Intermediate steps ride a separate `StepMessage` wire type, routed by an always-stamped `x-calf-wire` header, not on `Envelope.reply`

[Intermediate step streaming](../designs/intermediate-step-streaming-spec.md) publishes per-hop progress
to the run's inbox alongside the terminal reply. A step could ride the existing `Envelope.reply` union (a
third member beside `ReturnMessage`/`FaultMessage`) or be its own top-level wire body. We chose: a step is
its **own** top-level `StepMessage` model — **not** a member of `Envelope.reply` — routed to the right
handler by a new, **always-stamped** `x-calf-wire ∈ {envelope, step}` Kafka header (the value is each wire
model's `WIRE` ClassVar, the single source for both the outbound stamp and the inbound filter, so they
cannot drift), matched by **strict-positive** subscriber filters (`== "envelope"` / `== "step"`, no
absent-fallback). It is a **hard cutover** (no rolling-deploy compatibility), pre-1.0.

We did this for two structural reasons. **No-leak by construction:** a `StepMessage` carries only
correlation / hop-identity / events, so it cannot leak an `Envelope`'s `state`/`deps` the way a reply-union
member would (a reply member is decoded as an `Envelope` and carries the whole body). **No decode-floor
false-fault:** a subscriber's `filter` runs in `is_suitable` *before* the body is decoded into the handler's
type (verified against FastStream 0.7.1), so a header filter rejects a step body **without** triggering
`Envelope` validation — a step never trips the broker decode floor and faults a healthy run. The filter is
strict-positive so a future *third* wire type can never silently fall through to the `Envelope` decoder.

## Considered options

- **A third `Envelope.reply` member** (`StepMessage` beside `ReturnMessage`/`FaultMessage`). Rejected: it
  leaks the envelope body by construction, and an `Envelope`-decoded step must satisfy `Envelope`
  validation, so a malformed/schema-skewed step would hit the decode floor and be (mis)surfaced as a
  delivery fault — faulting the run for a best-effort observation.
- **A negative filter** (`x-calf-wire != "step"`) on the envelope handler. Rejected: a future wire type
  would fall through to the `Envelope` decoder. Positive equality is closed.
- **A rolling-deploy compatibility shim** (tolerate unstamped producers). Rejected per the pre-1.0
  "hard breaks over compat shims" bar; the stamp is centralized, so the cutover is clean.

## Consequences

- **The stamp must be universal.** Because the filter is strict-positive, an unstamped message misses the
  filter and is dropped (the intent for foreign producers). Every calfkit publish therefore stamps
  `x-calf-wire`, centralized to exactly the node-rail header builder (`_headers()`), the client ingress, and
  the step publish.
- **No node-side step handler.** A node carries only its `Envelope` handler; a step landing on a node topic
  raises `SubscriberNotFound`, which FastStream's consume loop swallows (the consumer survives) and
  ERROR-logs (the accepted drop signal). The client hub is the only step *consumer* — one groupless inbox
  subscriber with two filtered call-items (the envelope handler + the step handler).
- A future node that needs to *consume* steps would add a `@wire_entrypoint(Model)` registry (a faithful
  additive parallel to the existing `@handler`/`RegistryMixin` idiom) — named, deferred, not built in v1.
