---
status: accepted
---

# Faults travel the result rail and escalate the call stack

A terminal node failure becomes a typed wire value (`ErrorReport`) that travels **the same rail its success result would have**: to the call frame's `callback_topic`, or the broadcast `publish_topic`, or the log/broadcast floor for fire-and-forget. Unhandled faults **escalate frame-by-frame** up the durable call stack (each ancestor's `on_callee_error` gets a chance) until handled or they reach the workflow's reply address — never absorbed, never dropped. Fan-out batches **aggregate all** sibling results before escalating unhandled faults as one `ExceptionGroup`-style fault group (singletons flatten to the bare fault).

We chose this because the call stack already is a durable chain of return addresses that successes unwind — faults reusing it means zero new routing topology, aggregation-correctness by construction (a sibling fault always passes its waiting aggregator), and layered remediation (each ancestor may compact/retry/substitute without the top-level client ever knowing). MassTransit's `FaultAddress → ResponseAddress` precedence and OTP's escalation semantics are the closest prior art; Google ADK 2.x independently converged on escalate-by-default.

## Considered options

- **Short-circuit every fault to the root reply address (one hop).** Rejected as the *semantic*: it strips intermediate callers of remediation and orphans fan-out aggregators. Retained as a *future mechanical optimization*: since seam registration is static per class, frames can be stamped "intercepts faults" at `Call` time and the fault routed one hop to the nearest interested frame — same observable behavior, fewer hops.
- **Fail-fast fan-out batches.** Rejected: Kafka has no cancellation, so abandoning a batch saves no work — siblings complete and publish regardless. It only creates a straggler-to-dead-batch problem, discards partial successes, and reports one of possibly many faults.
- **Dedicated error topics beside every success topic.** Rejected: doubles topic count, splits ordering, forces double subscription. The fault is discriminated in-band (the `x-calf-kind` delivery-kind header; it rides the envelope's per-delivery reply slot — ADR-0006). The destination-preserving variant — a per-node error *lane* beside the reply lane — was separately considered and rejected (2026-06-11): see ADR-0006 / spec §4.1 (returns and faults share every transport property; splitting them races the batch state and can't retire the header).
- **Temporal/Restate-style "error re-raises at the caller's await point."** Rejected: presupposes a suspended caller. calfkit callers are stateless consumers; the continuation is a message delivery, so the fault must be a message.

## Consequences

- Retry/redelivery is explicitly *not* provided by the rail (`retryable` is advisory; ACK_FIRST stays) — that is the successor retry/redelivery issue (#143 itself — worker-side error propagation to the original caller — is implemented and closed by this rail).
- Each escalation hop is one at-most-once delivery; the frame-stamping optimization reduces loss windows later without changing semantics.
- Previously-hanging callers (silent drop) now receive typed faults — an observable behavior change.
- **Escalation is identity-preserving** (the Python model: propagation never wraps; wrapping happens only on transformation — fault groups at batch closure, deliberate conversion while handling, recovery-then-failure chaining, an uncaught exception's `__cause__` chain). A declining ancestor re-addresses the fault (`in_reply_to`/`tag`) but never alters the `ErrorReport`, so `error_type` checks hold at any depth without unwrap loops; the full path is recoverable from `frame_chain` (captured once at synthesis) plus per-hop logs.
