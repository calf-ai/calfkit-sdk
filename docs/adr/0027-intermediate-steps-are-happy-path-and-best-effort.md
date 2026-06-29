---
status: proposed
---

# Intermediate steps are happy-path-only and best-effort: projected once per hop at the disposition chokepoint, logged-and-dropped on any failure, never faulting the run

[Step streaming](../designs/intermediate-step-streaming-spec.md) is a quality-of-life observation channel
for live UI progress, not business logic. We project and publish a hop's steps at the single **disposition
chokepoint** in `_handle_delivery` — where the hop's `output` is fully determined, *before* it branches to
fan-out OPEN or `_publish_action` — so fan-out and non-fan-out hops emit uniformly. Emission is
**happy-path-only** (never on a fault / abort / decline / re-entry — those reach the caller as the terminal
`RunFailed` via the fault rail) and **best-effort / at-most-once**: the whole projection + publish is wrapped
in its own `try/except` that **logs-and-drops AND falls through** to the real action; there is **no dedup**
and **no retry**. Steps from **all depths** route to the original caller's root inbox (the bottom frame's
`callback_topic`), keyed by `correlation_id` so they co-partition with the terminal.

The single chokepoint is load-bearing: projecting only inside `_publish_action` would skip every fan-out /
`isolate_state`-consult hop. The log-and-drop wrap is load-bearing for a subtler reason — the chokepoint runs
**outside** the outer publish guard, and under ACK_FIRST the inbound is already acked, so an *unguarded*
projection/publish raise would escape to FastStream and **hang the run** (worse than a fault); the wrap must
both swallow the error and not short-circuit the action that follows. A failed, oversized, or transient
observation must never change the run outcome, and best-effort observation does not warrant retry (a stripped
event could not satisfy its own schema anyway).

## Considered options

- **Durable / at-least-once steps with dedup machinery.** Rejected: heavy machinery for a QoL firehose;
  there is no broker redelivery to dedup, and the hop-identity fields exist for *correlation*, not dedup.
- **Project inside `_publish_action` only** (the round-2 design). Rejected: it misses the fan-out /
  `isolate_state`-consult hops entirely; the chokepoint covers both publish branches once per hop.
- **A free-form `ctx.emit()` for intra-hop progress.** Rejected: it forfeits the once-per-hop guarantee and
  the single-publish-seam invariant; the grain is the whole turn, not sub-hop deltas.
- **Strip-and-retry a failed step publish.** Rejected: best-effort observation does not warrant it, and a
  stripped event could not satisfy its own schema.

## Consequences

- Under ACK_FIRST a worker that acks then crashes before the step publish loses that step permanently
  (accepted).
- A rare action-publish failure / fan-out-OPEN abort can leave a phantom step (e.g. a `ToolCallEvent`) ahead
  of the resulting `RunFailed` on the wire — within the best-effort / close-on-terminal contract, not
  designed around.
- Reception is consume-once and best-effort to match: a post-terminal reordered step no-ops on the closed
  per-run channel, and a malformed step is swallowed by a lenient per-call decoder (never the broker decode
  floor) so it cannot fault the run.
- Streaming the all-depths trace to the original caller is a deliberate goal; the consequence — the run's
  inbox carries the full transitive trace, so inbox read-access is as sensitive as the run's full content —
  is documented, not policed (deployment is ops' domain).
