---
status: accepted
---

# The client uses one inbox reader tee'd to a lossless per-run demux and a best-effort (bounded, drop-oldest) firehose

The [client caller-surface](../designs/client-caller-surface-spec.md) serves two read tiers off the
client's inbox: per-run `result()` / `stream()` (the hub demultiplexes replies by `correlation_id`
into each `InvocationHandle`'s in-memory channel) and a cross-run firehose (`client.events()`, the
raw stream of every reply on the inbox). The question is whether those tiers read through **one**
Kafka consumer or **several**, and — given one consumer — how the firehose handles a reader that falls
behind. An earlier draft gave each `events()` its own `assign()` consumer (lossless + decoupled, but
fetch amplification), and a later draft kept one consumer with a *blocking* firehose outlet (lossless,
but a slow firehose stalls the shared read).

Decision: **one inbox reader per client — a FastStream handler subscriber** on a **groupless topic
subscription** (`subscribe(inbox)`, `group_id=None`, `auto_offset_reset="latest"`; the shipped
`_ReplyDispatcher` is the same topic subscription but *with* a per-client group). Groupless +
topic-subscribe (not manual `assign()`) auto-assigns **all** partitions at consumer start with no
enumeration, so no per-run partition list is needed. Its handler tee's **in-process** to two outlets
that **neither block the handler**: (a) the **per-run demux** — a synchronous, non-blocking push into each
handle's **lossless** channel (sized by the run; this is the critical path and never drops); and (b)
**per-observer firehose outlets** — each open `events()` gets its own **bounded buffer** whose push is
a **non-blocking enqueue-or-drop**: when full, the **oldest** buffered event is dropped to admit the
new one (`onBackpressureLatest`). So the firehose is a **best-effort observation tier**, not lossless —
a reader that falls behind loses its oldest events. Drops are **signaled** (a dropped-count + WARNING,
never silent), the buffer size is a second-class `connect(firehose_buffer_size=…)` knob (a sensible
low-thousands default, tunable from the drop signal), and worst-case firehose memory is bounded
(`size × event_size × observers`). The handler path is chosen over a raw/polling consumer because it keeps the **live decode floor**,
Envelope decode, context injection, and **offline `TestKafkaBroker` testability** as single canonical
implementations (raw forks the decode floor and loses testability; polling does both *and* kills the
floor — its `consume_scope` wraps a no-op). The inbox is set **once at `connect()`** (no
`events(inbox_topic=…)` override — a different topic is the only thing that would force a separate
consumer); `connect()` is **lazy** — the subscriber is *registered* at `connect()` and *started* by the
first `broker.start()` (the first publish, or a co-located `Worker`'s `app.start()`), so the **Worker is
untouched** and there is no eager connect / seek gate. The M4 "co-located terminal precedes the reader"
race cannot bite: the broker starts before the first publish, so the consumer positions at tail within
the reply round-trip (a reply cannot beat a local first-poll).

Considered options — the constraint is that you cannot have all four of {one read, lossless firehose,
hub-never-blocks, bounded memory}: (1) **N independent consumers** — lossless + decoupled, but pays
fetch amplification and a second consumer model; (2) **one read, bounded *blocking* firehose outlet** —
lossless + bounded memory, but the blocking push stalls the single fetch loop, so an open-but-undrained
`events()` can **deadlock** an awaited `result()` (a load-dependent hang that passes on a quiet inbox
and fails on a busy one) — rejected; (3) **one read, *unbounded* outlet** — lossless + non-blocking, but
**OOMs** the whole process under a sustained slow reader — rejected; (4) **one read, bounded *drop*
outlet (chosen)** — the only option that is simultaneously non-blocking *and* memory-safe, bought by
giving up the *lossless-firehose* corner (the firehose becomes best-effort). We took (4): the critical
path must stay lossless and never-block (it serves every run), so the relaxation lands on the
*observation* firehose, where a bounded, signaled gap is the standard, acceptable cost (cf.
OpenTelemetry's `BatchSpanProcessor`, Reactor's `onBackpressureLatest`). The hard rule that falls out:
**groupless topic-subscribe, tail-only — no consumer groups anywhere in the client transport.**

Consequences. The explicit **no**: a load-balanced, restart-durable, worker-pool drain of a shared
inbox is **not** on the client surface (the earlier `drain(inbox_topic, group=)` method is dropped) —
that is a `@consumer` node's job, the framework's canonical durable/load-balanced topic-consumption
surface; a caller-side drainer would be the *only* consumer-group user on the client, large transport
weight for a node-free convenience (revisit alongside a future durable result store). **Guaranteed
delivery is hold-the-handle (`start`/`execute`) or a `@consumer` node; the firehose is observation,
not a delivery guarantee.** One client instance observes exactly one inbox. The whole client transport
collapses to a single groupless topic-subscribe reader with **no rebalance/rejoin surface and no committed-offset
window to skip**; reconnect resumes from the in-memory read position (a disconnect outliving log
retention is the one real loss path). Delivery is at-least-once at the broker, **at-most-once at the
application on a process crash** (the in-memory hub/handles are not recovered) — crash-survival is the
deferred durable store. This ADR records the transport shape; the design is specified in the
caller-surface spec, ahead of implementation. Decided 2026-06-25; the transport realization was refined
to a **FastStream handler subscriber + lazy connect** (from an earlier eager-connect / raw-or-polling
draft) on 2026-06-26 after a deep FastStream-source review — the single-reader-tee + best-effort-drop
decision itself is unchanged.
