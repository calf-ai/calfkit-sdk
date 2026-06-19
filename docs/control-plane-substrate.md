# About the control-plane substrate

calfkit nodes run spread across many workers and many processes. A recurring
question follows from that: *how does one part of the mesh learn what else
exists — which tools are online, which agents can be asked, what a node is
allowed to do — without standing up a central registry service or wiring
point-to-point chatter between every pair of nodes?*

The **control-plane substrate** is the shared answer. This document is
**understanding-oriented**: it explains what a control plane is, the one design
decision the whole thing turns on, and — the part most worth your time — how
discovery, access-policy, and other future planes are all the *same* machinery
over *different* topics. It is not a step-by-step guide (see
[How to add a control plane to your nodes](control-plane-howto.md)) and not the
full contract (see the [design spec](designs/control-plane-substrate-spec.md)).
Read it while thinking about a design, not while mid-task.

The short version, which the rest unpacks:

> A *control plane* is a compacted Kafka topic that nodes self-publish onto and
> others read back as a live map. The substrate is the reusable machinery for
> that — a record base, a worker-owned publisher, and a per-topic view —
> deliberately separated from any single use. The one load-bearing decision:
> records are keyed per **running instance** (`node_id × worker_id`) and
> collapsed to one-record-per-node on read, so a node's replicas — the normal
> scaling path — never clobber each other.

---

## The data plane and the control plane

Borrowing the distinction from networking, Kafka, and Kubernetes: the **data
plane** is the traffic that does the work — the calls, replies, and events nodes
exchange on their own topics. The **control plane** is metadata *about the nodes
themselves*: who is online, what each one offers, what each is permitted to do.

The substrate carries only control-plane data. It advertises *that* a node
exists and *what* it offers; it never carries a work message. Actually *calling*
a node you discovered is the data plane's job — see
[the peer-node pattern](peer-node-pattern.md) for the call side.

## Three pieces

A control plane is built from three generic types in `calfkit.controlplane`:

- **`ControlPlaneRecord` — the *what*.** A frozen pydantic model, one per node
  instance, living on a topic. It extends `ControlPlaneStamp` (the worker-stamped
  `started_at`, `last_heartbeat_at`, and the writer's `heartbeat_interval`) and
  adds a `schema_version`. A concrete plane subclasses it and adds its content —
  a capability record might add `tools`, an agent card an `ask_topic`, an ACL
  record an allow-list.
- **`ControlPlanePublisher` — the *write side*.** One per worker. On a timed
  loop it builds a fresh stamp, asks each hosted node's advert factory for a
  record, and writes it. Boot is fail-loud (a node that cannot advertise stops
  the worker rather than running dark); each subsequent tick is per-advert
  resilient. A clean shutdown tombstones the records it wrote.
- **`ControlPlaneView[R]` — the *read side*.** A typed wrapper that presents the
  topic as a live `node_id → record` map, hiding records that are stale or from
  an unsupported schema.

Writer and reader are decoupled through the topic. They are routinely in
*different processes* — an agent's worker reads the capability plane that a
toolbox's worker writes — and can even sit on different Kafka clusters. Nothing
holds an object reference across that gap; the topic is the only shared thing.

## Why instance-keying, and why collapse

This is the decision the substrate turns on, so it is worth unfolding.

Replicas of a node share a `node_id` — running three copies of an agent for
throughput is the *normal* path, not an edge case. If records were keyed flat by
`node_id`, those replicas would share one key. Two bad things follow: their
heartbeats overwrite each other under last-write-wins (the liveness timestamp
flaps between replicas' clocks), and — worse — when one replica shuts down
cleanly its tombstone *deletes the key*, blanking a node that is still alive on
another replica. That is the replica-flap defect.

The fix is to key records per running instance: `group = node_id`,
`member = worker_id`, stored in a ktables `GroupedKafkaTable`. Now each writer
owns its own key. Writes are single-writer-per-key by construction — no
cross-process read-modify-write, no lost-update race — and each replica's
tombstone removes only its own member. "The set of instances of a node" becomes
a *read-side grouping* rather than a contended shared value.

The view then **collapses** that grouping back to what a consumer actually wants:
one live record per node. Stale members are filtered out and the most-recently
heartbeated survivor is returned (replicas advertise equivalent content, so any
live one answers). The per-instance structure is deliberately hidden in v1; if a
later use case needs all instances of a node — say, to load-balance across
replicas — an `instances(node_id)` accessor is a purely additive change, because
the grouped storage already holds the data.

One detail makes this robust: **identity lives only in the key, never in the
record value.** The value carries the stamp, `schema_version`, and content;
`node_id`/`worker_id` are the wire key and every reader derives them from it.
Duplicating identity inside the value would make it *driftable* — a factory could
return a value whose `node_id` disagreed with its key. Keeping identity key-only
makes that disagreement unrepresentable. The reasoning is recorded in
[ADR-0010](adr/0010-instance-keyed-control-plane-storage-collapsed-view.md).

## Liveness is advisory, and it travels on the record

Whether an instance is still alive is judged on the *read* side: a record older
than `N × heartbeat_interval` is treated as stale and hidden (default `N = 3`).

The subtlety is that the reader is often a *different process* than the writer,
or a config-less out-of-process reader like an ops CLI. If the staleness
threshold lived only in the reader's config, a reader defaulting to `3 × 30s`
would wrongly mark a writer that heartbeats every `60s` as stale. So the writer's
cadence rides **on the record** (`heartbeat_interval`), and the view derives its
threshold from the value on the wire rather than from its own config — immune to
reader/writer drift. A reader can still override with an explicit `stale_after`.

"Advisory" is the operative word. Staleness is a heuristic over wall-clock
heartbeats, not a lease or a consensus protocol. A *crashed* node lingers in the
view until its records age past the threshold; a *cleanly shut down* node removes
itself at once via the tombstone. That asymmetry is intentional — it is what lets
the plane cost nothing more than a compacted topic, with no lease service to run.
It is also why a control plane is for *discovery*, not for correctness-critical
fencing: treat "online" as "very probably reachable," not as a guarantee.

## What a reader sees: pull, compaction, catch-up

Content is **pulled** from the node's factory every tick. The worker stamps
liveness fresh each time, while the record's own content only changes when the
node's underlying state does. A content change therefore lands within one
heartbeat interval — good enough for discovery, and a sub-tick push path is a
clean additive extension if a future consumer ever needs it.

The topic is **compacted** (`cleanup.policy=compact`): Kafka keeps the last
record per key and physically drops tombstoned keys, so the log stays
proportional to the number of live instances rather than growing forever. A fresh
reader rebuilds the entire map by replaying that compacted log.

Reads are therefore **eventually consistent**, and the view is honest about it.
It exposes catch-up gates (`barrier()`, `wait_until_caught_up()`, `is_caught_up`)
so a consumer can wait for the log to replay before its first read, and it passes
through the table's `status`/`failure` so a degraded reader is *observable*
rather than silently serving an empty map.

## The worker drives liveness; the node owns content

The publisher is **worker-owned** — one loop and one tombstone pass per worker,
covering every node it hosts. Liveness is a worker concern: the heartbeat
reflects the health of the *process*, so the process should own it. A node
contributes only *content*, through its `@advertises` factory; it never imports
ktables and never runs a loop. This split, recorded in
[ADR-0011](adr/0011-worker-owned-control-plane-publisher.md), is what lets a node
be use-case-rich while the worker stays use-case-blind: the worker calls an
opaque factory and publishes an opaque record, knowing nothing about capabilities
or ACLs.

## How future planes build on it

This is the reason the substrate exists as machinery rather than as one feature.
A new control plane is just **three small things**: a `ControlPlaneRecord`
subclass, an `@advertises` factory on the nodes that participate, and a
`ControlPlaneView[R]` over the plane's own topic. Everything else — keying,
collapse, staleness, tombstones, catch-up — comes for free.

- **Discoverability planes** (capability discovery, agent discovery) are the
  straightforward case: nodes *self-publish* what they offer as a heartbeat. A
  capability record carries a node's tools and dispatch topic; an agent card
  carries an ask-topic and skills. A consumer opens a `ControlPlaneView` over the
  plane and selects peers from the live map. The write side is exactly the
  heartbeat publisher described above.

- **An access-policy / ACL plane** ([#231](https://github.com/calf-ai/calfkit-sdk/issues/231))
  reuses the read side *unchanged* but has a different **write authority**: its
  records say who may call whom, and they are *operator-authored*, not
  self-published liveness. This is the sharpest illustration of why the substrate
  is shaped the way it is — the read machinery (`ControlPlaneView`, collapse,
  catch-up, health) is reused verbatim; only who writes the records, and why,
  differs.

That difference in write authority is also why each plane gets **its own topic**
rather than sharing one:

1. **One topic ⇒ one record schema.** A view decodes exactly one record type; a
   foreign payload on a shared topic would fail to decode and the node behind it
   would silently vanish. Separate topics keep each plane's schema isolated.
2. **Different write authorities.** A heartbeat plane is self-published and
   expires; an ACL plane is operator-authored durable policy that should *not*
   "go stale." One staleness posture cannot fit both.
3. **Different lifecycles.** Retention, access control, and partitioning are
   reasonably set per plane.

So the design is *machinery + per-use-case topics* — one generic view over many
topics — not a single shared "presence" topic. Each plane keeps its own schema,
authority, and lifecycle while sharing every line of the substrate.

## Choices and alternatives

The shape above was chosen against several plausible alternatives, each rejected
for a concrete reason:

- **Flat `node_id` keying.** Simplest — the table *is* the `node_id → record`
  map, no collapse needed — but it carries the replica flap and a lost-update
  race under the multi-replica default. Rejected.
- **One node-keyed record holding a collection of instances.** Needs
  cross-process read-modify-write, which Kafka cannot make safe without a
  co-partitioned aggregator (a stateful consume-aggregate-produce service with
  its own ownership and rebalance handling) — heavy infrastructure this project
  avoids, and it still wouldn't detect crashes without a TTL. Instance-keying
  with a read-side grouping is the lightweight, canonical secondary-index
  pattern. Rejected.
- **One shared control-plane topic for everything.** Fails the one-schema-per-
  topic rule and conflates write authorities and lifecycles. Rejected in favour
  of per-use-case topics.
- **Staleness from reader config.** Silently false-positives on any cadence
  mismatch and is uncomputable for a reader that never publishes. Rejected;
  cadence travels on the record instead.
- **A central registry service.** A process to deploy, scale, secure, and fail.
  The compacted topic *is* the registry, replicated and made durable by Kafka.
  Rejected.

## What the substrate deliberately is not

- **Not messaging or RPC.** It is a directory, not a transport. Discovering a
  node tells you it exists and how to reach it; the call itself goes over the
  data plane.
- **Not a presence feature.** Presence was dropped from v1; the substrate is the
  registry-only foundation that a presence or discovery feature would build on,
  not the feature itself.
- **Not per-instance, yet.** v1 collapses to one record per node and has no push
  path. Both are additive extensions, deferred until an adopter needs them.
- **Not a fence against operational failures.** Rebalance and crash-during-publish
  edge cases are out of scope by design; liveness is at-most-once and advisory,
  and durable tables handle the graceful cases.

---

See also:
[How to add a control plane to your nodes](control-plane-howto.md) for the
recipe, the [design spec](designs/control-plane-substrate-spec.md) for the full
contract, and [ADR-0010](adr/0010-instance-keyed-control-plane-storage-collapsed-view.md)
/ [ADR-0011](adr/0011-worker-owned-control-plane-publisher.md) for the two
decisions above.

*The source tree is always the source of truth. The types and behaviour
described here live in `calfkit/controlplane/`; if you find code that
contradicts this document, trust the code and flag the doc for reconciliation.*
