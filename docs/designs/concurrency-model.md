# Concurrency Model — In-Process Group Members (future knob)

**Status:** PROPOSED (future) — deliberately deferred from the in-node fan-out aggregation work to keep that design scoped to durability/correctness.
**Related:** [`in-node-fanout-aggregation-spec.md`](in-node-fanout-aggregation-spec.md) (pins `max_workers=1` for caller-capable nodes), [`fault-rail-and-policy-seams-spec.md`](fault-rail-and-policy-seams-spec.md) (§4.1, the serialization invariant).

## The constraint: `max_workers=1` is pinned for caller-capable nodes

Caller-capable nodes (agents — anything that folds fan-out batches or runs the seam pipeline) are pinned to FastStream `max_workers=1` (serial processing per consumer). Two independent reasons, both verified against installed FastStream 0.6.6 / aiokafka 0.13:

1. **FastStream's `max_workers>1` concurrent subscriber is unsafe for our read-modify-write.** Under ACK_FIRST it selects `ConcurrentDefaultSubscriber`: *one* consumer feeding a `Semaphore(max_workers)`-limited pool of handler **coroutines on one event loop** (`_internal/endpoint/subscriber/mixins.py:46-87`) — no partition affinity. Two sibling replies of one batch land on one partition (correlation-keyed), get dispatched to two pool coroutines, and the fold's `barrier → read → write` spans `await` points, so the coroutines interleave and lost-update the accumulating `FanoutState`. (Cooperative single-thread concurrency does **not** make an await-spanning read-modify-write atomic.)
2. **The partition-affine alternative is single-topic-only.** Under non-ACK_FIRST, `max_workers>1` selects `ConcurrentBetweenPartitionsSubscriber` — N *separate* group members, partition-affine, which *would* be safe — but FastStream truncates it to one topic and raises `SetupError` on multiple (`kafka/subscriber/factory.py:111,167-170`). Caller-capable nodes are inherently multi-topic (ingress + `{node}.private.return`), so they can't use it.

So intra-process concurrency *via FastStream's knob* is off the table. Serial-per-consumer stands.

## The knob: scale by adding group members, not by an in-process pool

Concurrency comes the way Kafka intends — **more consumers in the same group.** N consumers (same `group_id`) → Kafka assigns each partition to exactly one member → **serial within a partition, concurrent across partitions.** Safe by construction: a batch's siblings are correlation-keyed onto one partition, owned by one consumer, folded serially. No lock — the partition assignment *is* the single-writer guarantee. (`member_id` is per consumer-instance, so a process can hold many; Kafka doesn't care that members share a process.)

Two axes, both just "group members":

- **In-process (this knob):** a per-node `concurrency=N` → the worker registers the node's subscriber **N times** (same `group_id`, topics, handler, all `max_workers=1`). N aiokafka consumers in one process, their consume loops interleaving as N coroutines on the event loop; Kafka shards the partitions across them.
- **Cross-process (existing):** N replicas, one consumer each.

```python
AgentNodeDef(..., concurrency=4)   # → 4 group members in this worker process
```
```python
# worker.py registration loop:
for _ in range(node.concurrency):
    subscriber = connection.subscriber(*topics, group_id=node.name, max_workers=1)
    subscriber(node.handler)
# (dedup the N identical subscribers in AsyncAPI / registration logs — cosmetic)
```

## Why in-process is worth shipping (not just "run more replicas")

The N in-process consumers **share one batch store.** The fan-out ktables (state + basestate readers/writers) are per-*node*, created once per process — so all N consumers read/write the *same* materialized tables. That's **one batch store (two GlobalKTable materializations — `state` + `basestate`) serving N consumers**, versus N replicas each replaying *both* whole topics on startup (2N replays). It directly amortizes the per-node-table RAM and the startup catch-up cost that node-scoped tables trade against: raise throughput without multiplying table materializations.

Sharing is safe because the N consumers own **disjoint partitions → disjoint `fanout_id` keys**: no two write the same key; the shared `KafkaTableWriter` (aiokafka producer) handles concurrent sends to different keys; the shared reader's `_data` is read concurrently at different keys with no reader-side mutation; the shared `barrier()` simply has N awaiters on one watermark.

## Caveats

- **One event loop ⇒ I/O concurrency, not CPU parallelism.** Agent work is I/O-bound (the fold's `await`s and the resume's seconds-long model call), so N consumers overlap each other's I/O — real throughput. CPU-bound or loop-blocking work stalls all N; use **replicas** for CPU parallelism and fault isolation.
- **Capped at the topic's partition count.** Members beyond the partition count sit idle; `concurrency` is bounded by provisioning.
- **Shared-process blast radius:** one crash takes all N members down (their partitions rebalance elsewhere). Replicas isolate that.

So the full concurrency story is **`concurrency=N` (in-process group members) × replicas (processes)** — both adding group members, neither using FastStream `max_workers>1`.

## Deferred because

It is purely additive throughput: no bearing on correctness (the design is correct at `concurrency=1`), no new wire shapes, no change to the fold/close logic. Scoping it out keeps the in-node aggregation design focused on the durability core. Ship it when throughput-per-process becomes the constraint.
