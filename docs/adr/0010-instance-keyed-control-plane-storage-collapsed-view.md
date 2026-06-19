---
status: accepted
---

# Control-plane records are instance-keyed, but the view collapses to one record per node

A control-plane topic carries one self-published record per node that advertises on it.
Multiple replicas of the same node (same `node_id`) run across workers — that is the default
scaling path, not an edge case. If records were keyed flat by `node_id`, the replicas would share
one key: their heartbeats clobber each other under last-write-wins (the liveness timestamp flaps
between replicas' clocks), and — worse — one replica's clean-shutdown tombstone `delete`s the key,
blanking a node that is still alive on another replica. That is the replica-flap defect (CRITICAL-1
in the prior capability-plane review).

We store records **instance-keyed** in a ktables `GroupedKafkaTable`: `group = node_id`,
`member = worker_id` (= `Worker.id`). Each writer owns its own key, so writes are
single-writer-per-key by construction — no cross-process read-modify-write, no lost-update race, and
each replica's tombstone removes only its own member. The "set of instances per node" becomes a
read-side grouping rather than a contended shared value.

The read side, `ControlPlaneView`, **collapses** that grouping back to a live `node_id → record`
map: stale members are filtered out and one live record is returned per node (replicas of a node
advertise equivalent content, so any live instance answers; ties break on most-recent heartbeat).
`worker_id` and the per-instance structure are deliberately hidden from consumers in v1.

Considered and rejected:

- **Flat keying by `node_id`.** Simplest — the table *is* the `node_id → record` map with no
  collapse — but it carries the replica flap and the lost-update race under the multi-replica
  default. Rejected.
- **A single node-keyed record holding a collection of instances.** Needs cross-process
  read-modify-write, which Kafka cannot make safe without a co-partitioned aggregator (a stateful
  consume-aggregate-produce service with ownership/rebalance) — heavy infrastructure this project
  avoids, and it still wouldn't detect crashes without a TTL. Rejected. Instance-keying with a
  read-side grouping is the lightweight, canonical secondary-index pattern.

Identity lives **only in the key**, never in the record value. The value carries the worker stamp
(boot time, last heartbeat, cadence) + `schema_version` + content; `node_id`/`worker_id` are the wire
key (group × member) and every reader derives them from it. Duplicating them in the value would be
redundant and — worse — *driftable* (a factory could return a value whose `node_id` disagreed with
its key, and the collapsed view reads value and key from different sides). Keeping identity key-only
makes that disagreement unrepresentable; the modest cost is that a raw value-only dump identifies a
record by its key rather than a self-describing `node_id` field. The base `ControlPlaneRecord` then
extends a small `ControlPlaneStamp` (boot + liveness + cadence) so those worker-stamped fields are
declared once.

A related record-shape decision, made for the same reason — correct reads across the read/write
process boundary: **the writer's heartbeat cadence travels on the record.** Whether an instance is
still alive ("staleness") is judged on the *read* side, but a reader is routinely a *different*
process than the writer (an agent worker reads a plane a toolbox worker writes), or a config-less
out-of-process reader (an ops CLI). If the staleness threshold lived only in the reader's config, a
reader defaulting to `3 × 30s` would wrongly mark a writer that heartbeats every `60s` as stale. So
`ControlPlaneRecord` carries `heartbeat_interval`, and the view derives `stale_after = N ×
record.heartbeat_interval` from the value on the wire. *Rejected:* keeping the threshold in reader
config and documenting that readers must match the writers' cadence — it silently false-positives
staleness on any mismatch and is uncomputable for a reader that never publishes. The cost is one
small float duplicated on every record; the benefit is that staleness is self-describing and immune
to reader/writer config drift.

Consequences: collapsing in the view defers per-instance access; if a future use case needs all
instances of a node (e.g. load-balancing asks across replicas), an `instances(node_id) ->
dict[worker_id, R]` accessor is a purely additive change — the grouped storage already holds the
data. The keying is on the wire (the composite Kafka message key), so changing it later is a topic
re-key (a migration); that is why it is fixed now rather than deferred.
