"""The partition-keying seam: one place that decides an outbound message's Kafka key.

**The keying contract** (recorded in the key-ordered dispatch ADR and the task-keying
migration ADR): the partition key must co-locate every pair of messages whose handlers
mutate the same await-spanning workflow state — the key defines the serialization domain
that the key-ordered subscriber enforces (parallel across keys, strictly serial and
in-order within one). The policy keys by ``task_id`` — the mesh-wide unit of partition
affinity: one task's traffic serializes per node partition, and task granularity is the
caller's parallelism decision. Task-affinity ⊇ run-affinity (task and correlation are
both minted once and forwarded, so per-run co-partition sets are identical) — every
correlation-affinity guarantee survives the cutover.

This seam is the single authority for the WORKFLOW partition key: every calfkit publish
that carries one — client entry, the node-plane action/fault/re-entry/sibling sites,
and the step flush — flows through ``partition_key`` (the node-plane sweep landed with
the task-keying cutover). Deliberately outside it: the broadcast mirror is KEYLESS (the
``Response`` return path — the durable PR routes it through the chokepoint and keys
it), and control-plane / fan-out ktable writers key by their own compacted identity
keys (node identity, ``fanout_id``), which are storage keys, not workflow affinity. A
policy change edits this one place and must re-verify the contract above for its new
key; call sites and tests reference the seam, never an inline ``.encode()`` literal.

**Ordering scope:** per-key / per-task ordering is guaranteed only for deliveries
published with a ``task_id`` key. Per the ingress-mint guarantee (the identity
middleware mints a task for any envelope-wire delivery arriving without one), that is
every point-to-point calfkit delivery; keyless traffic — the broadcast mirror above,
and anything external — has no ordering guarantee, with the key-ordered subscriber's
throttled keyless warning as the backstop.
"""

from __future__ import annotations


def partition_key(task_id: str) -> bytes:
    """The Kafka partition key for an outbound calfkit message: the run's task id."""
    return task_id.encode()
