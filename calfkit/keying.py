"""The partition-keying seam: one place that decides an outbound message's Kafka key.

**The keying contract** (recorded in the key-ordered dispatch ADR): the partition key
must co-locate every pair of messages whose handlers mutate the same await-spanning
workflow state — the key defines the serialization domain that the key-ordered subscriber
enforces (parallel across keys, strictly serial and in-order within one). Today's policy
keys by ``correlation_id``, making the serialization domain the conversation/workflow.

A planned keying change will replace this policy; call sites and tests must go through
this seam (never an inline ``.encode()`` literal) so that change lands in one place and
must re-verify the contract above for its new key. The node-side publish sites in
``calfkit.nodes`` still carry today's inline keys — sweeping them onto this seam is that
later change's job, deliberately not pre-empted here.
"""

from __future__ import annotations


def partition_key(correlation_id: str) -> bytes:
    """The Kafka partition key for an outbound calfkit message under today's policy."""
    return correlation_id.encode()
