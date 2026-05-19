"""ConsumerRebalanceListener that drives state-store partition lifecycle.

Attached to the FanOutAggregator's returns subscriber via
``broker.subscriber(..., listener=_StateStoreRebalanceListener(state_store, returns_topic))``.

On ``on_partitions_assigned``, blocks the FastStream poll loop while it
rehydrates the corresponding state-topic partitions; on
``on_partitions_revoked``, evicts cache entries for revoked partitions
synchronously so the next worker that owns the partition starts from a
clean log read.
"""

from __future__ import annotations

import logging
from typing import Protocol

from aiokafka import ConsumerRebalanceListener, TopicPartition  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)


class _StateStoreProtocol(Protocol):
    """The state-store API the rebalance listener depends on.

    Defined here so the listener can be unit-tested against a stub store
    without importing :class:`_KafkaStateStore` (which would pull in
    aiokafka and FastStream).
    """

    async def rehydrate_partitions(self, partition_ids: set[int]) -> None: ...

    def evict_partitions(self, partition_ids: set[int]) -> None: ...


class _StateStoreRebalanceListener(ConsumerRebalanceListener):  # type: ignore[misc]
    """Drives state-store partition lifecycle from Kafka rebalance events.

    The listener is attached to the returns subscriber, so the
    ``TopicPartition`` sets it receives are for the
    ``{node_id}.fanout-returns`` topic. The aggregator's state topic is
    co-partitioned with returns (same partition count, same partitioner),
    so we map returns-topic partition IDs directly to state-topic
    partition IDs for rehydration / eviction.

    Partitions for other topics (the agent's main topic, or anything else
    a node happens to subscribe to) are filtered out via the
    ``returns_topic`` argument so this listener only acts on the topic
    it's responsible for.
    """

    def __init__(
        self,
        state_store: _StateStoreProtocol,
        returns_topic: str,
    ) -> None:
        self._state_store = state_store
        self._returns_topic = returns_topic

    async def on_partitions_assigned(self, assigned: set[TopicPartition]) -> None:
        partition_ids = {tp.partition for tp in assigned if tp.topic == self._returns_topic}
        if not partition_ids:
            return
        logger.info("aggregator partitions assigned: %s", sorted(partition_ids))
        try:
            await self._state_store.rehydrate_partitions(partition_ids)
        except Exception:
            # Rehydration may have populated _cache with some keys before
            # raising; without eviction those keys would float in the cache
            # while _owned_partitions doesn't include them, leading to
            # inconsistent reads on subsequent partition activations. Evict
            # everything we touched and re-raise so aiokafka surfaces the
            # rebalance failure to the operator.
            logger.exception(
                "aggregator rehydration failed for partitions=%s; evicting "
                "partially-loaded keys and aborting partition activation",
                sorted(partition_ids),
            )
            self._state_store.evict_partitions(partition_ids)
            raise

    async def on_partitions_revoked(self, revoked: set[TopicPartition]) -> None:
        partition_ids = {tp.partition for tp in revoked if tp.topic == self._returns_topic}
        if not partition_ids:
            return
        logger.info("aggregator partitions revoked: %s", sorted(partition_ids))
        self._state_store.evict_partitions(partition_ids)
