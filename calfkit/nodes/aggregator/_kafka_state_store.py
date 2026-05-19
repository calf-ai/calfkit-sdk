"""Production state store backing the fan-out aggregator.

Maintains a partition-scoped in-memory cache of in-flight batches, durably
backed by the ``{node_id}.fanout-state`` compacted Kafka topic. This is the
standard Kafka Streams / Faust / ksqlDB KTable pattern: one consumer reads
and writes (the FanOutAggregator's returns subscriber), cache updates are
atomic with the durable write, and the changelog topic is rehydrated on
worker startup and on partition reassignment via a one-shot
``AIOKafkaConsumer`` — the only place the aggregator subsystem reaches
beyond the FastStream API.

Writes go through FastStream's ``broker.publish()`` so they participate in
the same retry / lifecycle behaviour as other framework publishes.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, cast

from aiokafka import AIOKafkaConsumer, TopicPartition  # type: ignore[import-untyped]
from aiokafka.partitioner import murmur2  # type: ignore[import-untyped]
from faststream.kafka import KafkaBroker

from calfkit.nodes.aggregator._in_memory_store import _InFlightBatch, _TtlSet
from calfkit.nodes.aggregator._partitioner import build_composite_key, parse_composite_key
from calfkit.nodes.aggregator.state import FanOutState

logger = logging.getLogger(__name__)


class _KafkaStateStore:
    """Partition-scoped in-memory cache backed by a compacted Kafka topic.

    Standard Kafka Streams-style aggregation: the returns subscriber owns
    the cache and updates it synchronously alongside each durable write.
    There is no second consumer — :meth:`rehydrate_partitions` is called
    explicitly from the rebalance listener on partition assignment, and
    is bounded by the compacted topic size for the assigned partitions.

    The cache is partition-scoped: each worker only holds state for
    partitions it currently owns. :meth:`evict_partitions` drops entries
    on revoke; :meth:`rehydrate_partitions` rebuilds them on assignment.
    """

    def __init__(
        self,
        broker: KafkaBroker,
        state_topic: str,
        *,
        bootstrap_servers: str | list[str],
        partition_count: int | None = None,
        completion_ttl_seconds: float = 60.0,
        clock: Callable[[], float] | None = None,
    ) -> None:
        """Initialise the state store.

        Args:
            broker: FastStream KafkaBroker; used for durable publishes to the
                state topic.
            state_topic: ``{node_id}.fanout-state`` topic name.
            bootstrap_servers: Bootstrap servers for the transient
                ``AIOKafkaConsumer`` used during rehydration. Typically read
                from ``broker.config`` at construction time.
            partition_count: Total partition count for the state topic. When
                set, :meth:`partition_for_key` computes the partition
                deterministically from ``correlation_id``; otherwise callers
                must pass ``partition=`` explicitly to :meth:`put` /
                :meth:`tombstone`. Production code sets this via
                :class:`FanOutAggregator.setup`; tests typically omit it.
            completion_ttl_seconds: TTL for the recently-completed key set;
                matches the state topic's ``delete.retention.ms``.
            clock: Optional clock for deterministic TTL tests.
        """
        self._broker = broker
        self._state_topic = state_topic
        self._bootstrap_servers = bootstrap_servers
        self._partition_count = partition_count
        self._cache: dict[tuple[str, str], _InFlightBatch] = {}
        self._by_partition: dict[int, set[tuple[str, str]]] = {}
        self._owned_partitions: set[int] = set()
        self._recently_completed: _TtlSet = _TtlSet(completion_ttl_seconds, clock=clock)

    # ------------------------------------------------------------------
    # Read-side API (called by the FanOutAggregator returns handler)
    # ------------------------------------------------------------------

    def get(self, key: tuple[str, str]) -> _InFlightBatch | None:
        """Return the cached batch for ``key`` or ``None``."""
        return self._cache.get(key)

    def was_recently_completed(self, key: tuple[str, str]) -> bool:
        """True if ``key`` was tombstoned within the last
        ``completion_ttl_seconds`` (default 60s).

        Used to distinguish late returns (drop with INFO) from orphan
        returns (drop with WARN) — both are dropped, but the distinction
        helps debugging.
        """
        return key in self._recently_completed

    @property
    def owned_partitions(self) -> set[int]:
        """Snapshot of partition IDs currently owned by this store.

        Mutates as the rebalance listener calls
        :meth:`rehydrate_partitions` / :meth:`evict_partitions`.
        """
        return set(self._owned_partitions)

    # ------------------------------------------------------------------
    # Write-side API (called by the FanOutAggregator returns handler)
    # ------------------------------------------------------------------

    def partition_for_key(self, key: tuple[str, str]) -> int:
        """Compute the state-topic partition for ``(corr, fanout)``.

        Uses murmur2 over the ``correlation_id`` bytes — matching the
        :class:`FanOutAggregatorPartitioner` behaviour on the producer
        side. Requires :attr:`_partition_count` to be set.

        Raises:
            RuntimeError: if ``partition_count`` was not set at construction.
        """
        if self._partition_count is None:
            raise RuntimeError(
                "partition_count not configured on the state store; either "
                "pass partition=... explicitly to put/tombstone, or set "
                "partition_count when constructing the store"
            )
        return (cast(int, murmur2(key[0].encode())) & 0x7FFFFFFF) % self._partition_count

    async def put(
        self,
        key: tuple[str, str],
        batch: _InFlightBatch,
        *,
        partition: int | None = None,
    ) -> None:
        """Durably publish the batch state, then update the local cache.

        The publish completes before the cache write (``no_confirm=False``
        default), so on a crash between the two the durable log remains
        the source of truth and rehydration recovers correctly.

        ``partition`` is the state-topic partition this batch belongs to.
        When ``None``, it's auto-computed via :meth:`partition_for_key`
        (requires ``partition_count`` set at construction). The handler
        normally passes it explicitly from the inbound returns-topic
        message's partition (co-partitioned with the state topic).
        """
        if partition is None:
            partition = self.partition_for_key(key)
        state = batch.to_fanout_state()
        composite_key = build_composite_key(*key)
        await self._broker.publish(
            state,
            topic=self._state_topic,
            key=composite_key,
            partition=partition,
        )
        self._cache[key] = batch
        self._by_partition.setdefault(partition, set()).add(key)

    async def tombstone(
        self,
        key: tuple[str, str],
        *,
        partition: int | None = None,
    ) -> None:
        """Durably publish a null-value record (Kafka tombstone), then drop
        the cache entry and remember ``key`` for the recently-completed TTL.

        Late returns for ``key`` arriving within ``completion_ttl_seconds``
        of this call are dropped by :meth:`was_recently_completed`.

        ``partition`` may be ``None`` to auto-compute via
        :meth:`partition_for_key` (requires ``partition_count`` set).
        """
        if partition is None:
            partition = self.partition_for_key(key)
        composite_key = build_composite_key(*key)
        await self._broker.publish(
            None,
            topic=self._state_topic,
            key=composite_key,
            partition=partition,
        )
        self._cache.pop(key, None)
        if partition in self._by_partition:
            self._by_partition[partition].discard(key)
        self._recently_completed.add(key)

    def mark_completed(self, key: tuple[str, str]) -> None:
        """Local-only marker; idempotent with :meth:`tombstone`.

        :meth:`tombstone` already adds ``key`` to the recently-completed
        set; this method is kept for API parity with the in-memory store
        and as an explicit hook for callers that want to bypass the
        durable write (e.g., advanced testing scenarios).
        """
        self._recently_completed.add(key)

    # ------------------------------------------------------------------
    # Rebalance lifecycle (called by _StateStoreRebalanceListener)
    # ------------------------------------------------------------------

    async def rehydrate_partitions(self, partition_ids: set[int]) -> None:
        """One-shot read of state-topic partitions to (re)populate the cache.

        Called from the rebalance listener's ``on_partitions_assigned``
        BEFORE the FastStream consumer resumes processing the corresponding
        ``fanout-returns`` partitions. Uses a transient
        ``AIOKafkaConsumer`` with manual partition assignment and no
        ``group_id``, so it doesn't participate in group coordination —
        just reads each partition from beginning to current end-offset and
        stops.

        Idempotent: re-calling for an already-owned partition refreshes
        the cache from the log (bounded cost — compacted topic).
        """
        if not partition_ids:
            return

        consumer = AIOKafkaConsumer(
            bootstrap_servers=self._bootstrap_servers,
            enable_auto_commit=False,
        )
        await consumer.start()
        try:
            tps = [TopicPartition(self._state_topic, pid) for pid in partition_ids]
            consumer.assign(tps)
            await consumer.seek_to_beginning(*tps)
            end_offsets = await consumer.end_offsets(tps)

            # Empty partitions (end_offset == 0) have nothing to read.
            remaining: dict[TopicPartition, int] = {tp: end_offsets[tp] for tp in tps if end_offsets[tp] > 0}

            while remaining:
                batch_records = await consumer.getmany(timeout_ms=1000, max_records=500)
                progressed = False
                for tp, records in batch_records.items():
                    for record in records:
                        self._apply_record(tp.partition, record.key, record.value)
                        if record.offset + 1 >= remaining.get(tp, 0):
                            remaining.pop(tp, None)
                        progressed = True
                if not progressed:
                    # No records arrived in the polling window — partitions
                    # may have grown since we snapshotted end_offsets, but
                    # we've drained everything that existed at start.
                    logger.debug(
                        "rehydrate: empty poll with %d partitions still pending; finishing",
                        len(remaining),
                    )
                    break
        finally:
            await consumer.stop()

        self._owned_partitions.update(partition_ids)
        cached_count = sum(len(self._by_partition.get(p, set())) for p in partition_ids)
        logger.info(
            "rehydrated state-topic partitions=%s cached_keys=%d",
            sorted(partition_ids),
            cached_count,
        )

    def evict_partitions(self, partition_ids: set[int]) -> None:
        """Drop cache entries for revoked partitions.

        Called from the rebalance listener's ``on_partitions_revoked``
        BEFORE Kafka reassigns the partition to another worker. Uses the
        ``_by_partition`` index for O(evicted_keys) eviction.
        """
        for pid in partition_ids:
            for key in self._by_partition.pop(pid, set()):
                self._cache.pop(key, None)
            self._owned_partitions.discard(pid)
        if partition_ids:
            logger.info("evicted state-topic partitions=%s", sorted(partition_ids))

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _apply_record(
        self,
        partition: int,
        key_bytes: bytes | None,
        value_bytes: Any,
    ) -> None:
        """Apply a single state-topic record to the in-memory cache.

        ``value_bytes is None`` → tombstone (remove from cache, add to
        recently-completed). Otherwise parse as :class:`FanOutState` JSON.
        """
        if key_bytes is None:
            logger.warning(
                "state-topic record with null key on partition=%d; skipping",
                partition,
            )
            return
        try:
            key = parse_composite_key(key_bytes)
        except ValueError as exc:
            logger.warning(
                "state-topic record with malformed key %r: %s",
                key_bytes,
                exc,
            )
            return

        if value_bytes is None:
            self._cache.pop(key, None)
            if partition in self._by_partition:
                self._by_partition[partition].discard(key)
            self._recently_completed.add(key)
            return

        try:
            state = FanOutState.model_validate_json(value_bytes)
        except Exception as exc:
            logger.warning(
                "failed to parse FanOutState on partition=%d key=%r: %s",
                partition,
                key,
                exc,
            )
            return

        batch = _InFlightBatch.from_fanout_state(state)
        self._cache[key] = batch
        self._by_partition.setdefault(partition, set()).add(key)
