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
from calfkit.nodes.aggregator.errors import AggregatorStateStoreError
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

    # Rehydration retry budget. Each empty getmany poll waits 1s; after
    # this many consecutive empty polls with offsets still outstanding,
    # rehydrate_partitions raises rather than silently activating the
    # partition with partial state.
    #
    # The budget is CUMULATIVE across end_offsets re-polls — see
    # rehydrate_partitions for rationale. The value is sized for the
    # worst-case wall-clock budget when combined with
    # _REHYDRATE_MAX_ENDOFFSET_REPOLLS so a slow stream that advances
    # one partition per re-poll cannot silently exhaust multiple budgets.
    _REHYDRATE_MAX_EMPTY_POLLS: int = 15

    # End-offset re-poll budget. The previous owner of a partition may
    # still be flushing in-flight writes when the new owner snapshots
    # end_offsets — those writes land *after* the snapshot but *before*
    # the read completes, and would be silently missed without a re-poll.
    # After draining to the initial end_offsets, we re-poll; if offsets
    # advanced, we read the new tail and re-poll again. This bounds the
    # loop against an active producer that the new owner cannot safely
    # race against (rebalance listener has no producer-drain barrier).
    _REHYDRATE_MAX_ENDOFFSET_REPOLLS: int = 3

    def __init__(
        self,
        broker: KafkaBroker,
        state_topic: str,
        *,
        bootstrap_servers: str | list[str],
        partition_count: int | None = None,
        completion_ttl_seconds: float = 60.0,
        clock: Callable[[], float] | None = None,
        client_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Initialise the state store.

        Args:
            broker: FastStream KafkaBroker; used for durable publishes to the
                state topic.
            state_topic: ``{node_id}.fanout-state`` topic name.
            bootstrap_servers: Bootstrap servers for the transient
                ``AIOKafkaConsumer`` used during rehydration. Captured by
                ``Client.connect`` and threaded through the worker.
            partition_count: Total partition count for the state topic. When
                set, :meth:`partition_for_key` computes the partition
                deterministically from ``correlation_id``; otherwise callers
                must pass ``partition=`` explicitly to :meth:`put` /
                :meth:`tombstone`. Production code sets this via
                :class:`FanOutAggregator.setup`; tests typically omit it.
            completion_ttl_seconds: TTL for the recently-completed key set;
                matches the state topic's ``delete.retention.ms``.
            clock: Optional clock for deterministic TTL tests.
            client_kwargs: Extra Kafka client kwargs (security_protocol,
                sasl_mechanism, sasl_plain_username, sasl_plain_password,
                ssl_context, client_id, ...) forwarded to the transient
                :class:`AIOKafkaConsumer` used during rehydration. Without
                these, rehydration fails on any production cluster with
                SASL/SSL auth. ``None`` is treated as an empty dict.
        """
        self._broker = broker
        self._state_topic = state_topic
        self._bootstrap_servers = bootstrap_servers
        self._partition_count = partition_count
        self._client_kwargs: dict[str, Any] = dict(client_kwargs) if client_kwargs else {}
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

        Lookup used by the returns handler to classify a return that
        finds no in-flight batch in the cache: ``True`` means this is a
        legitimate late return for a batch the aggregator already
        completed (the handler drops it at INFO); ``False`` means this is
        an orphan return with no associated batch on an owned partition
        (the handler raises ``AggregatorStateStoreError``). The handler
        owns the policy decision; this method only answers the lookup.
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
            **self._client_kwargs,
        )
        try:
            await consumer.start()
            tps = [TopicPartition(self._state_topic, pid) for pid in partition_ids]
            consumer.assign(tps)
            await consumer.seek_to_beginning(*tps)

            # Track the highest observed end-offset per partition across
            # re-polls; we only need to read records whose offset is below
            # this watermark. When the post-read re-poll surfaces a higher
            # offset, we extend the watermark and resume reading.
            observed_end_offsets: dict[TopicPartition, int] = await consumer.end_offsets(tps)

            # Empty partitions (end_offset == 0) have nothing to read on
            # this iteration; they're still re-polled in case the previous
            # owner publishes after we snapshotted.
            remaining: dict[TopicPartition, int] = {tp: observed_end_offsets[tp] for tp in tps if observed_end_offsets[tp] > 0}

            empty_polls = 0
            endoffset_repolls = 0
            while True:
                # Drain to the current watermark for each partition.
                while remaining:
                    batch_records = await consumer.getmany(timeout_ms=1000, max_records=500)
                    progressed = False
                    for tp, records in batch_records.items():
                        for record in records:
                            self._apply_record(tp.partition, record.key, record.value, record.offset)
                            if record.offset + 1 >= remaining.get(tp, 0):
                                remaining.pop(tp, None)
                            progressed = True
                    if progressed:
                        empty_polls = 0
                    else:
                        empty_polls += 1
                        if empty_polls >= self._REHYDRATE_MAX_EMPTY_POLLS:
                            raise AggregatorStateStoreError(
                                f"rehydration stalled after {empty_polls} empty polls "
                                f"(~{empty_polls}s upper-bound, getmany timeout=1s) on "
                                f"state-topic partitions="
                                f"{sorted(tp.partition for tp in remaining)}; "
                                f"broker may be unhealthy or partition leadership in flux. "
                                f"Refusing to activate partition assignment with partial state.",
                                state_topic=self._state_topic,
                            )

                # Drain complete to the previously observed watermark.
                # Re-poll end_offsets to detect writes that landed after
                # our initial snapshot (the previous owner may still be
                # flushing in-flight publishes). If new offsets are higher
                # for any partition, resume reading from the previous end.
                latest_end_offsets: dict[TopicPartition, int] = await consumer.end_offsets(tps)
                advanced: dict[TopicPartition, int] = {tp: latest_end_offsets[tp] for tp in tps if latest_end_offsets[tp] > observed_end_offsets[tp]}
                if not advanced:
                    break

                endoffset_repolls += 1
                if endoffset_repolls >= self._REHYDRATE_MAX_ENDOFFSET_REPOLLS:
                    # An active producer is still writing — the previous
                    # owner has not flushed and released its publish
                    # pipeline. We cannot safely race against that
                    # producer; refuse to activate.
                    raise AggregatorStateStoreError(
                        f"rehydration end_offsets failed to stabilise after "
                        f"{endoffset_repolls} re-polls on state-topic "
                        f"partitions={sorted(tp.partition for tp in advanced)}; "
                        f"a producer is still writing during rebalance. "
                        f"Refusing to activate partition assignment with state "
                        f"that may already be stale. Last observed offsets: "
                        f"{ {tp.partition: latest_end_offsets[tp] for tp in tps} }",
                        state_topic=self._state_topic,
                    )

                # Extend the watermark and resume reading. We already
                # consumed up to observed_end_offsets[tp] for each
                # partition; the consumer's position is at that point,
                # so feeding the new gap into ``remaining`` is sufficient
                # — getmany will continue from where it left off.
                #
                # empty_polls is intentionally NOT reset here: it
                # accumulates across re-polls. A stalled poll counts
                # toward the budget even if a sibling partition advanced
                # on the next re-poll; resetting per-repoll would let a
                # slow stream silently exhaust the budget repeatedly
                # (max budget * max re-polls cumulative empties before
                # the cap fires). The progress-based reset inside the
                # inner drain loop still triggers on every record, so
                # genuine forward progress does refresh the counter.
                remaining = advanced
                observed_end_offsets = latest_end_offsets
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
        offset: int = -1,
    ) -> None:
        """Apply a single state-topic record to the in-memory cache.

        ``value_bytes is None`` → tombstone (remove from cache, add to
        recently-completed). Otherwise parse as :class:`FanOutState` JSON.

        ``offset`` is included in diagnostic error messages when a poison
        or contaminating record forces partition activation to abort; it
        defaults to ``-1`` for synthetic test calls that don't model a
        durable offset.
        """
        if key_bytes is None:
            # Null-key records on the state topic should never come from
            # this aggregator (all writes use composite keys). Their
            # presence violates the topic's trust invariant — something
            # other than this aggregator is producing to the state topic.
            # Refuse to activate the partition; the rebalance listener
            # evicts any partial state and re-raises so the operator can
            # diagnose the contamination before durable state is trusted.
            raise AggregatorStateStoreError(
                f"state-topic record with null key on partition={partition} "
                f"offset={offset}; aggregator writes always use composite "
                f"keys, so this record indicates the state topic has been "
                f"written by something other than this aggregator. "
                f"Refusing to activate partition with contaminated state.",
                state_topic=self._state_topic,
            )
        try:
            key = parse_composite_key(key_bytes)
        except ValueError as exc:
            # Same rationale as the null-key branch: a malformed composite
            # key cannot be produced by this aggregator's write path. Trust
            # invariant violated; refuse activation rather than silently
            # skipping records that may be tied to real in-flight batches.
            raise AggregatorStateStoreError(
                f"state-topic record with malformed key {key_bytes!r} on "
                f"partition={partition} offset={offset}: {exc}. "
                f"Refusing to activate partition with contaminated state.",
                state_topic=self._state_topic,
            ) from exc

        if value_bytes is None:
            self._cache.pop(key, None)
            if partition in self._by_partition:
                self._by_partition[partition].discard(key)
            self._recently_completed.add(key)
            return

        try:
            state = FanOutState.model_validate_json(value_bytes)
        except Exception as exc:
            # Poison record for a known key: we cannot recover the in-flight
            # batch state. Activating the partition would silently lose the
            # batch (returns for it would arrive as 'orphans' and be dropped).
            # Raise so the rebalance listener evicts and re-raises, surfacing
            # the corruption to the operator.
            raise AggregatorStateStoreError(
                f"failed to parse FanOutState on partition={partition} offset={offset} key={key!r}: {exc}. "
                f"Refusing to activate partition with corrupt durable state.",
                state_topic=self._state_topic,
            ) from exc

        batch = _InFlightBatch.from_fanout_state(state)
        # A put after a tombstone for the same key resurrects an active batch;
        # the completion marker must clear so returns aren't shadowed as 'late'.
        self._recently_completed.discard(key)
        self._cache[key] = batch
        self._by_partition.setdefault(partition, set()).add(key)
