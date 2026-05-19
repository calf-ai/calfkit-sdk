"""Unit tests for _StateStoreRebalanceListener.

Uses a stub state store to verify the listener filters TopicPartitions by
``returns_topic`` and calls rehydrate / evict with the right partition IDs.
The real state store needs aiokafka + Kafka; rebalance integration tests
land in the testcontainers milestone.
"""

from __future__ import annotations

import pytest
from aiokafka import TopicPartition

from calfkit.nodes.aggregator._rebalance import _StateStoreRebalanceListener


class _StubStateStore:
    """Records calls for test assertions."""

    def __init__(self, rehydrate_raises: Exception | None = None) -> None:
        self.rehydrate_calls: list[set[int]] = []
        self.evict_calls: list[set[int]] = []
        self._rehydrate_raises = rehydrate_raises

    async def rehydrate_partitions(self, partition_ids: set[int]) -> None:
        self.rehydrate_calls.append(set(partition_ids))
        if self._rehydrate_raises is not None:
            raise self._rehydrate_raises

    def evict_partitions(self, partition_ids: set[int]) -> None:
        self.evict_calls.append(set(partition_ids))


@pytest.fixture
def store() -> _StubStateStore:
    return _StubStateStore()


@pytest.fixture
def listener(store: _StubStateStore) -> _StateStoreRebalanceListener:
    return _StateStoreRebalanceListener(state_store=store, returns_topic="agent.fanout-returns")


async def test_on_partitions_assigned_rehydrates_returns_partition_ids(
    listener: _StateStoreRebalanceListener,
    store: _StubStateStore,
) -> None:
    assigned = {
        TopicPartition("agent.fanout-returns", 0),
        TopicPartition("agent.fanout-returns", 3),
    }

    await listener.on_partitions_assigned(assigned)

    assert store.rehydrate_calls == [{0, 3}]
    assert store.evict_calls == []


async def test_on_partitions_assigned_filters_other_topics(
    listener: _StateStoreRebalanceListener,
    store: _StubStateStore,
) -> None:
    """TPs for topics other than the returns topic are ignored — they belong
    to some other subscriber, not the aggregator."""
    assigned = {
        TopicPartition("agent.fanout-returns", 0),
        TopicPartition("agent.in", 7),
        TopicPartition("some.other.topic", 2),
    }

    await listener.on_partitions_assigned(assigned)

    assert store.rehydrate_calls == [{0}]


async def test_on_partitions_revoked_evicts_returns_partition_ids(
    listener: _StateStoreRebalanceListener,
    store: _StubStateStore,
) -> None:
    revoked = {
        TopicPartition("agent.fanout-returns", 1),
        TopicPartition("agent.fanout-returns", 5),
    }

    await listener.on_partitions_revoked(revoked)

    assert store.evict_calls == [{1, 5}]
    assert store.rehydrate_calls == []


async def test_on_partitions_revoked_filters_other_topics(
    listener: _StateStoreRebalanceListener,
    store: _StubStateStore,
) -> None:
    revoked = {
        TopicPartition("agent.fanout-returns", 1),
        TopicPartition("agent.in", 9),
    }

    await listener.on_partitions_revoked(revoked)

    assert store.evict_calls == [{1}]


async def test_on_partitions_assigned_no_returns_partitions_is_noop(
    listener: _StateStoreRebalanceListener,
    store: _StubStateStore,
) -> None:
    """No state-store work when none of the assigned TPs are for the returns topic."""
    assigned = {
        TopicPartition("agent.in", 0),
        TopicPartition("agent.in", 1),
    }

    await listener.on_partitions_assigned(assigned)

    assert store.rehydrate_calls == []
    assert store.evict_calls == []


async def test_on_partitions_revoked_with_empty_set_is_noop(
    listener: _StateStoreRebalanceListener,
    store: _StubStateStore,
) -> None:
    await listener.on_partitions_revoked(set())

    assert store.evict_calls == []
    assert store.rehydrate_calls == []


async def test_assigned_evicts_partial_state_when_rehydrate_raises() -> None:
    """If rehydration partially populates the cache then raises, the
    listener must evict any touched partitions and re-raise so aiokafka
    surfaces the rebalance failure. Activating the partition with partial
    state would corrupt the durability invariant."""
    raising_store = _StubStateStore(rehydrate_raises=RuntimeError("broker stalled"))
    listener = _StateStoreRebalanceListener(state_store=raising_store, returns_topic="agent.fanout-returns")

    assigned = {
        TopicPartition("agent.fanout-returns", 0),
        TopicPartition("agent.fanout-returns", 1),
    }

    with pytest.raises(RuntimeError, match="broker stalled"):
        await listener.on_partitions_assigned(assigned)

    # rehydrate was attempted, then evict was called with the same set.
    assert raising_store.rehydrate_calls == [{0, 1}]
    assert raising_store.evict_calls == [{0, 1}]
