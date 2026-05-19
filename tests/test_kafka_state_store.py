"""Unit tests for _KafkaStateStore — partition-scoped cache + durable writes.

The store's read/write API (get / put / tombstone / evict_partitions /
was_recently_completed) is mock-tested with a fake broker. Rehydration via
real AIOKafkaConsumer requires Kafka; that path lands in the testcontainers
integration milestone.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from calfkit.models.state import State
from calfkit.nodes.aggregator._in_memory_store import _InFlightBatch
from calfkit.nodes.aggregator._kafka_state_store import _KafkaStateStore


def _make_store(clock=None) -> tuple[_KafkaStateStore, MagicMock]:
    broker = MagicMock()
    broker.publish = AsyncMock()
    store = _KafkaStateStore(
        broker=broker,
        state_topic="agent.fanout-state",
        bootstrap_servers="localhost:9092",
        clock=clock,
    )
    return store, broker


def _make_batch(corr: str = "c1", fan_out: str = "f1") -> _InFlightBatch:
    return _InFlightBatch(
        correlation_id=corr,
        fan_out_id=fan_out,
        expected_tool_call_ids=frozenset({"t1", "t2"}),
        base_state=State(),
        received={},
        started_at_ms=1000,
        last_updated_ms=1000,
        agent_topic="agent.in",
    )


async def test_put_publishes_and_caches() -> None:
    store, broker = _make_store()
    batch = _make_batch()

    await store.put(("c1", "f1"), batch, partition=2)

    # Cache updated
    assert store.get(("c1", "f1")) is batch
    # Published to state topic with composite key and explicit partition
    broker.publish.assert_awaited_once()
    call = broker.publish.call_args
    assert call.kwargs["topic"] == "agent.fanout-state"
    assert call.kwargs["key"] == b"c1|f1"
    assert call.kwargs["partition"] == 2
    # First positional arg is the FanOutState
    published_state = call.args[0]
    assert published_state.correlation_id == "c1"
    assert published_state.fan_out_id == "f1"


async def test_put_forwards_caller_supplied_partition() -> None:
    """The state store MUST pass through the caller's partition kwarg
    to broker.publish so the durable write lands on the partition the
    worker owns (co-partitioning invariant)."""
    store, broker = _make_store()
    batch = _make_batch()

    await store.put(("c1", "f1"), batch, partition=5)

    broker.publish.assert_awaited_once()
    assert broker.publish.call_args.kwargs["partition"] == 5


async def test_put_tracks_partition_index() -> None:
    """The _by_partition index lets evict_partitions drop keys cheaply."""
    store, _ = _make_store()
    await store.put(("c1", "f1"), _make_batch(), partition=0)
    await store.put(("c2", "f2"), _make_batch("c2", "f2"), partition=3)

    store.evict_partitions({0})

    assert store.get(("c1", "f1")) is None
    assert store.get(("c2", "f2")) is not None  # different partition; unaffected


async def test_tombstone_publishes_null_and_remembers_completion() -> None:
    store, broker = _make_store()
    await store.put(("c1", "f1"), _make_batch(), partition=3)
    broker.publish.reset_mock()

    await store.tombstone(("c1", "f1"), partition=3)

    # Cache dropped, recently-completed set
    assert store.get(("c1", "f1")) is None
    assert store.was_recently_completed(("c1", "f1"))
    # Tombstone publish: value=None, with explicit partition
    broker.publish.assert_awaited_once()
    call = broker.publish.call_args
    assert call.args[0] is None
    assert call.kwargs["topic"] == "agent.fanout-state"
    assert call.kwargs["key"] == b"c1|f1"
    assert call.kwargs["partition"] == 3


async def test_was_recently_completed_expires_after_ttl() -> None:
    fake_time = [0.0]

    def clock() -> float:
        return fake_time[0]

    store, _ = _make_store(clock=clock)
    await store.put(("c1", "f1"), _make_batch(), partition=0)
    await store.tombstone(("c1", "f1"), partition=0)

    assert store.was_recently_completed(("c1", "f1"))

    fake_time[0] = 61.0  # default TTL is 60s
    assert not store.was_recently_completed(("c1", "f1"))


def test_owned_partitions_starts_empty() -> None:
    store, _ = _make_store()
    assert store.owned_partitions == set()


def test_evict_partitions_updates_owned_set() -> None:
    store, _ = _make_store()
    # Simulate having been assigned partitions 0 and 1.
    store._owned_partitions = {0, 1}

    store.evict_partitions({0})

    assert store.owned_partitions == {1}


async def test_evict_partitions_handles_unknown_partition_safely() -> None:
    """Evicting a partition that was never assigned is a no-op."""
    store, _ = _make_store()

    store.evict_partitions({99})

    # No exception; no state change.
    assert store.owned_partitions == set()


async def test_evict_drops_only_keys_in_evicted_partitions() -> None:
    store, _ = _make_store()
    await store.put(("c1", "f1"), _make_batch(), partition=0)
    await store.put(("c2", "f2"), _make_batch("c2", "f2"), partition=0)
    await store.put(("c3", "f3"), _make_batch("c3", "f3"), partition=1)

    store.evict_partitions({0})

    assert store.get(("c1", "f1")) is None
    assert store.get(("c2", "f2")) is None
    assert store.get(("c3", "f3")) is not None


async def test_mark_completed_adds_to_recently_completed_set() -> None:
    store, _ = _make_store()

    store.mark_completed(("c1", "f1"))

    assert store.was_recently_completed(("c1", "f1"))


def test_get_returns_none_for_unknown_key() -> None:
    store, _ = _make_store()
    assert store.get(("nope", "nope")) is None
