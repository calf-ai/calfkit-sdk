"""Unit tests for _KafkaStateStore — partition-scoped cache + durable writes.

The store's read/write API (get / put / tombstone / evict_partitions /
was_recently_completed) is mock-tested with a fake broker. Rehydration via
real AIOKafkaConsumer requires Kafka; that path lands in the testcontainers
integration milestone — except for the stalled-poll error branch, which
is unit-testable against a mocked consumer.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from calfkit.models.state import State
from calfkit.nodes.aggregator._in_memory_store import _InFlightBatch
from calfkit.nodes.aggregator._kafka_state_store import _KafkaStateStore
from calfkit.nodes.aggregator.errors import AggregatorStateStoreError


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


# ---------------------------------------------------------------------------
# Rehydration: stalled-poll error branch
# ---------------------------------------------------------------------------


async def test_rehydrate_raises_on_stalled_poll() -> None:
    """When the broker returns no records for MAX_EMPTY_POLLS consecutive
    polls while offsets remain outstanding, rehydration must raise rather
    than silently activate the partition with partial state.

    Stubs aiokafka's AIOKafkaConsumer so this test runs without a real
    broker — the failure mode under test is the framework's response to
    a broker stalled at startup, not the broker's I/O behaviour.
    """
    store, _ = _make_store()
    store._partition_count = 4

    mock_consumer = AsyncMock()
    mock_consumer.start = AsyncMock()
    mock_consumer.stop = AsyncMock()
    mock_consumer.assign = MagicMock()
    mock_consumer.seek_to_beginning = AsyncMock()

    from aiokafka import TopicPartition

    # End offsets show the partition has records, but every getmany call
    # returns an empty dict — simulating a stalled broker.
    mock_consumer.end_offsets = AsyncMock(return_value={TopicPartition("agent.fanout-state", 0): 5})
    mock_consumer.getmany = AsyncMock(return_value={})

    with patch(
        "calfkit.nodes.aggregator._kafka_state_store.AIOKafkaConsumer",
        return_value=mock_consumer,
    ):
        with pytest.raises(AggregatorStateStoreError, match="rehydration stalled"):
            await store.rehydrate_partitions({0})

    # The store must NOT have activated partition 0 — owned_partitions
    # is updated only AFTER successful drain.
    assert 0 not in store.owned_partitions
    # Consumer was cleaned up via the finally block.
    mock_consumer.stop.assert_awaited()


# ---------------------------------------------------------------------------
# _apply_record: poison-record handling
# ---------------------------------------------------------------------------


def test_apply_record_skips_null_key(caplog: pytest.LogCaptureFixture) -> None:
    """Null-key records on the state topic are unexpected (the aggregator
    always writes composite keys). Log loudly but don't raise — these are
    stray records from elsewhere, not corrupt aggregator state."""
    import logging

    store, _ = _make_store()

    with caplog.at_level(logging.ERROR):
        store._apply_record(partition=0, key_bytes=None, value_bytes=b"any")

    assert any("null key" in r.message for r in caplog.records)
    assert store._cache == {}


def test_apply_record_skips_malformed_key(caplog: pytest.LogCaptureFixture) -> None:
    """Malformed (non-composite) keys are unrecoverable for the
    aggregator (no key to associate state with). Log ERROR and skip."""
    import logging

    store, _ = _make_store()

    with caplog.at_level(logging.ERROR):
        # No "|" delimiter → parse_composite_key raises ValueError.
        store._apply_record(partition=0, key_bytes=b"no-delim", value_bytes=b"{}")

    assert any("malformed key" in r.message for r in caplog.records)
    assert store._cache == {}


def test_apply_record_raises_on_malformed_value() -> None:
    """Poison FanOutState records for a KNOWN key signal corrupt durable
    state. Raising aborts partition activation via the rebalance
    listener's error guard — far better than silently losing the batch
    (returns would arrive as 'orphans' and get dropped)."""
    store, _ = _make_store()

    with pytest.raises(AggregatorStateStoreError, match="failed to parse FanOutState"):
        store._apply_record(
            partition=0,
            key_bytes=b"c1|f1",
            value_bytes=b"not-valid-json",
        )


def test_apply_record_tombstone_drops_cache_entry() -> None:
    """A value=None record (Kafka tombstone) removes the key from the
    cache and marks it recently-completed."""
    store, _ = _make_store()
    # Seed an in-flight batch.
    batch = _make_batch()
    store._cache[("c1", "f1")] = batch
    store._by_partition[0] = {("c1", "f1")}

    store._apply_record(partition=0, key_bytes=b"c1|f1", value_bytes=None)

    assert ("c1", "f1") not in store._cache
    assert store.was_recently_completed(("c1", "f1"))


def test_apply_record_valid_state_populates_cache() -> None:
    """A well-formed FanOutState record rebuilds the in-memory batch."""
    store, _ = _make_store()

    from calfkit.nodes.aggregator.state import FanOutState

    state = FanOutState(
        correlation_id="c1",
        fan_out_id="f1",
        expected_tool_call_ids=frozenset({"t1", "t2"}),
        base_state=State(),
        received={"t1": "r1"},
        started_at_ms=1000,
        last_updated_ms=1500,
        agent_topic="agent.in",
    )

    store._apply_record(
        partition=0,
        key_bytes=b"c1|f1",
        value_bytes=state.model_dump_json().encode(),
    )

    batch = store.get(("c1", "f1"))
    assert batch is not None
    assert batch.received == {"t1": "r1"}
    assert batch.expected_tool_call_ids == frozenset({"t1", "t2"})


async def test_rehydrate_skips_empty_partitions() -> None:
    """Partitions with end_offset == 0 have nothing to read; they should
    be marked owned immediately without polling. This regression-checks
    that the bounded-retry logic doesn't accidentally raise for empty
    partitions."""
    store, _ = _make_store()
    store._partition_count = 4

    mock_consumer = AsyncMock()
    mock_consumer.start = AsyncMock()
    mock_consumer.stop = AsyncMock()
    mock_consumer.assign = MagicMock()
    mock_consumer.seek_to_beginning = AsyncMock()

    from aiokafka import TopicPartition

    mock_consumer.end_offsets = AsyncMock(return_value={TopicPartition("agent.fanout-state", 0): 0})
    mock_consumer.getmany = AsyncMock(return_value={})

    with patch(
        "calfkit.nodes.aggregator._kafka_state_store.AIOKafkaConsumer",
        return_value=mock_consumer,
    ):
        await store.rehydrate_partitions({0})

    assert 0 in store.owned_partitions
    mock_consumer.getmany.assert_not_awaited()


async def test_rehydrate_passes_client_kwargs() -> None:
    """The state store must forward client_kwargs (SASL/SSL) to the
    transient AIOKafkaConsumer so rehydration works in production
    clusters with broker auth."""
    broker = MagicMock()
    broker.publish = AsyncMock()
    store = _KafkaStateStore(
        broker=broker,
        state_topic="agent.fanout-state",
        bootstrap_servers="kafka:9092",
        partition_count=4,
        client_kwargs={"security_protocol": "SASL_SSL"},
    )

    mock_consumer = AsyncMock()
    mock_consumer.start = AsyncMock()
    mock_consumer.stop = AsyncMock()
    mock_consumer.assign = MagicMock()
    mock_consumer.seek_to_beginning = AsyncMock()
    from aiokafka import TopicPartition

    mock_consumer.end_offsets = AsyncMock(return_value={TopicPartition("agent.fanout-state", 0): 0})
    mock_consumer.getmany = AsyncMock(return_value={})

    with patch(
        "calfkit.nodes.aggregator._kafka_state_store.AIOKafkaConsumer",
        return_value=mock_consumer,
    ) as mock_cls:
        await store.rehydrate_partitions({0})

    construction_kwargs = mock_cls.call_args.kwargs
    assert construction_kwargs.get("security_protocol") == "SASL_SSL"
    assert construction_kwargs.get("bootstrap_servers") == "kafka:9092"


# ---------------------------------------------------------------------------
# Simulated restart: rehydration rebuilds cache from the durable log
# ---------------------------------------------------------------------------


async def test_simulate_restart_rebuilds_cache_from_log() -> None:
    """Restart recovery: put a few records, drop the store, construct a
    new store, replay records via rehydrate_partitions → assert the
    cache reflects what we wrote.

    Mocks AIOKafkaConsumer to replay whatever the previous broker.publish
    captured — no real broker required.
    """
    from aiokafka import TopicPartition

    # First "process": construct store, put two batches.
    broker_a = MagicMock()
    broker_a.publish = AsyncMock()
    store_a = _KafkaStateStore(
        broker=broker_a,
        state_topic="agent.fanout-state",
        bootstrap_servers="localhost:9092",
        partition_count=4,
    )
    batch_1 = _make_batch("c1", "f1")
    batch_2 = _make_batch("c2", "f2")
    batch_2.received["t1"] = "result1"

    await store_a.put(("c1", "f1"), batch_1, partition=0)
    await store_a.put(("c2", "f2"), batch_2, partition=0)

    # Capture what was published (the durable log).
    published = [c.args[0] for c in broker_a.publish.await_args_list]
    published_keys = [c.kwargs["key"] for c in broker_a.publish.await_args_list]

    # "Restart": new process, new store with no in-memory cache.
    broker_b = MagicMock()
    broker_b.publish = AsyncMock()
    store_b = _KafkaStateStore(
        broker=broker_b,
        state_topic="agent.fanout-state",
        bootstrap_servers="localhost:9092",
        partition_count=4,
    )

    # Stub the rehydration consumer to replay published records.
    mock_consumer = AsyncMock()
    mock_consumer.start = AsyncMock()
    mock_consumer.stop = AsyncMock()
    mock_consumer.assign = MagicMock()
    mock_consumer.seek_to_beginning = AsyncMock()
    tp = TopicPartition("agent.fanout-state", 0)
    mock_consumer.end_offsets = AsyncMock(return_value={tp: len(published)})

    class _Rec:
        def __init__(self, key: bytes, value: bytes, offset: int) -> None:
            self.key = key
            self.value = value
            self.offset = offset

    records = [_Rec(published_keys[i], published[i].model_dump_json().encode(), i) for i in range(len(published))]
    # getmany returns records on first call, empty on subsequent calls.
    mock_consumer.getmany = AsyncMock(side_effect=[{tp: records}, {}])

    with patch(
        "calfkit.nodes.aggregator._kafka_state_store.AIOKafkaConsumer",
        return_value=mock_consumer,
    ):
        await store_b.rehydrate_partitions({0})

    # Cache rebuilt with both batches.
    rehydrated_1 = store_b.get(("c1", "f1"))
    rehydrated_2 = store_b.get(("c2", "f2"))
    assert rehydrated_1 is not None
    assert rehydrated_2 is not None
    assert rehydrated_2.received == {"t1": "result1"}
    assert 0 in store_b.owned_partitions
