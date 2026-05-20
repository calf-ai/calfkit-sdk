"""Unit tests for _KafkaStateStore — partition-scoped cache + durable writes.

The store's read/write API (get / put / tombstone / evict_partitions /
was_recently_completed) is mock-tested with a fake broker. Rehydration via
real AIOKafkaConsumer requires Kafka; that path lands in the testcontainers
integration milestone — except for the stalled-poll error branch, which
is unit-testable against a mocked consumer.
"""

from __future__ import annotations

from typing import Any
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


async def test_rehydrate_partitions_stops_consumer_when_start_raises() -> None:
    """If ``AIOKafkaConsumer.start()`` raises (broker unreachable, auth
    failure, no metadata), the transient consumer must still be cleaned up
    via ``.stop()``. Without the finally guard around start(), the consumer
    object leaks background tasks and sockets on every failed rehydration
    attempt — on flaky brokers / repeated rebalance failures, this exhausts
    file descriptors or grows memory unboundedly."""
    store, _ = _make_store()
    store._partition_count = 4

    mock_consumer = AsyncMock()
    mock_consumer.start = AsyncMock(side_effect=RuntimeError("broker unreachable"))
    mock_consumer.stop = AsyncMock()
    mock_consumer.assign = MagicMock()
    mock_consumer.seek_to_beginning = AsyncMock()
    mock_consumer.end_offsets = AsyncMock(return_value={})
    mock_consumer.getmany = AsyncMock(return_value={})

    with patch(
        "calfkit.nodes.aggregator._kafka_state_store.AIOKafkaConsumer",
        return_value=mock_consumer,
    ):
        with pytest.raises(RuntimeError, match="broker unreachable"):
            await store.rehydrate_partitions({0})

    # The consumer object exists; even though start() failed, stop() must
    # have been invoked so its background tasks + client are released.
    mock_consumer.stop.assert_awaited()
    # Partition must NOT be activated since rehydration aborted.
    assert 0 not in store.owned_partitions


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
# Rehydration: end_offsets re-poll until stable
# ---------------------------------------------------------------------------


async def test_rehydrate_repolls_end_offsets_until_stable() -> None:
    """The previous partition owner may still be flushing in-flight
    writes when the new owner snapshots end_offsets. After draining to
    the snapshot, rehydration must re-poll end_offsets and continue
    reading if the offset advanced — otherwise the new owner activates
    with state that's already stale.

    Scenario: snapshot reports end=2, drain reads records 0-1, re-poll
    reports end=3 (one new record landed during the read), drain reads
    record 2, re-poll reports end=3 again → converged. All three records
    must end up in the cache."""
    from aiokafka import TopicPartition

    from calfkit.nodes.aggregator.state import FanOutState

    store, _ = _make_store()
    store._partition_count = 4

    tp = TopicPartition("agent.fanout-state", 0)

    def _state(fan_out: str) -> FanOutState:
        return FanOutState(
            correlation_id="c1",
            fan_out_id=fan_out,
            expected_tool_call_ids=frozenset({"t1"}),
            base_state=State(),
            received={},
            started_at_ms=0,
            last_updated_ms=0,
            agent_topic="agent.in",
        )

    class _Rec:
        def __init__(self, key: bytes, value: bytes, offset: int) -> None:
            self.key = key
            self.value = value
            self.offset = offset

    rec_0 = _Rec(b"c1|f0", _state("f0").model_dump_json().encode(), 0)
    rec_1 = _Rec(b"c1|f1", _state("f1").model_dump_json().encode(), 1)
    rec_2 = _Rec(b"c1|f2", _state("f2").model_dump_json().encode(), 2)

    mock_consumer = AsyncMock()
    mock_consumer.start = AsyncMock()
    mock_consumer.stop = AsyncMock()
    mock_consumer.assign = MagicMock()
    mock_consumer.seek_to_beginning = AsyncMock()
    # end_offsets sequence: initial snapshot returns 2, after first drain
    # returns 3 (active producer landed one more), after second drain
    # returns 3 again (converged).
    mock_consumer.end_offsets = AsyncMock(side_effect=[{tp: 2}, {tp: 3}, {tp: 3}])
    # getmany sequence: first call delivers records 0-1, second call (after
    # the re-poll detects the advance) delivers record 2.
    mock_consumer.getmany = AsyncMock(side_effect=[{tp: [rec_0, rec_1]}, {tp: [rec_2]}])

    with patch(
        "calfkit.nodes.aggregator._kafka_state_store.AIOKafkaConsumer",
        return_value=mock_consumer,
    ):
        await store.rehydrate_partitions({0})

    # All three records must be in the cache.
    assert store.get(("c1", "f0")) is not None
    assert store.get(("c1", "f1")) is not None
    assert store.get(("c1", "f2")) is not None
    assert 0 in store.owned_partitions
    # end_offsets called 3 times: initial + 2 re-polls.
    assert mock_consumer.end_offsets.await_count == 3


async def test_rehydrate_raises_after_max_endoffset_repolls() -> None:
    """If end_offsets keeps advancing on every re-poll, an active
    producer is still writing during the rebalance — the new owner
    cannot safely race against it. After ``_REHYDRATE_MAX_ENDOFFSET_REPOLLS``
    consecutive advances, rehydration must raise rather than loop
    forever or activate with state guaranteed to be stale."""
    from aiokafka import TopicPartition

    from calfkit.nodes.aggregator.state import FanOutState

    store, _ = _make_store()
    store._partition_count = 4

    tp = TopicPartition("agent.fanout-state", 0)

    def _record(offset: int) -> Any:
        state = FanOutState(
            correlation_id="c1",
            fan_out_id=f"f{offset}",
            expected_tool_call_ids=frozenset({"t1"}),
            base_state=State(),
            received={},
            started_at_ms=0,
            last_updated_ms=0,
            agent_topic="agent.in",
        )

        class _Rec:
            def __init__(self, key: bytes, value: bytes, offset: int) -> None:
                self.key = key
                self.value = value
                self.offset = offset

        return _Rec(f"c1|f{offset}".encode(), state.model_dump_json().encode(), offset)

    # End offsets always advance by one — the producer never lets up.
    # initial=1, after drain 1 -> 2, after drain 2 -> 3, after drain 3 -> 4.
    # That's 1 initial + _REHYDRATE_MAX_ENDOFFSET_REPOLLS (=3) re-polls
    # that each show an advance. The 3rd re-poll triggers the raise.
    end_offset_sequence = [{tp: 1}, {tp: 2}, {tp: 3}, {tp: 4}]
    mock_consumer = AsyncMock()
    mock_consumer.start = AsyncMock()
    mock_consumer.stop = AsyncMock()
    mock_consumer.assign = MagicMock()
    mock_consumer.seek_to_beginning = AsyncMock()
    mock_consumer.end_offsets = AsyncMock(side_effect=end_offset_sequence)
    # Each drain reads exactly the one record at the current tail.
    mock_consumer.getmany = AsyncMock(side_effect=[{tp: [_record(0)]}, {tp: [_record(1)]}, {tp: [_record(2)]}])

    with patch(
        "calfkit.nodes.aggregator._kafka_state_store.AIOKafkaConsumer",
        return_value=mock_consumer,
    ):
        with pytest.raises(AggregatorStateStoreError, match="end_offsets failed to stabilise"):
            await store.rehydrate_partitions({0})

    # Partition must not be activated when end_offsets never stabilises.
    assert 0 not in store.owned_partitions
    mock_consumer.stop.assert_awaited()


# ---------------------------------------------------------------------------
# _apply_record: poison-record handling
# ---------------------------------------------------------------------------


def test_apply_record_raises_on_null_key() -> None:
    """Null-key records on the state topic indicate something other than
    this aggregator has written to the topic — the aggregator's writes
    always use composite keys. The trust invariant is violated; refuse
    activation rather than silently skipping (and possibly missing valid
    records adjacent to the contamination)."""
    store, _ = _make_store()

    with pytest.raises(AggregatorStateStoreError, match="null key"):
        store._apply_record(partition=0, key_bytes=None, value_bytes=b"any", offset=42)


def test_apply_record_raises_on_malformed_key() -> None:
    """Malformed (non-composite) keys cannot be produced by this
    aggregator. Like null-key records, they indicate the state topic has
    been contaminated; refuse to activate the partition."""
    store, _ = _make_store()

    with pytest.raises(AggregatorStateStoreError, match="malformed key"):
        # No "|" delimiter → parse_composite_key raises ValueError.
        store._apply_record(partition=0, key_bytes=b"no-delim", value_bytes=b"{}", offset=99)


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


def test_apply_record_put_after_tombstone_clears_recently_completed() -> None:
    """Replaying [put_1, tombstone, put_2] for the same key must leave the
    cache holding put_2 AND clear the key from _recently_completed. Without
    the discard, a return for the redispatched batch would be dropped as
    'late' by was_recently_completed and the batch would hang forever."""
    from calfkit.nodes.aggregator.state import FanOutState

    store, _ = _make_store()
    key = ("c1", "f1")

    state_1 = FanOutState(
        correlation_id="c1",
        fan_out_id="f1",
        expected_tool_call_ids=frozenset({"t1", "t2"}),
        base_state=State(),
        received={},
        started_at_ms=1000,
        last_updated_ms=1000,
        agent_topic="agent.in",
    )
    state_2 = FanOutState(
        correlation_id="c1",
        fan_out_id="f1",
        expected_tool_call_ids=frozenset({"t3", "t4"}),
        base_state=State(),
        received={},
        started_at_ms=2000,
        last_updated_ms=2000,
        agent_topic="agent.in",
    )

    store._apply_record(partition=0, key_bytes=b"c1|f1", value_bytes=state_1.model_dump_json().encode())
    store._apply_record(partition=0, key_bytes=b"c1|f1", value_bytes=None)
    assert store.was_recently_completed(key)  # tombstone marker set

    store._apply_record(partition=0, key_bytes=b"c1|f1", value_bytes=state_2.model_dump_json().encode())

    rehydrated = store.get(key)
    assert rehydrated is not None
    assert rehydrated.expected_tool_call_ids == frozenset({"t3", "t4"})
    assert not store.was_recently_completed(key)


async def test_rehydration_with_interleaved_put_tombstone_put() -> None:
    """End-to-end rehydration: a partition whose durable log contains
    [put_1, tombstone, put_2] for the same key must rehydrate with put_2
    in the cache and the key absent from _recently_completed."""
    from aiokafka import TopicPartition

    from calfkit.nodes.aggregator.state import FanOutState

    store, _ = _make_store()
    store._partition_count = 4

    state_1 = FanOutState(
        correlation_id="c1",
        fan_out_id="f1",
        expected_tool_call_ids=frozenset({"t1"}),
        base_state=State(),
        received={},
        started_at_ms=1000,
        last_updated_ms=1000,
        agent_topic="agent.in",
    )
    state_2 = FanOutState(
        correlation_id="c1",
        fan_out_id="f1",
        expected_tool_call_ids=frozenset({"t9"}),
        base_state=State(),
        received={},
        started_at_ms=2000,
        last_updated_ms=2000,
        agent_topic="agent.in",
    )

    class _Rec:
        def __init__(self, key: bytes, value: bytes | None, offset: int) -> None:
            self.key = key
            self.value = value
            self.offset = offset

    tp = TopicPartition("agent.fanout-state", 0)
    records = [
        _Rec(b"c1|f1", state_1.model_dump_json().encode(), 0),
        _Rec(b"c1|f1", None, 1),
        _Rec(b"c1|f1", state_2.model_dump_json().encode(), 2),
    ]

    mock_consumer = AsyncMock()
    mock_consumer.start = AsyncMock()
    mock_consumer.stop = AsyncMock()
    mock_consumer.assign = MagicMock()
    mock_consumer.seek_to_beginning = AsyncMock()
    mock_consumer.end_offsets = AsyncMock(return_value={tp: len(records)})
    mock_consumer.getmany = AsyncMock(side_effect=[{tp: records}, {}])

    with patch(
        "calfkit.nodes.aggregator._kafka_state_store.AIOKafkaConsumer",
        return_value=mock_consumer,
    ):
        await store.rehydrate_partitions({0})

    key = ("c1", "f1")
    rehydrated = store.get(key)
    assert rehydrated is not None
    assert rehydrated.expected_tool_call_ids == frozenset({"t9"})
    assert not store.was_recently_completed(key)


async def test_late_return_after_redispatched_batch_is_not_dropped() -> None:
    """Higher-level: after rehydration of [put, tombstone, put] for the
    same key, a return whose partition is freshly active must NOT be
    classified as 'recently completed'. This is the scenario that hangs
    the agent in the production bug — the durable log races a redelivery
    after the 60s TTL, replay re-puts the key, but the in-memory completion
    marker shadows the live batch.
    """
    from aiokafka import TopicPartition

    from calfkit.nodes.aggregator.state import FanOutState

    store, _ = _make_store()
    store._partition_count = 4

    state_old = FanOutState(
        correlation_id="c1",
        fan_out_id="f1",
        expected_tool_call_ids=frozenset({"t1"}),
        base_state=State(),
        received={},
        started_at_ms=1000,
        last_updated_ms=1000,
        agent_topic="agent.in",
    )
    state_new = FanOutState(
        correlation_id="c1",
        fan_out_id="f1",
        expected_tool_call_ids=frozenset({"t2"}),
        base_state=State(),
        received={},
        started_at_ms=2000,
        last_updated_ms=2000,
        agent_topic="agent.in",
    )

    class _Rec:
        def __init__(self, key: bytes, value: bytes | None, offset: int) -> None:
            self.key = key
            self.value = value
            self.offset = offset

    tp = TopicPartition("agent.fanout-state", 0)
    records = [
        _Rec(b"c1|f1", state_old.model_dump_json().encode(), 0),
        _Rec(b"c1|f1", None, 1),
        _Rec(b"c1|f1", state_new.model_dump_json().encode(), 2),
    ]

    mock_consumer = AsyncMock()
    mock_consumer.start = AsyncMock()
    mock_consumer.stop = AsyncMock()
    mock_consumer.assign = MagicMock()
    mock_consumer.seek_to_beginning = AsyncMock()
    mock_consumer.end_offsets = AsyncMock(return_value={tp: len(records)})
    mock_consumer.getmany = AsyncMock(side_effect=[{tp: records}, {}])

    with patch(
        "calfkit.nodes.aggregator._kafka_state_store.AIOKafkaConsumer",
        return_value=mock_consumer,
    ):
        await store.rehydrate_partitions({0})

    # Simulate the aggregator handler's gate: a return for the redispatched
    # batch must NOT be classified as 'recently completed', otherwise it is
    # silently dropped on agent.py:486 and the agent hangs.
    key = ("c1", "f1")
    assert not store.was_recently_completed(key), (
        "key was tombstoned then re-put during rehydration; the live batch must be reachable — recently_completed must NOT shadow it"
    )
    assert store.get(key) is not None


async def test_rehydrate_raises_after_exactly_max_empty_polls() -> None:
    """Pin the retry budget value (``_REHYDRATE_MAX_EMPTY_POLLS``): the
    loop must tolerate up to N-1 consecutive empty polls and only raise
    on the Nth. A regression that shortened the budget to 1 (or removed
    it altogether) would break this — the original ``test_rehydrate_raises_on_stalled_poll``
    only checks that SOME number of empties raises, not which."""
    store, _ = _make_store()
    store._partition_count = 4

    mock_consumer = AsyncMock()
    mock_consumer.start = AsyncMock()
    mock_consumer.stop = AsyncMock()
    mock_consumer.assign = MagicMock()
    mock_consumer.seek_to_beginning = AsyncMock()

    from aiokafka import TopicPartition

    mock_consumer.end_offsets = AsyncMock(return_value={TopicPartition("agent.fanout-state", 0): 5})
    mock_consumer.getmany = AsyncMock(return_value={})

    with patch(
        "calfkit.nodes.aggregator._kafka_state_store.AIOKafkaConsumer",
        return_value=mock_consumer,
    ):
        with pytest.raises(AggregatorStateStoreError, match="rehydration stalled"):
            await store.rehydrate_partitions({0})

    assert mock_consumer.getmany.await_count == _KafkaStateStore._REHYDRATE_MAX_EMPTY_POLLS


async def test_rehydrate_empty_poll_counter_resets_on_progress() -> None:
    """A burst of empty polls followed by a successful poll must NOT
    accumulate toward the budget — the counter resets when records
    arrive. Without this reset, a long rehydration with occasional
    empty polls would falsely raise."""
    store, _ = _make_store()
    store._partition_count = 4

    mock_consumer = AsyncMock()
    mock_consumer.start = AsyncMock()
    mock_consumer.stop = AsyncMock()
    mock_consumer.assign = MagicMock()
    mock_consumer.seek_to_beginning = AsyncMock()

    from aiokafka import TopicPartition

    from calfkit.nodes.aggregator.state import FanOutState

    tp = TopicPartition("agent.fanout-state", 0)
    mock_consumer.end_offsets = AsyncMock(return_value={tp: 1})

    # Build a single valid record.
    state = FanOutState(
        correlation_id="c1",
        fan_out_id="f1",
        expected_tool_call_ids=frozenset({"t1"}),
        base_state=State(),
        started_at_ms=0,
        last_updated_ms=0,
        agent_topic="agent.in",
    )

    class _Rec:
        def __init__(self, key: bytes, value: bytes, offset: int) -> None:
            self.key = key
            self.value = value
            self.offset = offset

    record = _Rec(b"c1|f1", state.model_dump_json().encode(), 0)

    # Sequence: (N-1) empty polls, then one successful poll that completes
    # the partition. Counter should reset on the success and not raise.
    empty_polls = _KafkaStateStore._REHYDRATE_MAX_EMPTY_POLLS - 1
    poll_sequence: list[dict[TopicPartition, list[_Rec]]] = [{} for _ in range(empty_polls)]
    poll_sequence.append({tp: [record]})
    mock_consumer.getmany = AsyncMock(side_effect=poll_sequence)

    with patch(
        "calfkit.nodes.aggregator._kafka_state_store.AIOKafkaConsumer",
        return_value=mock_consumer,
    ):
        # Must NOT raise — the success resets the counter.
        await store.rehydrate_partitions({0})

    assert 0 in store.owned_partitions
    assert store.get(("c1", "f1")) is not None


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
    batch_2 = _make_batch("c2", "f2").with_received(
        {"t1": "result1"},
        last_updated_ms=1000,
    )

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
