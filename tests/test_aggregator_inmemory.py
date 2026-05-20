"""Pure-Python tests for InMemoryAggregator behavior — no Kafka required.

Exercises FanOutAggregator's three behavior overrides (merge,
should_complete, on_partial), all MergeErrorPolicy modes, persist-to-disk
semantics, the clock injection helper, and the async wait helpers.
"""

import asyncio
from pathlib import Path

import pytest

from calfkit.models.state import State
from calfkit.nodes.aggregator import (
    AggregatedReturn,
    AggregatorBatch,
    AggregatorMergeError,
    FanOutAggregator,
    MergeErrorPolicy,
)
from calfkit.nodes.aggregator.testing import InMemoryAggregator

# Default expected set for most tests
DEFAULT_EXPECTED = frozenset({"t1", "t2", "t3"})


async def _init_batch(
    agg: InMemoryAggregator,
    corr: str = "c1",
    fan_out: str = "f1",
    expected: frozenset[str] = DEFAULT_EXPECTED,
) -> None:
    await agg.initialize_batch(
        correlation_id=corr,
        fan_out_id=fan_out,
        expected_tool_call_ids=expected,
        base_state=State(),
    )


# ---------------------------------------------------------------------------
# Default merge + completion
# ---------------------------------------------------------------------------


async def test_default_merge_applies_all_results() -> None:
    agg = InMemoryAggregator(persist_to_disk=False)
    await _init_batch(agg)

    await agg.submit_return("c1", "f1", "t1", "result1")
    await agg.submit_return("c1", "f1", "t2", "result2")
    merged = await agg.submit_return("c1", "f1", "t3", "result3")

    assert merged is not None
    assert merged.state.tool_results == {
        "t1": "result1",
        "t2": "result2",
        "t3": "result3",
    }


async def test_default_should_complete_waits_for_all() -> None:
    agg = InMemoryAggregator(persist_to_disk=False)
    await _init_batch(agg)

    first = await agg.submit_return("c1", "f1", "t1", "r1")
    second = await agg.submit_return("c1", "f1", "t2", "r2")

    assert first is None
    assert second is None
    assert not agg._store.was_recently_completed(("c1", "f1"))


# ---------------------------------------------------------------------------
# Custom should_complete
# ---------------------------------------------------------------------------


class FirstSuccessAggregator(FanOutAggregator):
    """Completes as soon as the first tool returns successfully."""

    async def should_complete(self, batch: AggregatorBatch) -> bool:
        return batch.num_received >= 1


async def test_custom_should_complete_short_circuits() -> None:
    base = FirstSuccessAggregator()
    agg = InMemoryAggregator.wrap(base, persist_to_disk=False)
    await _init_batch(agg)

    merged = await agg.submit_return("c1", "f1", "t1", "first")

    assert merged is not None
    assert "t1" in merged.state.tool_results
    # Subsequent returns hit the recently-completed set
    later = await agg.submit_return("c1", "f1", "t2", "second")
    assert later is None


# ---------------------------------------------------------------------------
# Custom merge
# ---------------------------------------------------------------------------


class SummarizingAggregator(FanOutAggregator):
    """Joins all results into a single pipe-delimited summary."""

    async def merge(self, batch: AggregatorBatch) -> AggregatedReturn:
        state = batch.base_state.model_copy(deep=True)
        # Order results by tool_call_id for determinism.
        ordered = sorted(batch.received.items())
        summary = " | ".join(str(v) for _, v in ordered)
        state.add_tool_result("summary", summary)
        return AggregatedReturn(state=state)


async def test_custom_merge_synthesizes() -> None:
    base = SummarizingAggregator()
    agg = InMemoryAggregator.wrap(base, persist_to_disk=False)
    await _init_batch(agg)

    await agg.submit_return("c1", "f1", "t1", "alpha")
    await agg.submit_return("c1", "f1", "t2", "beta")
    merged = await agg.submit_return("c1", "f1", "t3", "gamma")

    assert merged is not None
    assert merged.state.tool_results.get("summary") == "alpha | beta | gamma"


# ---------------------------------------------------------------------------
# Merge error policies
# ---------------------------------------------------------------------------


class FailingMergeAggregator(FanOutAggregator):
    """merge() always raises — used to exercise all three error policies."""

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self.merge_call_count = 0

    async def merge(self, batch: AggregatorBatch) -> AggregatedReturn:
        self.merge_call_count += 1
        raise RuntimeError("merge always fails")


async def test_merge_error_abort_raises() -> None:
    base = FailingMergeAggregator(merge_error_policy=MergeErrorPolicy.ABORT)
    agg = InMemoryAggregator.wrap(
        base,
        persist_to_disk=False,
        merge_error_policy=MergeErrorPolicy.ABORT,
    )
    await _init_batch(agg, expected=frozenset({"t1"}))

    with pytest.raises(AggregatorMergeError):
        await agg.submit_return("c1", "f1", "t1", "x")


class FlakeOnceAggregator(FanOutAggregator):
    """First merge() call raises; second succeeds — tests RETRY policy."""

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self.attempts = 0

    async def merge(self, batch: AggregatorBatch) -> AggregatedReturn:
        self.attempts += 1
        if self.attempts == 1:
            raise RuntimeError("first attempt fails")
        state = batch.base_state.model_copy(deep=True)
        for tcid, result in batch.received.items():
            state.add_tool_result(tcid, result)
        return AggregatedReturn(state=state)


async def test_merge_error_retry_succeeds() -> None:
    base = FlakeOnceAggregator(merge_error_policy=MergeErrorPolicy.RETRY)
    agg = InMemoryAggregator.wrap(
        base,
        persist_to_disk=False,
        merge_error_policy=MergeErrorPolicy.RETRY,
    )
    await _init_batch(agg, expected=frozenset({"t1"}))

    result = await agg.submit_return("c1", "f1", "t1", "x")

    assert result is not None
    assert result.state.tool_results == {"t1": "x"}


async def test_merge_error_drop_logs_and_completes() -> None:
    base = FailingMergeAggregator(merge_error_policy=MergeErrorPolicy.DROP)
    agg = InMemoryAggregator.wrap(
        base,
        persist_to_disk=False,
        merge_error_policy=MergeErrorPolicy.DROP,
    )
    await _init_batch(agg, expected=frozenset({"t1"}))

    result = await agg.submit_return("c1", "f1", "t1", "value")

    # DROP falls back to the default merge; the tool result still lands on state.
    assert result is not None
    assert result.state.tool_results == {"t1": "value"}


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


async def test_persist_to_disk_round_trip(tmp_path: Path) -> None:
    disk_path = tmp_path / "aggregator.jsonl"
    agg = InMemoryAggregator(persist_to_disk=True, disk_path=disk_path)
    await _init_batch(agg)
    await agg.submit_return("c1", "f1", "t1", "r1")

    # New instance reads from disk on construction.
    agg2 = InMemoryAggregator(persist_to_disk=True, disk_path=disk_path)
    batch = agg2._store.get(("c1", "f1"))

    assert batch is not None
    assert batch.received == {"t1": "r1"}


def test_persist_to_disk_default_true() -> None:
    agg = InMemoryAggregator()
    assert agg.persist_to_disk is True


async def test_simulate_restart_with_persistence(tmp_path: Path) -> None:
    disk_path = tmp_path / "aggregator.jsonl"
    agg = InMemoryAggregator(persist_to_disk=True, disk_path=disk_path)
    await _init_batch(agg)
    await agg.submit_return("c1", "f1", "t1", "r1")

    agg.simulate_restart()

    batch = agg._store.get(("c1", "f1"))
    assert batch is not None
    assert batch.received == {"t1": "r1"}


async def test_simulate_restart_without_persistence_wipes() -> None:
    agg = InMemoryAggregator(persist_to_disk=False)
    await _init_batch(agg)
    await agg.submit_return("c1", "f1", "t1", "r1")

    agg.simulate_restart()

    assert agg._store.get(("c1", "f1")) is None


# ---------------------------------------------------------------------------
# Clock injection
# ---------------------------------------------------------------------------


async def test_set_clock_for_deterministic_timeout() -> None:
    """A custom clock makes the recently-completed TTL set deterministic."""
    fake_time = [0.0]

    def clock() -> float:
        return fake_time[0]

    agg = InMemoryAggregator(persist_to_disk=False, completion_ttl_seconds=60.0)
    agg.set_clock(clock)

    agg._store.tombstone(("c1", "f1"))
    assert agg._store.was_recently_completed(("c1", "f1"))

    fake_time[0] = 61.0
    assert not agg._store.was_recently_completed(("c1", "f1"))


# ---------------------------------------------------------------------------
# Async waiters
# ---------------------------------------------------------------------------


async def test_wait_for_completion_returns_batch() -> None:
    agg = InMemoryAggregator(persist_to_disk=False)
    await _init_batch(agg, expected=frozenset({"t1"}))

    async def submit_after_delay() -> None:
        await asyncio.sleep(0.01)
        await agg.submit_return("c1", "f1", "t1", "r")

    submit_task = asyncio.create_task(submit_after_delay())
    await agg.wait_for_completion(("c1", "f1"), timeout=1.0)
    await submit_task

    assert agg._store.was_recently_completed(("c1", "f1"))


async def test_wait_for_partial_state_returns_after_n_returns() -> None:
    agg = InMemoryAggregator(persist_to_disk=False)
    await _init_batch(agg)

    async def submit_after_delay() -> None:
        await asyncio.sleep(0.01)
        await agg.submit_return("c1", "f1", "t1", "r1")
        await asyncio.sleep(0.01)
        await agg.submit_return("c1", "f1", "t2", "r2")

    submit_task = asyncio.create_task(submit_after_delay())
    await agg.wait_for_partial_state(("c1", "f1"), 2, timeout=1.0)
    await submit_task

    batch = agg._store.get(("c1", "f1"))
    assert batch is not None
    assert len(batch.received) == 2


async def test_simulate_restart_cancels_pending_completion_waiter() -> None:
    """If simulate_restart fires while a test is awaiting completion,
    the waiter must wake up with RestartSimulatedError rather than
    hang forever on the wiped event reference."""
    import pytest

    from calfkit.nodes.aggregator.errors import RestartSimulatedError

    agg = InMemoryAggregator(persist_to_disk=False)
    await _init_batch(agg, expected=frozenset({"t1"}))

    async def restart_after_delay() -> None:
        await asyncio.sleep(0.01)
        agg.simulate_restart()

    restart_task = asyncio.create_task(restart_after_delay())
    with pytest.raises(RestartSimulatedError):
        await agg.wait_for_completion(("c1", "f1"), timeout=1.0)
    await restart_task


async def test_simulate_restart_cancels_pending_partial_state_waiter() -> None:
    """Same guarantee for wait_for_partial_state — restart must surface
    as a typed exception, not a hang."""
    import pytest

    from calfkit.nodes.aggregator.errors import RestartSimulatedError

    agg = InMemoryAggregator(persist_to_disk=False)
    await _init_batch(agg)

    async def restart_after_delay() -> None:
        await asyncio.sleep(0.01)
        agg.simulate_restart()

    restart_task = asyncio.create_task(restart_after_delay())
    with pytest.raises(RestartSimulatedError):
        await agg.wait_for_partial_state(("c1", "f1"), 2, timeout=1.0)
    await restart_task


def test_ttl_set_sweeps_periodically_on_add_without_reads() -> None:
    """A workload that only adds (e.g. a worker that tombstones many
    batches but rarely reads ``was_recently_completed``) must still
    bound _entries growth. The periodic sweep on add() — every
    _SWEEP_EVERY_N_ADDS calls — is the guarantee."""
    from calfkit.nodes.aggregator._in_memory_store import _TtlSet

    fake_time = [0.0]

    def clock() -> float:
        return fake_time[0]

    ttl_set = _TtlSet(ttl_seconds=10.0, clock=clock)

    # Fill nearly to the sweep threshold; no reads in between.
    for i in range(_TtlSet._SWEEP_EVERY_N_ADDS - 1):
        ttl_set.add(f"key-{i}")
    assert len(ttl_set._entries) == _TtlSet._SWEEP_EVERY_N_ADDS - 1

    # Advance well past the TTL so the earlier entries are all stale.
    fake_time[0] = 100.0

    # The Nth add() triggers the periodic sweep; expired entries drop,
    # leaving only the freshly-added one.
    ttl_set.add("key-fresh")
    assert len(ttl_set._entries) == 1
    assert "key-fresh" in ttl_set._entries


def test_ttl_set_contains_is_o1_and_evicts_expired_key() -> None:
    """``__contains__`` does an O(1) per-key expiry check rather than
    sweeping the full set on every call. An expired key still
    correctly returns False, and the entry is removed on access so
    later inspections see consistent state."""
    from calfkit.nodes.aggregator._in_memory_store import _TtlSet

    fake_time = [0.0]
    ttl_set = _TtlSet(ttl_seconds=10.0, clock=lambda: fake_time[0])
    ttl_set.add("k1")
    ttl_set.add("k2")
    assert "k1" in ttl_set  # not yet expired

    fake_time[0] = 11.0
    assert "k1" not in ttl_set  # expired → False, and evicted
    assert "k1" not in ttl_set._entries  # removed on the previous check
    # Other entries are NOT touched (no bulk sweep on the read path).
    assert "k2" in ttl_set._entries
