"""Dispatch semantics of ``KeyOrderedSubscriber`` (spec §6.2/§6.3, D1-D5).

Driven through the test seams (``_allocate_dispatch_state()`` + ``_spawn_lanes()``) with a
recorder in place of ``consume()`` — see ``conftest.py``. The recorder itself asserts the
per-key mutual-exclusion invariant at execution time, so every test here doubles as an
ordering check.
"""

from __future__ import annotations

import asyncio
import logging
import random
import zlib

import anyio
import pytest

from tests.unit.faststream_ext.conftest import (
    ConsumeRecorder,
    feed,
    keys_on_distinct_lanes,
    make_key_ordered_subscriber,
    make_record,
    start_dispatch,
    stop_lanes,
    wait_until,
)


async def test_same_key_messages_serialize_strictly_in_order(recorder: ConsumeRecorder) -> None:
    sub = make_key_ordered_subscriber(max_workers=4)
    start_dispatch(sub, recorder)

    key = b"hot-key"
    recorder.latency = 0.001
    records = [make_record(key) for _ in range(20)]
    for record in records:
        await sub.consume_one(record)
    await wait_until(lambda: recorder.done_count() == 20, recorder=recorder)

    recorder.assert_strict_per_key_order({key: [r.offset for r in records]})
    await stop_lanes(sub)


async def test_same_key_across_different_topics_serializes(recorder: ConsumeRecorder) -> None:
    """Calfkit's real traffic shape: one key arriving on entry + return topics."""
    sub = make_key_ordered_subscriber(max_workers=4, topics=("topic-a", "topic-b"))
    start_dispatch(sub, recorder)

    key = b"cross-topic-key"
    recorder.latency = 0.001
    records = [make_record(key, topic="topic-a" if i % 2 == 0 else "topic-b") for i in range(12)]
    for record in records:
        await sub.consume_one(record)
    await wait_until(lambda: recorder.done_count() == 12, recorder=recorder)

    recorder.assert_strict_per_key_order({key: [r.offset for r in records]})
    assert {topic for _, _, _, topic in recorder.events} == {"topic-a", "topic-b"}
    await stop_lanes(sub)


async def test_distinct_keys_execute_concurrently(recorder: ConsumeRecorder) -> None:
    sub = make_key_ordered_subscriber(max_workers=4)
    start_dispatch(sub, recorder)

    key_blocked, key_free = keys_on_distinct_lanes(2, 4)
    blocked_record = make_record(key_blocked)
    gate = recorder.gate(blocked_record)
    free_record = make_record(key_free)

    await sub.consume_one(blocked_record)
    await wait_until(lambda: recorder.active_count == 1, recorder=recorder)
    await sub.consume_one(free_record)
    # The free key's message starts AND finishes while the blocked key is still executing.
    await wait_until(lambda: recorder.done_count() == 1, recorder=recorder)
    assert recorder.active_keys == {key_blocked}
    assert recorder.peak_active == 2, "distinct keys did not overlap"

    gate.set()
    await wait_until(lambda: recorder.done_count() == 2, recorder=recorder)
    await stop_lanes(sub)


async def test_bound_admits_exactly_2x_max_workers_then_blocks(recorder: ConsumeRecorder) -> None:
    """D3/D5: the read path blocks at B = 2×max_workers in flight and resumes per completion."""
    sub = make_key_ordered_subscriber(max_workers=2)
    start_dispatch(sub, recorder)
    bound = sub._bound

    key = b"hot-key"
    records = [make_record(key) for _ in range(bound + 3)]
    gates = {r.offset: recorder.gate(r) for r in records}

    fed = 0

    async def feeder() -> None:
        nonlocal fed
        for record in records:
            await sub.consume_one(record)
            fed += 1

    feed_task = asyncio.create_task(feeder())
    # Polling variant: `fed` advances in the feeder, which emits no recorder events
    # (parked messages never reach consume), so the event-driven wait can't see it.
    await wait_until(lambda: fed == bound)
    await asyncio.sleep(0.02)  # give the feeder every chance to (wrongly) overshoot
    assert fed == bound, f"read path admitted {fed} > bound {bound}"
    assert not feed_task.done()

    gates[records[0].offset].set()  # one completion frees exactly one slot
    await wait_until(lambda: fed == bound + 1)
    await asyncio.sleep(0.02)
    assert fed == bound + 1

    for gate in gates.values():
        gate.set()
    await asyncio.wait_for(feed_task, timeout=2.0)
    await wait_until(lambda: recorder.done_count() == len(records), recorder=recorder)
    recorder.assert_strict_per_key_order({key: [r.offset for r in records]})
    await stop_lanes(sub)


async def test_executing_concurrency_capped_by_lane_count(recorder: ConsumeRecorder) -> None:
    """Structural cap: ≤ max_workers executing even with B admitted."""
    sub = make_key_ordered_subscriber(max_workers=3)
    start_dispatch(sub, recorder)
    bound = sub._bound

    keys = keys_on_distinct_lanes(3, 3)
    records = [make_record(keys[i % 3]) for i in range(bound)]
    gates = {r.offset: recorder.gate(r) for r in records}

    feed_task = asyncio.create_task(feed(sub, records))
    await wait_until(lambda: recorder.active_count == 3, recorder=recorder)
    await asyncio.sleep(0.02)
    assert recorder.active_count == 3
    assert recorder.peak_active == 3

    for gate in gates.values():
        gate.set()
    await asyncio.wait_for(feed_task, timeout=2.0)
    await wait_until(lambda: recorder.done_count() == len(records), recorder=recorder)
    assert recorder.peak_active == 3, "executing concurrency exceeded lane count"
    await stop_lanes(sub)


async def test_hot_key_flood_never_raises_wouldblock(recorder: ConsumeRecorder) -> None:
    """D3's zero-margin invariant under the worst case: one key owning every permit."""
    sub = make_key_ordered_subscriber(max_workers=4)
    start_dispatch(sub, recorder)

    key = b"hot-key"
    records = [make_record(key) for _ in range(10 * sub._bound)]
    recorder.latency = 0.0005
    try:
        await asyncio.wait_for(feed(sub, records), timeout=10.0)
    except anyio.WouldBlock:  # pragma: no cover - the failure this test exists to catch
        pytest.fail("send_nowait raised WouldBlock: buffer-≥-bound invariant broken")
    await wait_until(lambda: recorder.done_count() == len(records), timeout=10.0, recorder=recorder)
    recorder.assert_strict_per_key_order({key: [r.offset for r in records]})
    await stop_lanes(sub)


async def test_stress_many_keys_random_latency_preserves_per_key_order(recorder: ConsumeRecorder) -> None:
    """Stress: 12 keys × 300 messages, randomized latencies, full-bound backpressure.

    The recorder aborts on any same-key overlap; afterwards every key's event log must be
    exactly its submission order.
    """
    rng = random.Random(42)
    sub = make_key_ordered_subscriber(max_workers=4)

    class JitterRecorder(ConsumeRecorder):
        async def __call__(self, msg):
            self.latency = rng.choice([0.0, 0.0002, 0.001])
            await super().__call__(msg)

    jitter = JitterRecorder()
    start_dispatch(sub, jitter)

    keys = [f"key-{i}".encode() for i in range(12)]
    records = [make_record(rng.choice(keys), topic=rng.choice(["topic-a", "topic-b"])) for _ in range(300)]
    await asyncio.wait_for(feed(sub, records), timeout=20.0)
    await wait_until(lambda: jitter.done_count() == 300, timeout=20.0, recorder=jitter)

    expected: dict[bytes, list[int]] = {}
    for record in records:
        expected.setdefault(record.key, []).append(record.offset)
    jitter.assert_strict_per_key_order(expected)
    assert jitter.peak_active <= 4
    await stop_lanes(sub)


async def test_keyless_records_round_robin_and_warn_once() -> None:
    # _lane_for is pure dispatch math — no lane workers needed (spawning them across the
    # re-allocation below would orphan the first run's workers on never-closed streams).
    sub = make_key_ordered_subscriber(max_workers=4)
    warnings: list[tuple] = []
    sub._log = lambda *a, **k: warnings.append((a, k))
    sub._allocate_dispatch_state()

    lanes = [sub._lane_for(None) for _ in range(8)]
    assert lanes == [0, 1, 2, 3, 0, 1, 2, 3], "keyless dispatch must round-robin"
    assert len(warnings) == 1, "keyless WARNING must fire exactly once per run"
    args, _kwargs = warnings[0]
    assert args[0] == logging.WARNING, "keyless degradation must log at WARNING (D2: fail loud)"
    assert "keyless" in args[1]

    sub._allocate_dispatch_state()  # new run resets the warn-once flag (D11)
    sub._lane_for(None)
    assert len(warnings) == 2


async def test_keyed_routing_is_crc32_mod_lanes() -> None:
    sub = make_key_ordered_subscriber(max_workers=4)
    sub._allocate_dispatch_state()
    for key in (b"a", b"key-7", b"correlation-id-123", b"\x00\xff"):
        assert sub._lane_for(key) == zlib.crc32(key) % 4
    # No warning path for keyed records, and determinism across calls.
    assert sub._lane_for(b"a") == sub._lane_for(b"a")


async def test_max_workers_1_degenerates_to_serial(recorder: ConsumeRecorder) -> None:
    sub = make_key_ordered_subscriber(max_workers=1)
    start_dispatch(sub, recorder)

    records = [make_record(f"key-{i % 5}".encode()) for i in range(15)]
    recorder.latency = 0.0005
    await asyncio.wait_for(feed(sub, records), timeout=5.0)
    await wait_until(lambda: recorder.done_count() == 15, recorder=recorder)
    assert recorder.peak_active == 1
    # Total order (single lane): the full event stream is submission order.
    starts = [offset for phase, _, offset, _ in recorder.events if phase == "start"]
    assert starts == [r.offset for r in records]
    await stop_lanes(sub)


async def test_permit_released_when_consume_raises(recorder: ConsumeRecorder) -> None:
    """A BaseException escaping consume() (framework bug / cancellation) must not leak the
    permit — the lane's finally releases it (spec §6.2)."""
    sub = make_key_ordered_subscriber(max_workers=2)
    start_dispatch(sub, recorder)
    bound = sub._bound

    boom = make_record(b"doomed")
    recorder.raise_for.add(boom.offset)
    await sub.consume_one(boom)
    await wait_until(lambda: recorder.done_count() == 1, recorder=recorder)
    await wait_until(lambda: sub._limiter.value == bound)  # the permit came back

    # And the system stays functional: the supervisor restarts the crashed lane worker on
    # the SAME stream (D12), so a full bound admits and processes on the same key.
    records = [make_record(b"doomed") for _ in range(bound)]
    await asyncio.wait_for(feed(sub, records), timeout=2.0)
    await wait_until(lambda: recorder.done_count() == 1 + bound, recorder=recorder)
    await stop_lanes(sub)


async def test_startup_window_messages_park_until_lanes_spawn(recorder: ConsumeRecorder) -> None:
    """Spec §6.2: dispatches between allocation and lane spawn park in buffers (≤ B)."""
    sub = make_key_ordered_subscriber(max_workers=2)
    sub.consume = recorder
    sub._allocate_dispatch_state()
    bound = sub._bound

    records = [make_record(f"key-{i}".encode()) for i in range(bound)]
    await asyncio.wait_for(feed(sub, records), timeout=2.0)  # no lanes yet: must not block/raise
    assert recorder.done_count() == 0

    sub._spawn_lanes()
    await wait_until(lambda: recorder.done_count() == bound, recorder=recorder)
    await stop_lanes(sub)
