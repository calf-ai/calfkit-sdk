"""Lifecycle semantics of ``KeyOrderedSubscriber`` (spec §6.4, D7/D11/D12/D14).

Same harness as ``test_dispatch.py``. ``stop()`` here is the real production path; only
``consume()`` is a recorder and the read loop, when needed, runs against a scripted fake
consumer (the real one needs a broker).
"""

from __future__ import annotations

import asyncio
import logging
import time

import pytest
from aiokafka.errors import ConsumerStoppedError

from tests.unit.faststream_ext.conftest import (
    ConsumeRecorder,
    keys_on_distinct_lanes,
    make_key_ordered_subscriber,
    make_record,
    wait_until,
)


def _started(sub, recorder: ConsumeRecorder) -> None:
    sub.consume = recorder
    sub._allocate_dispatch_state()
    sub._spawn_lanes()


async def _feed(sub, records) -> None:
    for record in records:
        await sub.consume_one(record)


async def test_graceful_stop_drains_every_accepted_message(recorder: ConsumeRecorder) -> None:
    """D7: stop() returns only after every parked AND executing message is processed."""
    sub = make_key_ordered_subscriber(max_workers=2)
    _started(sub, recorder)
    bound = 4

    keys = keys_on_distinct_lanes(2, 2)
    records = [make_record(keys[i % 2]) for i in range(bound)]
    gates = {r.offset: recorder.gate(r) for r in records}
    await asyncio.wait_for(_feed(sub, records), timeout=2.0)

    stop_task = asyncio.create_task(sub.stop())
    await asyncio.sleep(0.05)
    assert not stop_task.done(), "stop() must wait for the drain"
    assert recorder.done_count() == 0

    for gate in gates.values():
        gate.set()
    await asyncio.wait_for(stop_task, timeout=5.0)
    assert recorder.done_count() == bound, "graceful stop dropped accepted messages"


async def test_escalation_is_bounded_by_one_graceful_timeout_plus_grace(recorder: ConsumeRecorder, monkeypatch: pytest.MonkeyPatch) -> None:
    from calfkit._faststream_ext import _subscriber as mod

    monkeypatch.setattr(mod, "_FINALIZATION_GRACE", 0.2)
    sub = make_key_ordered_subscriber(max_workers=2, graceful_timeout=0.3)
    _started(sub, recorder)

    stuck = make_record(b"stuck")
    recorder.gate(stuck)  # never released
    await sub.consume_one(stuck)
    await wait_until(lambda: recorder.active_count == 1, recorder=recorder)

    started = time.monotonic()
    await asyncio.wait_for(sub.stop(), timeout=3.0)
    elapsed = time.monotonic() - started
    assert elapsed >= 0.28, "escalation must first wait out the drain window"
    assert elapsed < 1.2, f"stop took {elapsed:.2f}s — double-timeout regression"
    # The stuck handler was cancelled (recorder's finally recorded the end).
    await wait_until(lambda: recorder.done_count() == 1, recorder=recorder)


async def test_falsy_graceful_timeout_skips_drain_and_drops_parked(recorder: ConsumeRecorder) -> None:
    """MultiLock-parity falsy semantics: no drain wait; parked messages are dropped."""
    sub = make_key_ordered_subscriber(max_workers=2, graceful_timeout=None)
    _started(sub, recorder)

    records = [make_record(b"hot") for _ in range(4)]
    gates = {r.offset: recorder.gate(r) for r in records}
    await asyncio.wait_for(_feed(sub, records), timeout=2.0)

    started = time.monotonic()
    await asyncio.wait_for(sub.stop(), timeout=3.0)
    assert time.monotonic() - started < 1.0
    assert recorder.done_count() < len(records), "falsy timeout must not drain"
    for gate in gates.values():
        gate.set()


class _LaneStopRecorder(ConsumeRecorder):
    """Calls subscriber.stop() from INSIDE a lane (the StopConsume/SystemExit shape).

    ``armed`` keeps the stop from firing before the test finishes feeding: a mid-feed
    stop would (correctly) kill the feeder via the post-stop dispatch guard, which is the
    racing-intake test's subject, not these tests'.
    """

    def __init__(self, stop_on_offsets: set[int]) -> None:
        super().__init__()
        self.sub = None
        self.stop_on = stop_on_offsets
        self.stop_returned: list[int] = []
        self.armed = asyncio.Event()

    async def __call__(self, msg) -> None:
        if msg.offset in self.stop_on:
            await self.armed.wait()
            await self.sub.stop()  # must return promptly, WITHOUT joining the drain
            self.stop_returned.append(msg.offset)
        await super().__call__(msg)


async def test_lane_initiated_stop_is_single_flight_and_drains_other_lanes() -> None:
    """D14: stop() from a lane detaches; exactly one _do_stop ever runs; parked messages on
    other lanes still drain."""
    sub = make_key_ordered_subscriber(max_workers=2)
    keys = keys_on_distinct_lanes(2, 2)
    trigger_a = make_record(keys[0])
    trigger_b = make_record(keys[0])  # same lane: second stop() call, same tick territory
    parked = [make_record(keys[1]) for _ in range(2)]

    recorder = _LaneStopRecorder({trigger_a.offset, trigger_b.offset})
    recorder.sub = sub
    _started(sub, recorder)

    do_stop_calls = 0
    real_do_stop = sub._do_stop

    async def counting_do_stop() -> None:
        nonlocal do_stop_calls
        do_stop_calls += 1
        await real_do_stop()

    sub._do_stop = counting_do_stop

    recorder.latency = 0.001
    await asyncio.wait_for(_feed(sub, [trigger_a, trigger_b, *parked]), timeout=2.0)
    recorder.armed.set()
    await wait_until(lambda: sub._stop_task is not None, recorder=recorder)
    await asyncio.wait_for(sub._stop_task, timeout=5.0)

    assert do_stop_calls == 1, "single-flight violated: multiple _do_stop runs"
    assert recorder.stop_returned, "lane-context stop() must return, not deadlock"
    assert recorder.done_count() == 4, "lane-initiated stop dropped parked messages"


async def test_external_stop_joins_an_inflight_lane_initiated_stop() -> None:
    """D14 property 4: `await subscriber.stop()` from broker context returns only once the
    drain completes — the SystemExit → app.exit() → broker.stop() producer-disconnect race."""
    sub = make_key_ordered_subscriber(max_workers=2)
    keys = keys_on_distinct_lanes(2, 2)
    trigger = make_record(keys[0])
    gated = make_record(keys[1])

    recorder = _LaneStopRecorder({trigger.offset})
    recorder.sub = sub
    _started(sub, recorder)
    gate = recorder.gate(gated)

    await sub.consume_one(gated)
    await wait_until(lambda: recorder.active_count == 1, recorder=recorder)
    await sub.consume_one(trigger)
    recorder.armed.set()
    await wait_until(lambda: sub._stop_task is not None, recorder=recorder)

    external_stop = asyncio.create_task(sub.stop())
    await asyncio.sleep(0.05)
    assert not external_stop.done(), "external stop() must JOIN the in-flight drain"

    gate.set()
    await asyncio.wait_for(external_stop, timeout=5.0)
    assert recorder.done_count() == 2


async def test_lane_crash_restart_preserves_queue_and_stop_still_drains(recorder: ConsumeRecorder) -> None:
    """D12 + D7 together: a crashed lane worker restarts on the same stream; a later stop
    still drains fully (the drain is permit-based, task-identity-immune)."""
    sub = make_key_ordered_subscriber(max_workers=2)
    _started(sub, recorder)

    records = [make_record(b"lane-key") for _ in range(4)]
    recorder.raise_for.add(records[0].offset)  # first message kills the worker post-consume
    recorder.latency = 0.001
    await asyncio.wait_for(_feed(sub, records), timeout=2.0)
    await wait_until(lambda: recorder.done_count() == 4, recorder=recorder)

    # Order survived the restart, and a graceful stop after it drains (trivially) clean.
    recorder.assert_strict_per_key_order({b"lane-key": [r.offset for r in records]})
    await asyncio.wait_for(sub.stop(), timeout=5.0)


class _ScriptedConsumer:
    """Stand-in for AIOKafkaConsumer.getone(): yields records, awaits gate Events (for
    deterministic sequencing), raises exceptions, then blocks forever."""

    def __init__(self, script: list) -> None:
        self.script = list(script)
        self.forever = asyncio.Event()

    async def getone(self):
        while self.script:
            item = self.script.pop(0)
            if isinstance(item, asyncio.Event):
                await item.wait()
                continue
            if isinstance(item, BaseException):
                raise item
            return item
        await self.forever.wait()
        raise ConsumerStoppedError


async def test_read_loop_self_registers_and_reregisters_after_supervisor_restart(
    recorder: ConsumeRecorder,
) -> None:
    """D12: `_intake_task` is restart-stable — the loop registers asyncio.current_task()
    on every (re)entry, so stop() cancels the LIVE loop, not a stale handle."""
    sub = make_key_ordered_subscriber(max_workers=1)
    _started(sub, recorder)
    sub.running = True  # what _post_start() does inside the real start()

    # First incarnation dies on an uncaught (non-Kafka) error after one good record; the
    # crash waits on a gate so the test can observe the FIRST task before it is replaced.
    crash_gate = asyncio.Event()
    consumer = _ScriptedConsumer([make_record(b"k1"), crash_gate, RuntimeError("boom")])
    sub.add_task(sub._run_consume_loop, (consumer,))
    await wait_until(lambda: sub._intake_task is not None)
    first = sub._intake_task
    await wait_until(lambda: recorder.done_count() == 1, recorder=recorder)
    crash_gate.set()

    # Supervisor restarts the loop (same scripted consumer: now blocks in getone).
    await wait_until(lambda: sub._intake_task is not None and sub._intake_task is not first and not sub._intake_task.done())
    live = sub._intake_task
    assert first.done()

    await asyncio.wait_for(sub.stop(), timeout=5.0)
    assert live.cancelled() or live.done(), "stop() must terminate the LIVE read loop"


async def test_racing_intake_dispatch_during_stop_exits_via_cancellation(recorder: ConsumeRecorder) -> None:
    """The consume_one guard: after stop initiation, a racing dispatch releases its permit
    and raises CancelledError (supervisor-ignored — no restart storm on closed streams)."""
    sub = make_key_ordered_subscriber(max_workers=2)
    _started(sub, recorder)
    await asyncio.wait_for(sub.stop(), timeout=5.0)

    permits_before = sub._limiter.value
    with pytest.raises(asyncio.CancelledError):
        await sub.consume_one(make_record(b"late"))
    assert sub._limiter.value == permits_before, "racing dispatch leaked a permit"
    assert recorder.done_count() == 0


async def test_restart_after_stop_gets_fresh_state_and_no_cross_run_release(recorder: ConsumeRecorder, monkeypatch: pytest.MonkeyPatch) -> None:
    """D11: run 2's semaphore cannot be inflated by run 1's cancelled workers."""
    from calfkit._faststream_ext import _subscriber as mod

    monkeypatch.setattr(mod, "_FINALIZATION_GRACE", 0.2)
    sub = make_key_ordered_subscriber(max_workers=2, graceful_timeout=0.2)
    _started(sub, recorder)

    stuck = make_record(b"stuck")
    recorder.gate(stuck)  # never released: forces escalation-cancel of run 1's worker
    await sub.consume_one(stuck)
    await wait_until(lambda: recorder.active_count == 1, recorder=recorder)
    await asyncio.wait_for(sub.stop(), timeout=3.0)

    # Run 2 (what start() does): fresh state, full bound, clean processing.
    recorder2 = ConsumeRecorder()
    sub.consume = recorder2
    sub._allocate_dispatch_state()
    sub._spawn_lanes()
    records = [make_record(f"k{i}".encode()) for i in range(4)]
    recorder2.latency = 0.001
    await asyncio.wait_for(_feed(sub, records), timeout=2.0)
    await wait_until(lambda: recorder2.done_count() == 4, recorder=recorder2)
    await asyncio.sleep(0.05)  # any straggling run-1 release would land by now
    assert sub._limiter.value == 4, "cross-run permit corruption (or leak) detected"
    await asyncio.wait_for(sub.stop(), timeout=5.0)


async def test_stop_while_read_loop_blocked_on_the_semaphore(recorder: ConsumeRecorder) -> None:
    """Round-2 checklist: the intake can be blocked at the BOUND (not in getone) at stop
    time; shutdown must not wedge and accepted messages still drain."""
    sub = make_key_ordered_subscriber(max_workers=1)
    _started(sub, recorder)
    bound = 2

    records = [make_record(b"hot") for _ in range(bound + 2)]
    gates = {r.offset: recorder.gate(r) for r in records}

    feeder = asyncio.get_running_loop().create_task(_feed(sub, records))
    await wait_until(lambda: recorder.active_count == 1)
    sub._intake_task = feeder  # what the real read loop's self-registration would hold

    stop_task = asyncio.create_task(sub.stop())
    await asyncio.sleep(0.05)
    for gate in gates.values():
        gate.set()
    await asyncio.wait_for(stop_task, timeout=5.0)
    assert feeder.cancelled(), "the semaphore-blocked intake must be cancelled by stop()"
    assert recorder.done_count() == bound, "accepted (in-bound) messages must drain"


async def test_cancel_swallowing_handler_degrades_latency_but_stop_returns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from calfkit._faststream_ext import _subscriber as mod

    monkeypatch.setattr(mod, "_FINALIZATION_GRACE", 0.3)
    sub = make_key_ordered_subscriber(max_workers=1, graceful_timeout=0.2)

    zombie_released = asyncio.Event()

    class Swallower(ConsumeRecorder):
        async def __call__(self, msg) -> None:
            self.events.append(("start", msg.key, msg.offset, msg.topic))
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                await zombie_released.wait()  # swallow the cancel and keep blocking

    swallower = Swallower()
    _started(sub, swallower)
    await sub.consume_one(make_record(b"zombie"))
    await wait_until(lambda: len(swallower.events) == 1)

    started = time.monotonic()
    await asyncio.wait_for(sub.stop(), timeout=3.0)
    assert time.monotonic() - started < 1.5, "stop() must not hang on a cancel-swallowing handler"
    zombie_released.set()


async def test_stop_before_start_is_a_clean_noop() -> None:
    """Round-3 fix: broker.stop() hits never-started subscribers (partial start failure,
    connect-only usage); the fields exist from __init__ so this must not raise."""
    sub = make_key_ordered_subscriber(max_workers=4)
    await asyncio.wait_for(sub.stop(), timeout=2.0)
    await asyncio.wait_for(sub.stop(), timeout=2.0)  # and it is idempotent


async def test_lane_detached_do_stop_failure_is_logged(recorder: ConsumeRecorder) -> None:
    """A lane-detached stop is never awaited; a _do_stop crash must be logged via the
    done-callback, not silently dropped."""
    sub = make_key_ordered_subscriber(max_workers=1)
    logged: list[tuple] = []
    sub._log = lambda *a, **k: logged.append((a, k))

    async def exploding_do_stop() -> None:
        raise RuntimeError("stop machinery bug")

    sub._do_stop = exploding_do_stop

    trigger = make_record(b"k")
    lane_recorder = _LaneStopRecorder({trigger.offset})
    lane_recorder.armed.set()  # no feeding to protect in this test
    lane_recorder.sub = sub
    _started(sub, lane_recorder)
    # _started replaced consume; re-point _do_stop AFTER state allocation reset _stop_task.
    sub._do_stop = exploding_do_stop

    await sub.consume_one(trigger)
    await wait_until(lambda: lane_recorder.done_count() == 1, recorder=lane_recorder)
    await wait_until(lambda: bool(logged))
    args, kwargs = logged[0]
    assert args[0] == logging.ERROR
    assert isinstance(kwargs.get("exc_info"), RuntimeError)

    # An external stop() after the failed background stop re-raises to its caller.
    with pytest.raises(RuntimeError, match="stop machinery bug"):
        await sub.stop()


async def test_external_stop_after_completed_stop_returns_immediately(recorder: ConsumeRecorder) -> None:
    sub = make_key_ordered_subscriber(max_workers=2)
    _started(sub, recorder)
    await asyncio.wait_for(sub.stop(), timeout=5.0)
    started = time.monotonic()
    await asyncio.wait_for(sub.stop(), timeout=1.0)
    assert time.monotonic() - started < 0.1


async def test_start_allocates_then_super_starts_then_spawns_lanes(recorder: ConsumeRecorder, monkeypatch: pytest.MonkeyPatch) -> None:
    """start() ordering (spec §6.2): fresh dispatch state BEFORE super().start() (whose
    read loop may dispatch into the buffers), lane workers spawned right after."""
    from faststream.kafka.subscriber.usecase import DefaultSubscriber

    sub = make_key_ordered_subscriber(max_workers=2)
    sub.consume = recorder
    observed: dict = {}

    async def fake_parent_start(self) -> None:  # noqa: ANN001
        observed["lanes_at_parent_start"] = len(self._lanes)
        observed["tasks_at_parent_start"] = len(self.tasks)

    monkeypatch.setattr(DefaultSubscriber, "start", fake_parent_start)
    await sub.start()

    assert observed["lanes_at_parent_start"] == 2, "dispatch state must exist before super().start()"
    assert observed["tasks_at_parent_start"] == 0, "lanes must spawn AFTER super().start()"
    assert len(sub.tasks) == 2
    await asyncio.wait_for(sub.stop(), timeout=5.0)


async def test_log_stop_failure_branches(recorder: ConsumeRecorder) -> None:
    sub = make_key_ordered_subscriber(max_workers=1)
    logged: list[tuple] = []
    sub._log = lambda *a, **k: logged.append((a, k))

    async def ok() -> None:
        return None

    task = asyncio.get_running_loop().create_task(ok())
    await task
    sub._log_stop_failure(task)  # exception() is None -> no log
    assert logged == []

    async def cancelled() -> None:
        await asyncio.sleep(30)

    task2 = asyncio.get_running_loop().create_task(cancelled())
    await asyncio.sleep(0)
    task2.cancel()
    await asyncio.gather(task2, return_exceptions=True)
    sub._log_stop_failure(task2)  # cancelled -> no log
    assert logged == []


async def test_consume_one_racing_stop_mid_acquire_hands_permit_to_drain(recorder: ConsumeRecorder) -> None:
    """The post-acquire guard: an intake blocked at the bound when stop initiates must
    hand its freshly-won permit back and bow out via CancelledError."""
    sub = make_key_ordered_subscriber(max_workers=1)
    _started(sub, recorder)
    bound = 2

    records = [make_record(b"hot") for _ in range(bound)]
    gates = {r.offset: recorder.gate(r) for r in records}
    await asyncio.wait_for(_feed(sub, records), timeout=2.0)

    blocked = asyncio.create_task(sub.consume_one(make_record(b"hot")))  # blocked in acquire
    await asyncio.sleep(0.02)
    assert not blocked.done()

    sub._stop_initiated = True  # what _do_stop sets first
    gates[records[0].offset].set()  # a completion releases one permit -> wakes the acquire
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(blocked, timeout=2.0)

    for gate in gates.values():
        gate.set()
    await wait_until(lambda: recorder.done_count() == bound, recorder=recorder)
    await asyncio.sleep(0.01)
    assert sub._limiter.value == bound, "the raced permit was not handed back"
    for send_stream, _ in sub._lanes:
        send_stream.close()
    await asyncio.wait_for(asyncio.gather(*sub.tasks, return_exceptions=True), timeout=5.0)


async def test_consume_one_releases_permit_when_enqueue_raises(recorder: ConsumeRecorder) -> None:
    """The except-BaseException guard around send_nowait: a closed lane (framework bug /
    shutdown race) re-raises and returns the permit."""
    import anyio

    sub = make_key_ordered_subscriber(max_workers=1)
    _started(sub, recorder)

    sub._lanes[0][0].close()  # every key maps to the single lane
    permits_before = sub._limiter.value
    with pytest.raises(anyio.ClosedResourceError):
        await sub.consume_one(make_record(b"any"))
    assert sub._limiter.value == permits_before
    await asyncio.wait_for(asyncio.gather(*sub.tasks, return_exceptions=True), timeout=5.0)
