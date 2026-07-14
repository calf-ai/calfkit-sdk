"""Harness for the `_faststream_ext` unit suites.

The extension's dispatch/lifecycle semantics are tested WITHOUT a broker connection:
construction needs none (verified — only ``start()``'s ``super().start()`` touches Kafka),
so tests build the subscriber through the REAL production factory against a never-connected
broker, call the two test seams ``_allocate_dispatch_state()`` / ``_spawn_lanes()`` instead
of ``start()`` (wrapped here as :func:`start_dispatch`), and drive ``consume_one`` with real
aiokafka ``ConsumerRecord``s while ``consume()`` is monkeypatched with an instrumented
recorder.

``TestKafkaBroker`` is no help here: it calls ``process_message`` directly, bypassing
``consume_one`` and all dispatch (verified against ``faststream/kafka/testing.py``).
"""

from __future__ import annotations

import asyncio
import itertools
import zlib
from typing import TYPE_CHECKING, cast

import pytest
from aiokafka import ConsumerRecord
from faststream.kafka import KafkaBroker

from calfkit._faststream_ext._factory import create_key_ordered_subscriber
from tests.utils import wait_until as _wait_until

if TYPE_CHECKING:
    from faststream.kafka.configs import KafkaBrokerConfig

    from calfkit._faststream_ext._subscriber import KeyOrderedSubscriber

_OFFSETS = itertools.count()


def make_record(key: bytes | None, topic: str = "topic-a", value: bytes = b"v") -> ConsumerRecord:
    """A real aiokafka ConsumerRecord; the extension reads only ``.key`` (and tests read
    ``.topic``/``.offset`` for their own bookkeeping)."""
    offset = next(_OFFSETS)
    return ConsumerRecord(
        topic=topic,
        partition=0,
        offset=offset,
        timestamp=0,
        timestamp_type=0,
        key=key,
        value=value,
        checksum=None,
        serialized_key_size=-1 if key is None else len(key),
        serialized_value_size=len(value),
        headers=(),
    )


def make_key_ordered_subscriber(max_workers: int, topics: tuple[str, ...] = ("topic-a", "topic-b"), **broker_kwargs):
    """Build a KeyOrderedSubscriber through the REAL factory against a never-connected
    broker — the tests exercise exactly what production constructs."""
    broker = KafkaBroker(**broker_kwargs)
    # What broker.connect() does at _internal/broker/broker.py:106 — without it the
    # supervisor's restart path (which logs first) raises IncorrectState in unit tests.
    broker.config.logger._setup(broker.config.fd_config.context)
    return create_key_ordered_subscriber(
        *topics,
        group_id="test-group",
        max_workers=max_workers,
        connection_args={},
        config=cast("KafkaBrokerConfig", broker.config),
    )


def start_dispatch(sub: KeyOrderedSubscriber, recorder: ConsumeRecorder) -> None:
    """What ``start()`` does minus the Kafka-touching ``super().start()``."""
    sub.consume = recorder
    sub._allocate_dispatch_state()
    sub._spawn_lanes()


async def feed(sub: KeyOrderedSubscriber, records) -> None:
    for record in records:
        await sub.consume_one(record)


async def stop_lanes(sub: KeyOrderedSubscriber) -> None:
    """Test-side teardown: close lanes and let workers exit via close+drain."""
    for lane in sub._lanes:
        lane.send.close()
    await asyncio.wait_for(
        asyncio.gather(*sub.tasks, return_exceptions=True),
        timeout=5.0,
    )


def spy_log(sub: KeyOrderedSubscriber) -> list[tuple]:
    """Capture the subscriber's ``_log(level, message, ...)`` calls as ``(args, kwargs)``."""
    logged: list[tuple] = []
    sub._log = lambda *a, **k: logged.append((a, k))
    return logged


def keys_on_distinct_lanes(n: int, max_workers: int) -> list[bytes]:
    """Generate n keys that crc32-hash onto n distinct lanes."""
    assert n <= max_workers
    found: dict[int, bytes] = {}
    for i in itertools.count():
        key = f"key-{i}".encode()
        lane = zlib.crc32(key) % max_workers
        found.setdefault(lane, key)
        if len(found) == n:
            return list(found.values())[:n]
    raise AssertionError("unreachable")


class ConsumeRecorder:
    """Instrumented stand-in for ``SubscriberUsecase.consume``.

    Mirrors the load-bearing upstream contract: it NEVER raises for handler errors
    (upstream ``consume()`` swallows them) unless a test injects ``raise_for`` to simulate
    a framework-level BaseException escaping. Asserts the per-key mutual-exclusion
    invariant *at execution time* (the strongest possible serialization check) and keeps a
    full start/end event log for order and overlap assertions.
    """

    def __init__(self) -> None:
        self.events: list[tuple[str, bytes | None, int, str]] = []  # (phase, key, offset, topic)
        self.active_keys: set[bytes] = set()
        self.active_count = 0
        self.peak_active = 0
        self.gates: dict[int, asyncio.Event] = {}  # offset -> release gate
        self.latency: float = 0.0
        self.raise_for: set[int] = set()  # offsets that raise (simulating BaseException escape)
        self.progressed = asyncio.Event()

    def gate(self, record: ConsumerRecord) -> asyncio.Event:
        event = asyncio.Event()
        self.gates[record.offset] = event
        return event

    def done_count(self) -> int:
        return sum(1 for phase, *_ in self.events if phase == "end")

    async def __call__(self, msg: ConsumerRecord) -> None:
        key = msg.key
        if key is not None:
            assert key not in self.active_keys, f"same-key concurrency for {key!r} — per-key ordering violated"
            self.active_keys.add(key)
        self.active_count += 1
        self.peak_active = max(self.peak_active, self.active_count)
        self.events.append(("start", key, msg.offset, msg.topic))
        self.progressed.set()
        try:
            if msg.offset in self.gates:
                await self.gates[msg.offset].wait()
            elif self.latency:
                await asyncio.sleep(self.latency)
            else:
                await asyncio.sleep(0)
            if msg.offset in self.raise_for:
                raise RuntimeError(f"injected failure for offset {msg.offset}")
        finally:
            self.active_count -= 1
            if key is not None:
                self.active_keys.discard(key)
            self.events.append(("end", key, msg.offset, msg.topic))
            self.progressed.set()

    def per_key_sequences(self) -> dict[bytes | None, list[tuple[str, int]]]:
        out: dict[bytes | None, list[tuple[str, int]]] = {}
        for phase, key, offset, _topic in self.events:
            out.setdefault(key, []).append((phase, offset))
        return out

    def assert_strict_per_key_order(self, expected_offsets: dict[bytes, list[int]]) -> None:
        """Each key's event stream must be start/end pairs in exactly submission order."""
        sequences = self.per_key_sequences()
        for key, offsets in expected_offsets.items():
            expected = [(phase, off) for off in offsets for phase in ("start", "end")]
            got = sequences.get(key, [])
            assert got == expected, f"key {key!r}: order violated\n got={got}\n want={expected}"


@pytest.fixture
def recorder() -> ConsumeRecorder:
    return ConsumeRecorder()


async def wait_until(predicate, timeout: float = 2.0, recorder: ConsumeRecorder | None = None) -> None:
    """Await a condition, waking on recorder progress (see ``tests.utils.wait_until``)."""
    await _wait_until(predicate, timeout=timeout, wake=recorder.progressed if recorder is not None else None)
