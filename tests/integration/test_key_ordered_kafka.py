"""Real-broker end-to-end tests for ``KeyOrderedSubscriber`` (``-m kafka`` lane).

Scope: **single instance, no rebalance** — exactly the guarantee's stated scope (the
subscriber serializes per key within one instance; cross-instance handoff windows are the
documented ACK_FIRST trade, not under test here).

The drain proof is by acceptance accounting: every message ``consume_one`` accepted (a
permit granted, the message in a lane) was processed by the time ``stop()`` returned —
plus gap-free per-key prefixes and zero duplicates at the handler level. (After a
successful drain ``_do_stop`` holds every permit, so ``_limiter.value == 0``.)
"""

from __future__ import annotations

import asyncio
import json
import random

import pytest
from faststream.kafka import KafkaBroker

from calfkit._faststream_ext import KeyOrderedRegistratorMixin

pytestmark = pytest.mark.kafka


class _KeyOrderedBroker(KeyOrderedRegistratorMixin, KafkaBroker):
    pass


class KeyTracker:
    """Handler-side recorder asserting per-key mutual exclusion at execution time."""

    def __init__(self, rng: random.Random) -> None:
        self.rng = rng
        self.active_keys: set[str] = set()
        self.peak_active = 0
        self.done: list[tuple[str, int, str]] = []  # (key, seq, topic)
        self.progressed = asyncio.Event()

    async def __call__(self, body: str) -> None:
        payload = json.loads(body)
        key, seq, topic = payload["key"], payload["seq"], payload["topic"]
        assert key not in self.active_keys, f"same-key concurrency for {key!r} on the real broker"
        self.active_keys.add(key)
        self.peak_active = max(self.peak_active, len(self.active_keys))
        try:
            await asyncio.sleep(self.rng.choice([0.0, 0.001, 0.003]))
        finally:
            self.active_keys.discard(key)
            self.done.append((key, seq, topic))
            self.progressed.set()

    async def wait_for(self, predicate, timeout: float = 30.0) -> None:
        async def _wait() -> None:
            while not predicate():
                self.progressed.clear()
                if predicate():
                    return
                await self.progressed.wait()

        await asyncio.wait_for(_wait(), timeout=timeout)


def _assert_per_key_order_and_uniqueness(done: list[tuple[str, int, str]], published: dict[str, list[int]]) -> None:
    """Every key's processed seqs must be exactly a gap-free prefix-consistent, duplicate-
    free replay of its publish order (per topic, since seq is per-key-per-topic)."""
    seen: dict[tuple[str, str], list[int]] = {}
    for key, seq, topic in done:
        seen.setdefault((key, topic), []).append(seq)
    for (key, topic), seqs in seen.items():
        assert seqs == sorted(seqs), f"{key}/{topic}: processed out of publish order: {seqs}"
        assert len(set(seqs)) == len(seqs), f"{key}/{topic}: duplicate processing: {seqs}"
        expected_prefix = published[f"{key}:{topic}"][: len(seqs)]
        assert seqs == expected_prefix, f"{key}/{topic}: gap in processed seqs (message lost mid-stream)"


async def test_per_key_order_and_cross_key_parallelism_end_to_end(kafka_bootstrap: str, topic_namespace: str) -> None:
    """~200 messages, 12 keys, two topics, randomized handler latency: strict per-key
    serialization + order, real cross-key overlap, every message processed exactly once."""
    rng = random.Random(4242)
    topic_a = f"{topic_namespace}-in-a"
    topic_b = f"{topic_namespace}-in-b"
    broker = _KeyOrderedBroker(kafka_bootstrap)
    tracker = KeyTracker(rng)

    subscriber = broker.key_ordered_subscriber(topic_a, topic_b, group_id=f"{topic_namespace}-g", max_workers=4)
    subscriber(tracker.__call__)

    keys = [f"key-{i}" for i in range(12)]
    published: dict[str, list[int]] = {}
    plan: list[tuple[str, str, int]] = []  # (key, topic, seq)
    counters: dict[str, int] = {}
    for _ in range(200):
        key = rng.choice(keys)
        topic = rng.choice([topic_a, topic_b])
        marker = f"{key}:{topic}"
        seq = counters.get(marker, 0)
        counters[marker] = seq + 1
        published.setdefault(marker, []).append(seq)
        plan.append((key, topic, seq))

    async with broker:
        await broker.start()
        for key, topic, seq in plan:
            await broker.publish(
                json.dumps({"key": key, "seq": seq, "topic": topic}),
                topic=topic,
                key=key.encode(),
            )
        await tracker.wait_for(lambda: len(tracker.done) == 200)

    assert len(tracker.done) == 200
    _assert_per_key_order_and_uniqueness(tracker.done, published)
    assert tracker.peak_active > 1, "no cross-key parallelism observed on the real broker"


async def test_graceful_stop_drains_accepted_messages_end_to_end(kafka_bootstrap: str, topic_namespace: str) -> None:
    """Stop mid-stream: stop() returns only after every accepted message is processed
    (all permits back), with zero duplicates and gap-free per-key prefixes."""
    rng = random.Random(77)
    topic = f"{topic_namespace}-drain"
    broker = _KeyOrderedBroker(kafka_bootstrap)
    tracker = KeyTracker(rng)

    subscriber = broker.key_ordered_subscriber(topic, group_id=f"{topic_namespace}-g", max_workers=4)
    subscriber(tracker.__call__)

    # Count ACCEPTED messages (consume_one returned ⇒ a permit was granted and the
    # message entered a lane): the drain guarantee is exactly accepted ⇒ processed.
    accepted = 0
    real_consume_one = subscriber.consume_one

    async def counting_consume_one(msg) -> None:
        nonlocal accepted
        await real_consume_one(msg)
        accepted += 1

    subscriber.consume_one = counting_consume_one

    keys = [f"key-{i}" for i in range(6)]
    published: dict[str, list[int]] = {}
    counters: dict[str, int] = {}
    plan: list[tuple[str, int]] = []
    for _ in range(60):
        key = rng.choice(keys)
        seq = counters.get(key, 0)
        counters[key] = seq + 1
        published.setdefault(f"{key}:{topic}", []).append(seq)
        plan.append((key, seq))

    async with broker:
        await broker.start()
        for key, seq in plan:
            await broker.publish(json.dumps({"key": key, "seq": seq, "topic": topic}), topic=topic, key=key.encode())
        # Stop as soon as processing is demonstrably mid-flight.
        await tracker.wait_for(lambda: len(tracker.done) >= 10)
        await asyncio.wait_for(subscriber.stop(), timeout=30.0)

    processed_at_stop = len(tracker.done)
    assert processed_at_stop >= 10, "stop() raced ahead of the mid-flight condition"
    assert processed_at_stop == accepted, (
        f"drain violated: {accepted} messages accepted into lanes but only {processed_at_stop} processed when stop() returned"
    )
    # A successful (non-escalated) drain ends with _do_stop holding every permit.
    assert subscriber._limiter.value == 0
    await asyncio.sleep(0.1)
    assert len(tracker.done) == processed_at_stop, "processing continued after stop() returned"
    _assert_per_key_order_and_uniqueness(tracker.done, published)
