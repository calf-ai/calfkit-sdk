"""``KeyOrderedSubscriber`` — parallel across keys, serial and in-order within a key.

Dispatch model (see the package docstring for why upstream has no equivalent):

- ``max_workers`` **lanes**, each one bounded in-memory stream plus one long-lived serial
  worker task. ``crc32(raw_key) % max_workers`` picks the lane, so equal keys always land
  on the same lane and are processed one at a time, in arrival (= per-partition log)
  order. Keyless records carry no ordering claim and round-robin (with a once-per-run
  WARNING: on a key-ordering subscriber, a keyless record usually means a producer
  dropped its keying).
- One global ``anyio.Semaphore(2 * max_workers)`` is the **only** blocking primitive: the
  read loop acquires before dispatch, the lane worker releases after ``consume()``
  returns. At the bound the read loop blocks (which pauses ``getone()`` — backpressure,
  never message drops). Lane buffers are sized equal to the bound, so the post-acquire
  ``send_nowait`` provably cannot raise ``WouldBlock``: at most ``bound`` permits are
  un-released, and a lane buffer holds that many. ``max_value`` on the semaphore turns
  any double-release accounting bug into an error at the bug site.
- A ``Semaphore``, not a ``CapacityLimiter``: the permit is released by a different task
  (lane worker) than the acquirer (read loop), which ``CapacityLimiter`` forbids.

Per-run dispatch state lives in ``_allocate_dispatch_state()`` (subscribers are
restartable); the shutdown-control fields are *declared* in ``__init__`` so that
``stop()`` on a constructed-but-never-started subscriber is a clean no-op — the broker
stops every registered subscriber, started or not. Lane workers close over their run's
stream and semaphore via task arguments, so a worker surviving into a later run (via
supervisor restart or slow cancellation) can only ever touch its own run's objects; the
supervisor also re-passes those same arguments on crash-restart, which preserves both the
queued messages and the permit accounting.

``_allocate_dispatch_state()`` and ``_spawn_lanes()`` are deliberate test seams: dispatch
semantics are unit-tested without a broker connection, which only ``start()``'s
``super().start()`` requires.
"""

from __future__ import annotations

import itertools
import logging
import zlib
from typing import TYPE_CHECKING, Any

import anyio
from faststream.kafka.subscriber.usecase import DefaultSubscriber

if TYPE_CHECKING:
    import asyncio

    from aiokafka import ConsumerRecord  # type: ignore[import-untyped]
    from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
    from faststream._internal.endpoint.subscriber import SubscriberSpecification
    from faststream._internal.endpoint.subscriber.call_item import CallsCollection
    from faststream.kafka.subscriber.config import KafkaSubscriberConfig

# Fetched-but-unprocessed ceiling, as a multiple of max_workers. 2 matches FastStream's
# own concurrent subscriber (its shared semaphore double-gates put + consume, yielding an
# emergent ~2x) and the Confluent Parallel Consumer's initial loading factor; Celery's
# prefetch multiplier (4) marks the ceiling of "boring default" territory. Internal by
# design — promoting it to a parameter is a one-line change if measurement ever justifies
# tuning it.
_DISPATCH_BOUND_FACTOR = 2

_Lane = tuple["MemoryObjectSendStream[ConsumerRecord]", "MemoryObjectReceiveStream[ConsumerRecord]"]


class KeyOrderedSubscriber(DefaultSubscriber):
    """Kafka subscriber with ``max_workers``-way parallelism and strict per-key ordering.

    For any two records with equal non-null keys consumed by one subscriber instance
    under a stable partition assignment: if the first was yielded by ``getone()`` before
    the second, its handler invocation completes before the second's begins. Cross-key
    and cross-partition ordering is unspecified, as in Kafka itself.

    ACK_FIRST only (enforced by the factory): with commit-on-receipt, offsets never
    depend on handler completion, so no offset-tracking machinery is needed.
    """

    def __init__(
        self,
        config: KafkaSubscriberConfig,
        specification: SubscriberSpecification[Any, Any],
        calls: CallsCollection[ConsumerRecord],
        *,
        max_workers: int,
    ) -> None:
        super().__init__(config, specification, calls)
        self.max_workers = max_workers
        # Shutdown-control fields exist from construction (upstream's own pattern:
        # consumer=None / tasks=[] / running=False live in __init__ precisely so a
        # stop-before-start is a no-op, not an AttributeError — the broker stops ALL
        # registered subscribers, e.g. after a partial broker.start() failure or
        # connect-only `async with broker:` usage). _allocate_dispatch_state() re-resets
        # them per run.
        self._lanes: list[_Lane] = []
        self._stop_task: asyncio.Task[None] | None = None
        self._stop_initiated = False
        self._intake_task: asyncio.Task[Any] | None = None

    # -- per-run dispatch state (test seams; see module docstring) -------------------

    def _allocate_dispatch_state(self) -> None:
        bound = _DISPATCH_BOUND_FACTOR * self.max_workers
        self._limiter = anyio.Semaphore(bound, max_value=bound)
        self._round_robin = itertools.count()
        self._lanes = [anyio.create_memory_object_stream["ConsumerRecord"](max_buffer_size=bound) for _ in range(self.max_workers)]
        self._warned_keyless = False
        self._stop_task = None
        self._stop_initiated = False
        self._intake_task = None

    def _spawn_lanes(self) -> None:
        limiter = self._limiter
        for _, receive_stream in self._lanes:
            # Closed-over (stream, limiter) task args — NOT self attributes — so a
            # supervisor crash-restart resumes the same queue with coherent permits, and
            # nothing can release into a later run's semaphore.
            self.add_task(self._serve_lane, (receive_stream, limiter))

    # -- dispatch ---------------------------------------------------------------------

    def _lane_for(self, key: bytes | None) -> int:
        if key is None:
            self._warn_keyless_once()
            return next(self._round_robin) % self.max_workers
        return zlib.crc32(key) % self.max_workers

    def _warn_keyless_once(self) -> None:
        if self._warned_keyless:
            return
        self._warned_keyless = True
        self._log(
            logging.WARNING,
            "keyless message on a key-ordered subscriber: routing round-robin with NO "
            "per-key ordering. A producer for these topics likely dropped its partition "
            "key.",
        )

    async def consume_one(self, msg: ConsumerRecord) -> None:
        await self._limiter.acquire()  # the ONLY wait point: at the bound, block
        try:
            send_stream, _ = self._lanes[self._lane_for(msg.key)]
            send_stream.send_nowait(msg)  # never blocks: lane buffer == bound
        except BaseException:
            self._limiter.release()
            raise

    async def _serve_lane(
        self,
        receive_stream: MemoryObjectReceiveStream[ConsumerRecord],
        limiter: anyio.Semaphore,
    ) -> None:
        async for msg in receive_stream:  # ends when the send side closes and drains
            try:
                await self.consume(msg)  # upstream: middleware, context, error swallowing
            finally:
                limiter.release()
