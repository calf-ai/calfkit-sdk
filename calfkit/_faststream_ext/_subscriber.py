"""``KeyOrderedSubscriber`` — parallel across keys, serial and in-order within a key.

Dispatch model (see the package docstring for why upstream has no equivalent):

- ``max_workers`` **lanes**, each one bounded in-memory stream plus one long-lived serial
  worker task. ``crc32(raw_key) % max_workers`` picks the lane, so equal keys always land
  on the same lane and are processed one at a time, in arrival (= per-partition log)
  order. Keyless records carry no ordering claim and round-robin (with a once-per-run
  WARNING plus a count in the stop-time summary: on a key-ordering subscriber, a keyless
  record usually means a producer dropped its keying).
- One global ``anyio.Semaphore(bound)`` with ``bound = 2 * max_workers`` is the **only**
  blocking primitive: the read loop acquires before dispatch, the lane worker releases
  after ``consume()`` returns. At the bound the read loop blocks (which pauses
  ``getone()`` — backpressure, never message drops). Lane buffers are sized equal to the
  bound, so the post-acquire ``send_nowait`` provably cannot raise ``WouldBlock``: at most
  ``bound`` permits are un-released, and a lane buffer holds that many. The bound is
  computed once at construction into ``self._bound`` (``max_workers`` is immutable) — the
  limiter capacity, every lane's buffer size, and the drain's acquire count all read that
  single field, because their equality IS the invariant. ``max_value`` on the semaphore turns any double-release accounting
  bug into an error at the bug site (guarded and logged in the lane, so it can neither
  mask an unwinding exception nor silently loop through supervisor restarts).
- A ``Semaphore``, not a ``CapacityLimiter``: the permit is released by a different task
  (lane worker) than the acquirer (read loop), which ``CapacityLimiter`` forbids.

Shutdown (``stop()``) is a **single joinable task**: the first caller creates the real
teardown (``_do_stop``) exactly once; callers *inside a lane* (a handler raising
``StopConsume``/``SystemExit`` — upstream ``consume()`` calls ``stop()`` inline) return
without joining so their lane can release its permit and the drain can finish, while
external callers (broker teardown, the ``app.exit()`` chain) await it, preserving the
upstream contract "``stop()`` returns ⟹ fully stopped" — so the broker never disconnects
the producer under still-draining handlers. The graceful drain acquires **all** permits:
that accounts for every parked and executing message regardless of task identity, which
makes it immune to the supervisor replacing crashed worker tasks. On drain timeout the
remaining tasks are cancelled *before* the inherited stop so its MultiLock wait doesn't
burn a second timeout — and the cancellation is WARNING-logged with the abandoned-handler
count, because under ACK_FIRST those messages are already committed and will not be
reprocessed. A bounded finalization wait then collects stragglers best-effort (safety
against cross-run interference comes from the closed-over per-run state, not from that
wait).

Per-run dispatch state lives in ``_allocate_dispatch_state()`` (subscribers are
restartable); the shutdown-control fields also exist from ``__init__`` so that ``stop()``
on a constructed-but-never-started subscriber is a clean no-op — the broker stops every
registered subscriber, started or not (partial ``broker.start()`` failure, connect-only
usage). Lane workers close over their run's stream and semaphore via task arguments, so a
worker surviving into a later run (via supervisor restart or slow cancellation) can only
ever touch its own run's objects; the supervisor also re-passes those same arguments on
crash-restart, which preserves both the queued messages and the permit accounting. The
read loop registers ``asyncio.current_task()`` on every (re)entry, so shutdown cancels
the *live* intake even after a supervisor restart.

``_allocate_dispatch_state()`` and ``_spawn_lanes()`` are deliberate test seams: dispatch
semantics are unit-tested without a broker connection, which only ``start()``'s
``super().start()`` requires.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import zlib
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, NamedTuple

import anyio
from faststream.kafka.subscriber.usecase import DefaultSubscriber

if TYPE_CHECKING:
    from aiokafka import AIOKafkaConsumer, ConsumerRecord  # type: ignore[import-untyped]
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

# Best-effort straggler collection after an escalated (cancel-path) stop. Bounded so a
# handler that swallows CancelledError degrades shutdown latency instead of hanging it;
# correctness never depends on this wait (closed-over per-run state keeps stragglers
# harmless), so the exact value is not load-bearing.
_FINALIZATION_GRACE = 5.0

# True inside a lane worker task (asyncio tasks copy their creator's context, so it is
# also inherited by anything a lane spawns — which is why _do_stop must never read it).
_serving_lane: ContextVar[bool] = ContextVar("_serving_lane", default=False)


class _Lane(NamedTuple):
    """One dispatch lane: the named pair keeps send/receive swap-proof at unpack sites."""

    send: MemoryObjectSendStream[ConsumerRecord]
    receive: MemoryObjectReceiveStream[ConsumerRecord]


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
        # Everything stop()/_do_stop() touches exists from construction (upstream's own
        # pattern: consumer=None / tasks=[] / running=False live in __init__ precisely so
        # a stop-before-start is a no-op, not an AttributeError). The limiter is a real,
        # untouched semaphore so the never-started drain succeeds trivially; lanes stay
        # empty until start() so a never-started subscriber allocates no streams.
        self._lanes: list[_Lane] = []
        self._bound = _DISPATCH_BOUND_FACTOR * max_workers
        self._reset_run_state()

    # -- per-run dispatch state (test seams; see module docstring) -------------------

    def _reset_run_state(self) -> None:
        """The single enumeration of the per-run control fields (called from both
        ``__init__`` and ``_allocate_dispatch_state`` — never let the two drift)."""
        self._limiter = anyio.Semaphore(self._bound, max_value=self._bound)
        self._round_robin = itertools.count()
        self._warned_keyless = False
        self._keyless_count = 0
        self._stop_task: asyncio.Task[None] | None = None
        self._stop_initiated = False
        self._intake_task: asyncio.Task[Any] | None = None

    def _allocate_dispatch_state(self) -> None:
        self._reset_run_state()
        self._lanes = [_Lane(*anyio.create_memory_object_stream["ConsumerRecord"](max_buffer_size=self._bound)) for _ in range(self.max_workers)]

    def _spawn_lanes(self) -> None:
        limiter = self._limiter
        for lane in self._lanes:
            # Closed-over (stream, limiter) task args — NOT self attributes — so a
            # supervisor crash-restart resumes the same queue with coherent permits, and
            # nothing can release into a later run's semaphore.
            self.add_task(self._serve_lane, (lane.receive, limiter))

    # -- lifecycle --------------------------------------------------------------------

    async def start(self) -> None:
        self._allocate_dispatch_state()
        await super().start()  # consumer + read-loop task; the loop task is scheduled as
        self._spawn_lanes()  # super().start()'s last statement and cannot run before
        # this synchronous spawn — and even if it could, dispatches would just park in
        # the (bound-sized) lane buffers until the workers exist.

    async def _run_consume_loop(self, consumer: AIOKafkaConsumer) -> None:
        # Restart-stable identity: re-registered on every (re)entry, so after a
        # supervisor restart _intake_task points at the LIVE loop, never a dead handle.
        self._intake_task = asyncio.current_task()
        await super()._run_consume_loop(consumer)

    async def stop(self) -> None:
        if self._stop_task is None:
            # Single-flight, created SYNCHRONOUSLY on first call (no await between the
            # check and the assignment): every later stop() joins this same task. Bare
            # create_task — NEVER add_task: a supervised / self.tasks-registered stop
            # would be cancelled by its own escalation pass and would await itself in
            # the finalization wait. _do_stop is guard-free and never reads
            # _serving_lane, so the lane context it may inherit is inert — no respawn.
            self._stop_task = asyncio.create_task(self._do_stop())
            # A lane-detached stop is never awaited; without this callback a _do_stop
            # crash (or cancellation by outer teardown) would be a silently
            # half-stopped subscriber.
            self._stop_task.add_done_callback(self._log_stop_failure)
        if _serving_lane.get():
            # StopConsume/SystemExit from a handler: upstream consume() calls stop()
            # INSIDE the lane worker, which still holds its permit — returning (instead
            # of joining) lets the lane release it so the drain can actually complete.
            return
        await self._stop_task  # external callers: stop() returns ⟹ fully stopped

    async def _do_stop(self) -> None:
        self._stop_initiated = True  # gates consume_one's dispatch
        if self._intake_task is not None:
            self._intake_task.cancel()  # a getone()-blocked loop can't see a flag
        for lane in self._lanes:
            lane.send.close()  # no more dispatch; receivers drain buffered items,
            # then their `async for` ends (anyio: EndOfStream only when the buffer is
            # empty AND no sender is open)
        timeout = self._outer_config.graceful_timeout
        drained = await self._drain(timeout)
        pending = [task for task in self.tasks if not task.done()]
        if not drained:
            if pending:
                # Under ACK_FIRST these messages are already committed: cancelling their
                # handlers abandons them permanently. Never let that look like a clean
                # shutdown.
                cause = f"graceful drain timed out after {timeout}s" if timeout else "graceful drain disabled (falsy graceful_timeout)"
                self._log(
                    logging.WARNING,
                    f"key-ordered subscriber: {cause}; force-cancelling {len(pending)} "
                    "worker task(s) with messages parked or in flight — their offsets "
                    "are already committed (ACK_FIRST) and they will NOT be reprocessed",
                )
            for task in pending:
                task.cancel()  # escalation: cancel NOW so the stuck handlers' MultiLock
                # entries unwind and super().stop()'s wait doesn't burn a second timeout
        if self._keyless_count:
            self._log(
                logging.WARNING,
                f"key-ordered subscriber consumed {self._keyless_count} keyless "
                "message(s) this run — those were round-robined with NO per-key "
                "ordering (see the earlier warning)",
            )
        await super().stop()  # running=False, MultiLock wait (≈instant after a
        # successful drain), TasksMixin cancel, consumer.stop()
        if pending:
            # Bounded, best-effort straggler collection (asyncio.wait never cancels on
            # timeout — a cancel-swallowing handler is abandoned, not waited on).
            await asyncio.wait(pending, timeout=_FINALIZATION_GRACE)

    async def _drain(self, timeout: float | None) -> bool:
        """Acquire every permit — i.e. wait until no message is parked or executing.

        Falsy timeout means no drain wait at all, mirroring upstream
        ``MultiLock.wait_release`` (and upstream's cancel-the-buffer behavior).
        """
        if not timeout:
            return False
        with anyio.move_on_after(timeout) as scope:
            for _ in range(self._bound):
                await self._limiter.acquire()
        return not scope.cancelled_caught

    def _log_stop_failure(self, task: asyncio.Task[None]) -> None:
        try:
            if task.cancelled():
                # An outer teardown cancelled the background stop: same half-stopped
                # state as a crash, so it gets the same trace.
                self._log(
                    logging.WARNING,
                    "key-ordered subscriber background stop was cancelled; subscriber may be half-stopped",
                )
                return
            exc = task.exception()
            if exc is None:
                return
            self._log(
                logging.ERROR,
                f"key-ordered subscriber background stop failed ({exc!r}); subscriber may be half-stopped",
                # _log's exc_info is typed Exception | None; a BaseException (exotic
                # here) still gets its repr in the message above.
                exc_info=exc if isinstance(exc, Exception) else None,
            )
        except Exception:  # noqa: BLE001 - the logger state may already be torn down
            # A raising done-callback would only reach asyncio's loop exception handler
            # and displace this trace; there is nowhere better left to report to.
            pass

    # -- dispatch ---------------------------------------------------------------------

    def _lane_for(self, key: bytes | None) -> int:
        if key is None:
            self._keyless_count += 1
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
            "key. (Warned once per run; the total count is reported at stop.)",
        )

    def _drop_after_stop(self) -> None:
        """Post-stop dispatch (racing intake, or a supervisor-restarted read loop): the
        message is dropped — documented at-most-once — with a trace, and the read loop
        exits via cancellation (supervisor-ignored)."""
        self._log(logging.DEBUG, "key-ordered subscriber dropped a message fetched after stop initiation")
        raise asyncio.CancelledError

    async def consume_one(self, msg: ConsumerRecord) -> None:
        if self._stop_initiated:
            # The lanes are closed and — after a successful drain — every permit is held.
            self._drop_after_stop()
        await self._limiter.acquire()  # the ONLY wait point: at the bound, block
        if self._stop_initiated:
            self._limiter.release()  # raced stop mid-acquire: hand the permit back to the drain
            self._drop_after_stop()
        try:
            self._lanes[self._lane_for(msg.key)].send.send_nowait(msg)  # never blocks:
            # lane buffer == bound
        except BaseException:
            self._limiter.release()
            raise

    async def _serve_lane(
        self,
        receive_stream: MemoryObjectReceiveStream[ConsumerRecord],
        limiter: anyio.Semaphore,
    ) -> None:
        _serving_lane.set(True)  # stop()-reentrancy detection (see stop())
        async for msg in receive_stream:  # ends when the send side closes and drains
            try:
                await self.consume(msg)  # upstream: middleware, context, error swallowing
            finally:
                try:
                    limiter.release()
                except ValueError:
                    # The max_value tripwire fired: permit accounting is violated — a
                    # framework bug. Log it specifically EVERY time (the supervisor's
                    # generic trace dedups per exception identity) and keep the lane
                    # alive; re-raising here would also mask whatever exception this
                    # finally is unwinding (e.g. the shutdown cancellation).
                    self._log(
                        logging.ERROR,
                        "key-ordered permit accounting invariant violated (double release) — a framework bug, please report; the lane continues",
                    )
