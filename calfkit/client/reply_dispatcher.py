from __future__ import annotations

import asyncio
import logging
import math
from dataclasses import dataclass
from typing import Annotated, Any

from faststream import Context
from faststream.kafka import KafkaBroker

from calfkit._protocol import HDR_EMITTER, HDR_EMITTER_KIND, decode_header_str
from calfkit.exceptions import ReplyExpiredError
from calfkit.models.envelope import Envelope

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _PendingEntry:
    """A pending reply: its future plus an optional eviction timer.

    Frozen so the ``(future, timer)`` pairing cannot be reassigned after
    construction (mirrors the frozen ``CallFrame``). ``timer`` is the
    ``asyncio.TimerHandle`` scheduled when a ``reply_ttl`` is configured; it is
    cancelled once the future completes so a stale timer never fires after
    resolution.
    """

    future: asyncio.Future[Envelope]
    timer: asyncio.TimerHandle | None = None


class _ReplyDispatcher:
    """Shared reply consumer that dispatches incoming envelopes to futures keyed by correlation_id.

    When *reply_ttl* is set, each expected reply is backed by a timer that evicts
    the pending future with :class:`ReplyExpiredError` once the TTL elapses. The
    default (``None``) disables eviction entirely: this is a deliberate
    caller-responsibility choice, not a default safety ceiling — callers who need
    a bounded ``_pending`` under lost replies or abandoned handles must opt in.
    """

    def __init__(self, reply_ttl: float | None = None) -> None:
        if reply_ttl is not None and not (math.isfinite(reply_ttl) and reply_ttl > 0):
            raise ValueError(f"reply_ttl must be a positive, finite number of seconds or None; got {reply_ttl!r}")
        self._pending: dict[str, _PendingEntry] = {}
        self._reply_ttl = reply_ttl

    def register(self, broker: KafkaBroker, reply_topic: str, group_id: str) -> None:
        """Register a FastStream subscriber on *reply_topic*. Must be called before ``broker.start()``."""

        @broker.subscriber(reply_topic, group_id=group_id, auto_offset_reset="latest")
        async def _handle_reply(
            envelope: Envelope,
            correlation_id: Annotated[str, Context()],
            headers: Annotated[dict[str, Any], Context("message.headers")],
        ) -> None:
            # The subscriber only binds transport-sourced values (correlation_id,
            # headers) and forwards to _on_reply, which holds the dispatch logic
            # so it is unit-testable without driving the broker.
            self._on_reply(envelope, correlation_id, headers)

    def _on_reply(self, envelope: Envelope, correlation_id: str, headers: dict[str, Any]) -> None:
        """Route an inbound reply envelope to its pending future by correlation id.

        Split out of the FastStream subscriber closure so the dispatch branches
        (unknown id → warning; already-done future → quiet drop; otherwise resolve)
        can be exercised directly. All identity is transport-sourced, never read
        from the envelope body.
        """
        # Stash the inbound correlation id + emitter so the context surfaces them
        # (ctx.correlation_id, NodeResult.emitter_node_id / kind).
        envelope.context._stamp_transport(
            correlation_id=correlation_id,
            emitter_node_id=decode_header_str(headers.get(HDR_EMITTER)),
            emitter_node_kind=decode_header_str(headers.get(HDR_EMITTER_KIND)),
        )

        entry = self._pending.get(correlation_id)
        if entry is None:
            logger.warning("[%s] reply received but no pending future", correlation_id[:8])
            return
        future = entry.future
        if future.done():
            # The future was already resolved or evicted (e.g. ReplyExpiredError
            # fired before this late reply arrived). Drop it quietly.
            logger.debug("[%s] reply received but future already done", correlation_id[:8])
            return
        future.set_result(envelope)

    def expect(self, correlation_id: str) -> asyncio.Future[Envelope]:
        """Create and return a future for *correlation_id*. Raises if already pending.

        When ``reply_ttl`` is configured, an eviction timer is scheduled so the
        future fails with :class:`ReplyExpiredError` if no reply arrives in time.
        """
        if correlation_id in self._pending:
            raise RuntimeError(f"Duplicate correlation_id: {correlation_id}")
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Envelope] = loop.create_future()
        timer: asyncio.TimerHandle | None = None
        if self._reply_ttl is not None:
            # Bind the concrete float into the callback so _evict receives a
            # non-optional ttl — no assert / re-read of self._reply_ttl (which
            # ``python -O`` would strip; see session_context.correlation_id).
            timer = loop.call_later(self._reply_ttl, self._evict, correlation_id, self._reply_ttl)
        self._pending[correlation_id] = _PendingEntry(future=future, timer=timer)
        future.add_done_callback(lambda _: self._discard(correlation_id))
        return future

    def _evict(self, correlation_id: str, ttl: float) -> None:
        """Timer callback: fail a still-pending future with ``ReplyExpiredError``."""
        entry = self._pending.get(correlation_id)
        if entry is None or entry.future.done():
            return
        entry.future.set_exception(ReplyExpiredError(correlation_id, ttl))

    @staticmethod
    def _retrieve_exception(future: asyncio.Future[Envelope]) -> None:
        """Mark a done future's exception retrieved so an abandoned (never-awaited)
        future cannot trigger asyncio's "Future exception was never retrieved" log.

        Idempotent — an awaiting caller still receives the exception — and a no-op
        for a result-resolved future (``exception()`` returns ``None``). Skipped
        for a cancelled future, where ``exception()`` would raise
        ``CancelledError``. The eviction path relies on this read; do not guard it
        behind ``if exception``.
        """
        if future.done() and not future.cancelled():
            future.exception()

    def _discard(self, correlation_id: str) -> None:
        """Done-callback: drop the entry, cancel its timer, retrieve any exception."""
        entry = self._pending.pop(correlation_id, None)
        if entry is None:
            return
        if entry.timer is not None:
            entry.timer.cancel()
        self._retrieve_exception(entry.future)

    def close(self) -> None:
        """Cancel pending timers + futures and clear the registry.

        A future already evicted (done with ``ReplyExpiredError``) but not yet
        discarded — ``_evict`` schedules ``_discard`` via ``call_soon``, so there
        is a window where the entry is still registered — has its exception
        retrieved here, so ``close()`` does not leak the "never retrieved" warning
        the eviction machinery exists to suppress. Still-pending futures are
        cancelled.
        """
        for entry in self._pending.values():
            if entry.timer is not None:
                entry.timer.cancel()
            if entry.future.done():
                self._retrieve_exception(entry.future)
            else:
                entry.future.cancel()
        self._pending.clear()
