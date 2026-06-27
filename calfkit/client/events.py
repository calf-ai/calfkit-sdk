"""Caller-surface streaming events + the firehose tuning default (spec §3.3 / §2.1).

The closed v1 ``RunEvent`` terminal union (``RunCompleted | RunFailed``). Intermediate event
types (AgentMessage, ToolCalled, HandoffOccurred) are a future shape — emitted only once
intermediate emission ships (spec §9.1) — so v1 yields exactly the terminal.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from calfkit.models.error_report import ErrorReport

if TYPE_CHECKING:
    from calfkit.client.hub import _Hub
    from calfkit.models.envelope import Envelope

logger = logging.getLogger(__name__)

DEFAULT_FIREHOSE_BUFFER_SIZE = 1024
"""Default per-observer firehose buffer bound, in events (spec §2.1 / §5.4).

A *starting* default — low thousands, seconds of transient-stall tolerance for coarse
whole-turn events at low memory — tunable from the ``EventStream.dropped`` signal.
"""


@dataclass(frozen=True)
class RunCompleted:
    """A run's successful terminal (spec §3.3).

    ``output`` is the **raw, type-agnostic** best-effort value (``extract_lenient``) — the firehose
    surfaces foreign runs whose ``output_type`` is unknown, so the terminal is never pre-projected.
    The carried ``_envelope`` (the decoded reply) is what ``result()`` projects to the developer's
    ``output_type`` → the rich ``InvocationResult`` (``InvocationResult.from_envelope``, spec §5.9).
    """

    output: Any
    correlation_id: str
    agent: str | None
    _envelope: Envelope = field(repr=False, compare=False)


@dataclass(frozen=True)
class RunFailed:
    """A run's fault terminal: the ``ErrorReport`` carried verbatim (mapped to ``NodeFaultError``
    by ``result()``, spec §5.9)."""

    report: ErrorReport
    correlation_id: str


RunEvent = RunCompleted | RunFailed
"""The closed v1 terminal union — a run's stream ends in exactly one of these. Widened when
intermediate emission ships (spec §3.3 / §9.1)."""


class EventStream:
    """The cross-run firehose (spec §3.2/§5.4) — an async context manager AND async iterator over
    **every** event on the client's one configured inbox while open. The caller demuxes by
    ``correlation_id`` and is responsible for its own dedup / terminal handling.

    **Best-effort, not lossless.** Each open stream is the hub's own bounded **drop-oldest** outlet
    (``buffer_size`` events): a reader that falls behind drops its *oldest* buffered events — signaled
    by the cumulative ``dropped`` counter (+ a WARNING) — so it can **never block the hub**. For
    guaranteed delivery, hold the run's handle (``start``/``execute``) or run a ``@consumer`` node.
    """

    def __init__(
        self,
        hub: _Hub,
        *,
        terminal_only: bool = False,
        buffer_size: int = DEFAULT_FIREHOSE_BUFFER_SIZE,
        on_enter: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self._hub = hub
        self._terminal_only = terminal_only
        self._buffer: deque[RunEvent] = deque(maxlen=buffer_size)
        self._wake = asyncio.Event()
        self.dropped: int = 0
        self._warned: bool = False
        # The client passes its guarded broker start here: a pure-observer client brings the broker up
        # via its first events() (else the subscriber never starts and the stream would hang, §5.1).
        self._on_enter = on_enter
        # Outlet registration + the broker start happen in __aenter__; iterating un-entered would
        # register nothing, never start the broker, and park forever — so guard it (CRITICAL).
        self._entered = False

    _NOT_ENTERED = "client.events() must be entered before iterating: `async with client.events() as stream:`"

    async def __aenter__(self) -> EventStream:
        if self._on_enter is not None:
            await self._on_enter()
        self._hub._add_outlet(self)
        self._entered = True
        return self

    async def __aexit__(self, *exc: object) -> None:
        self._hub._remove_outlet(self)

    def __aiter__(self) -> EventStream:
        if not self._entered:
            raise RuntimeError(self._NOT_ENTERED)
        return self

    async def __anext__(self) -> RunEvent:
        # The firehose is open-ended: park until an event is buffered, then drain in order. The reader
        # exits via the async-CM (or its own timeout), never StopAsyncIteration. A bare `async for`
        # (no `async with`) registered no outlet and never started the broker — raise, don't hang.
        if not self._entered:
            raise RuntimeError(self._NOT_ENTERED)
        while True:
            if self._buffer:
                return self._buffer.popleft()
            self._wake.clear()
            await self._wake.wait()

    def _offer(self, event: RunEvent) -> None:
        """Append an event to this outlet — called **synchronously** by the hub tee (await-free,
        non-blocking; spec §5.4). A full buffer drops the **oldest** to admit the newest (the deque's
        ``maxlen``); every drop increments ``dropped`` (the ongoing signal), and the **first** drop on
        this outlet logs a WARNING once (not re-logged — read ``dropped`` for the running total)."""
        if self._terminal_only and not isinstance(event, (RunCompleted, RunFailed)):
            return  # v1: every RunEvent is terminal, so this never filters yet (forward-compat, §3.3)
        if len(self._buffer) == self._buffer.maxlen:  # full → this append evicts the oldest
            self.dropped += 1
            if not self._warned:
                logger.warning(
                    "events() firehose outlet overflowed (buffer_size=%s) — dropping oldest; "
                    "drain faster or use a @consumer node for guaranteed delivery (spec §5.4)",
                    self._buffer.maxlen,
                )
                self._warned = True
        self._buffer.append(event)
        self._wake.set()
