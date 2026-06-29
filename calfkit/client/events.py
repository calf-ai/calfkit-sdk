"""Caller-surface streaming events + the firehose tuning default (spec §3.3 / §2.1).

The ``RunEvent`` union: intermediate step events (``AgentMessageEvent`` / ``ToolCallEvent`` / ``ToolResultEvent`` /
``HandoffEvent``, spec §3.2) followed by exactly one terminal (``RunCompleted`` / ``RunFailed``). A run's
stream yields zero or more intermediates then its terminal.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from calfkit.models.error_report import ErrorReport
from calfkit.models.step import (  # widen RunEvent (no cycle: models.step → models.payload)
    AgentMessageEvent,
    HandoffEvent,
    ToolCallEvent,
    ToolResultEvent,
)

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


RunTerminal = RunCompleted | RunFailed
"""A run's terminal — exactly one ends every run (and is itself a :data:`RunEvent`). The per-run
channel's cached, replayable terminal slot holds only this; the intermediate step events are the other
``RunEvent`` members."""

RunEvent = RunCompleted | RunFailed | AgentMessageEvent | ToolCallEvent | ToolResultEvent | HandoffEvent
"""A run's stream: zero or more intermediate step events (``AgentMessageEvent`` / ``ToolCallEvent`` /
``ToolResultEvent`` / ``HandoffEvent``, spec §3.2) then exactly one terminal (``RunCompleted`` / ``RunFailed``).
``AgentThinkingEvent`` is defined but not emitted in v1 (§5), so it is not in the union yet."""


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
        # Outlet registration + the broker start happen in __aenter__, and the outlet is removed on
        # __aexit__; iterating un-entered (or after exit) would have no registered outlet, so it would
        # never receive an event and park forever — guard both states (CRITICAL).
        self._entered = False
        self._closed = False

    _NOT_ENTERED = "client.events() must be entered before iterating: `async with client.events() as stream:`"
    _CLOSED = "client.events() stream is closed (its `async with` block exited); open a new events() stream to keep observing."

    async def __aenter__(self) -> EventStream:
        if self._on_enter is not None:
            await self._on_enter()
        self._hub._add_outlet(self)
        self._entered = True
        return self

    async def __aexit__(self, *exc: object) -> None:
        self._hub._remove_outlet(self)
        self._closed = True

    def _check_usable(self) -> None:
        # A bare `async for` (never entered) registered no outlet + never started the broker; a
        # post-exit iteration lost its outlet. Either way the reader would park forever — raise instead.
        if self._closed:
            raise RuntimeError(self._CLOSED)
        if not self._entered:
            raise RuntimeError(self._NOT_ENTERED)

    def __aiter__(self) -> EventStream:
        self._check_usable()
        return self

    async def __anext__(self) -> RunEvent:
        # The firehose is open-ended: park until an event is buffered, then drain in order. The reader
        # exits via the async-CM (or its own timeout), never StopAsyncIteration.
        self._check_usable()
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
            return  # a terminal_only outlet drops intermediate step events, surfacing only terminals (§3.3)
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
