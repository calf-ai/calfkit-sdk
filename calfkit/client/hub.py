"""The client's single inbox reader + per-run demultiplexer — the hub (spec §5.1/§5.2). It owns:

- :class:`_RunChannel` — the per-run in-memory channel (lossless, closed-once, replayable terminal);
- :class:`InvocationHandle` — the channel-bearing per-run handle (``result()`` / ``stream()``);
- the hub — a groupless topic-subscribe FastStream handler that classifies ``x-calf-kind`` and
  demuxes each reply by ``correlation_id`` into the owning handle's channel.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections import deque
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Annotated, Any, Generic
from weakref import WeakValueDictionary

from faststream import Context
from faststream.kafka import KafkaBroker
from faststream.message import StreamMessage
from pydantic import ValidationError

from calfkit._protocol import HDR_EMITTER, HDR_EMITTER_KIND, HDR_KIND, decode_header_str, wire_filter
from calfkit._types import OutputT
from calfkit.client.events import EventStream, RunCompleted, RunEvent, RunFailed, RunTerminal
from calfkit.exceptions import ClientClosedError, ClientTimeoutError, NodeFaultError
from calfkit.models.envelope import Envelope  # runtime: FastDepends needs it for the handler arg
from calfkit.models.error_report import ErrorReport, FaultTypes
from calfkit.models.node_result import InvocationResult, extract_lenient
from calfkit.models.reply import FaultMessage, ReturnMessage
from calfkit.models.step import AgentThinkingEvent, StepMessage

logger = logging.getLogger(__name__)


async def lenient_step_decoder(msg: StreamMessage[Any]) -> StepMessage | None:
    """The per-call-item decoder for the step subscriber (spec §3.4): decode the body to a
    :class:`StepMessage`; on a malformed/schema-skewed body **swallow** the decode error and return
    ``None`` (the lenient rail). A bad step must NEVER fault the run — swallowing keeps it off the
    broker decode-floor's topic-keyed sink (the floor only re-routes a *raised* decode error). The
    handler is typed ``StepMessage | None`` so FastDepends does not re-validate the sentinel."""
    try:
        return StepMessage.model_validate_json(msg.body)
    except (ValidationError, json.JSONDecodeError, UnicodeDecodeError):
        return None


class _RunChannel:
    """The per-run in-memory channel (spec §5.3) — handle-owned, lossless, closed-once.

    The hub **pushes synchronously** — there is no per-run ``asyncio`` task (spec §5.2) — and a
    reader awaits the terminal. v1 is **terminal-only** (``RunCompleted``/``RunFailed``); the
    intermediate deque + per-event stream signaling land with intermediate emission (spec §9.1).
    The terminal is **cached** so it stays replayable after close (spec §4.4); a post-close push
    is a **benign no-op** (the §5.2/§5.5 terminal-dedup). A typed close error (e.g.
    ``ClientClosedError`` from ``aclose()``, §5.8) is stored as a **value** and raised on read —
    never ``future.set_exception`` (§3 decision 1: avoids asyncio's "exception never retrieved").
    """

    def __init__(self) -> None:
        self._terminal: RunTerminal | None = None
        self._closed: bool = False
        self._closed_error: BaseException | None = None
        # The "terminal-arrived" signal — an Event, NOT a Future (§3 decision 1, corrected): a Future
        # is the shared object the reader awaits, so a ``result(timeout=)`` whose ``asyncio.wait_for``
        # cancels the awaiting task would cancel that Future and permanently break the channel (a later
        # terminal could never wake it). An Event is robust to per-waiter cancellation — each waiter
        # gets its own internal future and the set/unset state persists — so the run survives a timeout
        # (§4.3). Like a Future it is not a Task, so it never roots the handle in the loop (the weak map
        # stays the sole owner, §5.2), and it needs no running loop at construction.
        self._arrived = asyncio.Event()
        # Intermediate (step) events ride a SEPARATE consume-once queue with its OWN wake signal (both
        # asyncio.Events, cancel-safe). Different access semantics (spec §2.8): the terminal is cached +
        # replayable; intermediates drain once. terminal/close sets BOTH signals (a parked stream() wakes
        # on completion AND on error-close); an intermediate push sets ONLY the queue signal (never
        # spuriously wakes a parked result()).
        self._intermediates: deque[RunEvent] = deque()
        self._intermediate_ready = asyncio.Event()

    @property
    def closed(self) -> bool:
        return self._closed

    def push(self, event: RunTerminal) -> None:
        """Cache the terminal + close (first wins), waking an awaiting reader. Synchronous and
        non-blocking — never awaits. A push into an already-closed channel is a benign no-op."""
        if self._closed:
            return
        self._terminal = event
        self._closed = True
        self._wake()

    def close_with(self, error: BaseException) -> None:
        """Close the channel with a typed error to raise on read (``aclose()`` →
        ``ClientClosedError``, §5.8). No-op if already closed — a terminal that already arrived
        wins."""
        if self._closed:
            return
        self._closed_error = error
        self._closed = True
        self._wake()

    def _wake(self) -> None:
        # terminal/close wakes a parked result() (terminal signal) AND a parked stream() (queue signal),
        # so stream() drains the backlog then yields the cached terminal even when it parked on an empty queue.
        self._arrived.set()
        self._intermediate_ready.set()

    async def await_terminal(self) -> RunTerminal:
        """Park until the channel closes, then return the cached terminal ``RunEvent`` — or raise
        the typed close error (``ClientClosedError``) if it was closed by ``aclose()``. O(1)
        replay once arrived (spec §4.4)."""
        await self._arrived.wait()
        if self._closed_error is not None:
            raise self._closed_error
        if self._terminal is None:  # defensive: a closed channel always has a terminal or an error
            raise RuntimeError("run channel closed without a terminal or a close error")
        return self._terminal

    def push_intermediate(self, event: RunEvent) -> None:
        """Append an intermediate (step) event, waking a parked ``stream()`` (spec §2.8). Synchronous,
        non-blocking. A **no-op once closed** — a post-terminal reordered/duplicate step is dropped, not
        appended (§3.3). Sets ONLY the queue signal (never the terminal signal — that would spuriously
        wake a parked ``result()``)."""
        if self._closed:
            return
        self._intermediates.append(event)
        self._intermediate_ready.set()

    async def drain_intermediates(self) -> AsyncIterator[RunEvent]:
        """Yield buffered then live intermediates (consume-once) until the channel closes — used by
        ``stream()`` before the cached terminal. Keeps **no ``await`` between the empty-check and the
        signal-clear** (the lost-wakeup invariant, mirroring the firehose drain in ``events.py``)."""
        while True:
            while self._intermediates:
                yield self._intermediates.popleft()
            if self._closed:
                return
            self._intermediate_ready.clear()
            # Re-check after the clear: a push between the drain and the clear set the signal; loop
            # without waiting so it is not lost. Park only when genuinely empty AND still open.
            if self._intermediates or self._closed:
                continue
            await self._intermediate_ready.wait()


@dataclass
class InvocationHandle(Generic[OutputT]):
    """The per-run handle (spec §3.1/§5.2) — owns the run's in-memory channel; the hub pushes the
    run's terminal into it, demuxed by ``correlation_id``. The caller holds it for the run's
    lifetime; there is no reattach-by-correlation-id. ``result()`` maps the terminal to a value or a
    typed outcome; ``stream()`` yields the run's events (terminal-bearing).

    A plain (non-slots) dataclass so it is **weak-referenceable** (the hub's routing map holds it
    weakly) and **acyclic** (nothing strong-refs it back) — so it self-GCs the instant the caller
    drops it (spec §5.2).
    """

    correlation_id: str
    _channel: _RunChannel = field(repr=False, compare=False)
    # The mint-bound output type (``agent(output_type=…)``, default ``str``) — used to project a
    # successful terminal in ``result()``. The firehose's raw ``RunCompleted.output`` is separate.
    _output_type: type[Any] = field(default=str, repr=False, compare=False)
    _stream_active: bool = field(default=False, repr=False, compare=False)

    async def result(self, *, timeout: float | None = None) -> InvocationResult[OutputT]:
        """Await this run's terminal and map it to a value or a typed outcome (spec §4.3/§5.9): a
        success projects to the rich ``InvocationResult`` (or raises ``DeserializationError`` on a
        present-but-mismatched part); a fault raises ``NodeFaultError``. No default timeout — a durable
        run may legitimately pause; pass ``timeout=`` to bound *this client's* patience."""
        if timeout is None:
            terminal = await self._channel.await_terminal()
        else:
            try:
                terminal = await asyncio.wait_for(self._channel.await_terminal(), timeout)
            except (TimeoutError, asyncio.TimeoutError):
                # The client gave up; the RUN is unaffected. wait_for cancelled only this waiter — the
                # channel's Event (not a Future) keeps its state, so a later terminal still resolves a
                # subsequent result(). A typed signal, never a bare asyncio.TimeoutError (§2.5).
                raise ClientTimeoutError(self.correlation_id, timeout) from None
        if isinstance(terminal, RunFailed):
            raise NodeFaultError(terminal.report)  # the ErrorReport wrapped verbatim (§5.9)
        return InvocationResult.from_envelope(terminal._envelope, self._output_type, correlation_id=terminal.correlation_id)

    async def stream(self) -> AsyncIterator[RunEvent]:
        """Yield this run's events in order, **terminal-bearing** — intermediates (steps) first, the
        cached terminal always last (spec §2.8/§3.1/§4.4). The intermediate queue is consume-once (a
        late ``stream()`` drains the buffered backlog once); the raw events are surfaced un-projected
        (``result()`` is the projected face). The terminal is cached, so ``stream()`` is replayable after
        ``result()``; **at most one** live ``stream()`` per handle (a second concurrent one raises
        ``RuntimeError``); an early ``break`` releases the guard via the ``finally``."""
        if self._stream_active:
            raise RuntimeError("at most one live stream() per handle (spec §4.4)")
        self._stream_active = True
        try:
            async for event in self._channel.drain_intermediates():
                yield event
            yield await self._channel.await_terminal()
        finally:
            self._stream_active = False


class _Hub:
    """The client's single inbox reader + per-run demultiplexer (spec §5.1/§5.2).

    Holds a **weak** ``correlation_id → handle`` routing map so a dropped handle self-GCs (memory
    bounded by the handles the caller holds). The handler classifies ``x-calf-kind`` and pushes the matching
    terminal into the owning handle's channel; pushes are synchronous and non-blocking (no per-run
    ``asyncio`` task — spec §5.2).
    """

    def __init__(self) -> None:
        self._runs: WeakValueDictionary[str, InvocationHandle] = WeakValueDictionary()
        self._firehose: set[EventStream] = set()

    def _add_outlet(self, outlet: EventStream) -> None:
        """Register a firehose outlet (an open ``events()`` stream). Mutated only between handler
        invocations (on enter/exit), so the await-free tee never iterates a changing set mid-push."""
        self._firehose.add(outlet)

    def _remove_outlet(self, outlet: EventStream) -> None:
        self._firehose.discard(outlet)

    def track(self, handle: InvocationHandle) -> None:
        """Register a run's handle before its call is published (the single sync step of ``start()``,
        spec §5.2). Reject a duplicate of a *currently-registered* (live-handle) cid."""
        cid = handle.correlation_id
        if cid in self._runs:
            raise ValueError(f"correlation_id {cid!r} already has a live in-flight handle")
        self._runs[cid] = handle

    def register(self, broker: KafkaBroker, inbox_topic: str) -> None:
        """Register the hub's groupless reply subscriber on the inbox — a **topic** subscription with
        ``group_id=None`` (no consumer group / commits / rebalance; aiokafka auto-assigns all partitions)
        and ``auto_offset_reset="latest"`` (tail). Pure bookkeeping; started by the first
        ``broker.start()`` (spec §2.7/§5.1). Called from ``connect()``."""

        # ONE groupless inbox subscriber; the envelope reply handler is its first filtered call-item.
        # The filter (x-calf-wire == "envelope") runs BEFORE body decode, so a step body never triggers
        # Envelope validation (spec §2.4). Increment E attaches the step call-item to this same `sub`.
        sub = broker.subscriber(inbox_topic, group_id=None, auto_offset_reset="latest")

        @sub(filter=wire_filter(Envelope))
        async def _handle_reply(
            envelope: Envelope,
            correlation_id: Annotated[str, Context()],
            headers: Annotated[dict[str, Any], Context("message.headers")],
        ) -> None:
            # The subscriber binds only transport-sourced values and forwards to _on_reply, which holds
            # the classify/demux logic so it is unit-testable without driving the broker.
            self._on_reply(envelope, correlation_id, headers)

        @sub(filter=wire_filter(StepMessage), decoder=lenient_step_decoder)
        async def _handle_step(step: StepMessage | None) -> None:
            # Typed StepMessage | None (NOT StepMessage): the lenient decoder returns None for a malformed
            # step, and FastDepends must NOT re-validate that sentinel (a StepMessage-typed arg would raise
            # a 2nd ValidationError → the decode floor → fail_run, faulting a healthy run, §3.4).
            self._on_step(step)

    def _on_reply(self, envelope: Envelope, correlation_id: str, headers: dict[str, Any]) -> None:
        """Classify an inbound reply by ``x-calf-kind`` and demux it into the owning run's channel.

        Split out of the FastStream subscriber closure for unit-testability. Stamps the transport
        identity + the per-delivery reply slot onto the context before building the terminal, so
        ``result()``'s ``from_envelope`` projects correctly.
        """
        kind = decode_header_str(headers.get(HDR_KIND))
        emitter = decode_header_str(headers.get(HDR_EMITTER))
        emitter_kind = decode_header_str(headers.get(HDR_EMITTER_KIND))
        envelope.context._stamp_transport(correlation_id=correlation_id, emitter_node_id=emitter, emitter_node_kind=emitter_kind)
        envelope.context._reply = envelope.reply
        if kind == "return":
            self._dispatch(correlation_id, self._completed(envelope, correlation_id, emitter))
        elif kind == "fault":
            self._dispatch(correlation_id, self._failed(envelope, correlation_id))
        else:
            # Unknown/missing kind → ERROR + drop (§5.9): the client inbox is a REPLY channel, NOT
            # ingress, so do NOT apply the "missing header reads as call" norm — never resolve a run
            # on an unclassifiable reply.
            logger.error(
                "[%s] reply with unrecognized x-calf-kind=%r — dropped (emitter=%s)",
                (correlation_id or "n/a")[:8],
                kind,
                emitter,
            )

    def _completed(self, envelope: Envelope, cid: str, emitter: str | None) -> RunTerminal:
        reply = envelope.context._reply
        if not isinstance(reply, ReturnMessage):
            return self._slot_anomaly(cid, "return", reply)  # §5.1(a) defense — never AttributeError
        return RunCompleted(output=extract_lenient(reply.parts), correlation_id=cid, agent=emitter, _envelope=envelope)

    def _failed(self, envelope: Envelope, cid: str) -> RunFailed:
        reply = envelope.context._reply
        if not isinstance(reply, FaultMessage):
            return self._slot_anomaly(cid, "fault", reply)
        return RunFailed(report=reply.error, correlation_id=cid)

    def _slot_anomaly(self, cid: str, kind: str, reply: object) -> RunFailed:
        """An ``x-calf-kind`` ↔ reply-slot disagreement: a malformed-but-decodable terminal (§5.1a) —
        the bytes decoded but the producer's classification and payload contradict each other. Fail the
        channel with ``calf.delivery.malformed`` (distinct from ``undecodable``, an unreadable body) so a
        waiting ``result()`` raises ``NodeFaultError`` instead of hanging — never a silent loss, and
        never an escaped ``AttributeError``."""
        report = ErrorReport.build_safe(
            error_type=FaultTypes.DELIVERY_MALFORMED,
            message=f"reply x-calf-kind={kind!r} does not match its reply slot ({type(reply).__name__})",
            details={"correlation_id": cid, "x_calf_kind": kind, "slot_type": type(reply).__name__},
        )
        return RunFailed(report=report, correlation_id=cid)

    def _dispatch(self, cid: str, event: RunTerminal) -> None:
        # Per-run demux FIRST (spec §5.4 decision: hub-demux, then firehose; both non-blocking).
        handle = self._runs.get(cid)
        if handle is not None:
            handle._channel.push(event)  # synchronous, non-blocking (spec §5.1/§5.4)
        # No live handle (a shared-inbox / send() reply, or a dropped handle). A return drops with a
        # WARNING (benign, firehose-recoverable); a fault is ERROR-floored with the full report — never
        # silently dropped (spec §5.1, fault-rail §11).
        elif isinstance(event, RunFailed):
            logger.error("[%s] fault reply with no pending handle: %s", (cid or "n/a")[:8], event.report.model_dump_json())
        else:
            logger.warning("[%s] return reply with no pending handle — dropped (firehose-recoverable)", (cid or "n/a")[:8])
        # Firehose SECOND: every decodable reply is surfaced raw on the firehose (demux'd or not — on a
        # shared inbox it carries ids this client never dispatched). Best-effort, non-blocking (§5.4).
        self._tee(event)

    def _tee(self, event: RunEvent) -> None:
        # Snapshot the outlet set — registration mutates it only between handler calls, but a list copy
        # keeps the await-free push safe regardless. Each push is non-blocking (drop-oldest), so a slow
        # firehose reader can never stall the hub or another run.
        for outlet in list(self._firehose):
            outlet._offer(event)

    def _on_step(self, step: StepMessage | None) -> None:
        """Demux a decoded step (spec §3.4): push each event onto the owning run's consume-once
        intermediate queue AND tee it to the firehose. A ``None`` (a malformed step the lenient decoder
        swallowed) drops. A **no-handle** step (a ``send()`` run or a GC'd handle) drops **silently**
        from the per-run path — unlike a no-handle *terminal*, which WARN/ERROR-logs — but is still
        tee'd, so ``send()`` runs stay observable via ``events()``."""
        if step is None:
            logger.debug("dropped a malformed/undecodable step (best-effort; never faults the run)")
            return
        handle = self._runs.get(step.correlation_id)
        for event in step.events:
            if isinstance(event, AgentThinkingEvent):
                continue  # defined-not-emitted in v1 (§5): not a RunEvent member, never surfaced on a stream
            if handle is not None:
                handle._channel.push_intermediate(event)  # synchronous, non-blocking (consume-once queue)
            self._tee(event)  # firehose: every step is surfaced raw, demux'd or not (§5.4)

    def fail_run(self, correlation_id: str, report: ErrorReport) -> None:
        """The Option-B undecodable-sink target (§5.8): the decode floor calls this for an undecodable
        reply **on the inbox**, pushing ``RunFailed(report)`` into the matching run's channel. A no-op
        when no live handle (a ``send()`` / foreign-cid undecodable — the floor's ERROR-log is the whole
        story)."""
        handle = self._runs.get(correlation_id)
        if handle is not None:
            handle._channel.push(RunFailed(report=report, correlation_id=correlation_id))

    def close(self) -> None:
        """``aclose()`` resolves every pending ``result()`` with ``ClientClosedError`` (§5.8) — a typed,
        run-survives signal, never a bare ``CancelledError``."""
        for cid, handle in list(self._runs.items()):
            handle._channel.close_with(ClientClosedError(correlation_id=cid))
