"""The client's single inbox reader + per-run demultiplexer — the hub (spec §5.1/§5.2).

Built **standalone** here; wired into ``Client.connect()`` at the Commit-6 cutover (the shipped
``_ReplyDispatcher`` path is untouched until then). This module owns:

- :class:`_RunChannel` — the per-run in-memory channel (lossless, closed-once, replayable terminal);
- :class:`InvocationHandle` — the channel-bearing per-run handle (``result()``/``stream()`` land in
  a later commit);
- the hub — a groupless topic-subscribe FastStream handler that classifies ``x-calf-kind`` and
  demuxes each reply by ``correlation_id`` into the owning handle's channel.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Annotated, Any
from weakref import WeakValueDictionary

from faststream import Context
from faststream.kafka import KafkaBroker

from calfkit._protocol import HDR_EMITTER, HDR_EMITTER_KIND, HDR_KIND, decode_header_str
from calfkit.client.events import EventStream, RunCompleted, RunEvent, RunFailed
from calfkit.exceptions import ClientClosedError
from calfkit.models.envelope import Envelope  # runtime: FastDepends needs it for the handler arg
from calfkit.models.error_report import ErrorReport, FaultTypes
from calfkit.models.node_result import extract_lenient
from calfkit.models.reply import FaultMessage, ReturnMessage

logger = logging.getLogger(__name__)


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
        self._terminal: RunEvent | None = None
        self._closed: bool = False
        self._closed_error: BaseException | None = None
        # The "terminal-arrived" signal. Created in the running loop — the channel is always built
        # in an async context (start() / tests). A Future is not a Task, so it never roots the
        # handle in the event loop (spec §5.2: the weak map stays the sole owner).
        self._arrived: asyncio.Future[None] = asyncio.get_running_loop().create_future()

    @property
    def closed(self) -> bool:
        return self._closed

    def push(self, event: RunEvent) -> None:
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
        if not self._arrived.done():
            self._arrived.set_result(None)

    async def await_terminal(self) -> RunEvent:
        """Park until the channel closes, then return the cached terminal ``RunEvent`` — or raise
        the typed close error (``ClientClosedError``) if it was closed by ``aclose()``. O(1)
        replay once arrived (spec §4.4)."""
        await self._arrived
        if self._closed_error is not None:
            raise self._closed_error
        if self._terminal is None:  # defensive: a closed channel always has a terminal or an error
            raise RuntimeError("run channel closed without a terminal or a close error")
        return self._terminal


@dataclass
class InvocationHandle:
    """The per-run handle (spec §3.1/§5.2) — owns the run's in-memory channel; the hub pushes the
    run's terminal into it, demuxed by ``correlation_id``. The caller holds it for the run's
    lifetime; there is no reattach-by-correlation-id. ``result()``/``stream()`` are added later.

    A plain (non-slots) dataclass so it is **weak-referenceable** (the hub's routing map holds it
    weakly) and **acyclic** (nothing strong-refs it back) — so it self-GCs the instant the caller
    drops it (spec §5.2).
    """

    correlation_id: str
    _channel: _RunChannel = field(repr=False, compare=False)


class _Hub:
    """The client's single inbox reader + per-run demultiplexer (spec §5.1/§5.2).

    Standalone in v1; wired into ``Client.connect()`` at the Commit-6 cutover. Holds a **weak**
    ``correlation_id → handle`` routing map so a dropped handle self-GCs (memory bounded by the
    handles the caller holds). The handler classifies ``x-calf-kind`` and pushes the matching
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
        ``broker.start()`` (spec §2.7/§5.1). Called at ``connect()`` in the Commit-6 cutover."""

        @broker.subscriber(inbox_topic, group_id=None, auto_offset_reset="latest")
        async def _handle_reply(
            envelope: Envelope,
            correlation_id: Annotated[str, Context()],
            headers: Annotated[dict[str, Any], Context("message.headers")],
        ) -> None:
            # The subscriber binds only transport-sourced values and forwards to _on_reply, which holds
            # the classify/demux logic so it is unit-testable without driving the broker.
            self._on_reply(envelope, correlation_id, headers)

    def _on_reply(self, envelope: Envelope, correlation_id: str, headers: dict[str, Any]) -> None:
        """Classify an inbound reply by ``x-calf-kind`` and demux it into the owning run's channel.

        Split out of the FastStream subscriber closure for unit-testability (like the old
        ``_ReplyDispatcher._on_reply``). Mirrors that stamping so ``result()``'s ``from_envelope``
        projects correctly: the transport identity + the per-delivery reply slot are stamped onto
        the context before the terminal is built.
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

    def _completed(self, envelope: Envelope, cid: str, emitter: str | None) -> RunEvent:
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

    def _dispatch(self, cid: str, event: RunEvent) -> None:
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
