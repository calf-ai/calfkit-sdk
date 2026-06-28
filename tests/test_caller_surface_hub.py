"""Commit 2 — the hub: _RunChannel, the channel-bearing handle, demux/classify, the
undecodable-sink seam (spec §5.1/§5.2/§5.3/§5.8/§5.9).

Built standalone (not wired into Client.connect() until the Commit-6 cutover); the old
_ReplyDispatcher path is untouched. result()/stream() projection is Commit 4 — here the
channel exposes only its write side (push/close) + a raw terminal read.
"""

from __future__ import annotations

import asyncio
import logging

import pytest

from calfkit._protocol import HDR_EMITTER, HDR_EMITTER_KIND, HDR_KIND, HDR_WIRE
from calfkit.client.events import EventStream, RunCompleted, RunFailed
from calfkit.client.hub import InvocationHandle, _Hub, _RunChannel
from calfkit.exceptions import ClientClosedError, ClientTimeoutError, DeserializationError, NodeFaultError
from calfkit.models import CallFrameStack, Envelope, SessionRunContext, WorkflowState
from calfkit.models.error_report import ErrorReport, FaultTypes
from calfkit.models.payload import TextPart
from calfkit.models.reply import FaultMessage, ReturnMessage
from calfkit.models.state import State

_HUB_LOGGER = "calfkit.client.hub"
_EVENTS_LOGGER = "calfkit.client.events"


def _env() -> Envelope:
    return Envelope(
        context=SessionRunContext(state=State(), deps={}),
        internal_workflow_state=WorkflowState(call_stack=CallFrameStack()),
    )


def _completed(cid: str = "c", out: object = "x") -> RunCompleted:
    return RunCompleted(output=out, correlation_id=cid, agent=None, _envelope=_env())


def _failed(cid: str = "c") -> RunFailed:
    return RunFailed(report=ErrorReport(error_type="billing.quota"), correlation_id=cid)


# ── _RunChannel: synchronous push, close-once, replayable terminal, typed close ──


async def test_push_then_await_returns_the_terminal() -> None:
    ch = _RunChannel()
    ch.push(_completed(out="done"))
    ev = await ch.await_terminal()
    assert isinstance(ev, RunCompleted)
    assert ev.output == "done"


async def test_await_parks_until_push() -> None:
    ch = _RunChannel()
    task = asyncio.create_task(ch.await_terminal())
    await asyncio.sleep(0)  # let the reader park
    assert not task.done()
    ch.push(_completed(out="late"))
    ev = await asyncio.wait_for(task, timeout=1.0)
    assert ev.output == "late"


async def test_terminal_is_replayable_await_twice() -> None:
    ch = _RunChannel()
    ch.push(_completed(out="once"))
    first = await ch.await_terminal()
    second = await ch.await_terminal()  # O(1) cached replay (spec §4.4)
    assert first is second


async def test_duplicate_push_is_a_benign_no_op_close_once() -> None:
    ch = _RunChannel()
    ch.push(_completed(out="first"))
    ch.push(_completed(out="second"))  # redelivery into the closed channel — dropped (§5.2/§5.5)
    ev = await ch.await_terminal()
    assert ev.output == "first"


async def test_push_a_fault_terminal() -> None:
    ch = _RunChannel()
    ch.push(_failed())
    ev = await ch.await_terminal()
    assert isinstance(ev, RunFailed)
    assert ev.report.error_type == "billing.quota"


async def test_close_with_makes_await_raise_the_typed_error() -> None:
    ch = _RunChannel()
    ch.close_with(ClientClosedError(correlation_id="c"))
    with pytest.raises(ClientClosedError):
        await ch.await_terminal()


async def test_terminal_wins_over_a_later_close() -> None:
    ch = _RunChannel()
    ch.push(_completed(out="kept"))
    ch.close_with(ClientClosedError(correlation_id="c"))  # after a terminal → no-op
    ev = await ch.await_terminal()
    assert ev.output == "kept"


async def test_close_wins_over_a_later_push() -> None:
    ch = _RunChannel()
    ch.close_with(ClientClosedError(correlation_id="c"))
    ch.push(_completed(out="too-late"))  # after close → no-op
    with pytest.raises(ClientClosedError):
        await ch.await_terminal()


# ── the channel-bearing handle (Commit 2: owns the channel + correlation_id; result()/stream() = Commit 4) ──


async def test_handle_owns_a_channel_and_correlation_id() -> None:
    ch = _RunChannel()
    handle = InvocationHandle(correlation_id="cid-1", _channel=ch)
    assert handle.correlation_id == "cid-1"
    assert handle._channel is ch


async def test_handle_is_weak_referenceable() -> None:
    import weakref

    handle = InvocationHandle(correlation_id="cid-1", _channel=_RunChannel())
    ref = weakref.ref(handle)
    assert ref() is handle
    del handle
    assert ref() is None  # acyclic + non-slots → refcount-collected promptly (spec §5.2)


# ── the hub: track, classify (x-calf-kind), demux by correlation_id, no-handle path ──


def _reply_env(parts: list | None = None, *, error: ErrorReport | None = None) -> Envelope:
    """An Envelope carrying a reply slot — a ReturnMessage (default) or a FaultMessage (error=)."""
    reply: ReturnMessage | FaultMessage = (
        ReturnMessage(in_reply_to=None, tag=None, parts=parts or []) if error is None else FaultMessage(in_reply_to=None, tag=None, error=error)
    )
    return Envelope(
        context=SessionRunContext(state=State(), deps={}),
        internal_workflow_state=WorkflowState(call_stack=CallFrameStack()),
        reply=reply,
    )


def _headers(kind: str, emitter: str = "agent.x") -> dict[str, str]:
    return {HDR_KIND: kind, HDR_EMITTER: emitter, HDR_EMITTER_KIND: "agent", HDR_WIRE: "envelope"}


def _tracked(hub: _Hub, cid: str) -> InvocationHandle:
    """Create + track a handle for cid; return it (the caller holds it, keeping it live)."""
    handle = InvocationHandle(correlation_id=cid, _channel=_RunChannel())
    hub.track(handle)
    return handle


async def _terminal(handle: InvocationHandle, timeout: float = 1.0) -> RunCompleted | RunFailed:
    """Await a run's terminal with a bound, so a not-yet-pushed terminal fails fast (not hangs)."""
    return await asyncio.wait_for(handle._channel.await_terminal(), timeout=timeout)


async def test_track_rejects_a_duplicate_of_a_live_cid() -> None:
    hub = _Hub()
    handle = _tracked(hub, "cid-1")  # held → stays live in the weak map
    # a duplicate of a currently-registered (live-handle) cid → ValueError (spec §5.2)
    with pytest.raises(ValueError):
        hub.track(InvocationHandle(correlation_id="cid-1", _channel=_RunChannel()))
    assert handle.correlation_id == "cid-1"  # keep the strong ref alive to here


async def test_return_reply_pushes_run_completed_to_the_tracked_handle() -> None:
    hub = _Hub()
    handle = _tracked(hub, "cid-1")
    env = _reply_env(parts=[TextPart(text="hello")])
    hub._on_reply(env, "cid-1", _headers("return", emitter="summarizer"))
    ev = await handle._channel.await_terminal()
    assert isinstance(ev, RunCompleted)
    assert ev._envelope is env  # the decoded reply, for result()'s typed projection (spec §3.3/§5.9)
    assert ev.output == "hello"  # raw, type-agnostic best-effort value (extract_lenient)
    assert ev.correlation_id == "cid-1"
    assert ev.agent == "summarizer"  # the emitter id off HDR_EMITTER


async def test_demux_routes_each_reply_to_only_its_own_handle() -> None:
    hub = _Hub()
    h1 = _tracked(hub, "cid-1")
    h2 = _tracked(hub, "cid-2")
    hub._on_reply(_reply_env(parts=[TextPart(text="one")]), "cid-1", _headers("return"))
    ev1 = await h1._channel.await_terminal()
    assert ev1.output == "one"
    assert not h2._channel.closed  # cid-2's channel never saw cid-1's reply (no cross-run leak)


async def test_fault_reply_pushes_run_failed_with_the_report_verbatim() -> None:
    hub = _Hub()
    handle = _tracked(hub, "cid-1")
    report = ErrorReport(error_type="billing.quota", message="over limit")
    hub._on_reply(_reply_env(error=report), "cid-1", _headers("fault"))
    ev = await _terminal(handle)
    assert isinstance(ev, RunFailed)
    assert ev.report is report  # carried verbatim (not flattened) — spec §5.9
    assert ev.correlation_id == "cid-1"


async def test_return_reply_with_no_pending_handle_warns_and_drops(caplog: pytest.LogCaptureFixture) -> None:
    hub = _Hub()  # nothing tracked for this cid (a shared-inbox / send() reply, or a dropped handle)
    with caplog.at_level(logging.WARNING, logger=_HUB_LOGGER):
        hub._on_reply(_reply_env(parts=[TextPart(text="x")]), "cid-orphan", _headers("return"))
    assert "no pending handle" in caplog.text  # WARNING — a lost return is benign (firehose-recoverable)


async def test_fault_reply_with_no_pending_handle_error_floors_the_full_report(caplog: pytest.LogCaptureFixture) -> None:
    hub = _Hub()
    report = ErrorReport(error_type="billing.quota")
    with caplog.at_level(logging.ERROR, logger=_HUB_LOGGER):
        hub._on_reply(_reply_env(error=report), "cid-orphan", _headers("fault"))
    assert "no pending handle" in caplog.text
    assert "billing.quota" in caplog.text  # the full ErrorReport is ERROR-floored, never silently dropped (§5.1)
    assert "WARNING" not in caplog.text  # a fault floors at ERROR, not WARNING


async def test_unrecognized_kind_error_logs_and_does_not_resolve_the_run(caplog: pytest.LogCaptureFixture) -> None:
    hub = _Hub()
    handle = _tracked(hub, "cid-1")
    with caplog.at_level(logging.ERROR, logger=_HUB_LOGGER):
        hub._on_reply(_reply_env(parts=[TextPart(text="x")]), "cid-1", _headers("bogus"))
    assert "x-calf-kind" in caplog.text  # ERROR names the offending kind
    assert not handle._channel.closed  # never resolve a run on an unclassifiable reply (§5.9)


async def test_missing_kind_header_error_logs_and_does_not_resolve_the_run(caplog: pytest.LogCaptureFixture) -> None:
    # The client inbox is a REPLY channel, not ingress — a missing kind is a mis-addressed delivery,
    # NOT the framework's "missing header reads as call" norm (§5.9), so it must not resolve the run.
    hub = _Hub()
    handle = _tracked(hub, "cid-1")
    headers = {HDR_EMITTER: "agent.x", HDR_EMITTER_KIND: "agent"}  # no HDR_KIND
    with caplog.at_level(logging.ERROR, logger=_HUB_LOGGER):
        hub._on_reply(_reply_env(parts=[TextPart(text="x")]), "cid-1", headers)
    assert "x-calf-kind" in caplog.text
    assert not handle._channel.closed


async def test_kind_return_with_a_fault_slot_is_a_malformed_terminal() -> None:
    # Header says return, but the reply slot is a FaultMessage — a producer contract violation. The
    # hub must fail the channel with calf.delivery.malformed, never let an AttributeError escape (§5.1a).
    hub = _Hub()
    handle = _tracked(hub, "cid-1")
    env = _reply_env(error=ErrorReport(error_type="x.y"))  # slot = FaultMessage
    hub._on_reply(env, "cid-1", _headers("return"))  # header = return → disagreement
    ev = await _terminal(handle)
    assert isinstance(ev, RunFailed)
    assert ev.report.error_type == FaultTypes.DELIVERY_MALFORMED


async def test_kind_fault_with_a_return_slot_is_a_malformed_terminal() -> None:
    hub = _Hub()
    handle = _tracked(hub, "cid-1")
    env = _reply_env(parts=[TextPart(text="x")])  # slot = ReturnMessage
    hub._on_reply(env, "cid-1", _headers("fault"))  # header = fault → disagreement
    ev = await _terminal(handle)
    assert isinstance(ev, RunFailed)
    assert ev.report.error_type == FaultTypes.DELIVERY_MALFORMED


async def test_kind_return_with_an_absent_slot_is_a_malformed_terminal() -> None:
    hub = _Hub()
    handle = _tracked(hub, "cid-1")
    hub._on_reply(_env(), "cid-1", _headers("return"))  # _env() has reply=None (no slot)
    ev = await _terminal(handle)
    assert isinstance(ev, RunFailed)
    assert ev.report.error_type == FaultTypes.DELIVERY_MALFORMED


async def test_fail_run_pushes_run_failed_to_the_tracked_handle() -> None:
    # The Option-B undecodable-sink target: the floor calls this for an inbox undecodable (§5.8).
    hub = _Hub()
    handle = _tracked(hub, "cid-1")
    report = ErrorReport(error_type=FaultTypes.DELIVERY_UNDECODABLE)
    hub.fail_run("cid-1", report)
    ev = await _terminal(handle)
    assert isinstance(ev, RunFailed)
    assert ev.report is report


async def test_fail_run_with_no_handle_is_a_no_op() -> None:
    hub = _Hub()
    hub.fail_run("cid-orphan", ErrorReport(error_type="x.y"))  # no live handle → no-op, no crash (§5.8)


async def test_close_resolves_every_pending_run_with_client_closed_error() -> None:
    hub = _Hub()
    h1 = _tracked(hub, "cid-1")
    h2 = _tracked(hub, "cid-2")
    hub.close()  # aclose() resolves every pending result() with ClientClosedError (§5.8)
    for h in (h1, h2):
        with pytest.raises(ClientClosedError):
            await _terminal(h)


async def test_dropped_handle_self_gcs_and_a_later_reply_takes_the_no_handle_path(
    caplog: pytest.LogCaptureFixture,
) -> None:
    hub = _Hub()
    handle = InvocationHandle(correlation_id="cid-1", _channel=_RunChannel())
    hub.track(handle)
    assert "cid-1" in hub._runs  # registered
    del handle  # caller drops it → acyclic + non-slots → refcount-collected, weak map entry vanishes (§5.2)
    assert "cid-1" not in hub._runs
    with caplog.at_level(logging.WARNING, logger=_HUB_LOGGER):
        hub._on_reply(_reply_env(parts=[TextPart(text="late")]), "cid-1", _headers("return"))
    assert "no pending handle" in caplog.text  # the late reply finds no handle


# ── end-to-end via a TestKafkaBroker (drives hub.register's groupless subscriber) ──


async def test_register_routes_a_published_return_to_the_tracked_handle() -> None:
    from faststream.kafka import KafkaBroker, TestKafkaBroker

    from calfkit.client.middleware import ContextInjectionMiddleware

    hub = _Hub()
    # ContextInjectionMiddleware populates correlation_id into the context scope so the handler's
    # Context()-bound correlation_id resolves (mirrors what connect() wires — verified necessary).
    broker = KafkaBroker(middlewares=[ContextInjectionMiddleware])
    hub.register(broker, "inbox.topic")
    handle = _tracked(hub, "cid-99")
    async with TestKafkaBroker(broker):
        await broker.publish(
            _reply_env(parts=[TextPart(text="from-broker")]),
            "inbox.topic",
            correlation_id="cid-99",
            headers=_headers("return"),
        )
    ev = await _terminal(handle)
    assert isinstance(ev, RunCompleted)
    assert ev.output == "from-broker"
    assert ev.correlation_id == "cid-99"


async def test_register_routes_a_published_fault_to_the_tracked_handle() -> None:
    # Proves a FaultMessage survives the JSON wire round-trip + the discriminated reply union, then
    # classifies as a fault end-to-end (the return-path e2e exercises only ReturnMessage).
    from faststream.kafka import KafkaBroker, TestKafkaBroker

    from calfkit.client.middleware import ContextInjectionMiddleware

    hub = _Hub()
    broker = KafkaBroker(middlewares=[ContextInjectionMiddleware])
    hub.register(broker, "inbox.topic")
    handle = _tracked(hub, "cid-f")
    async with TestKafkaBroker(broker):
        await broker.publish(
            _reply_env(error=ErrorReport(error_type="billing.quota")),
            "inbox.topic",
            correlation_id="cid-f",
            headers=_headers("fault"),
        )
    ev = await _terminal(handle)
    assert isinstance(ev, RunFailed)
    assert ev.report.error_type == "billing.quota"


# ── the Option-B undecodable-sink seam, wired floor → hub.fail_run → channel (spec §5.8) ──


async def test_inbox_undecodable_floors_to_a_run_failed_in_the_tracked_channel() -> None:
    from faststream.kafka import KafkaBroker, TestKafkaBroker
    from pydantic import ValidationError

    from calfkit.client.middleware import ContextInjectionMiddleware, DecodeFloorMiddleware

    hub = _Hub()
    inbox = "inbox.topic"
    broker = KafkaBroker(middlewares=[DecodeFloorMiddleware.builder({inbox: hub.fail_run}), ContextInjectionMiddleware])
    hub.register(broker, inbox)
    handle = _tracked(hub, "cid-u")
    async with TestKafkaBroker(broker):
        with pytest.raises(ValidationError):  # the floor re-raises after handing the report to the sink
            await broker.publish(b'{"bad": "shape"}', inbox, correlation_id="cid-u", headers={HDR_WIRE: "envelope"})
    ev = await _terminal(handle)
    assert isinstance(ev, RunFailed)
    assert ev.report.error_type == FaultTypes.DELIVERY_UNDECODABLE  # result() will raise NodeFaultError, not hang


async def test_node_topic_undecodable_does_not_touch_a_same_cid_client_run() -> None:
    # A co-located node-hop undecodable carries the run's correlation_id but lands on a NODE topic with
    # no registered sink → topic-key scoping keeps it out of the client hub (no cid-conflation, §5.8).
    from faststream.kafka import KafkaBroker, TestKafkaBroker
    from pydantic import ValidationError

    from calfkit.client.middleware import ContextInjectionMiddleware, DecodeFloorMiddleware

    hub = _Hub()
    inbox = "inbox.topic"
    broker = KafkaBroker(middlewares=[DecodeFloorMiddleware.builder({inbox: hub.fail_run}), ContextInjectionMiddleware])

    @broker.subscriber("node.topic")  # a node hop — NOT the client inbox
    async def node(envelope: Envelope) -> None: ...

    hub.register(broker, inbox)
    handle = _tracked(hub, "cid-shared")  # same cid as the node-hop undecodable
    async with TestKafkaBroker(broker):
        with pytest.raises(ValidationError):
            await broker.publish(b'{"not": "an envelope"}', "node.topic", correlation_id="cid-shared")
    assert not handle._channel.closed  # the client run is untouched — no cid-conflation across topics


# ── Commit 3: the firehose — one reader tee'd to bounded, best-effort drop-oldest outlets (§5.4) ──


async def test_open_events_stream_receives_a_dispatched_event() -> None:
    hub = _Hub()
    handle = _tracked(hub, "cid-1")
    async with EventStream(hub, terminal_only=False, buffer_size=8) as stream:
        hub._on_reply(_reply_env(parts=[TextPart(text="hi")]), "cid-1", _headers("return"))
        ev = await asyncio.wait_for(anext(stream), timeout=1.0)
    assert isinstance(ev, RunCompleted)
    assert ev.correlation_id == "cid-1"
    assert ev.output == "hi"
    assert handle.correlation_id == "cid-1"  # keep the run alive to here


async def test_firehose_drops_oldest_keeps_newest_and_counts_drops_with_a_warning_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    hub = _Hub()
    async with EventStream(hub, terminal_only=False, buffer_size=2) as stream:
        with caplog.at_level(logging.WARNING, logger=_EVENTS_LOGGER):
            for i in range(5):  # 5 events into a size-2 outlet → the 3 oldest are dropped
                hub._on_reply(_reply_env(parts=[TextPart(text=str(i))]), f"c-{i}", _headers("return"))
        assert stream.dropped == 3  # signaled, never silent (spec §5.4)
        kept = [(await asyncio.wait_for(anext(stream), timeout=1.0)).output for _ in range(2)]
        assert kept == ["3", "4"]  # drop-oldest / keep-newest-N — a catching-up reader sees the latest
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING and r.name == _EVENTS_LOGGER]
    assert len(warnings) == 1  # WARNING once per outlet, then rate-limited (never per-drop spam)


async def test_undrained_firehose_never_blocks_the_per_run_channel() -> None:
    # CRITICAL standing regression (ADR-0023): a slow/undrained firehose drops its OWN oldest events;
    # it must never stall the hub. Open events(), never drain it, flood the inbox far past its buffer —
    # a tracked run's terminal still pushes and resolves. (A blocking outlet would deadlock this.)
    hub = _Hub()
    async with EventStream(hub, terminal_only=False, buffer_size=4) as _firehose:  # never drained
        for i in range(100):  # flood 25x the buffer with foreign returns
            hub._on_reply(_reply_env(parts=[TextPart(text=str(i))]), f"foreign-{i}", _headers("return"))
        handle = _tracked(hub, "cid-real")
        hub._on_reply(_reply_env(parts=[TextPart(text="done")]), "cid-real", _headers("return"))
        ev = await asyncio.wait_for(handle._channel.await_terminal(), timeout=1.0)
    assert ev.output == "done"  # the lossless per-run channel resolved despite the overflowing firehose
    assert _firehose.dropped >= 96  # the firehose shed its own oldest (best-effort), nobody else's


async def test_terminal_only_yields_terminal_events() -> None:
    hub = _Hub()
    async with EventStream(hub, terminal_only=True, buffer_size=8) as stream:
        hub._on_reply(_reply_env(parts=[TextPart(text="t")]), "c-1", _headers("return"))
        ev = await asyncio.wait_for(anext(stream), timeout=1.0)
    assert isinstance(ev, RunCompleted)  # a terminal passes the filter (v1: every event is terminal)


async def test_two_open_streams_each_receive_every_event() -> None:
    hub = _Hub()
    async with EventStream(hub, buffer_size=8) as s1, EventStream(hub, buffer_size=8) as s2:
        hub._on_reply(_reply_env(parts=[TextPart(text="x")]), "c-1", _headers("return"))
        e1 = await asyncio.wait_for(anext(s1), timeout=1.0)
        e2 = await asyncio.wait_for(anext(s2), timeout=1.0)
    assert e1.output == "x" and e2.output == "x"  # one read tee'd to every open outlet (§5.4)


async def test_exited_stream_is_unregistered_and_receives_no_more_events() -> None:
    hub = _Hub()
    stream = EventStream(hub, buffer_size=8)
    async with stream:
        pass  # enter registers the outlet; exit unregisters + the outlet is dropped
    assert stream not in hub._firehose
    hub._on_reply(_reply_env(parts=[TextPart(text="late")]), "c-1", _headers("return"))
    assert len(stream._buffer) == 0  # a post-exit event is never offered to the closed stream


async def test_firehose_surfaces_a_fault_as_a_raw_run_failed() -> None:
    hub = _Hub()
    async with EventStream(hub, buffer_size=8) as stream:
        hub._on_reply(_reply_env(error=ErrorReport(error_type="x.y")), "c-1", _headers("fault"))
        ev = await asyncio.wait_for(anext(stream), timeout=1.0)
    assert isinstance(ev, RunFailed)  # raw — the caller maps/branches itself (§4.5)
    assert ev.report.error_type == "x.y"


async def test_firehose_surfaces_a_reply_with_no_pending_handle() -> None:
    # The send()/shared-inbox case: a reply this client registered no handle for is still observable on
    # the firehose (demuxed by the caller via correlation_id) — that is how send() results are seen.
    hub = _Hub()  # nothing tracked
    async with EventStream(hub, buffer_size=8) as stream:
        hub._on_reply(_reply_env(parts=[TextPart(text="foreign")]), "foreign-cid", _headers("return"))
        ev = await asyncio.wait_for(anext(stream), timeout=1.0)
    assert ev.correlation_id == "foreign-cid"


# ── Commit 4: the handle — result()/stream() over the channel, typed outcomes (§4.3/§4.4/§5.9) ──


async def test_result_projects_a_completed_terminal_to_an_invocation_result() -> None:
    hub = _Hub()
    handle = _tracked(hub, "cid-1")  # _output_type defaults to str
    hub._on_reply(_reply_env(parts=[TextPart(text="hi")]), "cid-1", _headers("return"))
    res = await asyncio.wait_for(handle.result(), timeout=1.0)
    assert res.output == "hi"  # str default → first TextPart.text
    assert res.correlation_id == "cid-1"


async def test_result_raises_node_fault_error_verbatim_on_a_fault_terminal() -> None:
    hub = _Hub()
    handle = _tracked(hub, "cid-1")
    report = ErrorReport(error_type="billing.quota", message="over limit")
    hub._on_reply(_reply_env(error=report), "cid-1", _headers("fault"))
    with pytest.raises(NodeFaultError) as exc:
        await asyncio.wait_for(handle.result(), timeout=1.0)
    assert exc.value.report is report  # the ErrorReport wrapped verbatim (§5.9)
    assert exc.value.report.find("billing.quota") is report  # branch via find(), never ==


async def test_result_timeout_raises_client_timeout_error_and_the_run_survives() -> None:
    hub = _Hub()
    handle = _tracked(hub, "cid-1")  # no terminal pushed yet
    with pytest.raises(ClientTimeoutError) as exc:
        await handle.result(timeout=0.05)  # this client stops waiting after 50ms
    assert exc.value.correlation_id == "cid-1"
    # the run is unaffected — a later terminal still resolves a fresh result()
    hub._on_reply(_reply_env(parts=[TextPart(text="late")]), "cid-1", _headers("return"))
    res = await asyncio.wait_for(handle.result(), timeout=1.0)
    assert res.output == "late"


async def test_result_raises_deserialization_error_on_a_present_but_mismatched_part() -> None:
    # output_type=int but the reply carries a TextPart (no DataPart) → a projection mismatch on a
    # SUCCESS is a DeserializationError, never disguised as a fault (discriminator before projection).
    hub = _Hub()
    handle: InvocationHandle[int] = InvocationHandle(correlation_id="cid-1", _channel=_RunChannel(), _output_type=int)
    hub.track(handle)
    hub._on_reply(_reply_env(parts=[TextPart(text="not-an-int")]), "cid-1", _headers("return"))
    with pytest.raises(DeserializationError):
        await asyncio.wait_for(handle.result(), timeout=1.0)


async def test_result_is_idempotent_when_called_twice() -> None:
    hub = _Hub()
    handle = _tracked(hub, "cid-1")
    hub._on_reply(_reply_env(parts=[TextPart(text="once")]), "cid-1", _headers("return"))
    first = await asyncio.wait_for(handle.result(), timeout=1.0)
    second = await asyncio.wait_for(handle.result(), timeout=1.0)  # cached terminal — replayable (§4.4)
    assert first.output == second.output == "once"


async def test_stream_yields_the_terminal_raw_as_its_single_v1_element() -> None:
    hub = _Hub()
    handle = _tracked(hub, "cid-1")
    hub._on_reply(_reply_env(parts=[TextPart(text="hi")]), "cid-1", _headers("return"))
    events = [ev async for ev in handle.stream()]
    assert len(events) == 1  # v1: the fabric emits only the terminal (spec §3.1)
    assert isinstance(events[0], RunCompleted)
    assert events[0].output == "hi"  # stream() yields the RAW RunEvent (result() projects)


async def test_stream_then_result_reads_the_cached_terminal() -> None:
    hub = _Hub()
    handle = _tracked(hub, "cid-1")
    hub._on_reply(_reply_env(parts=[TextPart(text="x")]), "cid-1", _headers("return"))
    streamed = [ev async for ev in handle.stream()]
    res = await asyncio.wait_for(handle.result(), timeout=1.0)  # O(1) cached terminal (§4.4)
    assert streamed[0].correlation_id == "cid-1"
    assert res.output == "x"


async def test_result_then_stream_replays_the_cached_terminal() -> None:
    hub = _Hub()
    handle = _tracked(hub, "cid-1")
    hub._on_reply(_reply_env(parts=[TextPart(text="x")]), "cid-1", _headers("return"))
    await asyncio.wait_for(handle.result(), timeout=1.0)
    replayed = [ev async for ev in handle.stream()]  # terminal is replayable (§4.4)
    assert len(replayed) == 1
    assert replayed[0].output == "x"


async def test_second_concurrent_stream_raises_runtime_error() -> None:
    hub = _Hub()
    handle = _tracked(hub, "cid-1")
    s1 = handle.stream()
    task = asyncio.create_task(anext(s1))  # starts iterating s1 → it parks on await_terminal
    await asyncio.sleep(0)  # let s1 register as the live listener
    with pytest.raises(RuntimeError):
        await anext(handle.stream())  # a second concurrent stream() → RuntimeError (≤1 live, §4.4)
    hub._on_reply(_reply_env(parts=[TextPart(text="x")]), "cid-1", _headers("return"))
    assert (await asyncio.wait_for(task, timeout=1.0)).output == "x"  # s1 still completes


async def test_result_raises_node_fault_error_on_an_undecodable_inbox_reply() -> None:
    # The Option-B seam end-to-end at the handle: fail_run(undecodable) → result() raises NodeFaultError
    # — never hangs, never DeserializationError (§5.8/§5.9).
    hub = _Hub()
    handle = _tracked(hub, "cid-1")
    report = ErrorReport.build_safe(error_type=FaultTypes.DELIVERY_UNDECODABLE, message="bad body")
    hub.fail_run("cid-1", report)
    with pytest.raises(NodeFaultError) as exc:
        await asyncio.wait_for(handle.result(), timeout=1.0)
    assert exc.value.report.find(FaultTypes.DELIVERY_UNDECODABLE) is not None


async def test_result_raises_node_fault_error_on_a_malformed_terminal() -> None:
    hub = _Hub()
    handle = _tracked(hub, "cid-1")
    hub._on_reply(_env(), "cid-1", _headers("return"))  # kind=return but reply=None → malformed
    with pytest.raises(NodeFaultError) as exc:
        await asyncio.wait_for(handle.result(), timeout=1.0)
    assert exc.value.report.error_type == FaultTypes.DELIVERY_MALFORMED


async def test_result_raises_client_closed_error_after_close() -> None:
    hub = _Hub()
    handle = _tracked(hub, "cid-1")
    hub.close()  # aclose() → close_with(ClientClosedError)
    with pytest.raises(ClientClosedError):
        await asyncio.wait_for(handle.result(), timeout=1.0)
