"""Increments C + E — the inbox subscriber's two filtered call-items (step-streaming spec §2.4 / §3.4).

The single groupless inbox subscriber carries TWO filtered call-items: the envelope reply handler
(``x-calf-wire == "envelope"``) and the step handler (``== "step"``, with the lenient decoder). The
filter runs BEFORE body decode, so a foreign/unstamped body matches neither and is dropped
(``SubscriberNotFound``) without triggering ``Envelope`` validation; a step body routes to the step
handler and its events reach the owning run's channel. (Envelope reply routing + the decode floor are
regression-covered by the now-stamped fixtures in ``test_caller_surface_hub.py``; the worker-side
``SubscriberNotFound``-*survival* is the kafka lane — ``TestKafkaBroker`` re-raises it.)
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from faststream.exceptions import SubscriberNotFound
from faststream.kafka import KafkaBroker, TestKafkaBroker

from calfkit._protocol import HDR_WIRE
from calfkit.client.events import EventStream
from calfkit.client.hub import InvocationHandle, _Hub, _RunChannel, lenient_step_decoder
from calfkit.client.middleware import ContextInjectionMiddleware
from calfkit.models.payload import TextPart
from calfkit.models.step import AgentMessageEvent, AgentMessageStep, AgentThinkingStep, StepMessage


def _tracked(hub: _Hub, cid: str) -> InvocationHandle:
    handle = InvocationHandle(correlation_id=cid, _channel=_RunChannel())
    hub.track(handle)
    return handle


def _step(cid: str) -> StepMessage:
    return StepMessage(
        correlation_id=cid,
        emitter="agent.x",
        depth=2,
        frame_id="f1",
        events=[AgentMessageStep(parts=[TextPart(text="thinking out loud")])],
    )


async def test_unstamped_body_is_dropped_not_envelope_decoded() -> None:
    # A foreign/unstamped body (no x-calf-wire) matches NEITHER call-item → SubscriberNotFound; and the
    # filter runs before decode, so it never triggers Envelope validation (the no-false-fault property).
    hub = _Hub()
    broker = KafkaBroker(middlewares=[ContextInjectionMiddleware])
    hub.register(broker, "inbox.topic")
    handle = _tracked(hub, "cid-u")
    async with TestKafkaBroker(broker):
        with pytest.raises(SubscriberNotFound):
            await broker.publish(_step("cid-u"), "inbox.topic", correlation_id="cid-u")  # NO x-calf-wire
    assert not handle._channel.closed  # untouched: not routed, not floored


async def test_step_body_routes_to_the_step_handler_and_reaches_the_handle() -> None:
    # A step-stamped body routes to the step call-item (lenient decode → _on_step → push_intermediate),
    # so the owning run's channel receives the step event — NOT the envelope reply handler, and with no
    # Envelope ValidationError (the filter sends it to the step handler before any envelope decode).
    hub = _Hub()
    broker = KafkaBroker(middlewares=[ContextInjectionMiddleware])
    hub.register(broker, "inbox.topic")
    handle = _tracked(hub, "cid-s")
    async with TestKafkaBroker(broker):
        await broker.publish(_step("cid-s"), "inbox.topic", correlation_id="cid-s", headers={HDR_WIRE: "step"})
    intermediates = list(handle._channel._intermediates)
    assert len(intermediates) == 1
    assert isinstance(intermediates[0], AgentMessageEvent)
    assert intermediates[0].parts[0].text == "thinking out loud"
    assert intermediates[0].correlation_id == "cid-s"  # identity stamped onto the surface event by _to_surface
    assert not handle._channel.closed  # a step does NOT close the run (only a terminal does)


async def test_malformed_step_over_the_broker_does_not_fault_a_tracked_run() -> None:
    # C1 end-to-end (the headline reception guard): a malformed body STAMPED x-calf-wire=step routes to the
    # step call-item (filter before decode), the lenient decoder swallows it to None, and _on_step(None)
    # drops it — the tracked run is NOT faulted. A StepMessage-typed handler (instead of `| None`) would
    # re-validate the sentinel → decode floor → fail_run; this pins that it does not.
    hub = _Hub()
    broker = KafkaBroker(middlewares=[ContextInjectionMiddleware])
    hub.register(broker, "inbox.topic")
    handle = _tracked(hub, "cid-bad")
    async with TestKafkaBroker(broker):
        await broker.publish(b'{"not": "a valid step"}', "inbox.topic", correlation_id="cid-bad", headers={HDR_WIRE: "step"})
    assert handle._channel.closed is False  # the malformed step did NOT fault the run
    assert list(handle._channel._intermediates) == []  # nothing surfaced


async def test_lenient_step_decoder_decodes_valid_and_swallows_malformed() -> None:
    # The per-call-item decoder: a valid step body decodes; a malformed/undecodable body is swallowed to
    # None (kept off the decode floor's sink, so a bad step never faults the run, §3.4).
    decoded = await lenient_step_decoder(SimpleNamespace(body=_step("c").model_dump_json().encode()))
    assert isinstance(decoded, StepMessage) and decoded.correlation_id == "c"
    assert await lenient_step_decoder(SimpleNamespace(body=b'{"bad": "shape"}')) is None
    assert await lenient_step_decoder(SimpleNamespace(body=b"not json at all")) is None


def test_no_handle_step_drops_silently_but_tees_to_firehose() -> None:
    # A step for a run with NO live handle (a send() run or a GC'd handle) drops silently from the per-run
    # path (no raise, no WARN) but is STILL tee'd to the firehose, so send() runs stay observable (§3.4).
    hub = _Hub()
    outlet = EventStream(hub)
    hub._add_outlet(outlet)
    step = _step("no-such-run")  # no tracked handle for this correlation_id
    hub._on_step(step)  # must not raise
    teed = list(outlet._buffer)  # the firehose holds the MAPPED surface events (not the wire *Step)
    assert len(teed) == 1 and isinstance(teed[0], AgentMessageEvent)
    assert teed[0].parts[0].text == "thinking out loud" and teed[0].correlation_id == "no-such-run"


def test_on_step_drops_a_foreign_agent_thinking_event() -> None:
    # AgentThinkingStep is decodable on the wire (defined) but NOT surfaced in v1 (§5): _on_step filters it
    # (its kind is absent from _SURFACE_BY_KIND), so even a foreign producer's thinking event never reaches
    # the run's channel or the firehose (enforced on receive).
    hub = _Hub()
    handle = _tracked(hub, "cid-t")
    step = StepMessage(
        correlation_id="cid-t", emitter="a", depth=2, frame_id="f", events=[AgentThinkingStep(parts=[TextPart(text="secret thoughts")])]
    )
    hub._on_step(step)
    assert list(handle._channel._intermediates) == []  # filtered, not surfaced
