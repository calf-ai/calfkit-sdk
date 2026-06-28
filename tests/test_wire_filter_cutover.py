"""Increment C — the consume-side envelope wire-filter cutover (step-streaming spec §2.4).

The hard cutover: every Worker-registered node handler (worker.py) and the client hub's inbox
reply handler (hub.py) gain ``filter=wire_filter(Envelope)``. The filter runs in ``is_suitable``
BEFORE the body is decoded, so a non-envelope body on a filtered subscriber is rejected without
triggering ``Envelope`` validation (no decode-floor false-fault). Envelope routing + the floor on a
stamped-but-malformed envelope are regression-covered by the now-stamped fixtures in
``test_caller_surface_hub.py`` / ``test_consumer.py``; the worker-side ``SubscriberNotFound``
*survival* is the kafka lane (``TestKafkaBroker`` re-raises instead of swallowing it).
"""

from __future__ import annotations

import pytest
from faststream.exceptions import SubscriberNotFound
from faststream.kafka import KafkaBroker, TestKafkaBroker

from calfkit._protocol import HDR_WIRE
from calfkit.client.hub import InvocationHandle, _Hub, _RunChannel
from calfkit.client.middleware import ContextInjectionMiddleware
from calfkit.models.payload import TextPart
from calfkit.models.step import AgentMessage, StepMessage


def _tracked(hub: _Hub, cid: str) -> InvocationHandle:
    handle = InvocationHandle(correlation_id=cid, _channel=_RunChannel())
    hub.track(handle)
    return handle


async def test_step_body_is_filtered_before_envelope_decode() -> None:
    # A step-stamped body on the inbox is rejected by the envelope filter BEFORE decode →
    # SubscriberNotFound, NOT an Envelope ValidationError (the no-false-fault property, spec §2.4).
    # (Before E registers the step call-item, a step body simply finds no matching handler here.)
    hub = _Hub()
    broker = KafkaBroker(middlewares=[ContextInjectionMiddleware])
    hub.register(broker, "inbox.topic")
    handle = _tracked(hub, "cid-step")
    step = StepMessage(
        correlation_id="cid-step",
        emitter="agent.x",
        depth=2,
        frame_id="f1",
        events=[AgentMessage(parts=[TextPart(text="thinking out loud")])],
    )
    async with TestKafkaBroker(broker):
        with pytest.raises(SubscriberNotFound):
            await broker.publish(step, "inbox.topic", correlation_id="cid-step", headers={HDR_WIRE: "step"})
    assert not handle._channel.closed  # not routed, not floored — the run is untouched
