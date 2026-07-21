"""Increment B — the universal ``x-calf-wire="envelope"`` produce-side stamp (spec §2.4).

Once the consume-side filter lands (increment C, strict positive, no absent-fallback), every
calfkit publish must carry ``x-calf-wire`` or its run hangs. The stamp lives at exactly two
production sites: ``_headers()`` (the sole node-rail header builder) and the client ingress dict.
"""

from __future__ import annotations

from typing import Annotated, Any

from faststream import Context
from faststream.kafka import TestKafkaBroker

from calfkit._protocol import HDR_WIRE
from calfkit.client.caller import Client
from calfkit.models import Envelope
from calfkit.nodes.base import BaseNodeDef


class TestNodeRailStamp:
    def test_headers_carry_envelope_wire_for_every_kind(self) -> None:
        node = BaseNodeDef(node_id="orchestrator", subscribe_topics=["in"])
        for kind in ("call", "return", "fault"):
            assert node._headers(kind, task_id="task-under-test")[HDR_WIRE] == "envelope"

    def test_stamp_uses_the_envelope_wire_constant(self) -> None:
        # single source of truth — the stamp is the ClassVar, not a stray literal.
        node = BaseNodeDef(node_id="n", subscribe_topics=["in"])
        assert node._headers("call", task_id="task-under-test")[HDR_WIRE] == Envelope.WIRE


class TestClientIngressStamp:
    async def test_outbound_dispatch_carries_envelope_wire(self) -> None:
        client = Client.connect("localhost:9092", inbox_topic="inbox")
        captured: list[dict[str, Any]] = []

        @client._broker.subscriber("agent.summarizer.private.input")
        async def agent_node(env: Envelope, headers: Annotated[dict[str, Any], Context("message.headers")]) -> None:
            captured.append(dict(headers))

        async with TestKafkaBroker(client._broker):
            await client.agent("summarizer", output_type=str).start("hello", correlation_id="cid-1")
        assert captured, "the outbound dispatch did not reach the agent's private input topic"
        assert captured[0][HDR_WIRE] == "envelope"
