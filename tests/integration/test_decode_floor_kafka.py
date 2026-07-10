"""Suite D — the decode floor over the real broker.

A message whose body fails to decode never reaches a handler; the broker-wide
``DecodeFloorMiddleware`` (installed by ``Client.connect``, so every worker subscriber
and the client reply subscriber are covered) floors a typed ``calf.delivery.undecodable``
event at ERROR and re-raises — never a silent drop (catalogue FR-26/FR-27):

* **D-1** — a malformed body to a live agent's input topic is floored; the worker keeps
  consuming, proven by a valid invocation that completes afterwards.
* **D-2** — a malformed body on the client's inbox with **no surviving correlation id** is floored
  and cannot be routed to any run, so it does **not** resolve the pending run; the client's
  ``result(timeout=)`` surfaces a typed ``ClientTimeoutError`` rather than resolving with garbage.

Opt-in (``-m kafka`` / ``make test-kafka``); skips cleanly without Docker.
"""

from __future__ import annotations

import logging

import pytest
from aiokafka import AIOKafkaProducer

from calfkit._protocol import HDR_WIRE
from calfkit._vendor.pydantic_ai import models
from calfkit._vendor.pydantic_ai.messages import ToolCallPart
from calfkit.client import Client
from calfkit.exceptions import ClientTimeoutError
from calfkit.models.error_report import FaultTypes
from calfkit.nodes import Agent
from tests.integration._fault_kafka import ensure_topic, fault_worker
from tests.integration._fault_tools import ok_a
from tests.integration._roundtrip_helpers import FINAL_OUTPUT, scripted_model

pytestmark = pytest.mark.kafka
models.ALLOW_MODEL_REQUESTS = True

_MW_LOGGER = "calfkit.client.middleware"


async def test_undecodable_body_floored_and_worker_survives(kafka_bootstrap: str, topic_namespace: str, caplog: pytest.LogCaptureFixture) -> None:
    """D-1: a malformed body on an agent's input topic floors ``calf.delivery.undecodable``
    (ERROR) and is dropped; the worker keeps consuming and a valid invocation completes."""
    agent_in = f"{topic_namespace}.d1.input"
    agent = Agent(
        f"{topic_namespace}-d1",
        system_prompt="call ok_a",
        subscribe_topics=agent_in,
        model_client=scripted_model([ToolCallPart("ok_a", {}, tool_call_id="c1")]),
        tools=[ok_a],
    )
    await ensure_topic(kafka_bootstrap, agent_in)
    driver = Client.connect(kafka_bootstrap)
    worker = fault_worker(kafka_bootstrap, nodes=[agent, ok_a])

    try:
        with caplog.at_level(logging.ERROR, logger=_MW_LOGGER):
            async with worker:
                producer = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap)
                await producer.start()
                try:
                    # Stamp x-calf-wire=envelope so the body passes the wire filter and reaches the
                    # Envelope decoder (where it fails → floored). An UNSTAMPED body is filtered before
                    # the floor (SubscriberNotFound) — the floor scopes to stamped-but-undecodable bodies.
                    await producer.send_and_wait(agent_in, b'{"not": "a valid envelope"}', headers=[(HDR_WIRE, b"envelope")])
                finally:
                    await producer.stop()

                result = await driver.agent(topic=agent_in).execute("go", timeout=60)
                assert result.output is not None and FINAL_OUTPUT in result.output

        assert FaultTypes.DELIVERY_UNDECODABLE in caplog.text
    finally:
        await driver.aclose()
        await worker._client.aclose()


async def test_undecodable_reply_with_no_correlation_id_floors_and_does_not_resolve_the_run(
    kafka_bootstrap: str, topic_namespace: str, caplog: pytest.LogCaptureFixture
) -> None:
    """D-2: a malformed body on the client's inbox with **no surviving correlation id** is floored at
    ERROR and cannot be routed to any run (the floor's ERROR-log is the whole story, §5.8), so the run
    never resolves and the client's ``result(timeout=)`` patience surfaces a typed ``ClientTimeoutError``
    rather than resolving with garbage. (A cid-bearing undecodable reply instead raises
    ``NodeFaultError(calf.delivery.undecodable)`` via the seam — covered offline by the hub suite.)"""
    inbox_topic = f"{topic_namespace}.d2.reply"
    await ensure_topic(kafka_bootstrap, inbox_topic)
    # No worker runs, so a real reply never arrives; the only thing on the inbox is the malformed
    # message we inject (with no correlation id). A short result(timeout=) bounds the (correct) hang.
    driver = Client.connect(kafka_bootstrap, inbox_topic=inbox_topic)

    try:
        with caplog.at_level(logging.ERROR, logger=_MW_LOGGER):
            handle = await driver.agent(topic=f"{topic_namespace}.d2.input").start("go")

            producer = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap)
            await producer.start()
            try:
                # Stamp x-calf-wire=envelope (else the inbox's envelope/step filters drop it before the floor).
                await producer.send_and_wait(inbox_topic, b'{"garbage": true}', headers=[(HDR_WIRE, b"envelope")])
            finally:
                await producer.stop()

            with pytest.raises(ClientTimeoutError):
                await handle.result(timeout=3.0)  # floored + unroutable (no cid) → never resolved

        assert FaultTypes.DELIVERY_UNDECODABLE in caplog.text
    finally:
        await driver.aclose()
