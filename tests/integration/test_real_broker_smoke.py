"""Smoke test that wires up the real-broker (``kafka``) integration lane.

Validates the lane end-to-end against a REAL Redpanda broker:

* the ``kafka`` marker selects this module under ``-m kafka`` even though
  ``addopts`` deselects ``kafka`` by default (proves the CLI ``-m`` override),
* the ``kafka_bootstrap`` fixture yields a *reachable* broker, and
* a produce -> consume round-trip over a ``topic_namespace``-scoped topic
  actually flows.

It also shows the minimal shape every new real-broker suite uses: mark the
module ``kafka`` and take ``kafka_bootstrap`` (plus ``topic_namespace`` for
per-test isolation). Richer calfkit-level usage against a live broker lives in
``test_mcp_toolbox_capability.py`` and ``test_topic_provisioning.py``.
"""

from __future__ import annotations

import asyncio

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient

pytestmark = pytest.mark.kafka


async def test_broker_is_reachable(kafka_bootstrap: str) -> None:
    """The fixture yields a broker whose cluster metadata is queryable."""
    admin = AIOKafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    await admin.start()
    try:
        # The call succeeding is the proof of reachability; a fresh broker may
        # report only internal topics, which is fine.
        await admin.list_topics()
    finally:
        await admin.close()


async def test_produce_consume_round_trip(kafka_bootstrap: str, topic_namespace: str) -> None:
    """A message produced to a namespaced topic is consumed back, byte-for-byte."""
    topic = f"{topic_namespace}.roundtrip"
    payload = b"calf-redpanda-roundtrip"

    producer = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap)
    await producer.start()
    try:
        # send_and_wait blocks until the broker acks; with dev-container
        # auto-create the topic springs into existence here.
        await producer.send_and_wait(topic, payload)
    finally:
        await producer.stop()

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=kafka_bootstrap,
        auto_offset_reset="earliest",
        group_id=f"{topic_namespace}.group",
    )
    await consumer.start()
    try:
        record = await asyncio.wait_for(consumer.getone(), timeout=30.0)
        assert record.value == payload
    finally:
        await consumer.stop()
