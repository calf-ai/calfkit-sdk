"""Suite O — oversized-fault strip-and-retry against a real per-topic size limit.

When a fault's serialized envelope exceeds the callback topic's ``max.message.bytes``, the
publish raises ``MessageSizeTooLargeError``; the rail strips the report to its identity
(``to_minimal()`` — no ``causes`` / ``details`` / ``frame_chain``) and retries once, so an
oversized fault still reaches the caller instead of becoming a new silent drop (FR-21).

This drives that against a REAL broker size limit: the client's reply topic (the agent's
callback) is created with a small ``max.message.bytes`` so a fault carrying a large
``details`` blob trips the limit on the first publish; the minimal retry then fits. The tap
reads the reply topic (the callback) — where the *stripped* report lands (the
``publish_topic`` mirror, on a default-sized topic, keeps the full report).

Opt-in (``-m kafka`` / ``make test-kafka``); skips cleanly without Docker.
"""

from __future__ import annotations

import pytest

from calfkit._vendor.pydantic_ai import models
from calfkit._vendor.pydantic_ai.messages import ToolCallPart
from calfkit.client import Client
from calfkit.nodes import Agent
from tests.integration._fault_kafka import ensure_topic, fault_worker
from tests.integration._fault_tap import fault_tap
from tests.integration._fault_tools import oversized_fault
from tests.integration._roundtrip_helpers import scripted_model

pytestmark = pytest.mark.kafka
models.ALLOW_MODEL_REQUESTS = True


async def test_oversized_fault_strips_to_minimal_and_still_arrives(kafka_bootstrap: str, topic_namespace: str) -> None:
    """O-1: a fault too large for the callback topic is stripped to a minimal report and
    retried, so it still reaches the caller — identity preserved, ``details``/``causes``/
    ``frame_chain`` dropped."""
    reply_topic = f"{topic_namespace}.o1.reply"
    agent_in = f"{topic_namespace}.o1.input"
    # Constrain the callback (client reply) topic so the full ~8 KB fault overflows it but
    # the minimal report fits. (The agent's own return topic + publish mirror stay default.)
    await ensure_topic(kafka_bootstrap, reply_topic, config={"max.message.bytes": "4096"})

    agent = Agent(
        f"{topic_namespace}-o1",
        system_prompt="call oversized_fault",
        subscribe_topics=agent_in,
        model_client=scripted_model([ToolCallPart("oversized_fault", {"x": 1}, tool_call_id="c1")]),
        tools=[oversized_fault],
        sequential_only_mode=True,
    )
    driver = Client.connect(kafka_bootstrap, inbox_topic=reply_topic)
    worker = fault_worker(kafka_bootstrap, nodes=[agent, oversized_fault])

    try:
        async with worker, fault_tap(kafka_bootstrap, reply_topic) as tap:
            await driver.agent(topic=agent_in).start("go")

            fault, _ = await tap.next_fault(timeout=60)
            # identity survives; the heavy parts were stripped to fit
            assert fault.error.error_type == "billing.oversized"
            assert "blob" not in fault.error.details
            assert fault.error.causes == []
            assert fault.error.frame_chain == []
    finally:
        await driver.aclose()
        await worker._client.aclose()
