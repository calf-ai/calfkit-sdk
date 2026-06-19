"""Suite M — the ``calf.retry`` marker round-trips into a real agent loop.

A tool's ``ModelRetry`` is a *recoverable* failure, not a fault: it is rendered at origin
to a ``calf.retry``-marked ``TextPart`` that rides the reply slot as an ordinary
``return``, and the calling agent materializes it back into a ``RetryPromptPart`` so the
model sees the retry and adapts (catalogue XC-4). This exercises that whole round-trip
over a real broker — the marker surviving the wire and re-entering the agent loop.

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
from tests.integration._fault_tools import needs_retry
from tests.integration._roundtrip_helpers import FINAL_OUTPUT, retry_prompt_texts, scripted_model

pytestmark = pytest.mark.kafka
models.ALLOW_MODEL_REQUESTS = True


async def test_model_retry_round_trips_as_calf_retry_not_a_fault(kafka_bootstrap: str, topic_namespace: str) -> None:
    """M-1: a tool raising ``ModelRetry`` comes back as a ``calf.retry``-marked return
    (not a fault); the agent materializes a ``RetryPromptPart`` the model sees, then
    finalizes — and nothing is mirrored on the fault rail."""
    agent_in = f"{topic_namespace}.m1.input"
    agent_pub = f"{topic_namespace}.m1.mirror"
    agent = Agent(
        f"{topic_namespace}-m1",
        system_prompt="call needs_retry",
        subscribe_topics=agent_in,
        publish_topic=agent_pub,
        model_client=scripted_model([ToolCallPart("needs_retry", {"x": -1}, tool_call_id="c1")]),
        tools=[needs_retry],
        sequential_only_mode=True,
    )
    await ensure_topic(kafka_bootstrap, agent_pub)
    driver = Client.connect(kafka_bootstrap)
    worker = fault_worker(kafka_bootstrap, nodes=[agent, needs_retry])

    try:
        async with worker, fault_tap(kafka_bootstrap, agent_pub) as tap:
            result = await driver.execute("go", agent_in, timeout=60)

            # The agentic loop resumed past the retry to a clean finalization (a return,
            # not a fault) — the recoverable failure stayed model-visible.
            assert result.output is not None and FINAL_OUTPUT in result.output

            # The ModelRetry text crossed the wire (calf.retry marker) and materialized
            # back into a RetryPromptPart the model saw.
            retries = retry_prompt_texts(result.message_history)
            assert any("positive x" in text for text in retries)

            # And it never touched the fault rail: no FaultMessage on the mirror.
            with pytest.raises(AssertionError):
                await tap.next_fault(timeout=5)
    finally:
        await driver.close()
        await worker._client.close()
