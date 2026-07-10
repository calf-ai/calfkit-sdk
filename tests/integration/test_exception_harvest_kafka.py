"""Deterministic kafka-lane harvest test: a real provider ``ModelHTTPError`` (chained from an
``openai.BadRequestError``) rides a live broker, and its structured state + ``__cause__`` chain
survive the wire on both the broadcast-mirror tap (Channel A) and the client edge (Channel C).

Replays the EXACT OpenAI context-window-overflow JSON via ``_fault_tools.ctx_overflow`` — there is
NO live LLM (the offline ``FunctionModel`` scripts the tool call). This is the end-to-end proof of
``docs/designs/exception-harvest-spec.md``: the structured ``status_code`` / ``body.code`` a fault
flattened to a string today and the ``BadRequestError`` cause both reach the caller as JSON data,
with no producer class importable on the receiver. Opt-in (``-m kafka``); skips without Docker.
"""

from __future__ import annotations

import pytest

from calfkit._vendor.pydantic_ai import models
from calfkit._vendor.pydantic_ai.messages import ToolCallPart
from calfkit.client import Client
from calfkit.exceptions import NodeFaultError
from calfkit.models.error_report import FaultTypes
from calfkit.nodes import Agent
from tests.integration._fault_kafka import ensure_topic, fault_worker
from tests.integration._fault_tap import fault_tap
from tests.integration._fault_tools import ctx_overflow
from tests.integration._roundtrip_helpers import scripted_model

# Every test needs a real broker; FunctionModel is offline but pydantic-ai still gates "model
# requests" behind this flag (matches the other kafka-lane agent suites).
pytestmark = pytest.mark.kafka
models.ALLOW_MODEL_REQUESTS = True


def _agent(node_id: str, *, agent_in: str, agent_pub: str, call: ToolCallPart) -> Agent:
    return Agent(
        node_id,
        system_prompt="call the ctx_overflow tool",
        subscribe_topics=agent_in,
        publish_topic=agent_pub,
        model_client=scripted_model([call]),
        tools=[ctx_overflow],
    )


async def test_provider_error_harvest_and_cause_chain_survive_the_wire(kafka_bootstrap: str, topic_namespace: str) -> None:
    """A tool raising ``ModelHTTPError(...) from BadRequestError(...)`` → the agent escalates a
    ``calf.exception`` fault whose ``exception`` slot carries the provider's structured state
    (``status_code`` / ``body.code``) and whose ``causes`` carries the ``BadRequestError`` link —
    asserted on the broadcast-mirror tap (Channel A) and at the client's typed ``NodeFaultError``
    (Channel C). Neither read parses ``message``; both read JSON-native data off the slot."""
    agent_in = f"{topic_namespace}.harvest.input"
    agent_pub = f"{topic_namespace}.harvest.mirror"
    agent = _agent(
        f"{topic_namespace}-harvest",
        agent_in=agent_in,
        agent_pub=agent_pub,
        call=ToolCallPart("ctx_overflow", {"x": 1}, tool_call_id="c1"),
    )
    await ensure_topic(kafka_bootstrap, agent_pub)
    driver = Client.connect(kafka_bootstrap)
    worker = fault_worker(kafka_bootstrap, nodes=[agent, ctx_overflow])

    try:
        async with worker, fault_tap(kafka_bootstrap, agent_pub) as tap:
            handle = await driver.agent(topic=agent_in).start("go")

            # Channel A — the broadcast-mirror tap: the structured provider state survived the wire.
            fault, _ = await tap.next_fault(timeout=60)
            assert fault.error.error_type == FaultTypes.EXCEPTION
            assert fault.error.exception is not None
            assert fault.error.exception.type == "ModelHTTPError"
            assert fault.error.exception.attrs["status_code"] == 400
            assert fault.error.exception.attrs["body"]["code"] == "context_length_exceeded"
            # the __cause__ chain is mapped onto causes — the openai.BadRequestError link, carrying
            # its OWN structured state (and needing no openai class importable here to read it).
            chain = next((r for r in fault.error.walk() if r.exception and r.exception.type == "BadRequestError"), None)
            assert chain is not None and chain.exception is not None
            assert chain.exception.attrs["status_code"] == 400

            # Channel C — the client edge receives the same report verbatim as a typed NodeFaultError.
            with pytest.raises(NodeFaultError) as exc_info:
                await handle.result(timeout=60)
            report = exc_info.value.report
            assert report.exception is not None and report.exception.type == "ModelHTTPError"
            assert report.exception.attrs["body"]["code"] == "context_length_exceeded"
            assert any(r.exception and r.exception.type == "BadRequestError" for r in report.walk())
    finally:
        await driver.aclose()
        await worker._client.aclose()
