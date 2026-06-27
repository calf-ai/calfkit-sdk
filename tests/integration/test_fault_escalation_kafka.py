"""Suite F — fault escalation over the real broker.

The real-broker counterpart to the offline ``tests/test_fault_pipeline.py``: a tool's
terminal failure travels back through a live Worker to the caller as a typed
``FaultMessage`` (catalogue FR-2/FR-3/FR-11/FR-12/FR-13/FR-16/FR-25), observed on three
channels:

* **Channel A** — the agent's ``publish_topic`` broadcast mirror, tapped raw
  (``_fault_tap``): the only *typed* channel today. Asserts the ``ErrorReport`` and the
  ``x-calf-kind=fault`` / ``x-calf-error-type`` headers.
* **Channel B** — an ``on_callee_error`` recorder (``_fault_tools.CalleeErrorRecorder``)
  that sees the live fault in-process, then declines so it still escalates.
* **Channel C** — the client edge: a routed fault is received as a typed ``NodeFaultError``
  (#250 reception), so ``await handle.result()`` raises it carrying the verbatim ``ErrorReport``.

The agents run ``sequential_only_mode=True``: every case dispatches a single tool call,
so the fault path is identical to a fan-out-capable agent's, and the durable fan-out
store (a separate ktables dependency, covered by Suite X) stays out of the way.

Opt-in (``-m kafka`` / ``make test-kafka``); skips cleanly without Docker.
"""

from __future__ import annotations

import logging

import pytest
from aiokafka import AIOKafkaProducer

from calfkit._protocol import HDR_ERROR_TYPE, HDR_KIND
from calfkit._vendor.pydantic_ai import models
from calfkit._vendor.pydantic_ai.messages import ToolCallPart
from calfkit.client import Client
from calfkit.exceptions import NodeFaultError
from calfkit.models import CallFrame, CallFrameStack, Envelope, SessionRunContext, State, WorkflowState
from calfkit.models.error_report import FaultTypes
from calfkit.nodes import Agent
from tests.integration._fault_kafka import ensure_topic, fault_worker
from tests.integration._fault_tap import fault_tap
from tests.integration._fault_tools import CalleeErrorRecorder, boom, ok_a, quota
from tests.integration._roundtrip_helpers import FINAL_OUTPUT, scripted_model

# Every test needs a real broker; FunctionModel is offline but pydantic-ai still gates
# "model requests" behind this flag (matches the other kafka-lane agent suites).
pytestmark = pytest.mark.kafka
models.ALLOW_MODEL_REQUESTS = True


def _agent(node_id: str, *, agent_in: str, agent_pub: str, tool, call: ToolCallPart, **seams) -> Agent:
    return Agent(
        node_id,
        system_prompt=f"call the {tool.name} tool",
        subscribe_topics=agent_in,
        publish_topic=agent_pub,
        model_client=scripted_model([call]),
        tools=[tool],
        sequential_only_mode=True,
        **seams,
    )


async def test_tool_generic_raise_escalates_unhandled_fault(kafka_bootstrap: str, topic_namespace: str) -> None:
    """F-1: a tool's generic exception → the agent escalates a ``calf.unhandled``
    ``FaultMessage`` on its ``publish_topic`` mirror (typed report + filterable
    headers), and the client's routed reply is received as a typed ``NodeFaultError``."""
    agent_in = f"{topic_namespace}.f1.input"
    agent_pub = f"{topic_namespace}.f1.mirror"
    agent = _agent(
        f"{topic_namespace}-f1",
        agent_in=agent_in,
        agent_pub=agent_pub,
        tool=boom,
        call=ToolCallPart("boom", {"x": 7}, tool_call_id="c1"),
    )
    await ensure_topic(kafka_bootstrap, agent_pub)
    driver = Client.connect(kafka_bootstrap)
    worker = fault_worker(kafka_bootstrap, nodes=[agent, boom])

    try:
        async with worker, fault_tap(kafka_bootstrap, agent_pub) as tap:
            handle = await driver.agent(topic=agent_in).start("go")

            fault, headers = await tap.next_fault(timeout=60)
            assert headers[HDR_KIND] == "fault"
            assert headers[HDR_ERROR_TYPE] == FaultTypes.UNHANDLED
            assert fault.error.error_type == FaultTypes.UNHANDLED
            assert fault.error.find(FaultTypes.UNHANDLED) is not None
            assert fault.error.details.get(FaultTypes.EXCEPTION_TYPE) == "ValueError"
            assert len(fault.error.frame_chain) >= 1  # the faulting hop's topology is captured

            # Channel C — the routed fault is RECEIVED as a typed NodeFaultError (#250 reception):
            # result() raises it carrying the verbatim escalated ErrorReport.
            with pytest.raises(NodeFaultError) as exc_info:
                await handle.result(timeout=60)
            assert exc_info.value.report.find(FaultTypes.UNHANDLED) is not None
    finally:
        await driver.aclose()
        await worker._client.aclose()


async def test_callee_error_seam_observes_fault_then_escalates(kafka_bootstrap: str, topic_namespace: str) -> None:
    """F-1b (Channel B): an ``on_callee_error`` recorder sees the live ``calf.unhandled``
    fault (``delivery_kind=fault``, ``failing_call.tag``) and declines → the fault still
    escalates to the agent's ``publish_topic`` mirror."""
    recorder = CalleeErrorRecorder()
    agent_in = f"{topic_namespace}.f1b.input"
    agent_pub = f"{topic_namespace}.f1b.mirror"
    agent = _agent(
        f"{topic_namespace}-f1b",
        agent_in=agent_in,
        agent_pub=agent_pub,
        tool=boom,
        call=ToolCallPart("boom", {"x": 1}, tool_call_id="c1"),
        on_callee_error=recorder,
    )
    await ensure_topic(kafka_bootstrap, agent_pub)
    driver = Client.connect(kafka_bootstrap)
    worker = fault_worker(kafka_bootstrap, nodes=[agent, boom])

    try:
        async with worker, fault_tap(kafka_bootstrap, agent_pub) as tap:
            await driver.agent(topic=agent_in).start("go")
            fault, _ = await tap.next_fault(timeout=60)
            assert fault.error.error_type == FaultTypes.UNHANDLED
    finally:
        await driver.aclose()
        await worker._client.aclose()

    # Capture-in-callback, assert-in-test-body (the seam fired once, in the worker
    # process, before it produced the mirror this test just observed).
    assert len(recorder.calls) == 1
    (call,) = recorder.calls
    assert call["delivery_kind"] == "fault"
    assert call["error_type"] == FaultTypes.UNHANDLED
    assert call["tag"] == "c1"


async def test_tool_mint_escalates_verbatim_typed_fault(kafka_bootstrap: str, topic_namespace: str) -> None:
    """F-2: a tool minting ``NodeFaultError`` carries the user ``error_type`` verbatim
    (the mint rule), with ``retryable`` and ``details`` preserved end-to-end."""
    agent_in = f"{topic_namespace}.f2.input"
    agent_pub = f"{topic_namespace}.f2.mirror"
    agent = _agent(
        f"{topic_namespace}-f2",
        agent_in=agent_in,
        agent_pub=agent_pub,
        tool=quota,
        call=ToolCallPart("quota", {"x": 42}, tool_call_id="c1"),
    )
    await ensure_topic(kafka_bootstrap, agent_pub)
    driver = Client.connect(kafka_bootstrap)
    worker = fault_worker(kafka_bootstrap, nodes=[agent, quota])

    try:
        async with worker, fault_tap(kafka_bootstrap, agent_pub) as tap:
            await driver.agent(topic=agent_in).start("go")
            fault, headers = await tap.next_fault(timeout=60)
            assert fault.error.error_type == "billing.quota_exceeded"
            assert headers[HDR_ERROR_TYPE] == "billing.quota_exceeded"
            assert fault.error.retryable is False
            assert fault.error.details.get("x") == 42
    finally:
        await driver.aclose()
        await worker._client.aclose()


async def test_fire_and_forget_fault_mirrors_without_callback(kafka_bootstrap: str, topic_namespace: str) -> None:
    """F-4: a one-way ``send`` whose tool faults → the fault is mirrored on the agent's
    ``publish_topic`` (the fire-and-forget terminal floors + mirrors, no callback rail);
    ``send`` registered no future, so nothing hangs."""
    agent_in = f"{topic_namespace}.f4.input"
    agent_pub = f"{topic_namespace}.f4.mirror"
    agent = _agent(
        f"{topic_namespace}-f4",
        agent_in=agent_in,
        agent_pub=agent_pub,
        tool=boom,
        call=ToolCallPart("boom", {"x": 5}, tool_call_id="c1"),
    )
    await ensure_topic(kafka_bootstrap, agent_pub)
    driver = Client.connect(kafka_bootstrap)
    worker = fault_worker(kafka_bootstrap, nodes=[agent, boom])

    try:
        async with worker, fault_tap(kafka_bootstrap, agent_pub) as tap:
            correlation_id = (await driver.agent(topic=agent_in).send("go")).correlation_id  # one-way: returns the id, no future
            assert isinstance(correlation_id, str)

            fault, headers = await tap.next_fault(timeout=60)
            assert headers[HDR_KIND] == "fault"
            assert fault.error.error_type == FaultTypes.UNHANDLED
    finally:
        await driver.aclose()
        await worker._client.aclose()


async def test_stray_fault_does_not_disturb_the_live_worker(kafka_bootstrap: str, topic_namespace: str, caplog: pytest.LogCaptureFixture) -> None:
    """F-5: a ``kind=fault`` delivery whose reply slot is empty (kind/slot disagreement)
    injected onto a live agent's private return inbox is floored as a stray (WARNING +
    ignore, FR-16/FR-25) — the worker keeps consuming, proven by a valid invocation that
    completes normally afterwards."""
    agent_in = f"{topic_namespace}.f5.input"
    agent_pub = f"{topic_namespace}.f5.mirror"
    agent = _agent(
        f"{topic_namespace}-f5",
        agent_in=agent_in,
        agent_pub=agent_pub,
        tool=ok_a,
        call=ToolCallPart("ok_a", {}, tool_call_id="c1"),
    )
    return_topic = f"{agent.node_id}.private.return"
    await ensure_topic(kafka_bootstrap, agent_pub)
    await ensure_topic(kafka_bootstrap, return_topic)
    driver = Client.connect(kafka_bootstrap)
    worker = fault_worker(kafka_bootstrap, nodes=[agent, ok_a])

    # A decodable envelope whose header says kind=fault but whose reply slot is empty —
    # the kind↔slot disagreement the stray check catches before any seam runs.
    stack = CallFrameStack()
    stack.push(CallFrame(target_topic=agent_in, callback_topic="nobody.return", tag="t1"))
    stray = Envelope(
        internal_workflow_state=WorkflowState(call_stack=stack),
        context=SessionRunContext(state=State(), deps={}),
    )

    try:
        with caplog.at_level(logging.WARNING, logger="calfkit.nodes.base"):
            async with worker:
                producer = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap)
                await producer.start()
                try:
                    await producer.send_and_wait(
                        return_topic,
                        stray.model_dump_json().encode(),
                        headers=[(HDR_KIND, b"fault")],
                    )
                finally:
                    await producer.stop()

                # The worker survived the stray: a valid invocation still completes.
                result = await driver.agent(topic=agent_in).execute("go", timeout=60)
                assert result.output is not None and FINAL_OUTPUT in result.output

        assert any("stray" in record.getMessage().lower() for record in caplog.records)
    finally:
        await driver.aclose()
        await worker._client.aclose()
