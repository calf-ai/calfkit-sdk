"""Real-broker (kafka lane) e2e for intermediate step streaming (spec §2.4 / §2.7 / §2.8 / §3.4).

The over-the-wire counterpart to the offline channel / projection / reception units. Proves what
``TestKafkaBroker`` cannot:

* a node consumer **survives** an unmatched (unstamped) message after the envelope-filter cutover —
  the real ``consume()`` swallow that ``TestKafkaBroker.publish`` bypasses (it re-raises instead);
* a real agent run's per-hop steps reach a held handle's ``stream()`` over the wire, in order,
  terminal-last, with correct ``is_error``;
* **all depths** stream to the ORIGINAL caller — a consulted ``message_agent`` peer's reply, produced
  at depth > 1, reaches the client as a ``ToolResultEvent`` (the same root-callback propagation the
  single-agent test demonstrates for a depth-2 tool).

Opt-in (``-m kafka``); skips cleanly without Docker. CI-lane only.
"""

from __future__ import annotations

import asyncio

import pytest

from calfkit._vendor.pydantic_ai import models
from calfkit._vendor.pydantic_ai.messages import ToolCallPart
from calfkit.client import Client
from calfkit.client.hub import InvocationHandle
from calfkit.controlplane import ControlPlaneConfig, ControlPlaneView
from calfkit.models.agents import AGENTS_TOPIC, AgentCard
from calfkit.nodes import Agent, ToolNodeDef, agent_tool
from calfkit.peers import Messaging
from calfkit.worker import Worker
from tests.integration._kafka_helpers import fast_control_plane
from tests.integration._roundtrip_helpers import final_model, scripted_model

pytestmark = pytest.mark.kafka
models.ALLOW_MODEL_REQUESTS = True
_EARLIEST = {"auto_offset_reset": "earliest"}


def _worker(bootstrap: str, *, nodes: list, control_plane: ControlPlaneConfig) -> Worker:
    return Worker(Client.connect(bootstrap), nodes=nodes, control_plane=control_plane, extra_subscribe_kwargs=_EARLIEST)


def _tool(name: str) -> ToolNodeDef:
    """A per-test, uniquely-named tool node (the name is the wire address on the shared lane)."""

    def step_tool() -> str:
        return "TOOL_RESULT_OK"

    return agent_tool(step_tool, name=name)


async def _collect_stream(handle: InvocationHandle, *, timeout: float) -> list:
    """Drain a held handle's stream() to completion (intermediates then terminal) under a timeout."""
    events: list = []

    async def _drain() -> None:
        async for event in handle.stream():
            events.append(event)

    await asyncio.wait_for(_drain(), timeout=timeout)
    return events


async def _await_card(bootstrap: str, name: str, *, timeout: float) -> None:
    view: ControlPlaneView[AgentCard] = ControlPlaneView.open(
        bootstrap_servers=bootstrap, topic=AGENTS_TOPIC, record_type=AgentCard, ensure_topic=False
    )
    try:
        await view.start()
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            if view.get(name) is not None:
                return
            await asyncio.sleep(0.1)
        raise AssertionError(f"timed out waiting for agent card {name!r}")
    finally:
        await view.stop()


async def test_node_survives_unmatched_message_and_subsequent_routes(kafka_bootstrap: str, topic_namespace: str) -> None:
    # After the envelope-filter cutover an unstamped (foreign) message on a node topic is dropped via
    # SubscriberNotFound and SWALLOWED by consume() — the consumer SURVIVES, and a subsequent stamped run
    # routes normally. TestKafkaBroker can't reproduce this (its publish bypasses the consume() swallow).
    a_in = f"{topic_namespace}.A.input"
    agent = Agent(f"{topic_namespace}-A", system_prompt="x", subscribe_topics=a_in, model_client=final_model("answer"))
    driver = Client.connect(kafka_bootstrap)
    worker = _worker(kafka_bootstrap, nodes=[agent], control_plane=fast_control_plane(kafka_bootstrap))
    async with worker:
        # an UNSTAMPED foreign body into A's input topic → filtered → SubscriberNotFound → swallowed.
        await driver.broker.publish(b'{"foreign": "junk"}', topic=a_in, correlation_id="foreign-1")
        # the consumer survived: a normal (stamped) run still routes + completes.
        result = await driver.agent(topic=a_in).execute("hi", timeout=120)
    assert result.output is not None and "answer" in result.output
    await driver.aclose()
    await worker._client.aclose()


async def test_single_agent_stream_yields_steps_then_terminal(kafka_bootstrap: str, topic_namespace: str) -> None:
    a_in = f"{topic_namespace}.A.input"
    tool_name = f"{topic_namespace}-tool"
    tool = _tool(tool_name)
    agent = Agent(
        f"{topic_namespace}-A",
        system_prompt="x",
        subscribe_topics=a_in,
        model_client=scripted_model([ToolCallPart(tool_name, {}, tool_call_id="t1")]),
        tools=[tool],
        sequential_only_mode=True,
    )
    driver = Client.connect(kafka_bootstrap)
    worker = _worker(kafka_bootstrap, nodes=[agent, tool], control_plane=fast_control_plane(kafka_bootstrap))
    async with worker:
        handle = await driver.agent(topic=a_in).start("go", correlation_id=f"{topic_namespace}-run")
        events = await _collect_stream(handle, timeout=120)
    kinds = [type(e).__name__ for e in events]
    assert kinds[-1] == "RunCompleted"  # terminal-bearing, terminal last
    assert "ToolCallEvent" in kinds  # the agent hop's requested tool call
    tr = next((e for e in events if type(e).__name__ == "ToolResultEvent"), None)
    assert tr is not None and tr.is_error is False  # the tool's result (an inner-frame ReturnCall, depth > 1)
    assert "TOOL_RESULT_OK" in tr.parts[0].text
    await driver.aclose()
    await worker._client.aclose()


async def test_all_depths_peer_consult_reply_reaches_original_caller(kafka_bootstrap: str, topic_namespace: str) -> None:
    # All depths stream to the ORIGINAL caller (spec §2.7 / §4): A consults B (message_agent); B's reply,
    # produced at depth > 1, reaches A's CLIENT as a ToolResultEvent (keyed by the m1 tool_call_id) — the
    # cross-agent reach the offline projection unit covers, here proven over the wire to the original caller.
    a_name = f"{topic_namespace}-A"
    b_name = f"{topic_namespace}-B"
    a_in = f"{topic_namespace}.A.input"
    cp = fast_control_plane(kafka_bootstrap)
    agent_a = Agent(
        a_name,
        system_prompt="x",
        subscribe_topics=a_in,
        model_client=scripted_model([ToolCallPart("message_agent", {"name": b_name, "message": "balance?"}, tool_call_id="m1")]),
        peers=[Messaging(b_name)],
    )
    agent_b = Agent(b_name, system_prompt="x", subscribe_topics=f"{topic_namespace}.B.input", model_client=final_model("the balance is 42"))
    driver = Client.connect(kafka_bootstrap)
    b_worker = _worker(kafka_bootstrap, nodes=[agent_b], control_plane=cp)
    a_worker = _worker(kafka_bootstrap, nodes=[agent_a], control_plane=cp)
    async with b_worker:
        await _await_card(kafka_bootstrap, b_name, timeout=60)  # A's view must materialize B before it messages B
        async with a_worker:
            handle = await driver.agent(topic=a_in).start("ask B", correlation_id=f"{topic_namespace}-run")
            events = await _collect_stream(handle, timeout=120)
    kinds = [type(e).__name__ for e in events]
    assert kinds[-1] == "RunCompleted"
    # B's reply reached A's client as a ToolResultEvent (depth > 1 → original caller, the all-depths spine).
    tool_results = [e for e in events if type(e).__name__ == "ToolResultEvent"]
    assert any("42" in (tr.parts[0].text if tr.parts else "") for tr in tool_results)
    await driver.aclose()
    await b_worker._client.aclose()
    await a_worker._client.aclose()
