"""Real-broker (kafka lane) e2e for intermediate step streaming (spec §2.4 / §2.7 / §2.8 / §3.4).

The over-the-wire counterpart to the offline channel / projection / reception units. Proves what
``TestKafkaBroker`` cannot:

* a node consumer **survives** an unmatched (unstamped) message after the envelope-filter cutover —
  the real ``consume()`` swallow that ``TestKafkaBroker.publish`` bypasses (it re-raises instead);
* a real agent run's per-hop steps reach a held handle's ``stream()`` over the wire, in order,
  terminal-last, with the correct ``outcome`` and fold-hop identity (emitter = the folding caller);
* the parked-sibling TRICKLE (I5): each fan-out fold's result step reaches the client as that reply
  folds — the real broker preserves the trickle-before-terminal order the inline offline broker
  cannot (the completing fold's re-entry executes inline there);
* **all depths** stream to the ORIGINAL caller — a consulted ``message_agent`` peer's answer,
  folded by the caller, reaches the client as its fold-minted ``ToolResultEvent``
  (``name="message_agent"``, spec §5.2).

Opt-in (``-m kafka``); skips cleanly without Docker. CI-lane only.
"""

from __future__ import annotations

import asyncio

import pytest
from aiokafka import AIOKafkaProducer

from calfkit._vendor.pydantic_ai import models
from calfkit._vendor.pydantic_ai.messages import ToolCallPart
from calfkit.client import Client
from calfkit.client.hub import InvocationHandle
from calfkit.controlplane import ControlPlaneConfig, ControlPlaneView
from calfkit.models.agents import AGENTS_TOPIC, AgentCard
from calfkit.nodes import Agent, ToolNodeDef, agent_tool
from calfkit.peers import Messaging
from calfkit.worker import Worker
from tests.integration._fault_kafka import ensure_topic
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
    await ensure_topic(kafka_bootstrap, a_in)  # the worker + the foreign producer share an already-existing topic
    driver = Client.connect(kafka_bootstrap)
    worker = _worker(kafka_bootstrap, nodes=[agent], control_plane=fast_control_plane(kafka_bootstrap))
    async with worker:
        # an UNSTAMPED foreign body into A's input topic → filtered → SubscriberNotFound → swallowed. Inject
        # via a raw producer (driver.broker's producer isn't connected until the first client run).
        producer = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap)
        await producer.start()
        try:
            await producer.send_and_wait(a_in, b'{"foreign": "junk"}')  # no x-calf-wire → unstamped → dropped
        finally:
            await producer.stop()
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
    assert tr is not None and tr.outcome == "success"  # the CALLER's fold-minted result (spec §3.2)
    assert "TOOL_RESULT_OK" in tr.parts[0].text
    # Fold-hop identity (I12/§5.2): emitter = the folding caller at its inbound depth.
    assert tr.emitter == f"{topic_namespace}-A"
    assert tr.depth == 1
    await driver.aclose()
    await worker._client.aclose()


async def test_fanout_sibling_folds_trickle_over_the_wire(kafka_bootstrap: str, topic_namespace: str) -> None:
    # The trickle (I5, kafka-lane pin): each parked sibling fold publishes its ONE-event result step as
    # that reply folds — over the real broker both trickles precede the terminal (the offline inline
    # broker reorders the completing fold's trickle past the terminal; here the real wire preserves it).
    a_in = f"{topic_namespace}.A.input"
    t1, t2 = _tool(f"{topic_namespace}-t1"), _tool(f"{topic_namespace}-t2")
    agent = Agent(
        f"{topic_namespace}-A",
        system_prompt="x",
        subscribe_topics=a_in,
        model_client=scripted_model([ToolCallPart(t1.name, {}, tool_call_id="c1"), ToolCallPart(t2.name, {}, tool_call_id="c2")]),
        tools=[t1, t2],
    )
    driver = Client.connect(kafka_bootstrap)
    worker = _worker(kafka_bootstrap, nodes=[agent, t1, t2], control_plane=fast_control_plane(kafka_bootstrap))
    async with worker:
        handle = await driver.agent(topic=a_in).start("go", correlation_id=f"{topic_namespace}-fan")
        events = await _collect_stream(handle, timeout=120)
    assert type(events[-1]).__name__ == "RunCompleted"
    results = [e for e in events if type(e).__name__ == "ToolResultEvent"]
    # BOTH sibling folds trickled before the terminal, each a success closure (the pair law's fold side).
    assert {r.tool_call_id for r in results} == {"c1", "c2"}
    assert all(r.outcome == "success" for r in results)
    # Identity anchors (I12/§5.2): emitter = the folding caller; the parked-fold frame_id is the BATCH
    # frame id — identical across the batch's folds.
    assert all(r.emitter == f"{topic_namespace}-A" for r in results)
    assert len({r.frame_id for r in results}) == 1
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
    # B's answer reached A's client as A's FOLD-MINTED ToolResultEvent (the all-depths spine): keyed by
    # the m1 call id, name = marker.tool_name ("message_agent" — §5.2's hard break from the answering
    # peer's name), emitted by the folding caller A.
    (tr,) = [e for e in events if type(e).__name__ == "ToolResultEvent"]
    assert tr.tool_call_id == "m1"
    assert tr.name == "message_agent"
    assert tr.outcome == "success"
    assert tr.emitter == a_name
    assert "42" in tr.parts[0].text
    await driver.aclose()
    await b_worker._client.aclose()
    await a_worker._client.aclose()
