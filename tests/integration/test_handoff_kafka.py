"""Real-broker (``kafka`` lane) end-to-end agent-to-agent handoff (PR-C / §13).

The over-the-wire counterpart to the offline ``tests/test_handoff_dispatch.py`` unit suite. The unit tests
drive ``agent.run`` directly with a stubbed agents view; what they CANNOT prove — and what these do — is the
handoff spine against a live broker, end to end:

* agent B advertises its :class:`AgentCard`; agent A's gated agents view materializes it and renders the
  live directory into the reserved ``handoff_to_agent`` tool's description (handoff-tool-transport-spec §2);
* A CALLS ``handoff_to_agent``; calfkit arbitration wins the turn, authors the closing ModelRequest, and the
  framework retargets A's frame (preserving the original caller's callback) to B's derived input topic
  (``agent.{B}.private.input``) — A drops out;
* B continues the full conversation (A's briefing — the tool call's args — surfaces cross-agent via the
  POV projection) and answers A's ORIGINAL caller — the driver — not A;
* chained handoffs compose (A->B->C reaches the original caller), a handoff inside a peer-message folds
  into the messaging caller's tool result, and a direct handoff clears the caller's per-run overrides (C2).

Mirrors ``test_message_agent_kafka.py`` (the messaging spine). Opt-in (``-m kafka``); skips cleanly without
Docker. Run with ``uv run --group integration pytest tests/integration/test_handoff_kafka.py -m kafka``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

from calfkit._vendor.pydantic_ai import models
from calfkit._vendor.pydantic_ai.messages import ModelMessage, ModelResponse, TextPart, ToolCallPart
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit._vendor.pydantic_ai.tools import ToolDefinition
from calfkit.client import Client
from calfkit.controlplane import ControlPlaneConfig, ControlPlaneView
from calfkit.models.agents import AGENTS_TOPIC, AgentCard
from calfkit.models.tool_dispatch import ToolBinding
from calfkit.nodes import Agent, ToolNodeDef, agent_tool
from calfkit.peers import Handoff, Messaging
from calfkit.peers.directory import _NONE_REACHABLE
from calfkit.peers.handoff import _STUB_TOOL_NOT_EXECUTED, HANDOFF_TOOL
from calfkit.worker import Worker
from tests.integration._kafka_helpers import fast_control_plane
from tests.integration._roundtrip_helpers import FINAL_OUTPUT, final_model, retry_prompt_texts, returns_by_call_id, scripted_model

pytestmark = pytest.mark.kafka
models.ALLOW_MODEL_REQUESTS = True

_EARLIEST = {"auto_offset_reset": "earliest"}


def _worker(bootstrap: str, *, nodes: list, control_plane: ControlPlaneConfig) -> Worker:
    return Worker(Client.connect(bootstrap), nodes=nodes, control_plane=control_plane, extra_subscribe_kwargs=_EARLIEST)


async def _wait(predicate: Callable[[], bool], *, timeout: float, what: str) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.1)
    raise AssertionError(f"timed out after {timeout}s waiting for: {what}")


async def _await_agents_view(bootstrap: str, predicate: Callable[[ControlPlaneView[AgentCard]], bool], *, timeout: float, what: str) -> None:
    """Open a transient agents view, wait for ``predicate`` over it, then close it — so an agent's own view
    catch-up (started after) will include whatever the predicate confirmed is live."""
    view: ControlPlaneView[AgentCard] = ControlPlaneView.open(
        bootstrap_servers=bootstrap, topic=AGENTS_TOPIC, record_type=AgentCard, ensure_topic=False
    )
    try:
        await view.start()
        await _wait(lambda: predicate(view), timeout=timeout, what=what)
    finally:
        await view.stop()


def _emit_handoff(name: str, message: str, *siblings: ToolCallPart) -> FunctionModel:
    """A model that CALLS the reserved ``handoff_to_agent`` tool (handoff-tool-transport-spec §2) to
    transfer control to ``name`` — optionally co-emitting sibling tool calls (the arbitration stubs them)."""

    def _fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        assert any(t.name == HANDOFF_TOOL for t in info.function_tools)  # the reserved def rides the toolset
        return ModelResponse(parts=[*siblings, ToolCallPart(HANDOFF_TOOL, {"name": name, "message": message}, tool_call_id="h1")])

    return FunctionModel(_fn)


def _msg_call(name: str, message: str, *, tool_call_id: str) -> ToolCallPart:
    return ToolCallPart("message_agent", {"name": name, "message": message}, tool_call_id=tool_call_id)


async def test_handoff_transfers_to_peer_who_answers_original_caller(kafka_bootstrap: str, topic_namespace: str) -> None:
    """A (``peers=[Handoff(B)]``) calls ``handoff_to_agent``; control transfers to B over the wire; B
    continues the conversation and answers the ORIGINAL caller (the driver) — A dropped out. A's handoff
    turn rode the carried conversation, attributed to A."""
    a_name = f"{topic_namespace}-A"
    b_name = f"{topic_namespace}-B"
    a_in = f"{topic_namespace}.A.input"
    control_plane = fast_control_plane(kafka_bootstrap)

    agent_a = Agent(
        a_name, system_prompt="triage", subscribe_topics=a_in, model_client=_emit_handoff(b_name, "please handle the refund"), peers=[Handoff(b_name)]
    )
    agent_b = Agent(
        b_name, system_prompt="billing", subscribe_topics=f"{topic_namespace}.B.input", model_client=final_model("the refund is approved")
    )

    driver = Client.connect(kafka_bootstrap)
    b_worker = _worker(kafka_bootstrap, nodes=[agent_b], control_plane=control_plane)
    a_worker = _worker(kafka_bootstrap, nodes=[agent_a], control_plane=control_plane)

    async with b_worker:
        await _await_agents_view(kafka_bootstrap, lambda v: v.get(b_name) is not None, timeout=60, what=f"B's AgentCard {b_name!r}")
        async with a_worker:
            result = await driver.agent(topic=a_in).execute("handle my refund", timeout=120)

    # B answered the ORIGINAL caller (the driver); A relinquished and dropped out.
    assert result.output is not None and "refund is approved" in result.output
    # A's handoff output rode the carried conversation, attributed to A (a ModelResponse stamped with A's name).
    assert any(isinstance(m, ModelResponse) and m.name == a_name for m in result.message_history)

    await driver.aclose()
    await b_worker._client.aclose()
    await a_worker._client.aclose()


async def test_chained_handoff_reaches_original_caller(kafka_bootstrap: str, topic_namespace: str) -> None:
    """A->B->C: each handoff retargets the ONE frame (preserving the original caller's callback), so the
    last agent (C) answers A's original caller and both A and B drop out. The chain composes — handoff loops
    are NOT cycle-guarded (the TailCall replaces the frame, so the ancestor set never grows), so a chained
    handoff is the knowingly-unbounded-but-composes property (#251 owns the eventual hop bound)."""
    a_name, b_name, c_name = f"{topic_namespace}-A", f"{topic_namespace}-B", f"{topic_namespace}-C"
    a_in = f"{topic_namespace}.A.input"
    control_plane = fast_control_plane(kafka_bootstrap)

    agent_a = Agent(a_name, system_prompt="x", subscribe_topics=a_in, model_client=_emit_handoff(b_name, "to B"), peers=[Handoff(b_name)])
    agent_b = Agent(
        b_name, system_prompt="x", subscribe_topics=f"{topic_namespace}.B.input", model_client=_emit_handoff(c_name, "to C"), peers=[Handoff(c_name)]
    )
    agent_c = Agent(c_name, system_prompt="x", subscribe_topics=f"{topic_namespace}.C.input", model_client=final_model("C handled it"))

    driver = Client.connect(kafka_bootstrap)
    c_worker = _worker(kafka_bootstrap, nodes=[agent_c], control_plane=control_plane)
    b_worker = _worker(kafka_bootstrap, nodes=[agent_b], control_plane=control_plane)
    a_worker = _worker(kafka_bootstrap, nodes=[agent_a], control_plane=control_plane)

    async with c_worker:
        await _await_agents_view(kafka_bootstrap, lambda v: v.get(c_name) is not None, timeout=60, what=f"C's card {c_name!r}")
        async with b_worker:
            await _await_agents_view(kafka_bootstrap, lambda v: v.get(b_name) is not None and v.get(c_name) is not None, timeout=60, what="B+C cards")
            async with a_worker:
                result = await driver.agent(topic=a_in).execute("start the chain", timeout=120)

    assert result.output is not None and "C handled it" in result.output  # the last agent answered the original caller

    await driver.aclose()
    await c_worker._client.aclose()
    await b_worker._client.aclose()
    await a_worker._client.aclose()


async def test_handoff_inside_peer_message_folds_to_messaging_caller(kafka_bootstrap: str, topic_namespace: str) -> None:
    """Composition: A ``message_agent``s B (a Peer message — A keeps control); B HANDS OFF to C; C's answer
    folds into A's ``message_agent`` tool result (B's handoff retargeted B's frame — whose callback is A's
    message slot — to C), and A then finalizes. The two capabilities compose transparently."""
    a_name, b_name, c_name = f"{topic_namespace}-A", f"{topic_namespace}-B", f"{topic_namespace}-C"
    a_in = f"{topic_namespace}.A.input"
    control_plane = fast_control_plane(kafka_bootstrap)

    agent_a = Agent(
        a_name,
        system_prompt="x",
        subscribe_topics=a_in,
        model_client=scripted_model([_msg_call(b_name, "consult", tool_call_id="m1")]),
        peers=[Messaging(b_name)],
    )
    agent_b = Agent(
        b_name,
        system_prompt="x",
        subscribe_topics=f"{topic_namespace}.B.input",
        model_client=_emit_handoff(c_name, "you take it"),
        peers=[Handoff(c_name)],
    )
    agent_c = Agent(c_name, system_prompt="x", subscribe_topics=f"{topic_namespace}.C.input", model_client=final_model("C consulted answer"))

    driver = Client.connect(kafka_bootstrap)
    c_worker = _worker(kafka_bootstrap, nodes=[agent_c], control_plane=control_plane)
    b_worker = _worker(kafka_bootstrap, nodes=[agent_b], control_plane=control_plane)
    a_worker = _worker(kafka_bootstrap, nodes=[agent_a], control_plane=control_plane)

    async with c_worker:
        await _await_agents_view(kafka_bootstrap, lambda v: v.get(c_name) is not None, timeout=60, what=f"C's card {c_name!r}")
        async with b_worker:
            await _await_agents_view(kafka_bootstrap, lambda v: v.get(b_name) is not None and v.get(c_name) is not None, timeout=60, what="B+C cards")
            async with a_worker:
                result = await driver.agent(topic=a_in).execute("ask B, who may delegate", timeout=120)

    assert result.output is not None and FINAL_OUTPUT in result.output  # A kept control and finalized
    assert "C consulted answer" in str(returns_by_call_id(result.message_history)["m1"])  # C's answer folded into A's message slot

    await driver.aclose()
    await c_worker._client.aclose()
    await b_worker._client.aclose()
    await a_worker._client.aclose()


async def test_handoff_empty_directory_lets_agent_answer_directly(kafka_bootstrap: str, topic_namespace: str) -> None:
    """A ``Handoff`` agent with NO live in-scope peer (a curated ghost never deployed): the
    ``handoff_to_agent`` tool still rides the toolset with the "(no peer agents are currently reachable)"
    sentinel directory (spec §2 — the capability stays visible), and A answers directly over the wire."""
    a_name = f"{topic_namespace}-A"
    ghost = f"{topic_namespace}-ghost"  # in A's curated scope but never deployed -> not live
    a_in = f"{topic_namespace}.A.input"
    control_plane = fast_control_plane(kafka_bootstrap)

    def _answers_directly(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        # The tool RIDES the toolset with the sentinel directory (spec §2) — asserted in-model
        # so the over-the-wire claim in this docstring is actually checked (review round 1).
        tool = next(t for t in info.function_tools if t.name == HANDOFF_TOOL)
        assert tool.description.endswith(_NONE_REACHABLE)
        return ModelResponse(parts=[TextPart("I'll handle it myself")])

    agent_a = Agent(a_name, system_prompt="x", subscribe_topics=a_in, model_client=FunctionModel(_answers_directly), peers=[Handoff(ghost)])

    driver = Client.connect(kafka_bootstrap)
    a_worker = _worker(kafka_bootstrap, nodes=[agent_a], control_plane=control_plane)

    async with a_worker:
        result = await driver.agent(topic=a_in).execute("can you help", timeout=120)

    assert result.output is not None and "handle it myself" in result.output  # answered directly; sentinel tool was offered

    await driver.aclose()
    await a_worker._client.aclose()


async def test_direct_handoff_clears_caller_overrides(kafka_bootstrap: str, topic_namespace: str) -> None:
    """C2 over the wire: A is invoked with a per-run ``tool_override`` that REPLACES the tool surface; after
    a direct A->B handoff, B uses its OWN tools (the override is cleared on BOTH channels — ``state.overrides``
    and ``frame.overrides``), so B's own tool dispatches + folds. If the override had leaked to B, B's tool
    would be shadowed/unknown (a retry), not dispatched."""
    a_name, b_name = f"{topic_namespace}-A", f"{topic_namespace}-B"
    b_tool_name = f"{topic_namespace}-btool"
    a_in = f"{topic_namespace}.A.input"
    control_plane = fast_control_plane(kafka_bootstrap)
    # A's per-run override REPLACES the agent tool surface with a single tool A never calls (it hands off).
    override_tool = ToolBinding(
        dispatch_topic="unused.override.topic",
        tool_def=ToolDefinition(name="override_only", description="x", parameters_json_schema={"type": "object", "properties": {}}),
    )

    def _btool() -> str:
        return "B_OWN_TOOL_OK"

    b_tool: ToolNodeDef = agent_tool(_btool, name=b_tool_name)
    agent_a = Agent(a_name, system_prompt="x", subscribe_topics=a_in, model_client=_emit_handoff(b_name, "over to you"), peers=[Handoff(b_name)])
    agent_b = Agent(
        b_name,
        system_prompt="x",
        subscribe_topics=f"{topic_namespace}.B.input",
        model_client=scripted_model([ToolCallPart(b_tool_name, {}, tool_call_id="bt1")]),
        tools=[b_tool],
    )

    driver = Client.connect(kafka_bootstrap)
    b_worker = _worker(kafka_bootstrap, nodes=[agent_b, b_tool], control_plane=control_plane)
    a_worker = _worker(kafka_bootstrap, nodes=[agent_a], control_plane=control_plane)

    async with b_worker:
        await _await_agents_view(kafka_bootstrap, lambda v: v.get(b_name) is not None, timeout=60, what=f"B's card {b_name!r}")
        async with a_worker:
            result = await driver.agent(topic=a_in).execute("handle it (override set)", tool_overrides=[override_tool], timeout=120)

    # B used its OWN tool (the override was cleared): b_tool dispatched + folded, NOT shadowed as 'unknown'.
    assert result.output is not None and FINAL_OUTPUT in result.output
    assert "B_OWN_TOOL_OK" in str(returns_by_call_id(result.message_history)["bt1"])
    assert b_tool_name not in " ".join(retry_prompt_texts(result.message_history))  # not shadowed by A's override

    await driver.aclose()
    await b_worker._client.aclose()
    await a_worker._client.aclose()


async def test_winning_handoff_stubs_parallel_sibling_over_the_wire(kafka_bootstrap: str, topic_namespace: str) -> None:
    """Early semantics end to end (spec §3/§4): A co-emits a REAL tool call and the handoff in one
    response; the handoff wins — the sibling tool NEVER executes (its node would have returned a marker),
    its closing stub rides B's inherited history, and B answers the original caller."""
    a_name, b_name = f"{topic_namespace}-A", f"{topic_namespace}-B"
    a_tool_name = f"{topic_namespace}-atool"
    a_in = f"{topic_namespace}.A.input"
    control_plane = fast_control_plane(kafka_bootstrap)

    def _atool() -> str:
        return "SIBLING_TOOL_RAN"  # must never appear anywhere

    a_tool: ToolNodeDef = agent_tool(_atool, name=a_tool_name)
    agent_a = Agent(
        a_name,
        system_prompt="x",
        subscribe_topics=a_in,
        model_client=_emit_handoff(b_name, "take over", ToolCallPart(a_tool_name, {}, tool_call_id="t1")),
        tools=[a_tool],
        peers=[Handoff(b_name)],
    )
    agent_b = Agent(b_name, system_prompt="x", subscribe_topics=f"{topic_namespace}.B.input", model_client=final_model("B finished it"))

    driver = Client.connect(kafka_bootstrap)
    b_worker = _worker(kafka_bootstrap, nodes=[agent_b], control_plane=control_plane)
    a_worker = _worker(kafka_bootstrap, nodes=[agent_a, a_tool], control_plane=control_plane)

    async with b_worker:
        await _await_agents_view(kafka_bootstrap, lambda v: v.get(b_name) is not None, timeout=60, what=f"B's card {b_name!r}")
        async with a_worker:
            result = await driver.agent(topic=a_in).execute("do both things", timeout=120)

    assert result.output is not None and "B finished it" in result.output  # the handoff won; B answered the caller
    returns = returns_by_call_id(result.message_history)
    assert str(returns["t1"]) == _STUB_TOOL_NOT_EXECUTED  # the sibling was closed by the §4 stub...
    assert "SIBLING_TOOL_RAN" not in str(result.message_history)  # ...and its node never executed

    await driver.aclose()
    await b_worker._client.aclose()
    await a_worker._client.aclose()


async def test_briefing_reaches_the_peer_over_the_wire(kafka_bootstrap: str, topic_namespace: str) -> None:
    """spec §6 end to end: B's model demonstrably SEES A's briefing — the handoff tool call's args,
    surfaced by the POV projection as an attributed user turn — in its projected input."""
    a_name, b_name = f"{topic_namespace}-A", f"{topic_namespace}-B"
    a_in = f"{topic_namespace}.A.input"
    briefing = "the customer wants a refund for order 1234"
    control_plane = fast_control_plane(kafka_bootstrap)

    def _b_fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        flat = str(messages)
        briefed = briefing in flat and f"<{a_name}>" in flat  # attributed to A, carrying the message
        return ModelResponse(parts=[TextPart(f"BRIEFED={briefed}")])

    agent_a = Agent(a_name, system_prompt="x", subscribe_topics=a_in, model_client=_emit_handoff(b_name, briefing), peers=[Handoff(b_name)])
    agent_b = Agent(b_name, system_prompt="x", subscribe_topics=f"{topic_namespace}.B.input", model_client=FunctionModel(_b_fn))

    driver = Client.connect(kafka_bootstrap)
    b_worker = _worker(kafka_bootstrap, nodes=[agent_b], control_plane=control_plane)
    a_worker = _worker(kafka_bootstrap, nodes=[agent_a], control_plane=control_plane)

    async with b_worker:
        await _await_agents_view(kafka_bootstrap, lambda v: v.get(b_name) is not None, timeout=60, what=f"B's card {b_name!r}")
        async with a_worker:
            result = await driver.agent(topic=a_in).execute("please help", timeout=120)

    assert result.output is not None and "BRIEFED=True" in result.output

    await driver.aclose()
    await b_worker._client.aclose()
    await a_worker._client.aclose()
