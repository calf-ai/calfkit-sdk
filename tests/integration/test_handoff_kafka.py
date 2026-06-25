"""Real-broker (``kafka`` lane) end-to-end agent-to-agent handoff (PR-C / §13).

The over-the-wire counterpart to the offline ``tests/test_handoff_dispatch.py`` unit suite. The unit tests
drive ``agent.run`` directly with a stubbed agents view; what they CANNOT prove — and what these do — is the
handoff spine against a live broker, end to end:

* agent B advertises its :class:`AgentCard`; agent A's gated agents view materializes it and builds the
  per-turn ``HandoffRequest`` ``Literal`` over the live directory;
* A produces a ``HandoffRequest``; the framework retargets A's frame (preserving the original caller's
  callback) to B's PR-A derived input topic (``agent.{B}.private.input``) and A drops out;
* B continues the full conversation (it sees A's handoff output cross-agent) and answers A's ORIGINAL
  caller — the driver — not A;
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
from calfkit._vendor.pydantic_ai.messages import ModelMessage, ModelResponse, ToolCallPart
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit._vendor.pydantic_ai.tools import ToolDefinition
from calfkit.client import Client
from calfkit.controlplane import ControlPlaneConfig, ControlPlaneView
from calfkit.models.agents import AGENTS_TOPIC, AgentCard
from calfkit.models.tool_dispatch import ToolBinding
from calfkit.nodes import Agent, ToolNodeDef, agent_tool
from calfkit.peers import Handoff, Messaging
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


def _emit_handoff(name: str, message: str) -> FunctionModel:
    """A model that produces a ``HandoffRequest`` as its turn output — finding the output-tool name from the
    offered ``output_tools`` (``final_result`` for a str agent, ``final_result_HandoffRequest`` for a
    structured one) — to transfer control to ``name``."""

    def _fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        tool = next(t for t in info.output_tools if t.name == "final_result" or "HandoffRequest" in t.name)
        return ModelResponse(parts=[ToolCallPart(tool.name, {"name": name, "message": message}, tool_call_id="h1")])

    return FunctionModel(_fn)


def _msg_call(name: str, message: str, *, tool_call_id: str) -> ToolCallPart:
    return ToolCallPart("message_agent", {"name": name, "message": message}, tool_call_id=tool_call_id)


async def test_handoff_transfers_to_peer_who_answers_original_caller(kafka_bootstrap: str, topic_namespace: str) -> None:
    """A (``peers=[Handoff(B)]``) produces a HandoffRequest; control transfers to B over the wire; B
    continues the conversation and answers the ORIGINAL caller (the driver) — A dropped out. A's handoff
    output rode the carried conversation, attributed to A."""
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
            result = await driver.execute("handle my refund", a_in, timeout=120)

    # B answered the ORIGINAL caller (the driver); A relinquished and dropped out.
    assert result.output is not None and "refund is approved" in result.output
    # A's handoff output rode the carried conversation, attributed to A (a ModelResponse stamped with A's name).
    assert any(isinstance(m, ModelResponse) and m.name == a_name for m in result.message_history)

    await driver.close()
    await b_worker._client.close()
    await a_worker._client.close()


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
                result = await driver.execute("start the chain", a_in, timeout=120)

    assert result.output is not None and "C handled it" in result.output  # the last agent answered the original caller

    await driver.close()
    await c_worker._client.close()
    await b_worker._client.close()
    await a_worker._client.close()


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
                result = await driver.execute("ask B, who may delegate", a_in, timeout=120)

    assert result.output is not None and FINAL_OUTPUT in result.output  # A kept control and finalized
    assert "C consulted answer" in str(returns_by_call_id(result.message_history)["m1"])  # C's answer folded into A's message slot

    await driver.close()
    await c_worker._client.close()
    await b_worker._client.close()
    await a_worker._client.close()


async def test_handoff_empty_directory_lets_agent_answer_directly(kafka_bootstrap: str, topic_namespace: str) -> None:
    """A ``Handoff`` agent with NO live in-scope peer (a curated ghost never deployed): the HandoffRequest
    member is omitted (an empty ``Literal`` is unbuildable) and an ephemeral note is injected, so A answers
    directly over the wire — it never crashes or offers an action it can only fail."""
    a_name = f"{topic_namespace}-A"
    ghost = f"{topic_namespace}-ghost"  # in A's curated scope but never deployed -> not live
    a_in = f"{topic_namespace}.A.input"
    control_plane = fast_control_plane(kafka_bootstrap)

    agent_a = Agent(a_name, system_prompt="x", subscribe_topics=a_in, model_client=final_model("I'll handle it myself"), peers=[Handoff(ghost)])

    driver = Client.connect(kafka_bootstrap)
    a_worker = _worker(kafka_bootstrap, nodes=[agent_a], control_plane=control_plane)

    async with a_worker:
        result = await driver.execute("can you help", a_in, timeout=120)

    assert result.output is not None and "handle it myself" in result.output  # answered directly; no handoff member offered

    await driver.close()
    await a_worker._client.close()


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
            result = await driver.execute("handle it (override set)", a_in, tool_overrides=[override_tool], timeout=120)

    # B used its OWN tool (the override was cleared): b_tool dispatched + folded, NOT shadowed as 'unknown'.
    assert result.output is not None and FINAL_OUTPUT in result.output
    assert "B_OWN_TOOL_OK" in str(returns_by_call_id(result.message_history)["bt1"])
    assert b_tool_name not in " ".join(retry_prompt_texts(result.message_history))  # not shadowed by A's override

    await driver.close()
    await b_worker._client.close()
    await a_worker._client.close()
