"""Real-broker (``kafka`` lane) end-to-end MCP tool-call roundtrips.

The tests the suite was missing: a real MCP server + a programmatic (offline)
model + a real Worker topology, proving tool calls round-trip the whole
distributed loop over a live broker —

    the model emits a tool call
      -> the agent resolves the MCP binding from the Capability View
      -> the agent dispatches a ToolCallRef over Kafka to the MCPToolboxNode
      -> the toolbox calls the tool on the REAL MCP server (a stdio subprocess)
      -> the result returns over Kafka
      -> the agent re-enters the model loop and finalizes.

Coverage (happy + contract/edge paths — fault paths live with the fault-rail work):
  * single dispatch and concurrent fan-out (baseline);
  * fan-out slot routing keyed by tool_call_id (same tool twice);
  * routing across two MCP servers; reply routing for two agents on one toolbox;
  * the ``include`` trust boundary; and dynamic ``tools/list_changed`` re-discovery.

Reaching :data:`FINAL_OUTPUT` is the success signal: each scripted model
finalizes only after its tool call(s) have returned, so a finalized turn means
the full roundtrip completed.

The MCP servers (``_mcp_roundtrip_server*.py``) are built on the real ``mcp``
package and spawned over stdio by the toolbox; the model
(``_roundtrip_helpers.scripted_model``) is offline. Only the broker and the
MCP servers are real — no live LLM, so the only gate is ``kafka``.

Opt-in (``-m kafka`` / ``make test-kafka``); skips cleanly without Docker. Run
with ``uv run --group integration pytest tests/integration/test_mcp_roundtrip_kafka.py -m kafka``.
"""

from __future__ import annotations

import asyncio
import os
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pydantic_core
import pytest
from aiokafka.admin import AIOKafkaAdminClient

from calfkit._vendor.pydantic_ai import models
from calfkit._vendor.pydantic_ai.messages import ToolCallPart
from calfkit.client import Client
from calfkit.controlplane import ControlPlaneConfig, ControlPlaneView
from calfkit.mcp import MCPToolboxNode, StdioServerParameters
from calfkit.models.capability import CAPABILITY_TOPIC, CapabilityRecord
from calfkit.nodes import Agent
from calfkit.worker import Worker
from tests.integration._kafka_helpers import fast_control_plane
from tests.integration._roundtrip_helpers import (
    FINAL_OUTPUT,
    capturing_model,
    retry_prompt_texts,
    returns_by_call_id,
    scripted_model,
    tool_returns,
)

# Every test here needs a real broker. FunctionModel is offline, but pydantic-ai
# still gates "model requests" behind this flag, so set it (matches
# tests/integration/test_durable_fanout_agent_kafka.py).
pytestmark = pytest.mark.kafka
models.ALLOW_MODEL_REQUESTS = True

_HERE = Path(__file__).parent
_SERVER_SCRIPT = _HERE / "_mcp_roundtrip_server.py"
_SERVER_NAME = "roundtrip_server"
# Server A advertises more than the agent usually selects; `include` pins the
# agent's surface (see the trust-boundary test).
_TOOLS = ["add", "echo", "ping"]
_SERVER_B_SCRIPT = _HERE / "_mcp_roundtrip_server_b.py"
_SERVER_B_NAME = "roundtrip_server_b"

_EARLIEST = {"auto_offset_reset": "earliest"}


def _ns(node_id: str, tool: str) -> str:
    """The LLM-facing namespaced tool name (ADR-0018): ``<toolbox_node_id>__<tool>``.

    ``node_id`` is the toolbox's construction name — here the per-test-unique
    ``_server_name(topic_namespace)`` — NOT the bare server base. The model is
    advertised this namespaced name and emits it; the toolbox strips the prefix
    before calling the MCP server (which only ever sees the bare ``tool``).
    """
    return f"{node_id}__{tool}"


# ── builders ─────────────────────────────────────────────────────────────────


def _server_params(script: Path) -> StdioServerParameters:
    """Stdio params the toolbox uses to spawn an MCP server subprocess.

    Launched as ``sys.executable <script>`` so it runs under the same
    interpreter/venv as the test (``mcp`` resolves from that interpreter's
    site-packages regardless of env); env is forwarded for PATH parity.
    """
    return StdioServerParameters(command=sys.executable, args=[str(script)], env=dict(os.environ))


def _server_name(topic_namespace: str, base: str = _SERVER_NAME) -> str:
    """A per-test-unique toolbox id on the fixed ``calf.capabilities`` topic."""
    return f"{topic_namespace}.{base}"


def _worker(bootstrap: str, *, nodes: list, control_plane: ControlPlaneConfig) -> Worker:
    """A Worker on its own broker connection, control plane on, reading earliest.

    ``earliest`` is mandatory so a node's consumer-group join never races (and
    drops) the publish addressing it on a freshly-auto-created partition.
    """
    return Worker(Client.connect(bootstrap), nodes=nodes, control_plane=control_plane, extra_subscribe_kwargs=_EARLIEST)


# ── polling utilities (no bare sleeps) ───────────────────────────────────────


async def _wait(predicate: Callable[[], bool], *, timeout: float, what: str) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.1)
    raise AssertionError(f"timed out after {timeout}s waiting for: {what}")


async def _await_view(bootstrap: str, predicate: Callable[[ControlPlaneView[CapabilityRecord]], bool], *, timeout: float, what: str) -> None:
    """Open a transient capability view, wait for ``predicate`` over it, then close it.

    ``start()`` is inside the ``try`` so a failed open never leaks the view.
    """
    view: ControlPlaneView[CapabilityRecord] = ControlPlaneView.open(
        bootstrap_servers=bootstrap, topic=CAPABILITY_TOPIC, record_type=CapabilityRecord, ensure_topic=False
    )
    try:
        await view.start()
        await _wait(lambda: predicate(view), timeout=timeout, what=what)
    finally:
        await view.stop()


async def _await_capability(bootstrap: str, toolbox_id: str, *, timeout: float) -> None:
    """Block until the toolbox's CapabilityRecord is live on ``calf.capabilities``.

    Starting the agent worker only after the record exists makes the agent's
    Capability View catch-up include it, so the binding resolves on the turn.
    """
    await _await_view(bootstrap, lambda v: v.get(toolbox_id) is not None, timeout=timeout, what=f"capability record {toolbox_id!r}")


async def _await_tool_in_record(bootstrap: str, toolbox_id: str, tool: str, *, timeout: float) -> None:
    """Block until ``toolbox_id``'s live record advertises ``tool`` (re-list landed)."""

    def _present(v: ControlPlaneView[CapabilityRecord]) -> bool:
        record = v.get(toolbox_id)
        return record is not None and any(t.name == tool for t in record.tools)

    await _await_view(bootstrap, _present, timeout=timeout, what=f"tool {tool!r} in record {toolbox_id!r}")


async def _topics(bootstrap: str) -> set[str]:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        return set(await admin.list_topics())
    finally:
        await admin.close()


def _serialized(value: object) -> str:
    """JSON-serialize a materialized tool return for shape-tolerant assertions.

    An MCP return is a ``CallToolResult`` wrapped in a ``ToolReturn``; its exact
    nesting is an MCP/pydantic-ai detail, so assert on the presence of the
    computed value in the serialized form rather than a fixed attribute path.
    """
    return pydantic_core.to_json(value).decode()


# ── baseline: single dispatch + concurrent fan-out ───────────────────────────


async def test_single_tool_call_roundtrips_over_the_wire(kafka_bootstrap: str, topic_namespace: str) -> None:
    """One tool call: model -> agent -> Kafka -> toolbox -> real MCP server ->
    Kafka -> agent finalize. The server computes ``add(2, 3) == 5`` and that
    value travels all the way back into the agent's history."""
    agent_id = f"{topic_namespace}-mcp-agent"
    agent_in = f"{topic_namespace}.mcp-agent.input"
    control_plane = fast_control_plane(kafka_bootstrap)
    server_name = _server_name(topic_namespace)

    toolbox = MCPToolboxNode(server_name, connection_params=_server_params(_SERVER_SCRIPT))
    agent = Agent(
        agent_id,
        system_prompt="call the add tool",
        subscribe_topics=agent_in,
        model_client=scripted_model([ToolCallPart(_ns(server_name, "add"), {"a": 2, "b": 3}, tool_call_id="call-add")]),
        tools=[toolbox.select(include=_TOOLS)],  # include uses BARE names (C5)
    )

    driver = Client.connect(kafka_bootstrap)
    toolbox_worker = _worker(kafka_bootstrap, nodes=[toolbox], control_plane=control_plane)
    agent_worker = _worker(kafka_bootstrap, nodes=[agent], control_plane=control_plane)

    async with toolbox_worker:
        await _await_capability(kafka_bootstrap, server_name, timeout=60)
        async with agent_worker:
            result = await driver.agent(topic=agent_in).execute("add 2 and 3", timeout=120)

    assert result.output is not None and FINAL_OUTPUT in result.output
    returns = tool_returns(result.message_history)  # keyed by the model-facing (namespaced) name
    assert _ns(server_name, "add") in returns
    assert "5" in _serialized(returns[_ns(server_name, "add")])

    await driver.aclose()
    await toolbox_worker._client.aclose()
    await agent_worker._client.aclose()


async def test_mcp_iserror_result_passes_through_transparently(kafka_bootstrap: str, topic_namespace: str) -> None:
    """An MCP ``isError=True`` result (a domain failure by MCP convention) rides the reply
    slot as an ordinary, model-visible return — NOT the fault rail, and NOT ``calf.retry``-
    marked (the temporary PR-6 passthrough; catalogue XC-5). The agent sees it and
    finalizes normally, and the error content travels back in history."""
    agent_id = f"{topic_namespace}-mcp-iserror"
    agent_in = f"{topic_namespace}.mcp-iserror.input"
    control_plane = fast_control_plane(kafka_bootstrap)
    server_name = _server_name(topic_namespace)

    toolbox = MCPToolboxNode(server_name, connection_params=_server_params(_SERVER_SCRIPT))
    agent = Agent(
        agent_id,
        system_prompt="call domain_error",
        subscribe_topics=agent_in,
        model_client=scripted_model([ToolCallPart(_ns(server_name, "domain_error"), {}, tool_call_id="call-err")]),
        tools=[toolbox.select(include=["domain_error"])],  # include = BARE (C5)
    )

    driver = Client.connect(kafka_bootstrap)
    toolbox_worker = _worker(kafka_bootstrap, nodes=[toolbox], control_plane=control_plane)
    agent_worker = _worker(kafka_bootstrap, nodes=[agent], control_plane=control_plane)

    async with toolbox_worker:
        await _await_capability(kafka_bootstrap, server_name, timeout=60)
        async with agent_worker:
            result = await driver.agent(topic=agent_in).execute("trigger the error", timeout=120)

    # Finalized normally — the isError result was recoverable/model-visible, not a fault.
    assert result.output is not None and FINAL_OUTPUT in result.output
    # It arrived as an ORDINARY return (tool_returns collects ToolReturnParts only), carrying
    # the isError result through transparently.
    returns = tool_returns(result.message_history)
    assert _ns(server_name, "domain_error") in returns
    assert "isError" in _serialized(returns[_ns(server_name, "domain_error")])

    await driver.aclose()
    await toolbox_worker._client.aclose()
    await agent_worker._client.aclose()


async def test_concurrent_tool_calls_roundtrip_via_fanout(kafka_bootstrap: str, topic_namespace: str) -> None:
    """Two distinct tool calls in one model turn drive the durable in-node
    fan-out path: both dispatch concurrently, execute on the real MCP server,
    and each result lands back in its own slot."""
    agent_id = f"{topic_namespace}-mcp-fanout-agent"
    agent_in = f"{topic_namespace}.mcp-fanout-agent.input"
    control_plane = fast_control_plane(kafka_bootstrap)
    server_name = _server_name(topic_namespace)

    toolbox = MCPToolboxNode(server_name, connection_params=_server_params(_SERVER_SCRIPT))
    agent = Agent(
        agent_id,
        system_prompt="call both tools",
        subscribe_topics=agent_in,
        model_client=scripted_model(
            [
                ToolCallPart(_ns(server_name, "add"), {"a": 2, "b": 3}, tool_call_id="call-add"),
                ToolCallPart(_ns(server_name, "echo"), {"text": "hi"}, tool_call_id="call-echo"),
            ]
        ),
        tools=[toolbox.select(include=_TOOLS)],
    )
    assert agent._is_fanout_capable

    driver = Client.connect(kafka_bootstrap)
    toolbox_worker = _worker(kafka_bootstrap, nodes=[toolbox], control_plane=control_plane)
    agent_worker = _worker(kafka_bootstrap, nodes=[agent], control_plane=control_plane)

    async with toolbox_worker:
        await _await_capability(kafka_bootstrap, server_name, timeout=60)
        async with agent_worker:
            topics = await _topics(kafka_bootstrap)
            assert f"calf.fanout.{agent_id}.state" in topics
            assert f"calf.fanout.{agent_id}.basestate" in topics
            result = await driver.agent(topic=agent_in).execute("add 2 and 3, and echo hi", timeout=120)

    assert result.output is not None and FINAL_OUTPUT in result.output
    returns = tool_returns(result.message_history)
    assert _ns(server_name, "add") in returns and _ns(server_name, "echo") in returns
    assert "5" in _serialized(returns[_ns(server_name, "add")])
    assert "hi" in _serialized(returns[_ns(server_name, "echo")])

    await driver.aclose()
    await toolbox_worker._client.aclose()
    await agent_worker._client.aclose()


# ── Group A: happy paths the real broker uniquely exercises ──────────────────


async def test_duplicate_tool_concurrent_slots_route_by_call_id(kafka_bootstrap: str, topic_namespace: str) -> None:
    """The same tool called twice in one turn: fan-out keys slots by
    tool_call_id (not tool name), so both results return to their own slot."""
    agent_id = f"{topic_namespace}-dup-agent"
    agent_in = f"{topic_namespace}.dup-agent.input"
    control_plane = fast_control_plane(kafka_bootstrap)
    server_name = _server_name(topic_namespace)

    toolbox = MCPToolboxNode(server_name, connection_params=_server_params(_SERVER_SCRIPT))
    agent = Agent(
        agent_id,
        system_prompt="add two pairs",
        subscribe_topics=agent_in,
        model_client=scripted_model(
            [
                ToolCallPart(_ns(server_name, "add"), {"a": 2, "b": 3}, tool_call_id="call-a"),
                ToolCallPart(_ns(server_name, "add"), {"a": 10, "b": 20}, tool_call_id="call-b"),
            ]
        ),
        tools=[toolbox.select(include=_TOOLS)],
    )

    driver = Client.connect(kafka_bootstrap)
    toolbox_worker = _worker(kafka_bootstrap, nodes=[toolbox], control_plane=control_plane)
    agent_worker = _worker(kafka_bootstrap, nodes=[agent], control_plane=control_plane)

    async with toolbox_worker:
        await _await_capability(kafka_bootstrap, server_name, timeout=60)
        async with agent_worker:
            result = await driver.agent(topic=agent_in).execute("add 2+3 and 10+20", timeout=120)

    assert result.output is not None and FINAL_OUTPUT in result.output
    by_id = returns_by_call_id(result.message_history)
    # Same tool name, two ids, two distinct results in their own slots.
    assert "5" in _serialized(by_id["call-a"])
    assert "30" in _serialized(by_id["call-b"])

    await driver.aclose()
    await toolbox_worker._client.aclose()
    await agent_worker._client.aclose()


async def test_two_mcp_servers_route_each_call_to_its_server(kafka_bootstrap: str, topic_namespace: str) -> None:
    """An agent selecting tools from two different MCP servers routes each call
    to the correct toolbox topic: ``add`` resolves on server A, ``mul`` on B."""
    agent_id = f"{topic_namespace}-multi-server-agent"
    agent_in = f"{topic_namespace}.multi-server-agent.input"
    control_plane = fast_control_plane(kafka_bootstrap)
    server_a_name = _server_name(topic_namespace)
    server_b_name = _server_name(topic_namespace, _SERVER_B_NAME)

    box_a = MCPToolboxNode(server_a_name, connection_params=_server_params(_SERVER_SCRIPT))
    box_b = MCPToolboxNode(server_b_name, connection_params=_server_params(_SERVER_B_SCRIPT))
    agent = Agent(
        agent_id,
        system_prompt="use a tool from each server",
        subscribe_topics=agent_in,
        model_client=scripted_model(
            [
                ToolCallPart(_ns(server_a_name, "add"), {"a": 2, "b": 3}, tool_call_id="call-add"),
                ToolCallPart(_ns(server_b_name, "mul"), {"a": 4, "b": 5}, tool_call_id="call-mul"),
            ]
        ),
        tools=[box_a.select(include=["add"]), box_b.select(include=["mul"])],  # each include is BARE
    )

    driver = Client.connect(kafka_bootstrap)
    # Both toolboxes hosted in one worker (the agent reads the shared view either
    # way); each opens its own MCP session, and the shared worker publisher advertises
    # one record per toolbox.
    toolbox_worker = _worker(kafka_bootstrap, nodes=[box_a, box_b], control_plane=control_plane)
    agent_worker = _worker(kafka_bootstrap, nodes=[agent], control_plane=control_plane)

    async with toolbox_worker:
        await _await_capability(kafka_bootstrap, server_a_name, timeout=60)
        await _await_capability(kafka_bootstrap, server_b_name, timeout=60)
        async with agent_worker:
            result = await driver.agent(topic=agent_in).execute("add 2+3 and mul 4*5", timeout=120)

    assert result.output is not None and FINAL_OUTPUT in result.output
    returns = tool_returns(result.message_history)
    # Each call namespaced by its OWN toolbox; the server saw the bare name and produced these.
    assert "5" in _serialized(returns[_ns(server_a_name, "add")])  # only server A could produce 5
    assert "20" in _serialized(returns[_ns(server_b_name, "mul")])  # only server B could produce 20

    await driver.aclose()
    await toolbox_worker._client.aclose()
    await agent_worker._client.aclose()


async def test_two_agents_share_one_toolbox_replies_route_per_caller(kafka_bootstrap: str, topic_namespace: str) -> None:
    """Two agents call the same toolbox concurrently; replies route per call
    frame to each agent's own return topic — no cross-delivery."""
    a1_id = f"{topic_namespace}-agent-1"
    a1_in = f"{topic_namespace}.agent-1.input"
    a2_id = f"{topic_namespace}-agent-2"
    a2_in = f"{topic_namespace}.agent-2.input"
    control_plane = fast_control_plane(kafka_bootstrap)
    server_name = _server_name(topic_namespace)

    toolbox = MCPToolboxNode(server_name, connection_params=_server_params(_SERVER_SCRIPT))
    agent1 = Agent(
        a1_id,
        system_prompt="add",
        subscribe_topics=a1_in,
        model_client=scripted_model([ToolCallPart(_ns(server_name, "add"), {"a": 2, "b": 3}, tool_call_id="c1")]),
        tools=[toolbox.select(include=["add"])],
    )
    agent2 = Agent(
        a2_id,
        system_prompt="add",
        subscribe_topics=a2_in,
        model_client=scripted_model([ToolCallPart(_ns(server_name, "add"), {"a": 10, "b": 20}, tool_call_id="c2")]),
        tools=[toolbox.select(include=["add"])],
    )

    driver = Client.connect(kafka_bootstrap)
    toolbox_worker = _worker(kafka_bootstrap, nodes=[toolbox], control_plane=control_plane)
    agent_worker = _worker(kafka_bootstrap, nodes=[agent1, agent2], control_plane=control_plane)

    async with toolbox_worker:
        await _await_capability(kafka_bootstrap, server_name, timeout=60)
        async with agent_worker:
            h1 = await driver.agent(topic=a1_in).start("agent 1 add 2+3")
            h2 = await driver.agent(topic=a2_in).start("agent 2 add 10+20")
            r1 = await h1.result(timeout=120)
            r2 = await h2.result(timeout=120)

    assert r1.output is not None and FINAL_OUTPUT in r1.output
    assert r2.output is not None and FINAL_OUTPUT in r2.output
    # Each caller got ITS OWN result back, not the other's.
    assert "5" in _serialized(tool_returns(r1.message_history)[_ns(server_name, "add")])
    assert "30" in _serialized(tool_returns(r2.message_history)[_ns(server_name, "add")])

    await driver.aclose()
    await toolbox_worker._client.aclose()
    await agent_worker._client.aclose()


# ── Group B: contract / edge paths ───────────────────────────────────────────


async def test_include_pinning_blocks_unselected_tool(kafka_bootstrap: str, topic_namespace: str) -> None:
    """``include`` is a trust boundary: the server advertises ``danger`` but the
    agent did not select it, so a model call for ``danger`` is never dispatched
    — the agent answers the model with an unknown-tool retry and finalizes."""
    agent_id = f"{topic_namespace}-pin-agent"
    agent_in = f"{topic_namespace}.pin-agent.input"
    control_plane = fast_control_plane(kafka_bootstrap)
    server_name = _server_name(topic_namespace)

    toolbox = MCPToolboxNode(server_name, connection_params=_server_params(_SERVER_SCRIPT))
    agent = Agent(
        agent_id,
        system_prompt="try to call danger",
        subscribe_topics=agent_in,
        model_client=scripted_model([ToolCallPart(_ns(server_name, "danger"), {}, tool_call_id="call-danger")]),
        tools=[toolbox.select(include=["add"])],  # danger is advertised but NOT included (include is BARE)
    )

    driver = Client.connect(kafka_bootstrap)
    toolbox_worker = _worker(kafka_bootstrap, nodes=[toolbox], control_plane=control_plane)
    agent_worker = _worker(kafka_bootstrap, nodes=[agent], control_plane=control_plane)

    async with toolbox_worker:
        await _await_capability(kafka_bootstrap, server_name, timeout=60)
        async with agent_worker:
            result = await driver.agent(topic=agent_in).execute("call danger", timeout=120)

    assert result.output is not None and FINAL_OUTPUT in result.output
    # danger never reached the server / never round-tripped (check the namespaced key it WOULD have had).
    assert _ns(server_name, "danger") not in tool_returns(result.message_history)
    # The agent told the model the tool does not exist (retry text carries the emitted name).
    assert any("danger" in text for text in retry_prompt_texts(result.message_history))

    await driver.aclose()
    await toolbox_worker._client.aclose()
    await agent_worker._client.aclose()


async def test_tools_list_changed_grows_the_toolset(kafka_bootstrap: str, topic_namespace: str) -> None:
    """Dynamic discovery: a tool registers a NEW tool at runtime and emits
    ``notifications/tools/list_changed``; the toolbox re-lists into its cache and the
    worker's heartbeat carries the grown record on the next tick (pull), and a fresh
    agent (whose view catch-up includes the grown record) can call the new tool
    end-to-end."""
    enable_id = f"{topic_namespace}-enable-agent"
    enable_in = f"{topic_namespace}.enable-agent.input"
    bonus_id = f"{topic_namespace}-bonus-agent"
    bonus_in = f"{topic_namespace}.bonus-agent.input"
    control_plane = fast_control_plane(kafka_bootstrap)
    server_name = _server_name(topic_namespace)

    toolbox = MCPToolboxNode(server_name, connection_params=_server_params(_SERVER_SCRIPT))
    enable_agent = Agent(
        enable_id,
        system_prompt="enable the bonus tool",
        subscribe_topics=enable_in,
        model_client=scripted_model([ToolCallPart(_ns(server_name, "enable_bonus"), {}, tool_call_id="call-enable")]),
        tools=[toolbox.select(include=["enable_bonus"])],
    )
    bonus_agent = Agent(
        bonus_id,
        system_prompt="use the bonus tool",
        subscribe_topics=bonus_in,
        model_client=scripted_model([ToolCallPart(_ns(server_name, "bonus"), {}, tool_call_id="call-bonus")]),
        tools=[toolbox.select(include=["bonus"])],
    )

    driver = Client.connect(kafka_bootstrap)
    toolbox_worker = _worker(kafka_bootstrap, nodes=[toolbox], control_plane=control_plane)
    enable_worker = _worker(kafka_bootstrap, nodes=[enable_agent], control_plane=control_plane)
    bonus_worker = _worker(kafka_bootstrap, nodes=[bonus_agent], control_plane=control_plane)

    async with toolbox_worker:
        await _await_capability(kafka_bootstrap, server_name, timeout=60)

        # 1) Trigger the runtime tool registration + list_changed notification.
        async with enable_worker:
            r1 = await driver.agent(topic=enable_in).execute("enable the bonus tool", timeout=120)
        assert r1.output is not None and FINAL_OUTPUT in r1.output
        assert "enabled" in _serialized(tool_returns(r1.message_history)[_ns(server_name, "enable_bonus")])

        # 2) The toolbox re-listed into its cache; the next heartbeat carried the grown record — wait
        #    for it. The WIRE record carries BARE tool names, so probe for the bare "bonus".
        await _await_tool_in_record(kafka_bootstrap, server_name, "bonus", timeout=60)

        # 3) A fresh agent's view catch-up now includes 'bonus' deterministically.
        async with bonus_worker:
            r2 = await driver.agent(topic=bonus_in).execute("use the bonus tool", timeout=120)
        assert r2.output is not None and FINAL_OUTPUT in r2.output
        assert "bonus-result" in _serialized(tool_returns(r2.message_history)[_ns(server_name, "bonus")])

    await driver.aclose()
    await toolbox_worker._client.aclose()
    await enable_worker._client.aclose()
    await bonus_worker._client.aclose()


# ── Group C: the agent's point of view of its advertised tools ───────────────


async def test_agent_pov_is_namespaced_and_strips_to_bare_on_dispatch(
    kafka_bootstrap: str, topic_namespace: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Headline end-to-end namespacing proof, in one test (ADR-0018):

    1. every tool the agent presents in its POV (``AgentInfo.function_tools``) exists and is
       namespaced ``<toolbox_node_id>__<tool>`` — matched against the live ``CapabilityRecord``
       on ``calf.capabilities`` by NAME and by SCHEMA, nothing added or dropped;
    2. calls drawn from that POV (by their namespaced names) are dispatched namespaced and
       STRIPPED TO BARE at the ``MCPToolboxNode`` before the server — captured at the MCP
       ``call_tool`` boundary, the truest "what the server received" evidence; and
    3. the results round-trip back into history.

    Only the broker + MCP server are real; the model is offline (``capturing_model``).
    """
    import mcp

    agent_id = f"{topic_namespace}-pov-agent"
    agent_in = f"{topic_namespace}.pov-agent.input"
    control_plane = fast_control_plane(kafka_bootstrap)
    server_name = _server_name(topic_namespace)

    # Capture the tool name the MCP server actually receives (after the node strips its prefix).
    # monkeypatch auto-reverts; patching the class method spies every session in this test.
    server_names: list[str] = []
    _orig_call_tool = mcp.ClientSession.call_tool

    async def _spy_call_tool(self: Any, name: str, *args: Any, **kwargs: Any) -> Any:
        server_names.append(name)
        return await _orig_call_tool(self, name, *args, **kwargs)

    monkeypatch.setattr(mcp.ClientSession, "call_tool", _spy_call_tool)

    toolbox = MCPToolboxNode(server_name, connection_params=_server_params(_SERVER_SCRIPT))
    pov: dict[str, Any] = {}  # namespaced name -> ToolDefinition the agent presented to the model
    agent = Agent(
        agent_id,
        system_prompt="call add and echo",
        subscribe_topics=agent_in,
        model_client=capturing_model(
            pov,
            [
                ToolCallPart(_ns(server_name, "add"), {"a": 2, "b": 3}, tool_call_id="call-add"),
                ToolCallPart(_ns(server_name, "echo"), {"text": "hi"}, tool_call_id="call-echo"),
            ],
        ),
        tools=[toolbox],  # NO include: the agent sees the toolbox's full advertised set
    )

    advertised: dict[str, Any] = {}  # BARE name -> parameters_json_schema, read straight from the view record

    def _capture_advertised(view: ControlPlaneView[CapabilityRecord]) -> bool:
        record = view.get(server_name)
        if record is None:
            return False
        advertised.clear()
        advertised.update({t.name: t.parameters_json_schema for t in record.tools})
        return True

    driver = Client.connect(kafka_bootstrap)
    toolbox_worker = _worker(kafka_bootstrap, nodes=[toolbox], control_plane=control_plane)
    agent_worker = _worker(kafka_bootstrap, nodes=[agent], control_plane=control_plane)

    async with toolbox_worker:
        await _await_capability(kafka_bootstrap, server_name, timeout=60)
        # The source of truth the agent reads: what the toolbox advertised on the view (BARE names).
        await _await_view(kafka_bootstrap, _capture_advertised, timeout=60, what=f"advertised tools for {server_name!r}")
        async with agent_worker:
            result = await driver.agent(topic=agent_in).execute("add 2 and 3, and echo hi", timeout=120)

    returns = tool_returns(result.message_history)  # keyed by the model-facing (namespaced) name

    # (1) Existence + naming for the WHOLE set: the toolbox advertised the bare server set, and the
    #     agent's POV is exactly that set NAMESPACED — by NAME and by SCHEMA, nothing added or dropped.
    assert set(advertised) == {"add", "echo", "ping", "danger", "domain_error", "enable_bonus"}  # bare wire names
    assert pov, "the model never captured the agent's tool POV"
    assert set(pov) == {_ns(server_name, n) for n in advertised}
    assert {name: td.parameters_json_schema for name, td in pov.items()} == {_ns(server_name, n): s for n, s in advertised.items()}

    # (2) The called tools were drawn from the POV (by their namespaced names).
    assert {_ns(server_name, "add"), _ns(server_name, "echo")} <= set(pov)

    # (3) Strip verified at the node->server boundary: the server saw ONLY the BARE names; the
    #     namespaced prefix never crossed to it.
    assert set(server_names) == {"add", "echo"}
    assert all(n in advertised for n in server_names)
    assert _ns(server_name, "add") not in server_names

    # (4) Results round-trip back into history, keyed by the model-facing namespaced name.
    assert result.output is not None and FINAL_OUTPUT in result.output
    assert "5" in _serialized(returns[_ns(server_name, "add")])
    assert "hi" in _serialized(returns[_ns(server_name, "echo")])

    await driver.aclose()
    await toolbox_worker._client.aclose()
    await agent_worker._client.aclose()
