"""Real-broker (``kafka`` lane) end-to-end MCP tool-call roundtrips.

The tests the suite was missing: a real MCP server + a programmatic (offline)
model + a real Worker topology, proving tool calls round-trip the whole
distributed loop over a live broker —

    the model emits a tool call
      -> the agent resolves the MCP binding from the Capability View
      -> the agent dispatches a ToolCallRef over Kafka to the MCPToolbox node
      -> the toolbox calls the tool on the REAL MCP server (a stdio subprocess)
      -> the result returns over Kafka
      -> the agent re-enters the model loop and finalizes.

Coverage (happy + contract/edge paths — fault paths live with the fault-rail work):
  * single dispatch and concurrent fan-out (baseline);
  * fan-out slot routing keyed by tool_call_id (same tool twice);
  * sequential mode (one call per turn, no durable store);
  * routing across two MCP servers; reply routing for two agents on one toolbox;
  * the ``include`` trust boundary; and dynamic ``tools/list_changed`` re-discovery.

Reaching :data:`FINAL_OUTPUT` is the success signal: each scripted model
finalizes only after its tool call(s) have returned, so a finalized turn means
the full roundtrip completed.

The MCP servers (``_mcp_roundtrip_server*.py``) are built on the real ``mcp``
package and spawned over stdio by the toolbox; the model
(``_mcp_roundtrip_helpers.scripted_model``) is offline. Only the broker and the
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

import pydantic_core
import pytest
from aiokafka.admin import AIOKafkaAdminClient
from ktables import KafkaTable

from calfkit._vendor.pydantic_ai import models
from calfkit._vendor.pydantic_ai.messages import ToolCallPart
from calfkit.client import Client
from calfkit.mcp import MCPToolbox, StdioServerParameters
from calfkit.models.capability import CapabilityRecord
from calfkit.nodes import Agent
from calfkit.worker import Worker
from calfkit.worker.worker_config import MCPDiscoveryConfig
from tests.integration._mcp_roundtrip_helpers import (
    FINAL_OUTPUT,
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


# ── builders ─────────────────────────────────────────────────────────────────


def _server_params(script: Path) -> StdioServerParameters:
    """Stdio params the toolbox uses to spawn an MCP server subprocess.

    Launched as ``sys.executable <script>`` so it runs under the same
    interpreter/venv as the test (``mcp`` resolves from that interpreter's
    site-packages regardless of env); env is forwarded for PATH parity.
    """
    return StdioServerParameters(command=sys.executable, args=[str(script)], env=dict(os.environ))


def _discovery(cap_topic: str, bootstrap: str) -> MCPDiscoveryConfig:
    return MCPDiscoveryConfig(topic=cap_topic, heartbeat_interval=5.0, bootstrap_servers=bootstrap)


def _worker(bootstrap: str, *, nodes: list, discovery: MCPDiscoveryConfig) -> Worker:
    """A Worker on its own broker connection, MCP discovery on, reading earliest.

    ``earliest`` is mandatory so a node's consumer-group join never races (and
    drops) the publish addressing it on a freshly-auto-created partition.
    """
    return Worker(Client.connect(bootstrap), nodes=nodes, mcp_discovery=discovery, extra_subscribe_kwargs=_EARLIEST)


# ── polling utilities (no bare sleeps) ───────────────────────────────────────


async def _wait(predicate: Callable[[], bool], *, timeout: float, what: str) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.1)
    raise AssertionError(f"timed out after {timeout}s waiting for: {what}")


async def _await_capability(bootstrap: str, topic: str, toolbox_id: str, *, timeout: float) -> None:
    """Block until the toolbox's CapabilityRecord is visible on ``topic``.

    Starting the agent worker only after the record exists makes the agent's
    Capability View catch-up include it, so the binding resolves on the turn.
    """
    table: KafkaTable[CapabilityRecord] = KafkaTable.json(bootstrap_servers=bootstrap, topic=topic, model=CapabilityRecord, ensure_topic=False)
    async with table:
        await _wait(lambda: toolbox_id in table, timeout=timeout, what=f"capability record {toolbox_id!r}")


async def _await_tool_in_record(bootstrap: str, topic: str, toolbox_id: str, tool: str, *, timeout: float) -> None:
    """Block until ``toolbox_id``'s record advertises ``tool`` (re-list landed)."""
    table: KafkaTable[CapabilityRecord] = KafkaTable.json(bootstrap_servers=bootstrap, topic=topic, model=CapabilityRecord, ensure_topic=False)

    def _present() -> bool:
        record = table.get(toolbox_id)
        return record is not None and any(t.name == tool for t in record.tools)

    async with table:
        await _wait(_present, timeout=timeout, what=f"tool {tool!r} in record {toolbox_id!r}")


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
    cap_topic = f"{topic_namespace}.capabilities"
    discovery = _discovery(cap_topic, kafka_bootstrap)

    toolbox = MCPToolbox(_SERVER_NAME, connection_params=_server_params(_SERVER_SCRIPT))
    agent = Agent(
        agent_id,
        system_prompt="call the add tool",
        subscribe_topics=agent_in,
        model_client=scripted_model([ToolCallPart("add", {"a": 2, "b": 3}, tool_call_id="call-add")]),
        tools=[toolbox.select(include=_TOOLS)],
    )

    driver = Client.connect(kafka_bootstrap)
    toolbox_worker = _worker(kafka_bootstrap, nodes=[toolbox], discovery=discovery)
    agent_worker = _worker(kafka_bootstrap, nodes=[agent], discovery=discovery)

    async with toolbox_worker:
        await _await_capability(kafka_bootstrap, cap_topic, _SERVER_NAME, timeout=60)
        async with agent_worker:
            result = await driver.execute("add 2 and 3", agent_in, timeout=120)

    assert result.output is not None and FINAL_OUTPUT in result.output
    returns = tool_returns(result.message_history)
    assert "add" in returns
    assert "5" in _serialized(returns["add"])

    await driver.close()
    await toolbox_worker._client.close()
    await agent_worker._client.close()


async def test_concurrent_tool_calls_roundtrip_via_fanout(kafka_bootstrap: str, topic_namespace: str) -> None:
    """Two distinct tool calls in one model turn drive the durable in-node
    fan-out path: both dispatch concurrently, execute on the real MCP server,
    and each result lands back in its own slot."""
    agent_id = f"{topic_namespace}-mcp-fanout-agent"
    agent_in = f"{topic_namespace}.mcp-fanout-agent.input"
    cap_topic = f"{topic_namespace}.capabilities"
    discovery = _discovery(cap_topic, kafka_bootstrap)

    toolbox = MCPToolbox(_SERVER_NAME, connection_params=_server_params(_SERVER_SCRIPT))
    agent = Agent(
        agent_id,
        system_prompt="call both tools",
        subscribe_topics=agent_in,
        model_client=scripted_model(
            [
                ToolCallPart("add", {"a": 2, "b": 3}, tool_call_id="call-add"),
                ToolCallPart("echo", {"text": "hi"}, tool_call_id="call-echo"),
            ]
        ),
        tools=[toolbox.select(include=_TOOLS)],
    )
    assert agent._is_fanout_capable

    driver = Client.connect(kafka_bootstrap)
    toolbox_worker = _worker(kafka_bootstrap, nodes=[toolbox], discovery=discovery)
    agent_worker = _worker(kafka_bootstrap, nodes=[agent], discovery=discovery)

    async with toolbox_worker:
        await _await_capability(kafka_bootstrap, cap_topic, _SERVER_NAME, timeout=60)
        async with agent_worker:
            topics = await _topics(kafka_bootstrap)
            assert f"calf.fanout.{agent_id}.state" in topics
            assert f"calf.fanout.{agent_id}.basestate" in topics
            result = await driver.execute("add 2 and 3, and echo hi", agent_in, timeout=120)

    assert result.output is not None and FINAL_OUTPUT in result.output
    returns = tool_returns(result.message_history)
    assert "add" in returns and "echo" in returns
    assert "5" in _serialized(returns["add"])
    assert "hi" in _serialized(returns["echo"])

    await driver.close()
    await toolbox_worker._client.close()
    await agent_worker._client.close()


# ── Group A: happy paths the real broker uniquely exercises ──────────────────


async def test_duplicate_tool_concurrent_slots_route_by_call_id(kafka_bootstrap: str, topic_namespace: str) -> None:
    """The same tool called twice in one turn: fan-out keys slots by
    tool_call_id (not tool name), so both results return to their own slot."""
    agent_id = f"{topic_namespace}-dup-agent"
    agent_in = f"{topic_namespace}.dup-agent.input"
    cap_topic = f"{topic_namespace}.capabilities"
    discovery = _discovery(cap_topic, kafka_bootstrap)

    toolbox = MCPToolbox(_SERVER_NAME, connection_params=_server_params(_SERVER_SCRIPT))
    agent = Agent(
        agent_id,
        system_prompt="add two pairs",
        subscribe_topics=agent_in,
        model_client=scripted_model(
            [
                ToolCallPart("add", {"a": 2, "b": 3}, tool_call_id="call-a"),
                ToolCallPart("add", {"a": 10, "b": 20}, tool_call_id="call-b"),
            ]
        ),
        tools=[toolbox.select(include=_TOOLS)],
    )

    driver = Client.connect(kafka_bootstrap)
    toolbox_worker = _worker(kafka_bootstrap, nodes=[toolbox], discovery=discovery)
    agent_worker = _worker(kafka_bootstrap, nodes=[agent], discovery=discovery)

    async with toolbox_worker:
        await _await_capability(kafka_bootstrap, cap_topic, _SERVER_NAME, timeout=60)
        async with agent_worker:
            result = await driver.execute("add 2+3 and 10+20", agent_in, timeout=120)

    assert result.output is not None and FINAL_OUTPUT in result.output
    by_id = returns_by_call_id(result.message_history)
    # Same tool name, two ids, two distinct results in their own slots.
    assert "5" in _serialized(by_id["call-a"])
    assert "30" in _serialized(by_id["call-b"])

    await driver.close()
    await toolbox_worker._client.close()
    await agent_worker._client.close()


async def test_sequential_mode_dispatches_without_fanout(kafka_bootstrap: str, topic_namespace: str) -> None:
    """A sequential-only agent emitting two calls dispatches them one per turn
    and never opens the durable fan-out store (no calf.fanout topics), yet both
    results still round-trip."""
    agent_id = f"{topic_namespace}-seq-agent"
    agent_in = f"{topic_namespace}.seq-agent.input"
    cap_topic = f"{topic_namespace}.capabilities"
    discovery = _discovery(cap_topic, kafka_bootstrap)

    toolbox = MCPToolbox(_SERVER_NAME, connection_params=_server_params(_SERVER_SCRIPT))
    agent = Agent(
        agent_id,
        system_prompt="call both tools sequentially",
        subscribe_topics=agent_in,
        model_client=scripted_model(
            [
                ToolCallPart("add", {"a": 2, "b": 3}, tool_call_id="call-add"),
                ToolCallPart("echo", {"text": "hi"}, tool_call_id="call-echo"),
            ]
        ),
        tools=[toolbox.select(include=_TOOLS)],
        sequential_only_mode=True,
    )
    assert not agent._is_fanout_capable

    driver = Client.connect(kafka_bootstrap)
    toolbox_worker = _worker(kafka_bootstrap, nodes=[toolbox], discovery=discovery)
    agent_worker = _worker(kafka_bootstrap, nodes=[agent], discovery=discovery)

    async with toolbox_worker:
        await _await_capability(kafka_bootstrap, cap_topic, _SERVER_NAME, timeout=60)
        async with agent_worker:
            result = await driver.execute("add then echo", agent_in, timeout=120)
            topics = await _topics(kafka_bootstrap)

    assert result.output is not None and FINAL_OUTPUT in result.output
    returns = tool_returns(result.message_history)
    assert "5" in _serialized(returns["add"])
    assert "hi" in _serialized(returns["echo"])
    # No durable store was opened — the sequential path never fans out.
    assert f"calf.fanout.{agent_id}.state" not in topics
    assert f"calf.fanout.{agent_id}.basestate" not in topics

    await driver.close()
    await toolbox_worker._client.close()
    await agent_worker._client.close()


async def test_two_mcp_servers_route_each_call_to_its_server(kafka_bootstrap: str, topic_namespace: str) -> None:
    """An agent selecting tools from two different MCP servers routes each call
    to the correct toolbox topic: ``add`` resolves on server A, ``mul`` on B."""
    agent_id = f"{topic_namespace}-multi-server-agent"
    agent_in = f"{topic_namespace}.multi-server-agent.input"
    cap_topic = f"{topic_namespace}.capabilities"
    discovery = _discovery(cap_topic, kafka_bootstrap)

    box_a = MCPToolbox(_SERVER_NAME, connection_params=_server_params(_SERVER_SCRIPT))
    box_b = MCPToolbox(_SERVER_B_NAME, connection_params=_server_params(_SERVER_B_SCRIPT))
    agent = Agent(
        agent_id,
        system_prompt="use a tool from each server",
        subscribe_topics=agent_in,
        model_client=scripted_model(
            [
                ToolCallPart("add", {"a": 2, "b": 3}, tool_call_id="call-add"),
                ToolCallPart("mul", {"a": 4, "b": 5}, tool_call_id="call-mul"),
            ]
        ),
        tools=[box_a.select(include=["add"]), box_b.select(include=["mul"])],
    )

    driver = Client.connect(kafka_bootstrap)
    # Both toolboxes hosted in one worker (the agent reads the shared view either
    # way); each opens its own MCP session and publishes its own record.
    toolbox_worker = _worker(kafka_bootstrap, nodes=[box_a, box_b], discovery=discovery)
    agent_worker = _worker(kafka_bootstrap, nodes=[agent], discovery=discovery)

    async with toolbox_worker:
        await _await_capability(kafka_bootstrap, cap_topic, _SERVER_NAME, timeout=60)
        await _await_capability(kafka_bootstrap, cap_topic, _SERVER_B_NAME, timeout=60)
        async with agent_worker:
            result = await driver.execute("add 2+3 and mul 4*5", agent_in, timeout=120)

    assert result.output is not None and FINAL_OUTPUT in result.output
    returns = tool_returns(result.message_history)
    assert "5" in _serialized(returns["add"])  # only server A could produce 5
    assert "20" in _serialized(returns["mul"])  # only server B could produce 20

    await driver.close()
    await toolbox_worker._client.close()
    await agent_worker._client.close()


async def test_two_agents_share_one_toolbox_replies_route_per_caller(kafka_bootstrap: str, topic_namespace: str) -> None:
    """Two agents call the same toolbox concurrently; replies route per call
    frame to each agent's own return topic — no cross-delivery."""
    a1_id = f"{topic_namespace}-agent-1"
    a1_in = f"{topic_namespace}.agent-1.input"
    a2_id = f"{topic_namespace}-agent-2"
    a2_in = f"{topic_namespace}.agent-2.input"
    cap_topic = f"{topic_namespace}.capabilities"
    discovery = _discovery(cap_topic, kafka_bootstrap)

    toolbox = MCPToolbox(_SERVER_NAME, connection_params=_server_params(_SERVER_SCRIPT))
    agent1 = Agent(
        a1_id,
        system_prompt="add",
        subscribe_topics=a1_in,
        model_client=scripted_model([ToolCallPart("add", {"a": 2, "b": 3}, tool_call_id="c1")]),
        tools=[toolbox.select(include=["add"])],
    )
    agent2 = Agent(
        a2_id,
        system_prompt="add",
        subscribe_topics=a2_in,
        model_client=scripted_model([ToolCallPart("add", {"a": 10, "b": 20}, tool_call_id="c2")]),
        tools=[toolbox.select(include=["add"])],
    )

    driver = Client.connect(kafka_bootstrap)
    toolbox_worker = _worker(kafka_bootstrap, nodes=[toolbox], discovery=discovery)
    agent_worker = _worker(kafka_bootstrap, nodes=[agent1, agent2], discovery=discovery)

    async with toolbox_worker:
        await _await_capability(kafka_bootstrap, cap_topic, _SERVER_NAME, timeout=60)
        async with agent_worker:
            h1 = await driver.start("agent 1 add 2+3", a1_in)
            h2 = await driver.start("agent 2 add 10+20", a2_in)
            r1 = await h1.result(timeout=120)
            r2 = await h2.result(timeout=120)

    assert r1.output is not None and FINAL_OUTPUT in r1.output
    assert r2.output is not None and FINAL_OUTPUT in r2.output
    # Each caller got ITS OWN result back, not the other's.
    assert "5" in _serialized(tool_returns(r1.message_history)["add"])
    assert "30" in _serialized(tool_returns(r2.message_history)["add"])

    await driver.close()
    await toolbox_worker._client.close()
    await agent_worker._client.close()


# ── Group B: contract / edge paths ───────────────────────────────────────────


async def test_include_pinning_blocks_unselected_tool(kafka_bootstrap: str, topic_namespace: str) -> None:
    """``include`` is a trust boundary: the server advertises ``danger`` but the
    agent did not select it, so a model call for ``danger`` is never dispatched
    — the agent answers the model with an unknown-tool retry and finalizes."""
    agent_id = f"{topic_namespace}-pin-agent"
    agent_in = f"{topic_namespace}.pin-agent.input"
    cap_topic = f"{topic_namespace}.capabilities"
    discovery = _discovery(cap_topic, kafka_bootstrap)

    toolbox = MCPToolbox(_SERVER_NAME, connection_params=_server_params(_SERVER_SCRIPT))
    agent = Agent(
        agent_id,
        system_prompt="try to call danger",
        subscribe_topics=agent_in,
        model_client=scripted_model([ToolCallPart("danger", {}, tool_call_id="call-danger")]),
        tools=[toolbox.select(include=["add"])],  # danger is advertised but NOT included
    )

    driver = Client.connect(kafka_bootstrap)
    toolbox_worker = _worker(kafka_bootstrap, nodes=[toolbox], discovery=discovery)
    agent_worker = _worker(kafka_bootstrap, nodes=[agent], discovery=discovery)

    async with toolbox_worker:
        await _await_capability(kafka_bootstrap, cap_topic, _SERVER_NAME, timeout=60)
        async with agent_worker:
            result = await driver.execute("call danger", agent_in, timeout=120)

    assert result.output is not None and FINAL_OUTPUT in result.output
    # danger never reached the server / never round-tripped.
    assert "danger" not in tool_returns(result.message_history)
    # The agent told the model the tool does not exist.
    assert any("danger" in text for text in retry_prompt_texts(result.message_history))

    await driver.close()
    await toolbox_worker._client.close()
    await agent_worker._client.close()


async def test_tools_list_changed_grows_the_toolset(kafka_bootstrap: str, topic_namespace: str) -> None:
    """Dynamic discovery: a tool registers a NEW tool at runtime and emits
    ``notifications/tools/list_changed``; the toolbox re-lists and re-publishes,
    and a fresh agent (whose view catch-up includes the grown record) can call
    the new tool end-to-end."""
    enable_id = f"{topic_namespace}-enable-agent"
    enable_in = f"{topic_namespace}.enable-agent.input"
    bonus_id = f"{topic_namespace}-bonus-agent"
    bonus_in = f"{topic_namespace}.bonus-agent.input"
    cap_topic = f"{topic_namespace}.capabilities"
    discovery = _discovery(cap_topic, kafka_bootstrap)

    toolbox = MCPToolbox(_SERVER_NAME, connection_params=_server_params(_SERVER_SCRIPT))
    enable_agent = Agent(
        enable_id,
        system_prompt="enable the bonus tool",
        subscribe_topics=enable_in,
        model_client=scripted_model([ToolCallPart("enable_bonus", {}, tool_call_id="call-enable")]),
        tools=[toolbox.select(include=["enable_bonus"])],
    )
    bonus_agent = Agent(
        bonus_id,
        system_prompt="use the bonus tool",
        subscribe_topics=bonus_in,
        model_client=scripted_model([ToolCallPart("bonus", {}, tool_call_id="call-bonus")]),
        tools=[toolbox.select(include=["bonus"])],
    )

    driver = Client.connect(kafka_bootstrap)
    toolbox_worker = _worker(kafka_bootstrap, nodes=[toolbox], discovery=discovery)
    enable_worker = _worker(kafka_bootstrap, nodes=[enable_agent], discovery=discovery)
    bonus_worker = _worker(kafka_bootstrap, nodes=[bonus_agent], discovery=discovery)

    async with toolbox_worker:
        await _await_capability(kafka_bootstrap, cap_topic, _SERVER_NAME, timeout=60)

        # 1) Trigger the runtime tool registration + list_changed notification.
        async with enable_worker:
            r1 = await driver.execute("enable the bonus tool", enable_in, timeout=120)
        assert r1.output is not None and FINAL_OUTPUT in r1.output
        assert "enabled" in _serialized(tool_returns(r1.message_history)["enable_bonus"])

        # 2) The toolbox re-listed and re-published; wait for the grown record.
        await _await_tool_in_record(kafka_bootstrap, cap_topic, _SERVER_NAME, "bonus", timeout=60)

        # 3) A fresh agent's view catch-up now includes 'bonus' deterministically.
        async with bonus_worker:
            r2 = await driver.execute("use the bonus tool", bonus_in, timeout=120)
        assert r2.output is not None and FINAL_OUTPUT in r2.output
        assert "bonus-result" in _serialized(tool_returns(r2.message_history)["bonus"])

    await driver.close()
    await toolbox_worker._client.close()
    await enable_worker._client.close()
    await bonus_worker._client.close()
