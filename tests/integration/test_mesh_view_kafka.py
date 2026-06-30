"""Real-broker (``kafka`` lane) end-to-end ``client.mesh`` reads.

A live worker advertises an ``Agent`` (its ``AgentCard`` on ``calf.agents``) and a function
tool node (its ``CapabilityRecord`` on ``calf.capabilities``); an MCP toolbox advertises a
toolbox ``CapabilityRecord`` via the worker-owned publisher (a fake MCP session — publishing
only needs ``list_tools``; the full worker lifecycle would connect to a real MCP server). A
pure-observer ``Client.connect(...)`` reads them back through ``client.mesh.get_agents()`` /
``get_tools()``, projected to the public DTOs.

The missing-topic -> ``reason="open_failed"`` case runs on the NO-AUTO-CREATE provisioning lane
(``test_topic_provisioning.py``), not here: this lane's Redpanda dev-container has topic
auto-create ON, where a naive consumer's metadata fetch auto-creates a missing directory topic
(an empty roster, not ``open_failed``) — the ADR-0029 / spec §6.3 caveat.

Opt-in (``-m kafka`` / ``make test-kafka``); skips cleanly without Docker.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable, Mapping
from types import SimpleNamespace
from typing import Any

import pytest
from mcp.types import ListToolsResult, Tool

from calfkit import MeshUnavailableError
from calfkit._vendor.pydantic_ai import models
from calfkit.client import Client, MeshViewConfig, ToolboxInfo, ToolNodeInfo
from calfkit.controlplane import ControlPlaneConfig
from calfkit.controlplane.publisher import control_plane_writer_key
from calfkit.mcp.mcp_toolbox import MCPToolboxNode
from calfkit.mcp.mcp_transport import StreamableHttpParameters
from calfkit.models.capability import CAPABILITY_TOPIC
from calfkit.nodes import Agent, agent_tool
from calfkit.providers.pydantic_ai.model_client import PydanticModelClient
from calfkit.provisioning import ProvisioningConfig
from calfkit.tuning import KTableReaderTuning
from calfkit.worker import Worker
from tests.integration._kafka_helpers import fast_control_plane

# Every test here needs a real broker. The agent never runs a turn (advertise-only), but
# pydantic-ai still gates "model requests" behind this flag (matches the other kafka suites).
pytestmark = pytest.mark.kafka
models.ALLOW_MODEL_REQUESTS = True

_EARLIEST = {"auto_offset_reset": "earliest"}
_FAST_MESH = MeshViewConfig(reader_tuning=KTableReaderTuning(poll_timeout_ms=20, fetch_max_wait_ms=10))


class FakeModel(PydanticModelClient):
    """An advertise-only model: the agent is hosted to publish its AgentCard, never to run a turn."""

    @property
    def model_name(self) -> str:
        return "fake"

    @property
    def system(self) -> str:
        return "fake"

    async def request(self, *args: object, **kwargs: object) -> object:
        raise NotImplementedError


class FakeSession:
    """A fake MCP session — publishing a toolbox's CapabilityRecord only needs ``list_tools``."""

    def __init__(self, tool_names: tuple[str, ...]) -> None:
        self._tool_names = tool_names

    async def list_tools(self) -> ListToolsResult:
        return ListToolsResult(tools=[Tool(name=n, description=n, inputSchema={"type": "object", "properties": {}}) for n in self._tool_names])


def _add(a: int, b: int) -> int:
    return a + b


async def _wait_mesh(client: Client, predicate: Callable[[Mapping[str, Any], Mapping[str, Any]], bool], *, timeout: float) -> None:
    """Poll ``client.mesh`` until ``predicate(agents, tools)`` holds (tolerating catch-up raises)."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        with contextlib.suppress(MeshUnavailableError):
            agents = await client.mesh.get_agents()
            tools = await client.mesh.get_tools()
            if predicate(agents, tools):
                return
        await asyncio.sleep(0.2)
    raise AssertionError(f"client.mesh did not converge within {timeout}s")


async def test_mesh_reads_online_agents_and_tool_nodes(kafka_bootstrap: str, topic_namespace: str) -> None:
    """A worker hosting an Agent + a function tool node advertises both; client.mesh projects the
    agent to an AgentInfo and the tool node to a ToolNodeInfo, and repeated reads reuse one view."""
    agent_name = f"{topic_namespace}-billing"
    tool_name = f"{topic_namespace}-add"
    agent = Agent(agent_name, subscribe_topics=f"{agent_name}.in", model_client=FakeModel(), description="Handles invoices")
    tool = agent_tool(_add, name=tool_name)
    worker = Worker(
        Client.connect(kafka_bootstrap),
        nodes=[agent, tool],
        control_plane=fast_control_plane(kafka_bootstrap),
        extra_subscribe_kwargs=_EARLIEST,
    )
    client = Client.connect(kafka_bootstrap, mesh_config=_FAST_MESH)
    try:
        async with worker:
            await _wait_mesh(client, lambda agents, tools: agent_name in agents and tool_name in tools, timeout=60)

            agents = await client.mesh.get_agents()
            assert agents[agent_name].description == "Handles invoices"

            tools = await client.mesh.get_tools()
            node_info = tools[tool_name]
            assert isinstance(node_info, ToolNodeInfo)
            assert node_info.name == tool_name
            assert "a" in node_info.parameters_schema.get("properties", {})

            # repeated reads reuse the cached view (a single open) — assert cell identity
            cell = client.mesh._cells["agents"]
            await client.mesh.get_agents()
            assert client.mesh._cells["agents"] is cell
    finally:
        await client.aclose()
        await worker._client.aclose()


async def test_mesh_reads_an_online_toolbox(kafka_bootstrap: str, topic_namespace: str) -> None:
    """An MCP toolbox advertises a toolbox CapabilityRecord; client.mesh projects it to a ToolboxInfo
    carrying the toolbox's tools by their BARE names (not the LLM-facing ``<name>__tool`` form)."""
    name = f"{topic_namespace}-docs"
    toolbox = MCPToolboxNode(name, connection_params=StreamableHttpParameters(url="http://unused.local/mcp"))
    await toolbox._refresh_tools(FakeSession(("search", "fetch")))  # prime the cache (no real MCP server)

    # Host the toolbox + drive its control-plane publisher by hand (the full worker lifecycle would
    # connect to the real MCP server). Provisioning ensures the compacted calf.capabilities exists.
    hoster = Client.connect(kafka_bootstrap, provisioning=ProvisioningConfig(enabled=True))
    worker = Worker(hoster, nodes=[toolbox], control_plane=ControlPlaneConfig(heartbeat_interval=0.05, bootstrap_servers=kafka_bootstrap))
    worker._maybe_register_control_plane()
    key = control_plane_writer_key(CAPABILITY_TOPIC)
    [(_, genfn)] = [(n, g) for n, g in worker._resource_cms() if n == key]
    writer_gen = genfn(None)  # type: ignore[arg-type]
    writer = await anext(writer_gen)
    pub = worker._control_plane_publisher
    assert pub is not None
    ctx = SimpleNamespace(resources={key: writer})
    await pub.start(ctx)

    client = Client.connect(kafka_bootstrap, mesh_config=_FAST_MESH)
    try:
        await _wait_mesh(client, lambda agents, tools: name in tools, timeout=60)
        info = (await client.mesh.get_tools())[name]
        assert isinstance(info, ToolboxInfo)
        assert sorted(spec.name for spec in info.tools) == ["fetch", "search"]  # BARE names
    finally:
        await client.aclose()
        if pub._task is not None:
            await pub.stop(ctx)
        with contextlib.suppress(StopAsyncIteration):
            await anext(writer_gen)
        await hoster.aclose()
