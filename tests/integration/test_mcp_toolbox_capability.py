"""Integration: MCPToolbox publishes real CapabilityRecords a KafkaTable can read.

Real Kafka writer + reader (ktables) against a broker on localhost:9092 —
skipped when unreachable. The MCP session stays fake (no MCP server needed:
the session surface used by publishing is just ``list_tools``).
"""

from __future__ import annotations

import asyncio
import uuid

import pytest
from aiokafka.admin import AIOKafkaAdminClient
from ktables import KafkaTable
from mcp.types import ListToolsResult, Tool

from calfkit.mcp.mcp_toolbox import MCPToolbox
from calfkit.mcp.mcp_transport import StreamableHttpParameters
from calfkit.models.capability import CapabilityRecord
from calfkit.worker.lifecycle import ServingContext
from calfkit.worker.worker_config import MCPDiscoveryConfig

BOOTSTRAP = "localhost:9092"


class FakeSession:
    async def list_tools(self) -> ListToolsResult:
        return ListToolsResult(tools=[Tool(name="search", description="Search", inputSchema={"type": "object", "properties": {}})])


@pytest.fixture
async def capability_topic():
    admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    try:
        await admin.start()
    except Exception:
        pytest.skip(f"no Kafka broker reachable at {BOOTSTRAP}")
    name = f"calf.test.capabilities.{uuid.uuid4().hex[:8]}"
    yield name
    try:
        await admin.delete_topics([name])
    finally:
        await admin.close()


async def test_publish_heartbeat_and_tombstone_roundtrip(capability_topic: str) -> None:
    toolbox = MCPToolbox(
        "docs_server",
        connection_params=StreamableHttpParameters(url="http://unused.local/mcp"),
    )
    # Config flows from the hosting worker; stub it (no Worker in this test).
    from types import SimpleNamespace

    toolbox._worker = SimpleNamespace(
        _mcp_discovery=MCPDiscoveryConfig(topic=capability_topic, heartbeat_interval=0.05, bootstrap_servers=BOOTSTRAP),
        _client=None,
    )

    # Resource phase: enter the writer bracket for real (creates the topic).
    writer_gen = toolbox._capability_writer(None)  # type: ignore[arg-type]
    writer = await anext(writer_gen)
    toolbox.resources[toolbox._writer_resource_key] = writer
    toolbox.resources[toolbox._session_resource_key] = FakeSession()
    ctx = ServingContext(toolbox, toolbox.resources, broker=None)  # type: ignore[arg-type]

    table: KafkaTable[CapabilityRecord] = KafkaTable.json(
        bootstrap_servers=BOOTSTRAP, topic=capability_topic, model=CapabilityRecord, ensure_topic=False
    )
    async with table:
        try:
            # Connect-time publish lands in the view.
            await toolbox._publish_on_startup(ctx)
            for _ in range(200):
                if "docs_server" in table:
                    break
                await asyncio.sleep(0.01)
            record = table["docs_server"]
            assert record.toolbox_id == "docs_server"
            assert [t.name for t in record.tools] == ["search"]

            # Heartbeat advances published_at in the view.
            first_seen = record.published_at
            for _ in range(200):
                current = table.get("docs_server")
                if current is not None and current.published_at > first_seen:
                    break
                await asyncio.sleep(0.01)
            current = table["docs_server"]
            assert current.published_at > first_seen
        finally:
            # Clean shutdown: heartbeat stops, tombstone removes the key.
            await toolbox._tombstone_on_shutdown(ctx)
        for _ in range(200):
            if "docs_server" not in table:
                break
            await asyncio.sleep(0.01)
        assert "docs_server" not in table

    with pytest.raises(StopAsyncIteration):
        await anext(writer_gen)  # close the writer bracket


async def test_end_to_end_toolbox_to_agent_resolution(capability_topic: str) -> None:
    """Spec §8.7: toolbox publishes -> worker view catches up -> agent turn
    resolves bindings -> clean shutdown tombstones -> next turn degrades."""
    from calfkit.client.client import Client
    from calfkit.models.capability import CAPABILITY_VIEW_RESOURCE_KEY
    from calfkit.nodes.agent import Agent
    from calfkit.providers.pydantic_ai.model_client import PydanticModelClient
    from calfkit.worker.worker import Worker
    from calfkit.worker.worker_config import MCPDiscoveryConfig

    class FakeModel(PydanticModelClient):
        @property
        def model_name(self) -> str:
            return "fake"

        @property
        def system(self) -> str:
            return "fake"

        async def request(self, *args: object, **kwargs: object) -> object:
            raise NotImplementedError

    discovery = MCPDiscoveryConfig(topic=capability_topic, heartbeat_interval=5.0, bootstrap_servers=BOOTSTRAP)

    # Toolbox side (as if hosted in another worker): real writer, real publish.
    from types import SimpleNamespace

    toolbox = MCPToolbox("docs_server", connection_params=StreamableHttpParameters(url="http://unused.local/mcp"))
    toolbox._worker = SimpleNamespace(_mcp_discovery=discovery, _client=None)
    writer_gen = toolbox._capability_writer(None)  # type: ignore[arg-type]
    writer = await anext(writer_gen)
    toolbox.resources[toolbox._writer_resource_key] = writer
    toolbox.resources[toolbox._session_resource_key] = FakeSession()
    toolbox_ctx = ServingContext(toolbox, toolbox.resources, broker=None)  # type: ignore[arg-type]
    await toolbox._publish_on_startup(toolbox_ctx)

    # Agent side: a separate process in production — here a Worker whose
    # auto-registered view resource we enter for real.
    agent = Agent(
        "researcher",
        subscribe_topics="researcher.in",
        model_client=FakeModel(),
        tools=[toolbox.select(include=["search"])],
    )
    client = Client.connect(BOOTSTRAP)
    worker = Worker(client, nodes=[agent], mcp_discovery=discovery)
    worker._maybe_register_capability_view()
    [(name, genfn)] = [(n, g) for n, g in worker._resource_cms() if n == CAPABILITY_VIEW_RESOURCE_KEY]
    view_gen = genfn(None)  # type: ignore[arg-type]
    table = await anext(view_gen)
    try:
        # Catch-up gate already passed inside start(); the record is visible.
        registry: dict = {}
        agent._resolve_selector_tools({CAPABILITY_VIEW_RESOURCE_KEY: table, **agent._effective_resources()}, registry)
        assert sorted(registry) == ["search"]
        assert registry["search"].dispatch_topic == "mcp_server.docs_server"

        # Clean shutdown tombstones; the next turn degrades (warns, empty).
        await toolbox._tombstone_on_shutdown(toolbox_ctx)
        for _ in range(200):
            if "docs_server" not in table:
                break
            await asyncio.sleep(0.01)
        registry2: dict = {}
        agent._resolve_selector_tools({CAPABILITY_VIEW_RESOURCE_KEY: table, **agent._effective_resources()}, registry2)
        assert registry2 == {}
    finally:
        with pytest.raises(StopAsyncIteration):
            await anext(view_gen)
        with pytest.raises(StopAsyncIteration):
            await anext(writer_gen)
