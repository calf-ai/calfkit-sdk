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
        discovery=MCPDiscoveryConfig(
            topic=capability_topic,
            heartbeat_interval=0.05,
            bootstrap_servers=BOOTSTRAP,  # no Worker in this test: explicit override path
        ),
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
