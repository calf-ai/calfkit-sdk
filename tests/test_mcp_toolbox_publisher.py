"""PR B: MCPToolbox advertises its tools on the capability control plane.

Spec §3.3/§8.5: publish on connect (after_startup), re-publish on
``tools/list_changed`` and as a heartbeat, tombstone on clean shutdown
(on_shutdown — runs before resource teardown, so the writer is still live).

Tests drive the node's lifecycle hooks directly with fakes seeded into the
node's own resource bag (worker hooks pass ``ServingContext(owner,
owner.resources, broker)`` — verified against worker.py), so no broker is
needed; the real KafkaTableWriter is exercised by the integration test only.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from mcp.types import ListToolsResult, Tool, ToolListChangedNotification

from calfkit.models.capability import CapabilityRecord
from calfkit.worker.lifecycle import ServingContext
from calfkit.worker.worker_config import MCPDiscoveryConfig

mcp_toolbox = pytest.importorskip("calfkit.mcp.mcp_toolbox")
MCPToolbox = mcp_toolbox.MCPToolbox

from calfkit.mcp.mcp_transport import StreamableHttpParameters  # noqa: E402


class FakeWriter:
    """Records set/delete calls; quacks like KafkaTableWriter."""

    def __init__(self) -> None:
        self.sets: list[tuple[str, CapabilityRecord]] = []
        self.deletes: list[str] = []

    async def set(self, key: str, value: CapabilityRecord) -> None:
        self.sets.append((key, value))

    async def delete(self, key: str) -> None:
        self.deletes.append(key)


class FakeSession:
    def __init__(self, tools: list[Tool]) -> None:
        self._tools = tools
        self.list_tools_calls = 0

    async def list_tools(self) -> ListToolsResult:
        self.list_tools_calls += 1
        return ListToolsResult(tools=self._tools)


def make_tools() -> list[Tool]:
    return [
        Tool(name="search", description="Search the docs", inputSchema={"type": "object", "properties": {"q": {"type": "string"}}}),
        Tool(name="fetch", inputSchema={"type": "object", "properties": {}}),
    ]


def make_toolbox(discovery: MCPDiscoveryConfig | None = None) -> Any:
    toolbox = MCPToolbox(
        "docs_server",
        connection_params=StreamableHttpParameters(url="http://unused.local/mcp"),
    )
    if discovery is not None:
        # Config flows from the hosting worker (the single config surface);
        # tests stand in for it with a stub.
        from types import SimpleNamespace

        toolbox._worker = SimpleNamespace(_mcp_discovery=discovery, _client=None)
    return toolbox


def seed(toolbox: Any, session: FakeSession | None = None, writer: FakeWriter | None = None) -> tuple[FakeSession, FakeWriter]:
    session = session or FakeSession(make_tools())
    writer = writer or FakeWriter()
    toolbox.resources[toolbox._session_resource_key] = session
    toolbox.resources[toolbox._writer_resource_key] = writer
    return session, writer


def serving_ctx(toolbox: Any) -> ServingContext[Any]:
    return ServingContext(toolbox, toolbox.resources, broker=None)  # type: ignore[arg-type]


async def fire(toolbox: Any, phase: str) -> None:
    for hook in toolbox._hooks_for(phase):
        result = hook(serving_ctx(toolbox))
        if asyncio.iscoroutine(result):
            await result


class TestWriterResource:
    def test_writer_resource_registered_alongside_session(self) -> None:
        toolbox = make_toolbox()
        names = [name for name, _ in toolbox._resource_cms()]
        assert toolbox._session_resource_key in names
        assert toolbox._writer_resource_key in names


class TestPublishOnStartup:
    async def test_publishes_record_keyed_by_node_id(self) -> None:
        toolbox = make_toolbox()
        session, writer = seed(toolbox)
        await fire(toolbox, "after_startup")
        await fire(toolbox, "on_shutdown")  # cancel heartbeat for clean teardown

        [(key, record)] = writer.sets[:1]
        assert key == "docs_server"
        assert isinstance(record, CapabilityRecord)
        assert record.toolbox_id == "docs_server"
        assert record.dispatch_topic == "mcp_server.docs_server"
        assert [t.name for t in record.tools] == ["search", "fetch"]
        assert record.tools[0].parameters_json_schema == {"type": "object", "properties": {"q": {"type": "string"}}}
        assert record.tools[1].description is None
        assert record.published_at.tzinfo is not None


class TestHeartbeat:
    async def test_heartbeat_republishes_with_fresh_timestamp(self) -> None:
        toolbox = make_toolbox(discovery=MCPDiscoveryConfig(heartbeat_interval=0.02))
        session, writer = seed(toolbox)
        await fire(toolbox, "after_startup")
        try:
            await asyncio.sleep(0.09)
        finally:
            await fire(toolbox, "on_shutdown")

        assert len(writer.sets) >= 3  # initial + >=2 heartbeats
        first, last = writer.sets[0][1], writer.sets[-1][1]
        assert last.published_at > first.published_at
        # Heartbeats re-publish the cached tool list — no re-listing.
        assert session.list_tools_calls == 1

    async def test_heartbeat_stops_on_shutdown(self) -> None:
        toolbox = make_toolbox(discovery=MCPDiscoveryConfig(heartbeat_interval=0.02))
        _, writer = seed(toolbox)
        await fire(toolbox, "after_startup")
        await fire(toolbox, "on_shutdown")
        count = len(writer.sets)
        await asyncio.sleep(0.06)
        assert len(writer.sets) == count  # nothing published after shutdown


class TestTombstoneOnShutdown:
    async def test_shutdown_deletes_record(self) -> None:
        toolbox = make_toolbox()
        _, writer = seed(toolbox)
        await fire(toolbox, "after_startup")
        await fire(toolbox, "on_shutdown")
        assert writer.deletes == ["docs_server"]


class ReceiveLoopFakeSession(FakeSession):
    """Deadlock-faithful: list_tools() cannot complete until the message
    handler has RETURNED — exactly the real ClientSession receive-loop
    coupling. An implementation that awaits the re-list inline hangs here."""

    def __init__(self, tools: list[Tool]) -> None:
        super().__init__(tools)
        self.handler_returned = asyncio.Event()

    async def list_tools(self) -> ListToolsResult:
        await self.handler_returned.wait()
        return await super().list_tools()


class TestListChanged:
    async def test_handler_never_blocks_the_receive_loop(self) -> None:
        toolbox = make_toolbox()
        session = ReceiveLoopFakeSession(make_tools())
        _, writer = seed(toolbox, session=session)
        notification = ToolListChangedNotification(method="notifications/tools/list_changed")

        # Must return promptly even though list_tools() is gated on our return
        # — inline awaiting would deadlock (and trip this timeout).
        await asyncio.wait_for(toolbox._mcp_message_handler(notification), timeout=1.0)
        session.handler_returned.set()
        for _ in range(100):
            if writer.sets:
                break
            await asyncio.sleep(0.01)
        assert writer.sets and writer.sets[-1][0] == "docs_server"
        await fire(toolbox, "on_shutdown")

    async def test_shutdown_cancels_inflight_relist_so_tombstone_is_final(self) -> None:
        toolbox = make_toolbox()
        session = ReceiveLoopFakeSession(make_tools())
        _, writer = seed(toolbox, session=session)
        notification = ToolListChangedNotification(method="notifications/tools/list_changed")

        await toolbox._mcp_message_handler(notification)  # re-list now parked
        await fire(toolbox, "on_shutdown")  # cancels it, then tombstones
        session.handler_returned.set()  # unpark (too late)
        await asyncio.sleep(0.05)

        assert writer.deletes == ["docs_server"]
        assert writer.sets == []  # the cancelled re-list never resurrected the record

    async def test_notification_during_tombstone_awaits_cannot_resurrect(self) -> None:
        # The session receive loop is still alive while the tombstone awaits
        # (slow broker round-trip): a notification arriving in that window
        # must be dropped, not spawn a fresh re-list that lands after delete.
        class SlowDeleteWriter(FakeWriter):
            def __init__(self) -> None:
                super().__init__()
                self.delete_started = asyncio.Event()
                self.release_delete = asyncio.Event()

            async def delete(self, key: str) -> None:
                self.delete_started.set()
                await self.release_delete.wait()
                await super().delete(key)

        toolbox = make_toolbox()
        session = ReceiveLoopFakeSession(make_tools())
        writer = SlowDeleteWriter()
        seed(toolbox, session=session, writer=writer)
        session.handler_returned.set()  # any re-list would complete immediately

        shutdown = asyncio.create_task(fire(toolbox, "on_shutdown"))
        await asyncio.wait_for(writer.delete_started.wait(), timeout=1.0)
        # Mid-tombstone notification: must be ignored under the shutdown flag.
        await toolbox._mcp_message_handler(ToolListChangedNotification(method="notifications/tools/list_changed"))
        writer.release_delete.set()
        await shutdown
        await asyncio.sleep(0.05)

        assert writer.deletes == ["docs_server"]
        assert writer.sets == []  # nothing resurrected the tombstone

    async def test_list_changed_notification_republishes_fresh_listing(self) -> None:
        toolbox = make_toolbox()
        session, writer = seed(toolbox)
        await fire(toolbox, "after_startup")
        baseline = len(writer.sets)

        session._tools = [Tool(name="new_tool", inputSchema={"type": "object", "properties": {}})]
        await toolbox._handle_tools_list_changed(ToolListChangedNotification(method="notifications/tools/list_changed"))
        await fire(toolbox, "on_shutdown")

        assert len(writer.sets) == baseline + 1
        assert [t.name for t in writer.sets[-1][1].tools] == ["new_tool"]
        assert session.list_tools_calls == 2  # re-listed, not cached


class TestBootstrapDerivation:
    def test_explicit_config_override_wins(self) -> None:
        toolbox = make_toolbox(discovery=MCPDiscoveryConfig(bootstrap_servers="cp-kafka:9092"))
        assert toolbox._control_plane_bootstrap() == "cp-kafka:9092"

    def test_derives_from_workers_client(self) -> None:
        class StubClient:
            server_urls = "kafka-a:9092,kafka-b:9092"

        class StubWorker:
            _client = StubClient()

        toolbox = make_toolbox()
        toolbox._worker = StubWorker()  # type: ignore[assignment]
        assert toolbox._control_plane_bootstrap() == "kafka-a:9092,kafka-b:9092"

    def test_unreachable_raises_actionable_error(self) -> None:
        toolbox = make_toolbox()  # no worker, no override
        with pytest.raises(RuntimeError, match="bootstrap_servers"):
            toolbox._control_plane_bootstrap()
