"""MCPToolboxNode advertises its tools on the capability control plane.

Post-migration the node is a pure *content contributor*: it declares one
``@advertises`` factory that the worker-owned ``ControlPlanePublisher`` pulls
each heartbeat tick. The heartbeat loop + resurrection-safe tombstone now live
in the substrate's publisher (tested in ``test_controlplane_publisher.py``), so
they are not re-tested here.

The node caches the tool list — refreshed at session setup and on every
``tools/list_changed`` — and the factory reads that cache (never re-lists inline;
``content_updated_at`` advances only on a real tool change, never per heartbeat).
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import pytest
from mcp.types import ListToolsResult, Tool, ToolListChangedNotification

from calfkit.controlplane import ControlPlaneStamp
from calfkit.models.capability import CAPABILITY_TOPIC, CapabilityRecord

mcp_toolbox = pytest.importorskip("calfkit.mcp.mcp_toolbox")
MCPToolboxNode = mcp_toolbox.MCPToolboxNode

from calfkit.mcp.mcp_transport import StreamableHttpParameters  # noqa: E402


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


def make_toolbox() -> Any:
    return MCPToolboxNode("docs_server", connection_params=StreamableHttpParameters(url="http://unused.local/mcp"))


def make_stamp(*, hb: float = 30.0) -> ControlPlaneStamp:
    now = datetime.now(tz=timezone.utc)
    return ControlPlaneStamp(started_at=now, last_heartbeat_at=now, heartbeat_interval=hb)


class TestAdvertDeclaration:
    def test_declares_one_capability_advert(self) -> None:
        adverts = type(make_toolbox())._adverts
        assert CAPABILITY_TOPIC in adverts
        assert adverts[CAPABILITY_TOPIC].record is CapabilityRecord

    def test_advert_factory_is_a_bound_method(self) -> None:
        toolbox = make_toolbox()
        factories = toolbox.control_plane_adverts()
        assert CAPABILITY_TOPIC in factories
        assert callable(factories[CAPABILITY_TOPIC])

    def test_node_no_longer_owns_writer_heartbeat_or_tombstone(self) -> None:
        # The publisher (worker-owned) does heartbeats + tombstones now; the node
        # only keeps its MCP session resource and contributes content.
        toolbox = make_toolbox()
        names = [name for name, _ in toolbox._resource_cms()]
        assert toolbox._session_resource_key in names
        assert not any("writer" in n for n in names)
        assert not hasattr(toolbox, "_writer_resource_key")
        assert not hasattr(toolbox, "_heartbeat_task")
        assert list(toolbox._hooks_for("after_startup")) == []
        assert list(toolbox._hooks_for("on_shutdown")) == []


class TestCapabilityFactory:
    async def test_factory_builds_record_from_stamp_and_cache(self) -> None:
        toolbox = make_toolbox()
        await toolbox._refresh_tools(FakeSession(make_tools()))
        stamp = make_stamp(hb=45.0)
        record = toolbox._capability_record(stamp)
        assert isinstance(record, CapabilityRecord)
        # the worker-stamped fields ride through verbatim
        assert record.started_at == stamp.started_at
        assert record.last_heartbeat_at == stamp.last_heartbeat_at
        assert record.heartbeat_interval == 45.0
        # content
        assert record.dispatch_topic == "mcp_server.docs_server"
        assert [t.name for t in record.tools] == ["search", "fetch"]
        assert record.tools[0].parameters_json_schema == {"type": "object", "properties": {"q": {"type": "string"}}}
        assert record.tools[1].description is None

    async def test_factory_reads_cache_without_listing(self) -> None:
        # The factory runs on the shared heartbeat loop: it must never do I/O.
        toolbox = make_toolbox()
        session = FakeSession(make_tools())
        await toolbox._refresh_tools(session)
        assert session.list_tools_calls == 1
        toolbox._capability_record(make_stamp())
        toolbox._capability_record(make_stamp())
        assert session.list_tools_calls == 1  # factory never re-lists

    async def test_content_updated_at_stable_across_heartbeats(self) -> None:
        # CRITICAL-3: a new heartbeat stamp must NOT move content_updated_at.
        toolbox = make_toolbox()
        await toolbox._refresh_tools(FakeSession(make_tools()))
        r1 = toolbox._capability_record(make_stamp())
        r2 = toolbox._capability_record(make_stamp())
        assert r1.content_updated_at == r2.content_updated_at

    async def test_content_updated_at_advances_only_on_real_change(self) -> None:
        toolbox = make_toolbox()
        session = FakeSession(make_tools())
        await toolbox._refresh_tools(session)
        before = toolbox._capability_record(make_stamp()).content_updated_at

        # A redundant re-list (identical tools) must NOT advance content currency.
        await asyncio.sleep(0.005)
        await toolbox._refresh_tools(session)
        assert toolbox._capability_record(make_stamp()).content_updated_at == before

        # A genuine tool-set change advances it.
        await asyncio.sleep(0.005)
        session._tools = [Tool(name="new_tool", inputSchema={"type": "object", "properties": {}})]
        await toolbox._refresh_tools(session)
        after = toolbox._capability_record(make_stamp())
        assert after.content_updated_at > before
        assert [t.name for t in after.tools] == ["new_tool"]


class TestListChanged:
    async def test_list_changed_updates_cache_via_offloaded_relist(self) -> None:
        toolbox = make_toolbox()
        session = FakeSession(make_tools())
        toolbox.resources[toolbox._session_resource_key] = session
        await toolbox._refresh_tools(session)  # prime as session setup would

        session._tools = [Tool(name="new_tool", inputSchema={"type": "object", "properties": {}})]
        await toolbox._mcp_message_handler(ToolListChangedNotification(method="notifications/tools/list_changed"))
        for _ in range(100):
            if toolbox._last_tools and toolbox._last_tools[0].name == "new_tool":
                break
            await asyncio.sleep(0.01)
        assert [t.name for t in toolbox._last_tools] == ["new_tool"]  # cache updated, no publish
        await toolbox._cancel_relist_tasks()

    async def test_handler_never_blocks_the_receive_loop(self) -> None:
        # Deadlock-faithful: list_tools() cannot complete until the handler has
        # RETURNED (the real ClientSession receive-loop coupling). An inline await
        # would hang and trip this timeout.
        class ReceiveLoopFakeSession(FakeSession):
            def __init__(self, tools: list[Tool]) -> None:
                super().__init__(tools)
                self.handler_returned = asyncio.Event()

            async def list_tools(self) -> ListToolsResult:
                await self.handler_returned.wait()
                return await super().list_tools()

        toolbox = make_toolbox()
        session = ReceiveLoopFakeSession(make_tools())
        toolbox.resources[toolbox._session_resource_key] = session

        await asyncio.wait_for(
            toolbox._mcp_message_handler(ToolListChangedNotification(method="notifications/tools/list_changed")),
            timeout=1.0,
        )
        session.handler_returned.set()
        for _ in range(100):
            if session.list_tools_calls:
                break
            await asyncio.sleep(0.01)
        assert session.list_tools_calls == 1
        await toolbox._cancel_relist_tasks()

    async def test_relist_before_session_ready_is_ignored(self) -> None:
        toolbox = make_toolbox()  # no session seeded
        await toolbox._handle_tools_list_changed(ToolListChangedNotification(method="notifications/tools/list_changed"))
        assert toolbox._last_tools is None  # nothing to cache, no crash
