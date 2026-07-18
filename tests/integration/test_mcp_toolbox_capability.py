"""Integration (kafka lane): MCPToolboxNode advertises on the control-plane substrate.

A real worker-auto-wired ``GroupedKafkaTableWriter`` + a ``ControlPlaneView`` read
back the toolbox's ``CapabilityRecord``s; an agent resolves the same bindings
(parity), and the liveness/content split survives the wire (CRITICAL-3). The
generic publisher mechanics — heartbeat, tombstone, replica-keying, loud-fail —
are proven in ``test_controlplane_substrate_kafka.py``; this asserts the
MCP-specific wiring on top.

The MCP session stays fake (no MCP server needed: publishing only uses
``list_tools``). Broker from the ``kafka_bootstrap`` fixture; isolation via a
unique toolbox name on the fixed ``calf.capabilities`` topic.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from types import SimpleNamespace

import pytest
from mcp.types import ListToolsResult, Tool

from calfkit.client import Client
from calfkit.controlplane import ControlPlaneConfig, ControlPlaneView
from calfkit.controlplane.publisher import control_plane_writer_key
from calfkit.mcp.mcp_toolbox import MCPToolboxNode
from calfkit.mcp.mcp_transport import StreamableHttpParameters
from calfkit.models.capability import CAPABILITY_TOPIC, CAPABILITY_VIEW_RESOURCE_KEY, CapabilityRecord
from calfkit.nodes.agent import Agent
from calfkit.providers.pydantic_ai.model_client import PydanticModelClient
from calfkit.provisioning import ProvisioningConfig
from calfkit.worker.worker import Worker
from tests.integration._kafka_helpers import profile_for

# Every test here needs a real broker.
pytestmark = pytest.mark.kafka


class FakeSession:
    def __init__(self, tool_names: tuple[str, ...] = ("search",)) -> None:
        self._tool_names = tool_names

    async def list_tools(self) -> ListToolsResult:
        return ListToolsResult(tools=[Tool(name=n, description=n, inputSchema={"type": "object", "properties": {}}) for n in self._tool_names])


class FakeModel(PydanticModelClient):
    @property
    def model_name(self) -> str:
        return "fake"

    @property
    def system(self) -> str:
        return "fake"

    async def request(self, *args: object, **kwargs: object) -> object:
        raise NotImplementedError


async def _poll(refresh: Callable[[], Awaitable[object]], predicate: Callable[[], bool], *, tries: int = 300, delay: float = 0.02) -> bool:
    for _ in range(tries):
        await refresh()
        if predicate():
            return True
        await asyncio.sleep(delay)
    return predicate()


async def _host_toolbox(bootstrap: str, toolbox: MCPToolboxNode, *, heartbeat: float = 0.05):
    """Host the toolbox in a worker, enter its auto-wired control-plane writer
    resource, and start the worker-owned publisher (no broker serving loop)."""
    client = Client.connect(bootstrap, provisioning=ProvisioningConfig(enabled=True))
    worker = Worker(client, nodes=[toolbox], control_plane=ControlPlaneConfig(heartbeat_interval=heartbeat, bootstrap_servers=bootstrap))
    worker._maybe_register_control_plane()
    key = control_plane_writer_key(CAPABILITY_TOPIC)
    [(_, genfn)] = [(n, g) for n, g in worker._resource_cms() if n == key]
    writer_gen = genfn(None)  # type: ignore[arg-type]
    writer = await anext(writer_gen)  # ensure_topic=True (provisioning) creates calf.capabilities compacted
    pub = worker._control_plane_publisher
    assert pub is not None
    ctx = SimpleNamespace(resources={key: writer})
    await pub.start(ctx)
    return pub, ctx, writer_gen


async def _open_view(bootstrap: str) -> ControlPlaneView[CapabilityRecord]:
    view: ControlPlaneView[CapabilityRecord] = ControlPlaneView.open(
        connection=profile_for(bootstrap), topic=CAPABILITY_TOPIC, record_type=CapabilityRecord, ensure_topic=False
    )
    await view.start()
    return view


async def test_capability_parity_toolbox_to_agent(kafka_bootstrap: str) -> None:
    """A hosted toolbox advertises its tools; a ControlPlaneView reads them and an
    agent resolves the same bindings; clean shutdown tombstones and the next turn
    degrades (the migration's 'no loss of behaviour for tool selectors' claim)."""
    name = f"docs-{uuid.uuid4().hex[:8]}"
    toolbox = MCPToolboxNode(name, connection_params=StreamableHttpParameters(url="http://unused.local/mcp"))
    await toolbox._refresh_tools(FakeSession(("search",)))  # prime cache (no real MCP server)
    agent = Agent("researcher", subscribe_topics="researcher.in", model_client=FakeModel(), tools=[toolbox.select(include=["search"])])

    pub, ctx, writer_gen = await _host_toolbox(kafka_bootstrap, toolbox)
    view = await _open_view(kafka_bootstrap)
    try:
        assert await _poll(view.barrier, lambda: view.get(name) is not None)
        record = view.get(name)
        assert record is not None
        assert [t.name for t in record.tools] == ["search"]
        assert record.dispatch_topic == f"mcp_server.{name}"

        registry: dict = {}
        agent._resolve_selector_tools({CAPABILITY_VIEW_RESOURCE_KEY: view}, registry)
        # The agent's resolved registry is keyed by the NAMESPACED tool name (C1, ADR-0018);
        # the wire record (asserted above) stays BARE. dispatch_topic is unchanged.
        assert sorted(registry) == [f"{name}__search"]
        assert registry[f"{name}__search"].dispatch_topic == f"mcp_server.{name}"

        # clean shutdown tombstones the instance; the next turn degrades (empty)
        await pub.stop(ctx)
        assert await _poll(view.barrier, lambda: view.get(name) is None)
        registry2: dict = {}
        agent._resolve_selector_tools({CAPABILITY_VIEW_RESOURCE_KEY: view}, registry2)
        assert registry2 == {}
    finally:
        if pub._task is not None:
            await pub.stop(ctx)
        await view.stop()
        with pytest.raises(StopAsyncIteration):
            await anext(writer_gen)


async def test_liveness_content_split_through_the_wire(kafka_bootstrap: str) -> None:
    """CRITICAL-3: across real heartbeats, last_heartbeat_at advances while
    content_updated_at stays fixed; a re-list (tool change) advances content."""
    name = f"docs-{uuid.uuid4().hex[:8]}"
    toolbox = MCPToolboxNode(name, connection_params=StreamableHttpParameters(url="http://unused.local/mcp"))
    await toolbox._refresh_tools(FakeSession(("search",)))

    pub, ctx, writer_gen = await _host_toolbox(kafka_bootstrap, toolbox)
    view = await _open_view(kafka_bootstrap)
    try:
        assert await _poll(view.barrier, lambda: view.get(name) is not None)
        r1 = view.get(name)
        assert r1 is not None

        def _liveness_advanced() -> bool:
            r = view.get(name)
            return r is not None and r.last_heartbeat_at > r1.last_heartbeat_at

        assert await _poll(view.barrier, _liveness_advanced)
        r2 = view.get(name)
        assert r2 is not None
        assert r2.last_heartbeat_at > r1.last_heartbeat_at  # liveness moved
        assert r2.content_updated_at == r1.content_updated_at  # content did NOT

        # a real tool-set change advances content_updated_at (pull: lands within a heartbeat)
        await toolbox._refresh_tools(FakeSession(("search", "fetch")))

        def _content_changed() -> bool:
            r = view.get(name)
            return r is not None and [t.name for t in r.tools] == ["search", "fetch"]

        assert await _poll(view.barrier, _content_changed)
        r3 = view.get(name)
        assert r3 is not None
        assert r3.content_updated_at > r1.content_updated_at
    finally:
        if pub._task is not None:
            await pub.stop(ctx)
        await view.stop()
        with pytest.raises(StopAsyncIteration):
            await anext(writer_gen)
