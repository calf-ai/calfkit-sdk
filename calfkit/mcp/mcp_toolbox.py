import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, ClassVar

import pydantic_core
from ktables import KafkaTableWriter
from mcp import ClientSession
from mcp.types import CallToolResult as MCPCallToolResult
from mcp.types import ToolListChangedNotification

from calfkit._protocol import NodeKind
from calfkit._registry import handler
from calfkit.exceptions import LifecycleConfigError
from calfkit.mcp.mcp_transport import StdioServerParameters, StreamableHttpParameters, http_client, stdio_client
from calfkit.models.actions import NodeResult, ReturnCall
from calfkit.models.capability import CapabilityRecord, CapabilityToolDef, SelectorResult, resolve_capability
from calfkit.models.session_context import SessionRunContext
from calfkit.models.state import State
from calfkit.models.tool_dispatch import ToolCallRef
from calfkit.nodes.base import BaseNodeDef
from calfkit.worker.lifecycle import ResourceSetupContext, ServingContext
from calfkit.worker.worker_config import MCPDiscoveryConfig

logger = logging.getLogger(__name__)

_TransportCM = AbstractAsyncContextManager[tuple[object, object]]


class MCPToolboxNode(BaseNodeDef):
    _node_kind: ClassVar[NodeKind] = "toolbox"

    async def _mcp_session(self, ctx: ResourceSetupContext) -> AsyncIterator[ClientSession]:
        params = self._connection_params
        transport: _TransportCM
        if isinstance(params, StreamableHttpParameters):
            transport = http_client(params)
        elif isinstance(params, StdioServerParameters):
            transport = stdio_client(params)
        else:
            raise LifecycleConfigError(
                f"MCPToolboxNode {self.node_id!r}: unsupported connection_params type {type(params).__name__}; "
                "expected StreamableHttpParameters or StdioServerParameters"
            )
        async with transport as (read_stream, write_stream):
            async with ClientSession(read_stream, write_stream, message_handler=self._mcp_message_handler) as session:
                await session.initialize()
                yield session

    def __init__(
        self,
        name: str,
        connection_params: StreamableHttpParameters | StdioServerParameters,
    ):
        super().__init__(node_id=name, subscribe_topics=[f"mcp_server.{name}"])
        self._connection_params = connection_params
        self._session_resource_key = f"mcp_session.{name}"
        self._writer_resource_key = f"mcp_writer.{name}"
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._relist_tasks: set[asyncio.Task[None]] = set()
        self._shutting_down = False
        self._last_record: CapabilityRecord | None = None
        self.resource(name=self._session_resource_key)(self._mcp_session)
        self.resource(name=self._writer_resource_key)(self._capability_writer)
        self.after_startup(self._publish_on_startup)
        self.on_shutdown(self._tombstone_on_shutdown)

    @property
    def _discovery(self) -> MCPDiscoveryConfig:
        """Control-plane config, inherited from the hosting worker.

        The worker is the single config surface (``Worker(...,
        mcp_discovery=...)``); the toolbox reads it through the ``_worker``
        back-reference — same pattern as bootstrap derivation. Defaults apply
        when unhosted (tests, bare construction).
        """
        cfg = getattr(self._worker, "_mcp_discovery", None)
        return cfg if cfg is not None else MCPDiscoveryConfig()

    # -- tool selection (spec §8.4) --------------------------------------------

    def resolve_tools(self, view: Mapping[str, CapabilityRecord]) -> SelectorResult:
        """All advertised tools, resolved against the Capability View.

        Implements ``ToolSelector`` by delegating to this node's handle —
        passing the node in ``tools=[...]`` and passing ``MCPToolbox(name)``
        resolve identically; the handle is the canonical resolution path.
        """
        return MCPToolbox(self.node_id).resolve_tools(view)

    def select(self, *, include: Sequence[str] | None = None, strict: bool = False) -> "MCPToolbox":
        """A scoped/strict selector for this toolbox's tools.

        ``include`` pins the exact tool names the agent may see (the trust
        boundary: a server suddenly advertising new tools cannot enlarge the
        agent's surface). ``strict=True`` fails the agent's turn when the
        selection cannot be fully satisfied; the default degrades with a
        warning.
        """
        return MCPToolbox(
            name=self.node_id,
            include=tuple(include) if include is not None else None,
            strict=strict,
        )

    # -- capability publishing (spec §3.3/§8.5) -------------------------------

    def _control_plane_bootstrap(self) -> str:
        """Bootstrap servers for the capability topic, zero-config (spec §8.3).

        Chain: explicit config override -> the connected client's retained URL
        -> the broker's connection kwargs (guarded internals fallback).
        """
        if self._discovery.bootstrap_servers:
            return self._discovery.bootstrap_servers
        client = getattr(self._worker, "_client", None)
        urls = getattr(client, "server_urls", None)
        if urls:
            return str(urls)
        kwargs = getattr(getattr(client, "broker", None), "_connection_kwargs", None) or {}
        servers = kwargs.get("bootstrap_servers")
        if servers:
            return servers if isinstance(servers, str) else ",".join(servers)
        raise RuntimeError(
            f"MCPToolboxNode {self.node_id!r}: cannot derive Kafka bootstrap servers for the "
            "capability topic (no worker/client attached). Host this node via Worker, or set "
            "MCPDiscoveryConfig(bootstrap_servers=...) explicitly."
        )

    def _provisioning_enabled(self) -> bool:
        provisioning = getattr(getattr(self._worker, "_client", None), "_provisioning", None)
        return bool(getattr(provisioning, "enabled", False))

    async def _capability_writer(self, ctx: ResourceSetupContext) -> AsyncIterator[KafkaTableWriter[CapabilityRecord]]:
        writer: KafkaTableWriter[CapabilityRecord] = KafkaTableWriter.json(
            bootstrap_servers=self._control_plane_bootstrap(),
            topic=self._discovery.topic,
            model=CapabilityRecord,
            ensure_topic=self._provisioning_enabled(),
        )
        await writer.start()
        yield writer
        await writer.stop()

    async def _build_record(self, session: Any) -> CapabilityRecord:
        listing = await session.list_tools()
        return CapabilityRecord(
            toolbox_id=self.node_id,
            dispatch_topic=self.subscribe_topics[0],
            tools=[
                CapabilityToolDef(name=tool.name, description=tool.description, parameters_json_schema=tool.inputSchema) for tool in listing.tools
            ],
            published_at=datetime.now(tz=timezone.utc),
        )

    async def _publish_on_startup(self, ctx: ServingContext["MCPToolboxNode"]) -> None:
        """Connect-time advertisement + heartbeat start. Raising here aborts
        worker startup — a toolbox that cannot advertise must fail loudly."""
        session = ctx.resources[self._session_resource_key]
        writer = ctx.resources[self._writer_resource_key]
        record = await self._build_record(session)
        self._last_record = record
        await writer.set(self.node_id, record)
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(writer), name=f"mcp-heartbeat:{self.node_id}")

    async def _heartbeat_loop(self, writer: Any) -> None:
        # Re-publish the cached record with a fresh timestamp — liveness signal
        # only; no re-listing (tools/list_changed drives content changes).
        while True:
            await asyncio.sleep(self._discovery.heartbeat_interval)
            record = self._last_record
            if record is None:
                continue
            record = record.model_copy(update={"published_at": datetime.now(tz=timezone.utc)})
            self._last_record = record
            try:
                await writer.set(self.node_id, record)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("toolbox=%s heartbeat publish failed; will retry next interval", self.node_id)

    async def _handle_tools_list_changed(self, notification: ToolListChangedNotification) -> None:
        """Server signalled a tool-list change: re-list and re-publish."""
        session = self.resources.get(self._session_resource_key)
        writer = self.resources.get(self._writer_resource_key)
        if session is None or writer is None:
            logger.warning("toolbox=%s tools/list_changed before resources ready; ignoring", self.node_id)
            return
        record = await self._build_record(session)
        self._last_record = record
        await writer.set(self.node_id, record)

    def _on_relist_done(self, task: asyncio.Task[None]) -> None:
        self._relist_tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            logger.error("toolbox=%s tools/list_changed re-publish failed", self.node_id, exc_info=task.exception())

    async def _mcp_message_handler(self, message: Any) -> None:
        # ClientSession message handler; ServerNotification is a RootModel wrapper.
        inner = getattr(message, "root", message)
        if isinstance(inner, ToolListChangedNotification):
            if self._shutting_down:
                # The receive loop outlives on_shutdown (session closes in the
                # resource phase): a late notification must not spawn a re-list
                # that lands after the tombstone and resurrects the record.
                logger.debug("toolbox=%s ignoring tools/list_changed during shutdown", self.node_id)
                return
            # NEVER await the re-list inline: this handler runs ON the
            # session's receive loop, and list_tools()'s response is delivered
            # by that same loop — awaiting here deadlocks the session,
            # unbounded (no read timeout). Offload and track for shutdown.
            task = asyncio.create_task(self._handle_tools_list_changed(inner), name=f"mcp-relist:{self.node_id}")
            self._relist_tasks.add(task)
            task.add_done_callback(self._on_relist_done)

    async def _tombstone_on_shutdown(self, ctx: ServingContext["MCPToolboxNode"]) -> None:
        # Flag FIRST, synchronously (no await above this line): after it is
        # set, _mcp_message_handler can never create a new re-list task, so
        # the snapshot below is complete. Then cancel heartbeat AND in-flight
        # re-lists BEFORE the tombstone: a straggler set() landing after
        # delete() would resurrect the record.
        self._shutting_down = True
        tasks = [self._heartbeat_task, *self._relist_tasks]
        self._heartbeat_task = None
        self._relist_tasks.clear()
        for task in tasks:
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        writer = ctx.resources.get(self._writer_resource_key)
        if writer is not None:
            # Clean shutdown = deliberate removal (spec §6); a crash never gets
            # here, so the record stands and goes stale instead.
            await writer.delete(self.node_id)

    @handler("*", schema=ToolCallRef)
    async def run(self, ctx: SessionRunContext, payload: ToolCallRef) -> NodeResult[State]:  # type: ignore[override]
        session = ctx.resources.get(self._session_resource_key)
        if not isinstance(session, ClientSession):
            # No live session (resource setup failed / torn down). RAISE so it escapes to the
            # chokepoint → on_node_error → the fault rail carries a typed fault to the agent, exactly
            # like any other terminal node failure (MCP is a tool-identical caller node — no special
            # fault path, no FailedToolCall blob).
            logger.error(
                "[%s] no live MCP session for toolbox=%s resource=%s tool=%s",
                ctx.correlation_id[:8],
                self.node_id,
                self._session_resource_key,
                payload.name,
            )
            raise RuntimeError(f"no live MCP session for server {self.node_id!r}")

        # A transport error escapes to the chokepoint (on_node_error → fault). The B1 eager wire-safety
        # check (pydantic_core.to_json) likewise raises HERE on a non-wire-safe result, not mid-publish.
        # ``isError=True`` results pass through TRANSPARENTLY (B2): the MCPCallToolResult rides the reply
        # slot as an ordinary successful return — the agent/model sees it exactly as today.
        tool_result: MCPCallToolResult = await session.call_tool(name=payload.name, arguments=payload.args)
        pydantic_core.to_json(tool_result)
        return ReturnCall[State](state=ctx.state, value=tool_result)


@dataclass(frozen=True)
class MCPToolbox:
    """The identity-only, deployment-free handle to an MCP toolbox (#212).

    The call-side counterpart to the hosting :class:`MCPToolboxNode` (the
    peer-node pattern's reference/servant split): constructible anywhere with
    just the toolbox's name — no connection params, no secrets — and resolved
    per agent turn against the Capability View. At the MCP protocol layer the
    toolbox is the cluster's MCP client; agents never speak MCP at all — they
    hold one of these.

    ``include`` pins the exact tool names the agent may see; ``strict=True``
    fails the turn when the selection cannot be fully satisfied (default
    degrades with a warning). Frozen with value semantics: equal handles
    compare and hash equal.
    """

    name: str
    include: tuple[str, ...] | None = None
    strict: bool = False

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("name must be non-empty")
        if self.include is not None and not isinstance(self.include, tuple):
            object.__setattr__(self, "include", tuple(self.include))

    def resolve_tools(self, view: Mapping[str, CapabilityRecord]) -> SelectorResult:
        return resolve_capability(view, self.name, include=self.include, strict=self.strict)
