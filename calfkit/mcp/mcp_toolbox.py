import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from datetime import datetime, timezone
from typing import Any, ClassVar

import pydantic_core
from ktables import KafkaTableWriter
from mcp import ClientSession
from mcp.types import CallToolResult as MCPCallToolResult
from mcp.types import ToolListChangedNotification

from calfkit._protocol import NodeKind
from calfkit._registry import handler
from calfkit._vendor.pydantic_ai.messages import ToolReturn
from calfkit.exceptions import LifecycleConfigError, safe_exc_message
from calfkit.mcp.mcp_transport import StdioServerParameters, StreamableHttpParameters, http_client, stdio_client
from calfkit.models.actions import NodeResult, ReturnCall
from calfkit.models.capability import CapabilityRecord, CapabilityToolDef
from calfkit.models.session_context import SessionRunContext
from calfkit.models.state import FailedToolCall, State
from calfkit.models.tool_dispatch import ToolBinding, ToolCallRef
from calfkit.nodes.base import BaseNodeDef
from calfkit.worker.lifecycle import ResourceSetupContext, ServingContext
from calfkit.worker.worker_config import MCPDiscoveryConfig

logger = logging.getLogger(__name__)

_TransportCM = AbstractAsyncContextManager[tuple[object, object]]


class MCPToolbox(BaseNodeDef):
    _node_kind: ClassVar[NodeKind] = "toolbox"

    def tool_bindings(self) -> list[ToolBinding]:
        # MCP tools are discovered from the live session (list_tools is async,
        # and the session exists only after resource setup), so sync collection
        # at agent-construction time is impossible. Fail fast rather than
        # return [] — an empty list would silently register the toolbox with no
        # tools. Async discovery / lazy-provider resolution is a pending design.
        raise NotImplementedError(
            f"MCPToolbox {self.node_id!r} cannot provide tool bindings synchronously; "
            "MCP tool discovery requires a live session (pending async-provider design)"
        )

    async def _mcp_session(self, ctx: ResourceSetupContext) -> AsyncIterator[ClientSession]:
        params = self._connection_params
        transport: _TransportCM
        if isinstance(params, StreamableHttpParameters):
            transport = http_client(params)
        elif isinstance(params, StdioServerParameters):
            transport = stdio_client(params)
        else:
            raise LifecycleConfigError(
                f"MCPToolbox {self.node_id!r}: unsupported connection_params type {type(params).__name__}; "
                "expected StreamableHttpParameters or StdioServerParameters"
            )
        async with transport as (read_stream, write_stream):
            async with ClientSession(read_stream, write_stream, message_handler=self._mcp_message_handler) as session:
                await session.initialize()
                yield session

    def __init__(
        self,
        server_name: str,
        connection_params: StreamableHttpParameters | StdioServerParameters,
        discovery: MCPDiscoveryConfig | None = None,
    ):
        super().__init__(node_id=server_name, subscribe_topics=[f"mcp_server.{server_name}"])
        self._connection_params = connection_params
        self._discovery = discovery or MCPDiscoveryConfig()
        self._session_resource_key = f"mcp_session.{server_name}"
        self._writer_resource_key = f"mcp_writer.{server_name}"
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._relist_tasks: set[asyncio.Task[None]] = set()
        self._shutting_down = False
        self._last_record: CapabilityRecord | None = None
        self.resource(name=self._session_resource_key)(self._mcp_session)
        self.resource(name=self._writer_resource_key)(self._capability_writer)
        self.after_startup(self._publish_on_startup)
        self.on_shutdown(self._tombstone_on_shutdown)

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
            f"MCPToolbox {self.node_id!r}: cannot derive Kafka bootstrap servers for the "
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

    async def _publish_on_startup(self, ctx: ServingContext["MCPToolbox"]) -> None:
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

    async def _tombstone_on_shutdown(self, ctx: ServingContext["MCPToolbox"]) -> None:
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

    def _fail(self, ctx: SessionRunContext, payload: ToolCallRef, *, exc_type: str, exc_message: str) -> ReturnCall[State]:
        """Record a :class:`FailedToolCall` for ``payload`` and return to the caller.

        The agent reads the result under ``payload.tool_call_id`` and escalates any
        ``FailedToolCall`` to a ``ToolExecutionError`` (it is *not* shown to the model),
        so this is the toolbox's hard-failure channel: a dead session, a transport error,
        or a non-wire-safe result. Replying (rather than raising) is what keeps the
        awaiting agent from stalling on its reply-TTL.
        """
        marker = FailedToolCall.build_safe(
            tool_name=payload.name,
            tool_call_id=payload.tool_call_id,
            exc_type=exc_type,
            exc_message=exc_message,
        )
        ctx.state.add_tool_result(payload.tool_call_id, marker)
        return ReturnCall(ctx.state)

    @handler("*", schema=ToolCallRef)
    async def run(self, ctx: SessionRunContext, payload: ToolCallRef) -> NodeResult[State]:  # type: ignore[override]
        session = ctx.resources.get(self._session_resource_key)
        if not isinstance(session, ClientSession):
            # Resource setup failed or was torn down. Reply with a marker rather than
            # raising, which would strand the awaiting agent on its reply-TTL instead
            # of surfacing a ToolExecutionError.
            logger.error(
                "[%s] no live MCP session for toolbox=%s resource=%s tool=%s",
                ctx.correlation_id[:8],
                self.node_id,
                self._session_resource_key,
                payload.name,
            )
            return self._fail(ctx, payload, exc_type="MCPSessionUnavailable", exc_message=f"no live MCP session for server {self.node_id!r}")

        # Build and serialize the ToolReturn inside the try so a transport error OR a
        # non-wire-safe result both surface as a FailedToolCall — never a handler crash
        # after the point a reply can still be published (mirrors ToolNodeDef.run).
        try:
            tool_result: MCPCallToolResult = await session.call_tool(name=payload.name, arguments=payload.args)
            tool_return = ToolReturn(return_value=tool_result)
            pydantic_core.to_json(tool_return)
        except Exception as e:
            logger.exception(
                "[%s] mcp tool=%s tool_call_id=%s on toolbox=%s raised %s; surfacing FailedToolCall",
                ctx.correlation_id[:8],
                payload.name,
                payload.tool_call_id,
                self.node_id,
                type(e).__name__,
            )
            return self._fail(ctx, payload, exc_type=type(e).__name__, exc_message=safe_exc_message(e))

        ctx.state.add_tool_result(tool_call_id=payload.tool_call_id, tool_result=tool_return)
        return ReturnCall(ctx.state)
