import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from datetime import datetime, timezone
from typing import Any, ClassVar

import pydantic_core
from mcp import ClientSession
from mcp.types import CallToolResult as MCPCallToolResult
from mcp.types import ToolListChangedNotification

from calfkit._protocol import NodeKind
from calfkit._registry import handler
from calfkit.controlplane import ControlPlaneStamp, advertises
from calfkit.exceptions import LifecycleConfigError
from calfkit.mcp.mcp_transport import StdioServerParameters, StreamableHttpParameters, http_client, stdio_client
from calfkit.models.actions import NodeResult, ReturnCall
from calfkit.models.capability import (
    CAPABILITY_TOPIC,
    CapabilityLookup,
    CapabilityRecord,
    CapabilityToolDef,
    SelectorResult,
    resolve_capability,
)
from calfkit.models.session_context import SessionRunContext
from calfkit.models.state import State
from calfkit.models.tool_dispatch import ToolCallRef
from calfkit.nodes.base import BaseNodeDef
from calfkit.worker.lifecycle import ResourceSetupContext

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
                # Prime the tool cache before the worker's publisher pulls the
                # advert factory (resources set up before after_startup). A list
                # failure here aborts boot — a toolbox that cannot advertise must
                # fail loudly.
                await self._refresh_tools(session)
                try:
                    yield session
                finally:
                    # Cancel any in-flight tools/list_changed re-list before the
                    # session closes, so none outlives the receive loop.
                    await self._cancel_relist_tasks()

    def __init__(
        self,
        name: str,
        connection_params: StreamableHttpParameters | StdioServerParameters,
    ):
        super().__init__(node_id=name, subscribe_topics=[f"mcp_server.{name}"])
        self._connection_params = connection_params
        self._session_resource_key = f"mcp_session.{name}"
        self._relist_tasks: set[asyncio.Task[None]] = set()
        # The advertised tool list, cached from the MCP session. The @advertises
        # factory reads it; the worker-owned ControlPlanePublisher heartbeats it
        # and tombstones on shutdown.
        self._last_tools: list[CapabilityToolDef] | None = None
        self._tools_changed_at: datetime | None = None
        self.resource(name=self._session_resource_key)(self._mcp_session)

    # -- tool selection (spec §8.4) --------------------------------------------

    def resolve_tools(self, view: CapabilityLookup) -> SelectorResult:
        """All advertised tools, resolved against the Capability View.

        Implements ``ToolSelector`` via the resolution kernel directly — passing the node in
        ``tools=[...]`` and naming its Toolbox with ``Toolboxes(name)`` resolve identically.
        """
        return resolve_capability(view, self.node_id, expected_kind="toolbox")

    # -- capability advertisement (control-plane substrate) -------------------

    async def _refresh_tools(self, session: Any) -> None:
        """Re-list the server's tools into the cache; bump content currency on a real change.

        Called at session setup and on every ``tools/list_changed``. The
        ``@advertises`` factory reads this cache, so a change propagates at the
        next worker heartbeat tick (pull). ``content_updated_at`` advances ONLY
        when the tool set actually changes — never per heartbeat — so liveness
        and content currency stay distinct (CRITICAL-3).

        On the offloaded ``tools/list_changed`` path a failed ``list_tools()`` raises
        before the cache is touched, so the previous tool list stays cached and keeps
        being advertised as live (logged by :meth:`_on_relist_done`); it self-heals on
        the next successful re-list. Startup is fail-loud instead — the session resource
        aborts boot.
        """
        listing = await session.list_tools()
        tools = [CapabilityToolDef(name=tool.name, description=tool.description, parameters_json_schema=tool.inputSchema) for tool in listing.tools]
        if tools != self._last_tools:
            self._last_tools = tools
            self._tools_changed_at = datetime.now(tz=timezone.utc)

    @advertises(topic=CAPABILITY_TOPIC, record=CapabilityRecord)
    def _capability_record(self, stamp: ControlPlaneStamp) -> CapabilityRecord:
        """Content factory the worker-owned publisher pulls each heartbeat tick.

        Reads the cached tool list — NEVER re-lists inline (this runs on the
        shared heartbeat loop). Splat the bare ``stamp`` (boot + liveness +
        cadence + kind); identity (``toolbox_id`` × ``worker_id``) is the wire key, not
        carried in the value.
        """
        if self._last_tools is None or self._tools_changed_at is None:
            # The session resource primes the cache in the resource phase, before the
            # publisher's after_startup pulls this factory; reaching here means that
            # ordering broke. Fail loud with a named cause (not a `-O`-stripped assert).
            raise RuntimeError(
                f"MCPToolboxNode {self.node_id!r}: capability factory ran before the session resource primed the tool cache (lifecycle ordering bug)"
            )
        return CapabilityRecord(
            **stamp.model_dump(),
            dispatch_topic=self.subscribe_topics[0],
            tools=self._last_tools,
            content_updated_at=self._tools_changed_at,
        )

    async def _handle_tools_list_changed(self, notification: ToolListChangedNotification) -> None:
        """Server signalled a tool-list change: re-list into the cache.

        No publish — the worker's heartbeat carries the new content at the next
        tick (pull). A change lands within one ``heartbeat_interval``.
        """
        session = self.resources.get(self._session_resource_key)
        if session is None:
            logger.warning("toolbox=%s tools/list_changed before session ready; ignoring", self.node_id)
            return
        await self._refresh_tools(session)

    def _on_relist_done(self, task: asyncio.Task[None]) -> None:
        self._relist_tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            logger.error("toolbox=%s tools/list_changed re-list failed", self.node_id, exc_info=task.exception())

    async def _mcp_message_handler(self, message: Any) -> None:
        # ClientSession message handler; ServerNotification is a RootModel wrapper.
        inner = getattr(message, "root", message)
        if isinstance(inner, ToolListChangedNotification):
            # NEVER await the re-list inline: this handler runs ON the session's
            # receive loop, and list_tools()'s response is delivered by that same
            # loop — awaiting here deadlocks the session (no read timeout). Offload
            # and track so the session resource cancels it at teardown. A late
            # re-list only updates a cache; with no node-side writer there is
            # nothing to resurrect (the publisher owns the tombstone).
            task = asyncio.create_task(self._handle_tools_list_changed(inner), name=f"mcp-relist:{self.node_id}")
            self._relist_tasks.add(task)
            task.add_done_callback(self._on_relist_done)

    async def _cancel_relist_tasks(self) -> None:
        """Cancel any in-flight ``tools/list_changed`` re-lists (session teardown)."""
        tasks = list(self._relist_tasks)
        self._relist_tasks.clear()
        for task in tasks:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

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

        # Strip this toolbox's own ``<node_id>__`` prefix to recover the BARE server-side tool
        # name (ADR-0018, C6). Exact-prefix ``removeprefix``, NOT ``split("__")`` — a server tool
        # name (``a__b``) or a toolbox name (``my__server``) may legitimately contain ``__``;
        # ``removeprefix`` of the exact ``<node_id>__`` round-trips and is a no-op on an already-bare
        # name. The namespace is the agent's cluster-side projection; it never crosses to the server,
        # and the strip is local here so the agent's dispatch path stays generic.
        server_tool_name = payload.name.removeprefix(f"{self.node_id}__")
        # A transport error escapes to the chokepoint (on_node_error → fault). The B1 eager wire-safety
        # check (pydantic_core.to_json) likewise raises HERE on a non-wire-safe result, not mid-publish.
        # ``isError=True`` results pass through TRANSPARENTLY (B2): the MCPCallToolResult rides the reply
        # slot as an ordinary successful return — the agent/model sees it exactly as today.
        tool_result: MCPCallToolResult = await session.call_tool(name=server_tool_name, arguments=payload.args)
        pydantic_core.to_json(tool_result)
        return ReturnCall[State](state=ctx.state, value=tool_result)
