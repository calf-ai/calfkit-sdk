import logging
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from typing import ClassVar

import pydantic_core
from mcp import ClientSession
from mcp.types import CallToolResult as MCPCallToolResult

from calfkit._protocol import NodeKind
from calfkit._registry import handler
from calfkit._vendor.pydantic_ai.messages import ToolReturn
from calfkit.exceptions import LifecycleConfigError, safe_exc_message
from calfkit.mcp.mcp_transport import StdioServerParameters, StreamableHttpParameters, http_client, stdio_client
from calfkit.models.actions import NodeResult, ReturnCall
from calfkit.models.session_context import SessionRunContext
from calfkit.models.state import FailedToolCall, State
from calfkit.models.tool_dispatch import ToolBinding, ToolCallRef
from calfkit.nodes.base import BaseNodeDef
from calfkit.worker.lifecycle import ResourceSetupContext

logger = logging.getLogger(__name__)

_TransportCM = AbstractAsyncContextManager[tuple[object, object]]


class MCPBridge(BaseNodeDef):
    _node_kind: ClassVar[NodeKind] = "bridge"

    def tool_bindings(self) -> list[ToolBinding]:
        # MCP tools are discovered from the live session (list_tools is async,
        # and the session exists only after resource setup), so sync collection
        # at agent-construction time is impossible. Fail fast rather than
        # return [] — an empty list would silently register the bridge with no
        # tools. Async discovery / lazy-provider resolution is a pending design.
        raise NotImplementedError(
            f"MCPBridge {self.node_id!r} cannot provide tool bindings synchronously; "
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
                f"MCPBridge {self.node_id!r}: unsupported connection_params type {type(params).__name__}; "
                "expected StreamableHttpParameters or StdioServerParameters"
            )
        async with transport as (read_stream, write_stream):
            async with ClientSession(read_stream, write_stream) as session:
                await session.initialize()
                yield session

    def __init__(self, server_name: str, connection_params: StreamableHttpParameters | StdioServerParameters):
        super().__init__(node_id=server_name, subscribe_topics=[f"mcp_server.{server_name}"])
        self._connection_params = connection_params
        self._session_resource_key = f"mcp_session.{server_name}"
        self.resource(name=self._session_resource_key)(self._mcp_session)

    def _fail(self, ctx: SessionRunContext, payload: ToolCallRef, *, exc_type: str, exc_message: str) -> ReturnCall[State]:
        """Record a :class:`FailedToolCall` for ``payload`` and return to the caller.

        The agent reads the result under ``payload.tool_call_id`` and escalates any
        ``FailedToolCall`` to a ``ToolExecutionError`` (it is *not* shown to the model),
        so this is the bridge's hard-failure channel: a dead session, a transport error,
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
                "[%s] no live MCP session for bridge=%s resource=%s tool=%s",
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
                "[%s] mcp tool=%s tool_call_id=%s on bridge=%s raised %s; surfacing FailedToolCall",
                ctx.correlation_id[:8],
                payload.name,
                payload.tool_call_id,
                self.node_id,
                type(e).__name__,
            )
            return self._fail(ctx, payload, exc_type=type(e).__name__, exc_message=safe_exc_message(e))

        ctx.state.add_tool_result(tool_call_id=payload.tool_call_id, tool_result=tool_return)
        return ReturnCall(ctx.state)
