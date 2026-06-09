from typing import ClassVar

from mcp import ClientSession
from mcp.types import CallToolResult as MCPCallToolResult

from calfkit._protocol import NodeKind
from calfkit._registry import handler
from calfkit._vendor.pydantic_ai.messages import ToolReturn
from calfkit.mcp.mcp_transport import StdioServerParameters, StreamableHttpParameters, http_client, stdio_client
from calfkit.models.actions import NodeResult, ReturnCall
from calfkit.models.session_context import SessionRunContext
from calfkit.models.state import State
from calfkit.models.tool_dispatch import ToolCallRef
from calfkit.nodes.base import BaseNodeDef
from calfkit.worker.lifecycle import ResourceSetupContext


class MCPBridge(BaseNodeDef):
    _node_kind: ClassVar[NodeKind] = "bridge"

    async def _mcp_session(self, ctx: ResourceSetupContext):
        if isinstance(self._connection_params, StreamableHttpParameters):
            async with http_client(self._connection_params) as (read_stream, write_stream):
                async with ClientSession(read_stream, write_stream) as session:
                    await session.initialize()
                    yield session
        elif isinstance(self._connection_params, StdioServerParameters):
            async with stdio_client(self._connection_params) as (read_stream, write_stream):
                async with ClientSession(read_stream, write_stream) as session:
                    await session.initialize()
                    yield session

    def __init__(self, server_name: str, connection_params: StreamableHttpParameters | StdioServerParameters):
        super().__init__(node_id=server_name, subscribe_topics=[f"mcp_server.{server_name}"])
        self._connection_params = connection_params
        self._session_resource_key = f"mcp_session.{server_name}"
        self.resource(name=self._session_resource_key)(self._mcp_session)

    @handler("*", schema=ToolCallRef)
    async def run(self, ctx: SessionRunContext, payload: ToolCallRef) -> NodeResult[State]:  # type: ignore[override]

        client_session = ctx.resources.get(self._session_resource_key)
        if not client_session or not isinstance(client_session, ClientSession):
            raise Exception("No mcp client session")
        tool_result: MCPCallToolResult = await client_session.call_tool(name=payload.name, arguments=payload.args or None)
        ctx.state.add_tool_result(tool_call_id=payload.tool_call_id, tool_result=ToolReturn(return_value=tool_result))
        return ReturnCall(ctx.state)
