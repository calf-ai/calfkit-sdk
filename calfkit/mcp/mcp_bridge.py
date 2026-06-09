from typing import ClassVar

from mcp import ClientSession, stdio_client

from calfkit._protocol import NodeKind
from calfkit.models.actions import NodeResult
from calfkit.models.session_context import SessionRunContext
from calfkit.models.state import State
from calfkit.nodes.base import BaseNodeDef


class MCPBridge(BaseNodeDef):
    _node_kind: ClassVar[NodeKind] = "bridge"
    
    async def _mcp_session(self):
        async with stdio_client(server) as (read_stream, write_stream):
            async with ClientSession(read_stream, write_stream) as session:
                await session.initialize()          # handshake — always first

                tools = await session.list_tools()
                print("stdio tools:", [t.name for t in tools.tools])

                result = await session.call_tool("add", {"a": 2, "b": 3})
                print("stdio add(2, 3) ->", _text_of(result))

    def __init__(self, mcp_server_name: str):
        subscribe_topic = f"mcp_server.{mcp_server_name}"
        super().__init__(node_id=mcp_server_name, subscribe_topics=[subscribe_topic])

    async def run(self, ctx: SessionRunContext) -> NodeResult[State]:
        mcp_client = ctx.resources.get('mcp_client')
        if not mcp_client or not isinstance(mcp_client, ClientSession):
            raise Exception('No mcp client session')
        mcp_client.
