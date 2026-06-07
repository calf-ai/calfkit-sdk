from typing import ClassVar

from mcp import ClientSession

from calfkit._protocol import NodeKind
from calfkit.models.actions import NodeResult
from calfkit.models.session_context import SessionRunContext
from calfkit.models.state import State
from calfkit.nodes.base import BaseNodeDef


class MCPBridge(BaseNodeDef):
    _node_kind: ClassVar[NodeKind] = "bridge"

    def __init__(self):
        pass

    # async def run(self, ctx: SessionRunContext) -> NodeResult[State]:
    #     mcp_client = ctx.resources.get('mcp_client')
    #     if not mcp_client or not isinstance(mcp_client, ClientSession):
    #         raise Exception('No mcp client session')
    #     mcp_client.
