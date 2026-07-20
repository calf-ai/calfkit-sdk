"""MCP integration: the hosting toolbox node and the call-side entry alias.

`MCPToolboxNode` hosts an MCP server's tools as a calfkit node (deploy side); the call side
selects toolboxes with `Toolboxes(...)` and scopes one with a `Toolbox` entry —
`MCPToolbox` is a code-level alias of `Toolbox` for MCP-flavored call sites, re-exported
here so deploy-side and call-side names travel together (ADR-0045).
"""

from calfkit.mcp.mcp_toolbox import MCPToolboxNode
from calfkit.mcp.mcp_transport import StdioServerParameters, StreamableHttpParameters
from calfkit.nodes.toolbox import Toolbox as MCPToolbox

__all__ = [
    "MCPToolboxNode",
    "MCPToolbox",
    "StdioServerParameters",
    "StreamableHttpParameters",
]
