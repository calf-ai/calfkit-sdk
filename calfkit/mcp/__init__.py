"""MCP integration: the hosting toolbox node and its identity-only reference.

`MCPToolboxNode` hosts an MCP server's tools as a calfkit node (deploy side);
`MCPToolbox` references one by name from anywhere (call side) — they are
exported together deliberately, the same way a servant and its handle travel
together in the peer-node pattern.
"""

from calfkit.mcp.mcp_toolbox import MCPToolbox, MCPToolboxNode
from calfkit.mcp.mcp_transport import StdioServerParameters, StreamableHttpParameters

__all__ = [
    "MCPToolboxNode",
    "MCPToolbox",
    "StdioServerParameters",
    "StreamableHttpParameters",
]
