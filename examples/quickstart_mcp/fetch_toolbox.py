"""Deploy a toolbox node that fronts the official ``fetch`` MCP server.

On startup the toolbox spawns ``uvx mcp-server-fetch`` as a subprocess (stdio),
lists its tools, and advertises them on the capability control plane so any agent
in the cluster can use them by name. Agents never speak MCP themselves — the
toolbox is the cluster's MCP client.

Prerequisites: a running broker, and ``uv`` on your PATH (for ``uvx``).

Run it with:  ck run fetch_toolbox:fetcher
"""

from calfkit.mcp import MCPToolboxNode, StdioServerParameters

# The toolbox node manages the MCP server's lifecycle for you (spawn + connect).
fetcher = MCPToolboxNode(
    "fetcher",
    connection_params=StdioServerParameters(command="uvx", args=["mcp-server-fetch"]),
)
