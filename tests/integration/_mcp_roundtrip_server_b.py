"""A second minimal MCP server for the multi-server routing test.

Identical shape to ``_mcp_roundtrip_server.py`` but with a DISJOINT tool set
(``mul``/``upper``). Disjoint names matter: an agent resolving tools from two
MCP selectors keys its registry by tool name (last-writer-wins across
selectors), so two servers exposing the same name would collide. Distinct names
let a test prove each call routed to the correct server.
"""

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("roundtrip-test-server-b")


@mcp.tool()
def mul(a: int, b: int) -> int:
    """Return ``a * b`` — distinct from server A's ``add`` so a result of 20 can
    only have come from this server."""
    return a * b


@mcp.tool()
def upper(text: str) -> str:
    """Return ``text`` upper-cased."""
    return text.upper()


if __name__ == "__main__":
    mcp.run()
