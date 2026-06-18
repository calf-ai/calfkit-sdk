"""A minimal, self-contained MCP server for the real-broker roundtrip test.

Built on the *real* ``mcp`` package (``mcp.server.fastmcp.FastMCP``) and
deliberately free of any ``calfkit`` or vendored-pydantic-ai imports, so the
test exercises a genuine external MCP server rather than a stub. The
:class:`~calfkit.mcp.MCPToolbox` spawns this module as a subprocess over stdio
via ``StdioServerParameters(command=sys.executable, args=[<this file>])`` and
talks the MCP protocol to it.

Every tool's output is a pure function of its input, so the roundtrip assertion
on the value that travels back through the broker is exact. The set spans the
argument arities a real toolbox must handle: multi-arg (:func:`add`), single-arg
(:func:`echo`), and no-arg (:func:`ping`).
"""

from mcp.server.fastmcp import Context, FastMCP

mcp = FastMCP("roundtrip-test-server")


@mcp.tool()
def add(a: int, b: int) -> int:
    """Return ``a + b`` — a multi-argument, deterministic scalar result."""
    return a + b


@mcp.tool()
def echo(text: str) -> str:
    """Return ``text`` unchanged — a single-argument passthrough."""
    return text


@mcp.tool()
def ping() -> str:
    """Return a constant — exercises the no-argument dispatch path."""
    return "pong"


@mcp.tool()
def danger() -> str:
    """A real, advertised tool used to test the ``include`` trust boundary: an
    agent that does not include it must never be able to call it."""
    return "danger-result"


def _bonus_impl() -> str:
    """The tool registered at runtime by :func:`enable_bonus`."""
    return "bonus-result"


@mcp.tool()
async def enable_bonus(ctx: Context) -> str:
    """Register a new ``bonus`` tool at runtime and notify clients of the
    change (``notifications/tools/list_changed``). Exercises dynamic discovery:
    the toolbox should re-list and re-publish, growing its capability record."""
    mcp.add_tool(_bonus_impl, name="bonus", description="A tool enabled at runtime.")
    await ctx.session.send_tool_list_changed()
    return "enabled"


if __name__ == "__main__":
    # Default transport is stdio: read/write the MCP protocol on stdin/stdout,
    # which is exactly what ``stdio_client`` connects to from the toolbox side.
    mcp.run()
