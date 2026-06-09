"""
MCP Python SDK — a hands-on tutorial scratch file.

Verified against `mcp` 1.27.2 (the version installed in this project).

What this file covers, end to end:
    1. Building a server with FastMCP (tools / resources / prompts).
    2. Talking to a server over stdio (client spawns the server as a subprocess).
    3. Talking to a server over Streamable HTTP (client connects to a running URL).
    4. The full client lifecycle: connect -> ClientSession -> initialize ->
       list_tools -> call_tool -> read the result.
    5. The fastest way to test a client+server together: connect them in-memory
       with NO transport, NO subprocess, NO network. Great for unit tests.

Run the in-memory demo (no external server needed):
    uv run python scratch/mcp_client_tutorial.py

Mental model
------------
MCP separates THREE concerns:

    [ transport ]      how bytes move:  stdio | streamable-http | sse | in-memory
          |
    [ ClientSession ]  the protocol:    initialize, list_tools, call_tool, ...
          |
    [ your code ]      what you do with tools/resources/prompts

A *client* always does the same dance regardless of transport:

    async with <transport>(...) as (read_stream, write_stream, *_):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()          # MUST be first
            await session.list_tools()
            await session.call_tool(name, args)

The transport context manager yields raw (read, write) anyio streams.
ClientSession wraps those streams in the JSON-RPC protocol.
"""

from __future__ import annotations

import asyncio

from mcp import ClientSession, types
from mcp.server.fastmcp import FastMCP

# ---------------------------------------------------------------------------
# PART 1 — Build a server with FastMCP
# ---------------------------------------------------------------------------
# FastMCP is the high-level, decorator-based server. You annotate plain Python
# functions and FastMCP derives the JSON Schema for inputs from your type hints
# and the tool description from the docstring.

mcp = FastMCP("Tutorial Server")


@mcp.tool()
def add(a: int, b: int) -> int:
    """Add two numbers and return the sum."""
    return a + b


@mcp.tool()
def greet(name: str, excited: bool = False) -> str:
    """Greet a person by name. Set excited=True for enthusiasm."""
    suffix = "!!!" if excited else "."
    return f"Hello, {name}{suffix}"


@mcp.resource("config://app-name")
def app_name() -> str:
    """A static resource — clients read it, they don't 'call' it."""
    return "calf-sdk-tutorial"


@mcp.resource("greeting://{name}")
def greeting_template(name: str) -> str:
    """A templated resource: the {name} segment is filled in by the request URI."""
    return f"A warm welcome to {name}."


@mcp.prompt()
def summarize(text: str) -> str:
    """A prompt template the client can fetch and feed to an LLM."""
    return f"Please summarize the following text concisely:\n\n{text}"


# ---------------------------------------------------------------------------
# PART 1b — How you'd actually RUN that server as a standalone process
# ---------------------------------------------------------------------------
# You normally put the server in its own module and give it an entrypoint.
# Uncomment ONE of these in a real server file:
#
#   if __name__ == "__main__":
#       mcp.run()                              # stdio transport (the default)
#       # mcp.run(transport="streamable-http") # HTTP server on /mcp (default :8000)
#
# - stdio:           the client launches this script as a subprocess and speaks
#                    over stdin/stdout. Best for local tools / editor plugins.
# - streamable-http: this script stays up as a web server; clients connect by URL.
#                    Best for remote / shared / multi-client servers.


# ---------------------------------------------------------------------------
# PART 2 — A client over stdio (client spawns the server subprocess)
# ---------------------------------------------------------------------------
async def client_over_stdio() -> None:
    """
    Connect to a server that the CLIENT launches as a subprocess.

    `StdioServerParameters` is literally "the command line to start the server".
    `stdio_client(...)` spawns it and hands you its (read, write) streams.
    """
    from mcp.client.stdio import StdioServerParameters, stdio_client

    server = StdioServerParameters(
        command="python",                       # the executable to run
        args=["scratch/my_server.py"],          # the server script
        env=None,                               # optionally override env vars
    )

    async with stdio_client(server) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()          # handshake — always first

            tools = await session.list_tools()
            print("stdio tools:", [t.name for t in tools.tools])

            result = await session.call_tool("add", {"a": 2, "b": 3})
            print("stdio add(2, 3) ->", _text_of(result))


# ---------------------------------------------------------------------------
# PART 3 — A client over Streamable HTTP (connect to a running URL)
# ---------------------------------------------------------------------------
async def client_over_http() -> None:
    """
    Connect to an already-running server at an HTTP URL.

    Note the transport yields a THIRD value (a callback to read the negotiated
    session id); the leading `*_` / `_` swallows it when you don't need it.

    In 1.27.2 the canonical name is `streamable_http_client`; the older
    `streamablehttp_client` (no underscore) still exists but is deprecated.
    """
    from mcp.client.streamable_http import streamable_http_client

    async with streamable_http_client("http://localhost:8000/mcp") as (
        read_stream,
        write_stream,
        _get_session_id,
    ):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()

            tools = await session.list_tools()
            print("http tools:", [t.name for t in tools.tools])

            result = await session.call_tool("greet", {"name": "Ada", "excited": True})
            print("http greet(...) ->", _text_of(result))


# ---------------------------------------------------------------------------
# PART 4 — The full ClientSession surface, annotated
# ---------------------------------------------------------------------------
async def explore_session(session: ClientSession) -> None:
    """
    Everything you commonly do with a session once it's initialized.
    `session` is already initialized when this is called.
    """
    # --- TOOLS -------------------------------------------------------------
    tools = await session.list_tools()
    for tool in tools.tools:
        # `inputSchema` is the JSON Schema for the tool's arguments — this is how
        # an LLM (or your bridge) knows what to pass.
        print(f"  tool {tool.name!r}: {tool.description}")
        print(f"       input schema: {tool.inputSchema}")

    # call_tool(name, arguments_dict) -> CallToolResult
    result = await session.call_tool("add", {"a": 10, "b": 32})
    print("  add(10, 32) ->", _text_of(result))

    # --- Reading a CallToolResult ------------------------------------------
    # A CallToolResult has these fields (verified on 1.27.2):
    #   .content           list[ContentBlock]  -> TextContent | ImageContent | ...
    #   .structuredContent dict | None         -> populated when the tool returns
    #                                             structured/typed data
    #   .isError           bool                -> True if the tool raised; the
    #                                             error text lands in .content
    #   .meta              dict | None
    print("  isError:", result.isError)
    print("  structuredContent:", result.structuredContent)

    # --- RESOURCES ---------------------------------------------------------
    resources = await session.list_resources()
    print("  resources:", [str(r.uri) for r in resources.resources])

    # Templated resources are listed separately:
    templates = await session.list_resource_templates()
    print("  templates:", [t.uriTemplate for t in templates.resourceTemplates])

    # Read one. read_resource returns contents as a list of text/blob blocks.
    content = await session.read_resource("config://app-name")  # type: ignore[arg-type]
    print("  config://app-name ->", content.contents[0].text)  # type: ignore[union-attr]

    # --- PROMPTS -----------------------------------------------------------
    prompts = await session.list_prompts()
    print("  prompts:", [p.name for p in prompts.prompts])

    got = await session.get_prompt("summarize", {"text": "MCP is a protocol."})
    print("  prompt messages:", got.messages)


def _text_of(result: types.CallToolResult) -> str:
    """
    Helper: pull plain text out of a CallToolResult.

    `.content` is a list of content blocks; TextContent blocks carry `.text`.
    Real code should handle ImageContent / EmbeddedResource etc. too.
    """
    parts = [block.text for block in result.content if isinstance(block, types.TextContent)]
    return " ".join(parts)


# ---------------------------------------------------------------------------
# PART 5 — In-memory: wire a client directly to a server (no transport!)
# ---------------------------------------------------------------------------
# This is the BEST way to learn and to unit-test. There's no subprocess and no
# network — a pair of in-memory streams connects the two. The server task runs
# concurrently inside the same event loop.
async def client_in_memory() -> None:
    """Connect a ClientSession straight to our `mcp` server via memory streams."""
    from mcp.shared.memory import create_connected_server_and_client_session

    # This helper spins up the server on one end of an in-memory pipe and hands
    # you a fully-constructed, ready-to-use ClientSession on the other end.
    # NOTE: it calls `session.initialize()` for you.
    async with create_connected_server_and_client_session(
        mcp._mcp_server,  # the low-level server FastMCP wraps
    ) as session:
        print("=== in-memory session ===")
        await explore_session(session)


# ---------------------------------------------------------------------------
# Entrypoint — runs the no-dependencies in-memory demo.
# ---------------------------------------------------------------------------
async def main() -> None:
    await client_in_memory()

    # The stdio / http demos need a real server to talk to, so they're not
    # called by default. To try them:
    #   1. Save a server file `scratch/my_server.py` that defines `mcp` and ends
    #      with `mcp.run()` (stdio) or `mcp.run(transport="streamable-http")`.
    #   2. For HTTP, start it first, then call `client_over_http()`.
    #   3. For stdio, just call `client_over_stdio()` — it launches the server.
    #
    # await client_over_stdio()
    # await client_over_http()


if __name__ == "__main__":
    asyncio.run(main())
