"""E2E tests against a real MCP server.

Uses ``@modelcontextprotocol/server-everything`` — the reference MCP
server that exposes a variety of tools for testing — via npx. These tests
are skipped if npx isn't on PATH.

They're slow (subprocess startup ~2-5 seconds each) so they're segregated
from the main test suite. Run with::

    uv run pytest tests/mcp/e2e -v

Phase 8 of MCP adaptor v1 (#158). Tests are the smoke check that the SDK
integration actually works against a non-mocked server.
"""

from __future__ import annotations

import pytest
from mcp.types import CallToolResult

from calfkit.mcp._adapt import adapt_call_tool_result
from calfkit.mcp._codegen import render_module
from calfkit.mcp._session import McpSession, StdioTransport
from tests.utils import skip_if_no_npx

# All tests in this module skip if npx isn't installed.
pytestmark = skip_if_no_npx


# ``@modelcontextprotocol/server-everything`` is the canonical reference
# MCP server. Pinned by npx; first run downloads, subsequent runs are cached.
EVERYTHING_TRANSPORT = StdioTransport(
    command="npx",
    args=("-y", "@modelcontextprotocol/server-everything"),
)


# ---------------------------------------------------------------------------
# McpSession lifecycle
# ---------------------------------------------------------------------------


async def test_session_open_initialize_close() -> None:
    """Spawn → initialize → close round-trip against a real MCP server.

    NB: the trailing ``list_tools`` call is intentional. MCP SDK 1.27's
    stdio transport has a known anyio teardown race when ``__aexit__``
    runs immediately after ``initialize`` without any intervening SDK
    operation — the stdout reader's send fails with BrokenResourceError
    as the receive loop exits in parallel. Any subsequent SDK call (here,
    ``list_tools``) lets the receive loop settle before teardown.
    """
    async with McpSession(EVERYTHING_TRANSPORT) as session:
        result = await session.initialize()
        # The server identifies itself
        assert result.serverInfo is not None
        assert isinstance(result.serverInfo.name, str)
        assert result.protocolVersion
        # See docstring: extra call avoids the SDK 1.27 teardown race.
        await session.list_tools()


async def test_list_tools_returns_real_tools() -> None:
    """`@modelcontextprotocol/server-everything` exposes several tools."""
    async with McpSession(EVERYTHING_TRANSPORT) as session:
        await session.initialize()
        tools = await session.list_tools()
        assert len(tools) > 0
        names = {t.name for t in tools}
        # `echo` is a canonical tool in server-everything
        assert "echo" in names, f"expected 'echo' tool, got {sorted(names)}"


async def test_call_tool_echo_round_trip() -> None:
    """Full round-trip: call the echo tool and verify the response."""
    async with McpSession(EVERYTHING_TRANSPORT) as session:
        await session.initialize()
        result = await session.call_tool("echo", {"message": "hello from calfkit"})
        assert isinstance(result, CallToolResult)
        assert not result.isError
        # The result content should contain the echoed message
        content_text = " ".join(getattr(b, "text", "") for b in result.content if hasattr(b, "text"))
        assert "hello from calfkit" in content_text


async def test_adapt_call_tool_result_with_real_response() -> None:
    """The adapter handles a real (non-mocked) CallToolResult correctly."""
    async with McpSession(EVERYTHING_TRANSPORT) as session:
        await session.initialize()
        result = await session.call_tool("echo", {"message": "round trip"})
        tool_return = adapt_call_tool_result(result, tool_call_id="tc-e2e")
        # echo returns text content (no structuredContent), so we expect
        # the flattened text as the return value
        assert "round trip" in str(tool_return.return_value)
        assert tool_return.metadata is not None
        assert tool_return.metadata["tool_call_id"] == "tc-e2e"


# ---------------------------------------------------------------------------
# Codegen end-to-end
# ---------------------------------------------------------------------------


async def test_codegen_generates_importable_module_from_real_server(tmp_path: pytest.TempPathFactory) -> None:
    """Generate a schemas module from server-everything; verify it's importable."""
    import sys
    from importlib import util

    async with McpSession(EVERYTHING_TRANSPORT) as session:
        await session.initialize()
        tools = await session.list_tools()

    rendered = render_module(server_name="everything", tools=tools, source="e2e test")

    # Write to a temp file and import it
    output_path = tmp_path / "everything_schemas.py"  # type: ignore[union-attr]
    output_path.write_text(rendered, encoding="utf-8")

    spec = util.spec_from_file_location("everything_schemas_e2e", str(output_path))
    assert spec is not None and spec.loader is not None
    module = util.module_from_spec(spec)
    sys.modules["everything_schemas_e2e"] = module
    try:
        spec.loader.exec_module(module)
        assert hasattr(module, "Everything")
        assert len(module.Everything.ALL) == len(tools)
        assert {t.name for t in module.Everything.ALL} == {t.name for t in tools}
    finally:
        sys.modules.pop("everything_schemas_e2e", None)


# ---------------------------------------------------------------------------
# End-to-end via McpServer + FakeMcpServer-like flow
# ---------------------------------------------------------------------------


async def test_mcp_server_open_bridge_session_against_real_server() -> None:
    """McpServer._open_bridge_session works against the real reference MCP server."""
    from calfkit.mcp import McpServer

    # First discover the tools (codegen would normally do this offline)
    async with McpSession(EVERYTHING_TRANSPORT) as discovery:
        await discovery.initialize()
        tools = await discovery.list_tools()

    # Then construct an McpServer with the discovered tools and open its session
    server = McpServer.stdio(
        "npx",
        "-y",
        "@modelcontextprotocol/server-everything",
        tools=tools,
        name="everything",
    )
    try:
        await server._open_bridge_session()
        # Session is open and initialize_result is populated
        assert server.session is not None
        assert server._initialize_result is not None
    finally:
        await server._close_bridge_session()
        assert server.session is None
