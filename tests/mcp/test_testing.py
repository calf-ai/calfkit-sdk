"""Unit tests for ``calfkit.mcp._testing``.

Coverage focus:
- FakeMcpServer is a true McpServer (isinstance) so Phase 4 Worker code
  treats it identically.
- ``_open_bridge_session`` installs a fake session without doing real I/O.
- The fake session's ``call_tool`` routes to the user's invoker and tracks
  history for assertions.
- Sync and async invokers both work.
- Invoker return-type guard catches user mistakes early.
- ``call_history`` accessor surfaces useful diagnostic info.
"""

from __future__ import annotations

from typing import Any

import pytest
from mcp.types import CallToolResult, TextContent

from calfkit.mcp._server import McpServer
from calfkit.mcp._testing import FakeMcpServer, FakeMcpTransport, _FakeMcpSession
from calfkit.mcp._tool_def import McpToolDef


def _td(name: str = "t") -> McpToolDef:
    return McpToolDef(name=name, input_schema={"type": "object"})


def _ok_result(text: str = "ok") -> CallToolResult:
    return CallToolResult(content=[TextContent(type="text", text=text)], isError=False)


# ---------------------------------------------------------------------------
# Subclass relationship — Phase 4 Worker uses isinstance(node, McpServer)
# ---------------------------------------------------------------------------


def test_fake_is_mcp_server() -> None:
    """isinstance check — Worker code must accept FakeMcpServer."""
    fake = FakeMcpServer(name="x", tools=[_td()], invoker=lambda n, a, m: _ok_result())
    assert isinstance(fake, McpServer)


def test_fake_transport_infers_supplied_name() -> None:
    t = FakeMcpTransport(fake_name="my-fake")
    assert t.infer_name() == "my-fake"


def test_fake_transport_default_name() -> None:
    t = FakeMcpTransport()
    assert t.infer_name() == "fake"


# ---------------------------------------------------------------------------
# __iter__ — same surface as real McpServer
# ---------------------------------------------------------------------------


def test_fake_yields_tool_node_schemas() -> None:
    fake = FakeMcpServer(name="x", tools=[_td("search"), _td("send")], invoker=lambda n, a, m: _ok_result())
    schemas = list(fake)
    assert len(schemas) == 2
    assert [s.tool_schema.name for s in schemas] == ["search", "send"]


def test_fake_supports_filter_chains() -> None:
    fake = FakeMcpServer(name="x", tools=[_td("a"), _td("b")], invoker=lambda n, a, m: _ok_result())
    filtered = fake.only("a")
    # Note: .only() returns a McpServer (not FakeMcpServer) per our _copy.
    # That's OK because tests typically iterate / inspect the result.
    assert [s.tool_schema.name for s in filtered] == ["a"]


# ---------------------------------------------------------------------------
# _open_bridge_session lifecycle
# ---------------------------------------------------------------------------


async def test_open_bridge_installs_fake_session() -> None:
    fake = FakeMcpServer(name="x", tools=[_td()], invoker=lambda n, a, m: _ok_result())
    assert fake.session is None
    await fake._open_bridge_session()
    assert fake.session is not None
    assert isinstance(fake.session, _FakeMcpSession)


async def test_open_bridge_sets_initialize_result() -> None:
    fake = FakeMcpServer(name="x", tools=[_td()], invoker=lambda n, a, m: _ok_result(), server_name="my-srv", server_version="1.2.3")
    await fake._open_bridge_session()
    assert fake._initialize_result is not None
    # The synthetic InitializeResult carries our configured server info
    assert fake._initialize_result.serverInfo.name == "my-srv"
    assert fake._initialize_result.serverInfo.version == "1.2.3"


async def test_close_bridge_session() -> None:
    fake = FakeMcpServer(name="x", tools=[_td()], invoker=lambda n, a, m: _ok_result())
    await fake._open_bridge_session()
    inner = fake.session
    assert inner is not None
    await fake._close_bridge_session()
    assert fake.session is None
    # Underlying fake session marked closed
    assert inner.closed is True  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# call_tool dispatch via invoker
# ---------------------------------------------------------------------------


async def test_sync_invoker_dispatch() -> None:
    captured: dict[str, Any] = {}

    def invoker(name: str, args: dict[str, Any], meta: dict[str, Any] | None) -> CallToolResult:
        captured["name"] = name
        captured["args"] = args
        captured["meta"] = meta
        return _ok_result(f"called {name}")

    fake = FakeMcpServer(name="x", tools=[_td("greet")], invoker=invoker)
    await fake._open_bridge_session()
    assert fake.session is not None

    result = await fake.session.call_tool("greet", {"who": "world"}, meta={"user_id": "alice"})
    assert isinstance(result, CallToolResult)
    assert "called greet" in result.content[0].text  # type: ignore[union-attr]
    assert captured == {"name": "greet", "args": {"who": "world"}, "meta": {"user_id": "alice"}}


async def test_async_invoker_dispatch() -> None:
    async def invoker(name: str, args: dict[str, Any], meta: dict[str, Any] | None) -> CallToolResult:
        return _ok_result(f"async-called {name}")

    fake = FakeMcpServer(name="x", tools=[_td("t")], invoker=invoker)
    await fake._open_bridge_session()
    assert fake.session is not None

    result = await fake.session.call_tool("t", {})
    assert "async-called t" in result.content[0].text  # type: ignore[union-attr]


async def test_invoker_with_no_args_defaults_to_empty_dict() -> None:
    """args=None becomes {} so the invoker can rely on a dict."""
    captured_args: dict[str, Any] = {}

    def invoker(name: str, args: dict[str, Any], meta: dict[str, Any] | None) -> CallToolResult:
        captured_args.update(args)
        return _ok_result()

    fake = FakeMcpServer(name="x", tools=[_td()], invoker=invoker)
    await fake._open_bridge_session()
    assert fake.session is not None
    await fake.session.call_tool("t")  # no args
    # invoker received an empty dict, not None — the assertion above can run
    # without TypeError. Just ensure no crash.
    assert captured_args == {}


async def test_invoker_meta_defaults_to_none() -> None:
    """meta=None passes through if the caller didn't supply meta."""
    captured: dict[str, Any] = {}

    def invoker(name: str, args: dict[str, Any], meta: dict[str, Any] | None) -> CallToolResult:
        captured["meta"] = meta
        return _ok_result()

    fake = FakeMcpServer(name="x", tools=[_td()], invoker=invoker)
    await fake._open_bridge_session()
    assert fake.session is not None
    await fake.session.call_tool("t", {})
    assert captured["meta"] is None


# ---------------------------------------------------------------------------
# Invoker return-type guard
# ---------------------------------------------------------------------------


async def test_invoker_must_return_call_tool_result() -> None:
    """Returning the wrong type from the invoker raises clearly."""

    def bad_invoker(name: str, args: dict[str, Any], meta: dict[str, Any] | None) -> Any:
        return {"not": "a CallToolResult"}

    fake = FakeMcpServer(name="x", tools=[_td()], invoker=bad_invoker)
    await fake._open_bridge_session()
    assert fake.session is not None

    with pytest.raises(TypeError, match="CallToolResult"):
        await fake.session.call_tool("t", {})


# ---------------------------------------------------------------------------
# call_history
# ---------------------------------------------------------------------------


async def test_call_history_tracks_invocations() -> None:
    fake = FakeMcpServer(name="x", tools=[_td()], invoker=lambda n, a, m: _ok_result())
    await fake._open_bridge_session()
    assert fake.session is not None

    await fake.session.call_tool("alpha", {"x": 1}, meta={"u": "a"})
    await fake.session.call_tool("beta", {"y": 2}, meta=None)

    assert fake.call_history == [
        ("alpha", {"x": 1}, {"u": "a"}),
        ("beta", {"y": 2}, None),
    ]


def test_call_history_before_open_raises() -> None:
    """Accessing call_history without first opening the bridge raises a clear error."""
    fake = FakeMcpServer(name="x", tools=[_td()], invoker=lambda n, a, m: _ok_result())
    with pytest.raises(RuntimeError, match="before _open_bridge_session"):
        _ = fake.call_history


# ---------------------------------------------------------------------------
# list_tools — for the drift-detection sanity check
# ---------------------------------------------------------------------------


async def test_list_tools_returns_declared_tools() -> None:
    tools = [_td("a"), _td("b"), _td("c")]
    fake = FakeMcpServer(name="x", tools=tools, invoker=lambda n, a, m: _ok_result())
    await fake._open_bridge_session()
    assert fake.session is not None
    listed = await fake.session.list_tools()
    assert [t.name for t in listed] == ["a", "b", "c"]
