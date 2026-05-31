"""Unit tests for ``calfkit.mcp._session``.

Covers pure logic (transport descriptors, header synthesis, name inference,
env merging) plus the lifecycle behaviors that don't require a real MCP
server. Real-transport tests against ``@modelcontextprotocol/server-everything``
live in the Phase 8 E2E lane.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from calfkit.mcp._session import (
    HttpTransport,
    McpSession,
    StdioTransport,
    _basename_from_command,
)
from calfkit.mcp._tool_def import McpToolDef
from calfkit.mcp.exceptions import McpConfigError

# ---------------------------------------------------------------------------
# Name inference
# ---------------------------------------------------------------------------


def test_basename_from_npx_scoped_package() -> None:
    """The npm convention: ``npx -y @scope/pkg`` → use ``pkg`` as the name."""
    assert _basename_from_command("npx", ("-y", "@modelcontextprotocol/server-gmail")) == "server-gmail"


def test_basename_from_python_module() -> None:
    """``python -m my_mcp`` → use ``my_mcp``."""
    assert _basename_from_command("python", ("-m", "my_mcp")) == "my_mcp"


def test_basename_falls_back_to_command_basename() -> None:
    """Bare command with no informative args → strip path components."""
    assert _basename_from_command("/usr/local/bin/gmail-mcp-server", ()) == "gmail-mcp-server"


def test_basename_skips_flag_args() -> None:
    """Flag-only args don't override; first non-flag wins."""
    assert _basename_from_command("npx", ("-y", "--silent", "@scope/foo")) == "foo"


def test_basename_empty_command_fallback() -> None:
    """Empty command + empty args → 'mcp' fallback (last-resort)."""
    assert _basename_from_command("", ()) == "mcp"


def test_stdio_transport_infer_name() -> None:
    t = StdioTransport(command="npx", args=("-y", "@mcp/server-gmail"))
    assert t.infer_name() == "server-gmail"


def test_http_transport_infer_name() -> None:
    t = HttpTransport(url="https://api.github.com/mcp/v1")
    assert t.infer_name() == "api.github.com"


def test_http_transport_infer_name_no_host() -> None:
    """Malformed/no-host URL falls back to 'mcp' (last-resort)."""
    t = HttpTransport(url="")
    assert t.infer_name() == "mcp"


# ---------------------------------------------------------------------------
# HttpTransport.build_session_headers
# ---------------------------------------------------------------------------


def test_headers_no_token_no_explicit() -> None:
    t = HttpTransport(url="https://x.com/mcp")
    assert t.build_session_headers() == {}


def test_headers_token_synthesizes_authorization() -> None:
    t = HttpTransport(url="https://x.com/mcp", token="raw-token-value")
    assert t.build_session_headers() == {"Authorization": "Bearer raw-token-value"}


def test_headers_token_with_bearer_prefix_preserved() -> None:
    """If the user passed 'Bearer X' as the token literal, don't double-prefix."""
    t = HttpTransport(url="https://x.com/mcp", token="Bearer already-prefixed")
    assert t.build_session_headers() == {"Authorization": "Bearer already-prefixed"}


def test_headers_token_with_lowercase_bearer() -> None:
    """Bearer-detection is case-insensitive on the prefix."""
    t = HttpTransport(url="https://x.com/mcp", token="bearer lowercase-prefix")
    assert t.build_session_headers() == {"Authorization": "bearer lowercase-prefix"}


def test_headers_explicit_authorization_wins_over_token() -> None:
    """If explicit ``headers={'Authorization': ...}`` is set, ``token`` is ignored.

    The user was explicit; the convenience kwarg shouldn't silently override.
    """
    t = HttpTransport(
        url="https://x.com/mcp",
        token="ignored",
        headers={"Authorization": "Basic abc123"},
    )
    assert t.build_session_headers() == {"Authorization": "Basic abc123"}


def test_headers_custom_headers_preserved() -> None:
    """Custom non-Authorization headers always flow through."""
    t = HttpTransport(
        url="https://x.com/mcp",
        token="tok",
        headers={"X-Trace": "abc"},
    )
    headers = t.build_session_headers()
    assert headers["X-Trace"] == "abc"
    assert headers["Authorization"] == "Bearer tok"


def test_headers_token_whitespace_stripped() -> None:
    """Stray whitespace around the token is harmless — strip before formatting."""
    t = HttpTransport(url="https://x.com/mcp", token="  raw  ")
    assert t.build_session_headers() == {"Authorization": "Bearer raw"}


# ---------------------------------------------------------------------------
# StdioTransport defaults
# ---------------------------------------------------------------------------


def test_stdio_defaults() -> None:
    """Defaults: no env, no cwd, 5s shutdown grace, safe_env_only=False."""
    t = StdioTransport(command="x")
    assert t.args == ()
    assert t.env is None
    assert t.cwd is None
    assert t.shutdown_grace_seconds == 5.0
    assert t.safe_env_only is False


def test_stdio_frozen() -> None:
    """Immutable: mutation raises."""
    t = StdioTransport(command="x")
    with pytest.raises(Exception):
        t.command = "y"  # type: ignore[misc]


def test_http_frozen() -> None:
    t = HttpTransport(url="https://x")
    with pytest.raises(Exception):
        t.url = "https://y"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# McpSession — env merging (Pattern 1 / Q1)
# ---------------------------------------------------------------------------


def test_build_stdio_params_full_env_passthrough(monkeypatch: pytest.MonkeyPatch) -> None:
    """v1 default: ``{**os.environ, **user_env}``. User wins on conflicts."""
    monkeypatch.setenv("CALFKIT_TEST_PASSTHROUGH", "from-env")
    monkeypatch.setenv("CALFKIT_TEST_CONFLICT", "from-env")
    transport = StdioTransport(
        command="x",
        env={"CALFKIT_TEST_USER": "from-user", "CALFKIT_TEST_CONFLICT": "user-wins"},
    )
    session = McpSession(transport)
    params = session._build_stdio_params()

    assert params.env is not None
    assert params.env["CALFKIT_TEST_PASSTHROUGH"] == "from-env"  # inherited
    assert params.env["CALFKIT_TEST_USER"] == "from-user"  # explicit
    assert params.env["CALFKIT_TEST_CONFLICT"] == "user-wins"  # user wins


def test_build_stdio_params_safe_env_only(monkeypatch: pytest.MonkeyPatch) -> None:
    """safe_env_only=True: do NOT merge os.environ; defer to MCP SDK's allowlist."""
    monkeypatch.setenv("CALFKIT_TEST_NOT_PASSED", "should-not-leak")
    transport = StdioTransport(command="x", env={"USER_SET": "v"}, safe_env_only=True)
    session = McpSession(transport)
    params = session._build_stdio_params()

    # When safe_env_only=True, we pass the user env only; the SDK's
    # get_default_environment() augments it from its own allowlist.
    assert params.env == {"USER_SET": "v"}
    assert "CALFKIT_TEST_NOT_PASSED" not in (params.env or {})


def test_build_stdio_params_empty_user_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """env=None default → still merges os.environ (in passthrough mode)."""
    monkeypatch.setenv("CALFKIT_TEST_E", "v")
    transport = StdioTransport(command="x")  # env=None
    session = McpSession(transport)
    params = session._build_stdio_params()
    assert params.env is not None
    assert params.env["CALFKIT_TEST_E"] == "v"


def test_build_stdio_params_propagates_cwd() -> None:
    t = StdioTransport(command="x", cwd="/tmp/foo")
    session = McpSession(t)
    params = session._build_stdio_params()
    assert params.cwd == "/tmp/foo"


def test_build_stdio_params_args_as_list() -> None:
    """Internal tuple converts to list (SDK API expects list[str])."""
    t = StdioTransport(command="x", args=("-y", "--verbose"))
    session = McpSession(t)
    params = session._build_stdio_params()
    assert params.args == ["-y", "--verbose"]


# ---------------------------------------------------------------------------
# McpSession lifecycle — guards
# ---------------------------------------------------------------------------


async def test_call_tool_before_open_raises() -> None:
    """Operations before open/initialize raise a clear error."""
    session = McpSession(StdioTransport(command="x"))
    with pytest.raises(RuntimeError, match="not open"):
        await session.call_tool("t")


async def test_list_tools_before_open_raises() -> None:
    session = McpSession(StdioTransport(command="x"))
    with pytest.raises(RuntimeError, match="not open"):
        await session.list_tools()


async def test_initialize_before_open_raises() -> None:
    session = McpSession(StdioTransport(command="x"))
    with pytest.raises(RuntimeError, match="not open"):
        await session.initialize()


async def test_aclose_idempotent_on_unopened() -> None:
    """aclose() on a never-opened session is a no-op, not an error."""
    session = McpSession(StdioTransport(command="x"))
    await session.aclose()
    await session.aclose()  # safe to call again


# ---------------------------------------------------------------------------
# McpSession — client_info construction
# ---------------------------------------------------------------------------


def test_client_info_defaults_to_calfkit_version() -> None:
    """Without explicit version override, advertise calfkit's installed version."""
    session = McpSession(StdioTransport(command="x"))
    # The Implementation type from mcp.types has name + version fields.
    assert session._client_info.name == "calfkit"
    assert session._client_info.version  # non-empty


def test_client_info_explicit_version_override() -> None:
    session = McpSession(StdioTransport(command="x"), client_info_version="0.9.9-test")
    assert session._client_info.version == "0.9.9-test"


def test_client_info_explicit_name_override() -> None:
    """Some users may want to identify as something other than 'calfkit'."""
    session = McpSession(StdioTransport(command="x"), client_info_name="my-app")
    assert session._client_info.name == "my-app"


# ---------------------------------------------------------------------------
# McpSession — list_tools / call_tool with mocked underlying ClientSession
#
# The strategy: bypass the real subprocess + ClientSession lifecycle by
# directly stuffing a MagicMock into session._session. This lets us assert
# that our wrapper translates inputs (e.g. timedelta wrapping, mcp.types.Tool
# → McpToolDef) without paying the subprocess startup cost.
# ---------------------------------------------------------------------------


def _make_open_session_with_mock_underlying(mock_underlying: Any) -> McpSession:
    """Construct a McpSession with the underlying ClientSession replaced by a mock.

    Bypasses the real ``_connect`` path so we don't have to spin up a subprocess.
    """
    session = McpSession(StdioTransport(command="x"))
    session._session = mock_underlying
    return session


async def test_list_tools_maps_sdk_types_to_mcp_tool_def() -> None:
    """``list_tools`` adapts ``mcp.types.Tool`` (camelCase) → ``McpToolDef`` (snake_case)."""
    from mcp.types import ListToolsResult
    from mcp.types import Tool as McpSdkTool

    underlying = MagicMock()
    underlying.list_tools = AsyncMock(
        return_value=ListToolsResult(
            tools=[
                McpSdkTool(name="alpha", inputSchema={"type": "object"}),
                McpSdkTool(name="beta", inputSchema={"type": "object"}, description="b"),
            ]
        )
    )
    session = _make_open_session_with_mock_underlying(underlying)

    tools = await session.list_tools()
    assert all(isinstance(t, McpToolDef) for t in tools)
    assert [t.name for t in tools] == ["alpha", "beta"]
    assert tools[1].description == "b"
    underlying.list_tools.assert_called_once()


async def test_call_tool_wraps_timeout_in_timedelta() -> None:
    """The wrapper accepts ``float`` seconds but the SDK wants ``timedelta``."""
    from mcp.types import CallToolResult, TextContent

    underlying = MagicMock()
    underlying.call_tool = AsyncMock(return_value=CallToolResult(content=[TextContent(type="text", text="ok")], isError=False))
    session = _make_open_session_with_mock_underlying(underlying)

    await session.call_tool("t", {"x": 1}, read_timeout_seconds=42.5)

    call_kwargs = underlying.call_tool.call_args.kwargs
    assert isinstance(call_kwargs["read_timeout_seconds"], timedelta)
    assert call_kwargs["read_timeout_seconds"] == timedelta(seconds=42.5)


async def test_call_tool_omits_timeout_when_none() -> None:
    """No per-call timeout → pass None (let session-level default apply)."""
    from mcp.types import CallToolResult, TextContent

    underlying = MagicMock()
    underlying.call_tool = AsyncMock(return_value=CallToolResult(content=[TextContent(type="text", text="ok")], isError=False))
    session = _make_open_session_with_mock_underlying(underlying)

    await session.call_tool("t", {"x": 1})

    assert underlying.call_tool.call_args.kwargs["read_timeout_seconds"] is None


async def test_call_tool_forwards_meta() -> None:
    """meta=dict is forwarded as the SDK's keyword-only meta= arg."""
    from mcp.types import CallToolResult, TextContent

    underlying = MagicMock()
    underlying.call_tool = AsyncMock(return_value=CallToolResult(content=[TextContent(type="text", text="ok")], isError=False))
    session = _make_open_session_with_mock_underlying(underlying)

    await session.call_tool("t", {"x": 1}, meta={"user_id": "alice"})
    assert underlying.call_tool.call_args.kwargs["meta"] == {"user_id": "alice"}


async def test_call_tool_meta_defaults_to_none() -> None:
    from mcp.types import CallToolResult, TextContent

    underlying = MagicMock()
    underlying.call_tool = AsyncMock(return_value=CallToolResult(content=[TextContent(type="text", text="ok")], isError=False))
    session = _make_open_session_with_mock_underlying(underlying)

    await session.call_tool("t", {"x": 1})
    assert underlying.call_tool.call_args.kwargs["meta"] is None


async def test_call_tool_forwards_name_and_args_positional() -> None:
    """``name`` and ``args`` flow through as the first two SDK positional args."""
    from mcp.types import CallToolResult, TextContent

    underlying = MagicMock()
    underlying.call_tool = AsyncMock(return_value=CallToolResult(content=[TextContent(type="text", text="ok")], isError=False))
    session = _make_open_session_with_mock_underlying(underlying)

    await session.call_tool("search", {"q": "hello"})
    args, _ = underlying.call_tool.call_args
    assert args[0] == "search"
    assert args[1] == {"q": "hello"}


# ---------------------------------------------------------------------------
# McpSession._connect — unknown transport guard
# ---------------------------------------------------------------------------


async def test_connect_unknown_transport_type() -> None:
    """A non-Stdio/Http transport raises a clear McpConfigError at connect time."""

    class FakeTransport:
        def infer_name(self) -> str:
            return "fake"

    session = McpSession(FakeTransport())  # type: ignore[arg-type]
    with pytest.raises(McpConfigError, match="unknown transport"):
        await session._connect()


# ---------------------------------------------------------------------------
# McpSession — double-connect idempotency
# ---------------------------------------------------------------------------


async def test_double_connect_is_noop() -> None:
    """Calling _connect twice does not re-open — current session is reused."""
    underlying = MagicMock()
    session = _make_open_session_with_mock_underlying(underlying)

    # session._session is already set; _connect should short-circuit without
    # touching the AsyncExitStack.
    with patch("calfkit.mcp._session.stdio_client") as mock_stdio:
        await session._connect()
        mock_stdio.assert_not_called()


# ---------------------------------------------------------------------------
# transport property
# ---------------------------------------------------------------------------


def test_transport_property_exposed() -> None:
    t = StdioTransport(command="x")
    session = McpSession(t)
    assert session.transport is t


# ---------------------------------------------------------------------------
# httpx_client_kwargs threading (HttpTransport escape hatch)
# ---------------------------------------------------------------------------


async def test_make_httpx_factory_returns_client_with_user_kwargs() -> None:
    """User kwargs (e.g. ``verify=False`` for custom SSL) flow into the
    constructed httpx.AsyncClient and override built-in defaults.
    """
    import httpx

    from calfkit.mcp._session import _make_httpx_factory

    factory = _make_httpx_factory({"verify": False, "follow_redirects": False})
    client = factory(headers={"X-K": "v"}, timeout=httpx.Timeout(10.0))
    try:
        assert isinstance(client, httpx.AsyncClient)
        # User-supplied kwarg wins on conflict.
        assert client.follow_redirects is False
    finally:
        await client.aclose()


async def test_make_httpx_factory_merges_headers_instead_of_replacing() -> None:
    """User headers in ``httpx_client_kwargs`` merge per-key with the SDK's
    session headers — must NOT silently wipe the Authorization that the SDK
    passes in via ``headers=``.
    """

    from calfkit.mcp._session import _make_httpx_factory

    factory = _make_httpx_factory({"headers": {"X-User-Custom": "yes"}})
    client = factory(headers={"Authorization": "Bearer sdk-token", "Accept": "application/json"})
    try:
        # Both SDK and user headers must be present
        assert client.headers["Authorization"] == "Bearer sdk-token"
        assert client.headers["x-user-custom"] == "yes"
        assert client.headers["accept"] == "application/json"
    finally:
        await client.aclose()


async def test_make_httpx_factory_user_header_overrides_sdk_header_for_same_key() -> None:
    """A user header for the *same* key as the SDK's wins (explicit override)."""
    from calfkit.mcp._session import _make_httpx_factory

    factory = _make_httpx_factory({"headers": {"Accept": "text/plain"}})
    client = factory(headers={"Accept": "application/json"})
    try:
        assert client.headers["accept"] == "text/plain"
    finally:
        await client.aclose()


async def test_make_httpx_factory_user_header_collides_case_insensitively() -> None:
    """User ``authorization`` (lowercase) must override SDK ``Authorization``.

    httpx would otherwise concatenate both values into a single header
    on the wire (``'Bearer sdk-token, Custom-Auth'``), breaking auth.
    """
    from calfkit.mcp._session import _make_httpx_factory

    factory = _make_httpx_factory({"headers": {"authorization": "Custom-Auth"}})
    client = factory(headers={"Authorization": "Bearer sdk-token"})
    try:
        # Single canonical header, user value wins
        assert client.headers["authorization"] == "Custom-Auth"
        assert "sdk-token" not in client.headers["authorization"]
    finally:
        await client.aclose()


async def test_make_httpx_factory_empty_user_headers_preserves_sdk_headers() -> None:
    """Empty user headers dict must NOT clobber SDK headers (no-op merge)."""
    from calfkit.mcp._session import _make_httpx_factory

    factory = _make_httpx_factory({"headers": {}})
    client = factory(headers={"Authorization": "Bearer sdk-token"})
    try:
        assert client.headers["authorization"] == "Bearer sdk-token"
    finally:
        await client.aclose()


async def test_make_httpx_factory_sdk_passes_no_headers() -> None:
    """SDK calling factory(headers=None) — user headers still apply."""
    from calfkit.mcp._session import _make_httpx_factory

    factory = _make_httpx_factory({"headers": {"X-User": "yes"}})
    client = factory(headers=None)
    try:
        assert client.headers["x-user"] == "yes"
    finally:
        await client.aclose()


async def test_make_httpx_factory_default_matches_documented_invariants() -> None:
    """Sanity-check that with no user kwargs the factory produces a client
    with the MCP-documented defaults. Guards against silent divergence if
    the SDK adds a new default we should mirror.
    """
    from calfkit.mcp._session import _make_httpx_factory

    factory = _make_httpx_factory({})
    client = factory()
    try:
        # Documented invariants we mirror from create_mcp_http_client:
        assert client.follow_redirects is True
    finally:
        await client.aclose()


def test_headers_token_lowercase_authorization_wins_over_token() -> None:
    """The collision check is case-insensitive — ``headers={'authorization': ...}``
    (lowercase) also wins over ``token=``. Avoids duplicate Authorization headers.
    """
    t = HttpTransport(url="https://x.com/mcp", token="should-be-ignored", headers={"authorization": "Basic abc"})
    out = t.build_session_headers()
    # Only one Authorization header (case-insensitive), and it's the user's
    auth_keys = [k for k in out if k.lower() == "authorization"]
    assert len(auth_keys) == 1
    assert out[auth_keys[0]] == "Basic abc"


async def test_connect_threads_factory_when_kwargs_set() -> None:
    """When ``httpx_client_kwargs`` is non-empty, ``_connect`` passes a
    custom ``httpx_client_factory`` to ``streamablehttp_client``.
    """
    transport = HttpTransport(url="https://example.com/mcp", httpx_client_kwargs={"verify": False})
    session = McpSession(transport)

    mock_cm = MagicMock()
    mock_cm.__aenter__ = AsyncMock(return_value=(MagicMock(), MagicMock(), MagicMock()))
    mock_cm.__aexit__ = AsyncMock(return_value=None)
    with patch("calfkit.mcp._session.streamablehttp_client", return_value=mock_cm) as mock_streamable:
        with patch("calfkit.mcp._session.ClientSession") as mock_cs:
            mock_cs.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
            mock_cs.return_value.__aexit__ = AsyncMock(return_value=None)
            await session._connect()
    _, call_kwargs = mock_streamable.call_args
    assert "httpx_client_factory" in call_kwargs
    assert callable(call_kwargs["httpx_client_factory"])


async def test_connect_omits_factory_when_no_kwargs() -> None:
    """No ``httpx_client_kwargs`` → ``httpx_client_factory`` not set; SDK uses default."""
    transport = HttpTransport(url="https://example.com/mcp")
    session = McpSession(transport)

    mock_cm = MagicMock()
    mock_cm.__aenter__ = AsyncMock(return_value=(MagicMock(), MagicMock(), MagicMock()))
    mock_cm.__aexit__ = AsyncMock(return_value=None)
    with patch("calfkit.mcp._session.streamablehttp_client", return_value=mock_cm) as mock_streamable:
        with patch("calfkit.mcp._session.ClientSession") as mock_cs:
            mock_cs.return_value.__aenter__ = AsyncMock(return_value=MagicMock())
            mock_cs.return_value.__aexit__ = AsyncMock(return_value=None)
            await session._connect()
    _, call_kwargs = mock_streamable.call_args
    assert "httpx_client_factory" not in call_kwargs


# ---------------------------------------------------------------------------
# shutdown_grace_seconds bounds the close
# ---------------------------------------------------------------------------


async def test_aclose_respects_shutdown_grace_seconds(caplog: pytest.LogCaptureFixture) -> None:
    """A hung stdio aclose is hard-cancelled after ``shutdown_grace_seconds``
    and raises :class:`McpTransportError` so the worker logs the leak.
    """
    import asyncio

    from calfkit.mcp.exceptions import McpTransportError

    transport = StdioTransport(command="x", shutdown_grace_seconds=0.05)
    session = McpSession(transport)
    # Pretend a session is open so aclose() runs the close path.
    session._session = MagicMock()

    async def _slow_aclose() -> None:
        await asyncio.sleep(5)  # WAY longer than the grace window

    with patch.object(session._stack, "aclose", side_effect=_slow_aclose):
        with caplog.at_level("ERROR", logger="calfkit.mcp._session"):
            with pytest.raises(McpTransportError, match="timed out after 0.05s"):
                await session.aclose()

    # ERROR fired (operators must see leaked subprocesses); session field reset.
    assert any(r.levelname == "ERROR" and "orphan" in r.getMessage() for r in caplog.records)
    assert session._session is None


async def test_aclose_zero_grace_means_unbounded() -> None:
    """``shutdown_grace_seconds=0`` means "no bound" — opt out of wait_for.

    Setting 0 must NOT cause a clean close to raise TimeoutError. The
    `wait_for(coro, timeout=0)` semantic raises immediately even for
    instant coroutines; sentinel ``0`` skips that wrap entirely.
    """
    transport = StdioTransport(command="x", shutdown_grace_seconds=0.0)
    session = McpSession(transport)
    session._session = MagicMock()

    closed = False

    async def _quick_aclose() -> None:
        nonlocal closed
        closed = True

    with patch.object(session._stack, "aclose", side_effect=_quick_aclose):
        # Must NOT raise; clean close completes naturally.
        await session.aclose()

    assert closed
    assert session._session is None


async def test_aclose_zero_grace_unbounded_for_slow_close() -> None:
    """grace=0 truly is unbounded — even a slow aclose runs to completion."""
    import asyncio

    transport = StdioTransport(command="x", shutdown_grace_seconds=0.0)
    session = McpSession(transport)
    session._session = MagicMock()

    completed = False

    async def _slow_but_finite() -> None:
        nonlocal completed
        await asyncio.sleep(0.02)
        completed = True

    with patch.object(session._stack, "aclose", side_effect=_slow_but_finite):
        await session.aclose()
    assert completed


async def test_aclose_non_timeout_exception_propagates_and_resets_state() -> None:
    """An unrelated RuntimeError from stack.aclose propagates; finally still resets."""
    transport = StdioTransport(command="x", shutdown_grace_seconds=5.0)
    session = McpSession(transport)
    session._session = MagicMock()

    async def _bad_aclose() -> None:
        raise RuntimeError("transport blew up")

    with patch.object(session._stack, "aclose", side_effect=_bad_aclose):
        with pytest.raises(RuntimeError, match="transport blew up"):
            await session.aclose()

    assert session._session is None  # finally ran


async def test_aclose_no_grace_for_http_transport() -> None:
    """HttpTransport has no grace kwarg; aclose runs unbounded.

    Asserts ``asyncio.wait_for`` is NOT called for HTTP transports so a
    future change that bounded HTTP closes (potentially a leak risk for
    legitimate slow servers) would trip this test.
    """
    transport = HttpTransport(url="https://example.com/mcp")
    session = McpSession(transport)
    session._session = MagicMock()

    async def _quick_aclose() -> None:
        return None

    with (
        patch.object(session._stack, "aclose", side_effect=_quick_aclose),
        patch("calfkit.mcp._session.asyncio.wait_for") as mock_wait_for,
    ):
        await session.aclose()
        mock_wait_for.assert_not_called()
    assert session._session is None


async def test_aclose_is_no_op_after_timeout() -> None:
    """The aclose docstring promises 'subsequent aclose() is a no-op even
    after a timeout' — pin that contract so a refactor that moves the
    ``_session = None`` reset out of ``finally`` breaks this test.
    """
    import asyncio

    from calfkit.mcp.exceptions import McpTransportError

    transport = StdioTransport(command="x", shutdown_grace_seconds=0.05)
    session = McpSession(transport)
    session._session = MagicMock()

    async def _slow_aclose() -> None:
        await asyncio.sleep(5)

    with patch.object(session._stack, "aclose", side_effect=_slow_aclose) as mock_close:
        with pytest.raises(McpTransportError):
            await session.aclose()
        # Second call must be a no-op (no raise) because _session was reset
        mock_close.side_effect = None
        mock_close.return_value = None
        await session.aclose()  # must not raise
    assert session._session is None


# ---------------------------------------------------------------------------
# list_tools pagination warning (P1 #15)
# ---------------------------------------------------------------------------


async def test_list_tools_warns_on_paginated_response(caplog: pytest.LogCaptureFixture) -> None:
    """Server with >1 page of tools triggers a WARNING — v1 reads page 1 only."""
    transport = StdioTransport(command="x")
    session = McpSession(transport)
    mock_session = MagicMock()
    paginated = MagicMock()
    paginated.tools = []
    paginated.nextCursor = "cursor-page-2"
    mock_session.list_tools = AsyncMock(return_value=paginated)
    session._session = mock_session

    with caplog.at_level("WARNING", logger="calfkit.mcp._session"):
        await session.list_tools()

    assert any("nextCursor" in r.message for r in caplog.records)
