"""In-memory MCP fixtures for tests.

``FakeMcpServer`` is a drop-in stand-in for :class:`McpServer` that bypasses
real subprocesses / HTTP and routes ``call_tool`` to a user-supplied
callable. Used by Phase 3+ integration tests with ``TestKafkaBroker`` to
exercise the full envelope flow without spinning up an MCP server.

Design:

- Subclasses :class:`McpServer` so ``isinstance(fake, McpServer)`` is True
  and the Worker (Phase 4) treats it identically.
- Provides a ``FakeMcpTransport`` that satisfies the abstract base but
  panics if accidentally connected to (we should always be using the
  injected session).
- Overrides ``_open_bridge_session`` to install a ``_FakeMcpSession``
  that resolves ``call_tool`` via the user's invoker callable.
- Asserts the user invoker's return type to surface mistakes early
  (must return ``mcp.types.CallToolResult``).
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock

from mcp.types import CallToolResult, Implementation, InitializeResult

from calfkit.mcp._server import McpServer
from calfkit.mcp._session import McpTransport
from calfkit.mcp._tool_def import McpToolDef

# Type alias for the invoker callable. Sync or async; receives the tool
# name + args + meta; returns a CallToolResult (real SDK type — so tests
# can exercise the real adapt_call_tool_result path).
_FakeInvoker = Callable[[str, dict[str, Any], dict[str, Any] | None], CallToolResult | Awaitable[CallToolResult]]


# ---------------------------------------------------------------------------
# Fake transport — present for type satisfaction, refuses real connection.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FakeMcpTransport(McpTransport):
    """No-op transport for tests. Refuses real connection."""

    fake_name: str = "fake"

    def infer_name(self) -> str:
        return self.fake_name


# ---------------------------------------------------------------------------
# Fake session — routes call_tool to the user invoker
# ---------------------------------------------------------------------------


class _FakeMcpSession:
    """In-memory stand-in for :class:`calfkit.mcp._session.McpSession`.

    Exposes the same methods (``initialize``, ``list_tools``, ``call_tool``,
    ``aclose``) but skips transport entirely. The Phase 3 ``McpBridge``
    consumes this via ``server.session.call_tool(...)`` exactly the same
    way it would consume the real thing.
    """

    def __init__(
        self,
        *,
        tools: tuple[McpToolDef, ...],
        invoker: _FakeInvoker,
        server_name: str = "fake-mcp-server",
        server_version: str = "0.0.0-fake",
    ) -> None:
        self._tools = tools
        self._invoker = invoker
        self._server_name = server_name
        self._server_version = server_version
        # Track invocations for test assertions
        self.call_history: list[tuple[str, dict[str, Any], dict[str, Any] | None]] = []
        self.closed = False

    async def initialize(self) -> InitializeResult:
        """Return a synthetic InitializeResult matching the SDK shape."""
        # MagicMock the capabilities + protocol_version fields — tests
        # rarely care about these; if they do, they can construct a real
        # InitializeResult themselves.
        result = MagicMock(spec=InitializeResult)
        result.serverInfo = Implementation(name=self._server_name, version=self._server_version)
        result.protocolVersion = "2025-11-25"
        result.capabilities = MagicMock()
        return result

    async def list_tools(self) -> list[McpToolDef]:
        """Return the user-declared tools as-is.

        Bypasses the SDK's ``mcp.types.Tool → McpToolDef`` adapter step
        because we already have McpToolDef instances. Sufficient for the
        drift-detection sanity check in ``McpServer._open_bridge_session``.
        """
        return list(self._tools)

    async def call_tool(
        self,
        name: str,
        args: dict[str, Any] | None = None,
        *,
        meta: dict[str, Any] | None = None,
        read_timeout_seconds: Any = None,  # accepted for signature parity; ignored
    ) -> CallToolResult:
        """Dispatch to the user-supplied invoker and validate its return type."""
        args_dict = args or {}
        self.call_history.append((name, args_dict, meta))

        result = self._invoker(name, args_dict, meta)
        if inspect.isawaitable(result):
            result = await result

        if not isinstance(result, CallToolResult):
            raise TypeError(f"FakeMcpServer invoker for {name!r} must return mcp.types.CallToolResult; got {type(result).__name__}")
        return result

    async def aclose(self) -> None:
        self.closed = True


# ---------------------------------------------------------------------------
# FakeMcpServer — drop-in McpServer for tests
# ---------------------------------------------------------------------------


class FakeMcpServer(McpServer):
    """In-memory MCP server stub for tests.

    Skips subprocess + transport entirely. ``Worker._on_startup`` (Phase 4)
    treats it identically to a real ``McpServer``: same ``_open_bridge_session``
    contract, same iteration → ``BaseToolNodeSchema`` instances. Only the
    underlying session is different.

    Use in integration tests with ``TestKafkaBroker`` to exercise the full
    envelope flow without spinning up a real MCP server.

    Example::

        from mcp.types import CallToolResult, TextContent
        from calfkit.mcp import McpToolDef
        from calfkit.mcp._testing import FakeMcpServer

        def my_invoker(name, args, meta):
            return CallToolResult(
                content=[TextContent(type="text", text=f"called {name}({args})")],
                isError=False,
            )

        gmail = FakeMcpServer(
            name="gmail",
            tools=[McpToolDef(name="search", input_schema={"type": "object"})],
            invoker=my_invoker,
        )
    """

    def __init__(
        self,
        *,
        name: str,
        tools: list[McpToolDef],
        invoker: _FakeInvoker,
        server_name: str = "fake-mcp-server",
        server_version: str = "0.0.0-fake",
    ) -> None:
        # Use FakeMcpTransport so isinstance checks for stdio/HTTP transport
        # don't accidentally pick this up.
        super().__init__(
            transport=FakeMcpTransport(fake_name=name),
            tools=tools,
            name=name,
        )
        self._invoker = invoker
        self._fake_server_name = server_name
        self._fake_server_version = server_version

    async def _open_bridge_session(self) -> None:
        """Install the fake session — no real subprocess or HTTP call."""
        fake_session = _FakeMcpSession(
            tools=self._tools,
            invoker=self._invoker,
            server_name=self._fake_server_name,
            server_version=self._fake_server_version,
        )
        # Direct assignment is safe; the McpServer-managed lifecycle does
        # not care whether self._session is a real or fake McpSession.
        self._session = fake_session  # type: ignore[assignment]
        self._initialize_result = await fake_session.initialize()

    @property
    def call_history(self) -> list[tuple[str, dict[str, Any], dict[str, Any] | None]]:
        """List of (tool_name, args, meta) tuples for every call made via the bridge.

        Populated by ``_FakeMcpSession.call_tool``. Raises if the session
        isn't open yet (you can't have made calls without opening).
        """
        if self._session is None:
            raise RuntimeError("FakeMcpServer.call_history accessed before _open_bridge_session() ran")
        # The session is always a _FakeMcpSession in this subclass; narrow it.
        assert isinstance(self._session, _FakeMcpSession)
        return self._session.call_history
