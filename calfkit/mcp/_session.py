"""MCP transport + session lifecycle wrappers.

Wraps the ``mcp`` Python SDK's ``ClientSession`` + ``stdio_client`` /
``streamablehttp_client`` transports behind a single lifecycle object
(``McpSession``) that:

- Spawns a subprocess (stdio) or opens an HTTP connection
- Sends MCP ``initialize`` with calfkit's ``clientInfo`` (and does NOT
  advertise the sampling/elicitation capabilities — see design doc §4.6)
- Exposes ``list_tools()`` (mapping ``mcp.types.Tool`` → :class:`McpToolDef`)
  and ``call_tool(name, args, *, meta=None)``
- Cleanly tears down via ``AsyncExitStack``

Transport descriptors are kept separate from the session so the Phase 6
``McpServers`` config flow can build a transport from parsed mcp.json and
hand it to the session at startup.

Pattern 1 multi-tenancy (design doc §10): credentials are **session-static**
and baked into the transport at construction. Per-call ``meta=`` is the
only per-call hook — it travels in the JSON-RPC message body, race-free.

See ``docs/mcp-v1-plan.md`` §6.3 and ``docs/mcp-adaptor-implementation-plan.md``
§5.3 for design context.
"""

from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any
from urllib.parse import urlparse

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamablehttp_client
from mcp.types import Implementation

from calfkit.mcp._tool_def import McpToolDef
from calfkit.mcp.exceptions import McpConfigError

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Transports
# ---------------------------------------------------------------------------


class McpTransport(ABC):
    """Base type for normalised MCP transport descriptors.

    Subclasses are immutable dataclasses; ``McpSession.open()`` consumes one
    to spawn the underlying transport. Naming is inferred from the transport
    for default ``McpServer.name`` derivation.
    """

    @abstractmethod
    def infer_name(self) -> str:
        """Default server name when none is supplied.

        For stdio: the basename of the executable, with leading ``@scope/``
        stripped if present (e.g. ``@modelcontextprotocol/server-gmail``
        from npx args → ``server-gmail``).

        For HTTP: the URL host.
        """


@dataclass(frozen=True)
class StdioTransport(McpTransport):
    """stdio MCP server descriptor.

    ``env`` is **merged into ``os.environ``** when the session opens (v1
    plan Q1: full passthrough is the documented default; matches Docker /
    subprocess defaults). To get the MCP SDK's safe-allowlist behavior,
    pass ``safe_env_only=True``.
    """

    command: str
    args: tuple[str, ...] = ()
    env: dict[str, str] | None = None
    cwd: str | None = None
    shutdown_grace_seconds: float = 5.0
    # When True, do NOT merge os.environ — use MCP SDK's safe allowlist
    # (HOME, PATH, USER, LANG, ...) plus user_env only. Off by default.
    safe_env_only: bool = False

    def infer_name(self) -> str:
        return _basename_from_command(self.command, self.args)


@dataclass(frozen=True)
class HttpTransport(McpTransport):
    """Streamable HTTP MCP server descriptor.

    ``headers`` and ``token`` are **session-static** (resolved at
    construction; baked into every request for the session's lifetime).
    Per Pattern 1 (design doc §10), per-call credential rotation is not
    supported in v1.

    ``token``, if set, populates the ``Authorization`` header as
    ``Bearer <token>`` (unless the literal value already starts with
    ``Bearer `` case-insensitively, in which case it is used as-is).

    ``httpx_client_kwargs`` (v1 plan Q11): pass-through escape hatch for
    custom SSL / proxy / etc. Reserved here; threaded into a custom
    ``httpx_client_factory`` if non-empty.
    """

    url: str
    token: str | None = None
    headers: dict[str, str] = field(default_factory=dict)
    httpx_client_kwargs: dict[str, Any] = field(default_factory=dict)
    timeout_seconds: float = 30.0
    sse_read_timeout_seconds: float = 300.0

    def infer_name(self) -> str:
        parsed = urlparse(self.url)
        return parsed.hostname or "mcp"

    def build_session_headers(self) -> dict[str, str]:
        """Compose the static headers dict that will be passed to
        ``streamablehttp_client(..., headers=...)`` once per session.

        Precedence: explicit ``headers`` dict; then ``token`` populates
        ``Authorization`` if absent (does NOT overwrite an explicit
        ``Authorization`` header in ``headers``).
        """
        out: dict[str, str] = dict(self.headers)
        if self.token is not None and "Authorization" not in {k for k in out}:
            tok = self.token.strip()
            if tok.lower().startswith("bearer "):
                out["Authorization"] = tok
            else:
                out["Authorization"] = f"Bearer {tok}"
        return out


def _basename_from_command(command: str, args: tuple[str, ...]) -> str:
    """Best-effort name inference from ``command`` + ``args``.

    Examples:
        npx -y @modelcontextprotocol/server-gmail → server-gmail
        /usr/bin/python -m my_mcp                  → my_mcp
        gmail-mcp-server                           → gmail-mcp-server
    """
    # If the args contain something that looks like an npm scoped package or
    # a module name, prefer that over the command (`npx` / `python` alone is
    # uninformative).
    for arg in args:
        if arg.startswith("-"):
            continue
        # Strip @scope/ prefix and any path components.
        last = arg.rsplit("/", 1)[-1]
        if last and not last.startswith("-"):
            return last
    # Fall back to the command's basename.
    return os.path.basename(command) or "mcp"


# ---------------------------------------------------------------------------
# McpSession
# ---------------------------------------------------------------------------


# Default client_info advertised in the MCP `initialize` handshake.
# Versions are loaded lazily so we don't depend on installed metadata
# import at module load.
_DEFAULT_CLIENT_NAME = "calfkit"


def _calfkit_version() -> str:
    """Lookup calfkit's installed version, with a defensive fallback."""
    try:
        from importlib.metadata import version

        return version("calfkit")
    except Exception:
        return "0+unknown"


class McpSession:
    """Owns one MCP ``ClientSession`` plus its transport context.

    Construct with :meth:`open`. Always use as an async context manager OR
    call :meth:`aclose` explicitly.

    The session is **not** safe for concurrent ``open()`` / ``aclose()``
    calls; it IS safe for concurrent ``call_tool()`` invocations (the MCP
    SDK pipelines via JSON-RPC request IDs — design doc §4.5).
    """

    def __init__(
        self,
        transport: McpTransport,
        *,
        client_info_name: str = _DEFAULT_CLIENT_NAME,
        client_info_version: str | None = None,
        read_timeout_seconds: float = 120.0,
    ) -> None:
        self._transport = transport
        self._client_info = Implementation(
            name=client_info_name,
            version=client_info_version or _calfkit_version(),
        )
        self._read_timeout = read_timeout_seconds
        self._stack = AsyncExitStack()
        self._session: ClientSession | None = None

    # ----- lifecycle -----

    @classmethod
    async def open(
        cls,
        transport: McpTransport,
        *,
        client_info_name: str = _DEFAULT_CLIENT_NAME,
        client_info_version: str | None = None,
        read_timeout_seconds: float = 120.0,
    ) -> McpSession:
        """Factory: construct + open the transport + wrap the session.

        Does NOT send ``initialize`` — that is :meth:`initialize`. Splitting
        the two lets callers handle the handshake's failure mode separately
        (e.g. the bridge worker can log a specific "MCP initialize failed"
        error vs a transport-connect failure).
        """
        self = cls(
            transport,
            client_info_name=client_info_name,
            client_info_version=client_info_version,
            read_timeout_seconds=read_timeout_seconds,
        )
        await self._connect()
        return self

    async def __aenter__(self) -> McpSession:
        if self._session is None:
            await self._connect()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.aclose()

    async def _connect(self) -> None:
        """Open the underlying transport and wrap a ``ClientSession``.

        Idempotent against double-call: if already connected, returns
        without re-opening. The reverse — re-opening after aclose — is
        NOT supported (a session is one-shot).
        """
        if self._session is not None:
            return

        if isinstance(self._transport, StdioTransport):
            read, write = await self._stack.enter_async_context(stdio_client(self._build_stdio_params()))
        elif isinstance(self._transport, HttpTransport):
            # mcp 1.20+ streamablehttp_client returns 3-tuple
            # (read, write, get_session_id). We don't currently use the
            # session ID accessor; reserved for future observability.
            read, write, _get_session_id = await self._stack.enter_async_context(
                streamablehttp_client(
                    self._transport.url,
                    headers=self._transport.build_session_headers() or None,
                    timeout=timedelta(seconds=self._transport.timeout_seconds),
                    sse_read_timeout=timedelta(seconds=self._transport.sse_read_timeout_seconds),
                )
            )
        else:
            raise McpConfigError(f"unknown transport type: {type(self._transport).__name__}")

        self._session = await self._stack.enter_async_context(
            ClientSession(
                read,
                write,
                read_timeout_seconds=timedelta(seconds=self._read_timeout),
                client_info=self._client_info,
                # NB: not passing sampling_callback / elicitation_callback /
                # list_roots_callback — those capabilities are therefore
                # NOT advertised in our InitializeRequest. v1 deliberately
                # declines them. See design doc §3.2.
            )
        )

    def _build_stdio_params(self) -> StdioServerParameters:
        """Apply v1's full-passthrough env policy unless ``safe_env_only=True``."""
        assert isinstance(self._transport, StdioTransport)  # narrowed by caller
        t = self._transport
        if t.safe_env_only:
            # Defer to the MCP SDK's allowlist; only the user-supplied env
            # is added on top.
            merged_env = t.env
        else:
            # Full passthrough: every env var the calfkit process has, plus
            # whatever the user explicitly set (user wins on conflicts).
            merged_env = {**os.environ, **(t.env or {})}
        return StdioServerParameters(
            command=t.command,
            args=list(t.args),
            env=merged_env,
            cwd=t.cwd,
        )

    async def aclose(self) -> None:
        """Tear down the session + transport. Safe to call multiple times."""
        await self._stack.aclose()
        self._session = None

    # ----- protocol operations -----

    async def initialize(self) -> Any:
        """Send MCP ``initialize`` handshake. Returns the ``InitializeResult``.

        Must be called once before ``list_tools`` / ``call_tool`` can run.
        """
        self._require_open()
        assert self._session is not None
        return await self._session.initialize()

    async def list_tools(self) -> list[McpToolDef]:
        """Fetch the server's tool catalog and adapt to :class:`McpToolDef`.

        Pagination: aiokafka's MCP SDK transparently handles cursor-based
        pagination up to the SDK's internal limit. For v1 we treat the
        first page as the full catalog; v2 may add explicit cursor loops.
        """
        self._require_open()
        assert self._session is not None
        result = await self._session.list_tools()
        return [McpToolDef.from_mcp_tool(t) for t in result.tools]

    async def call_tool(
        self,
        name: str,
        args: dict[str, Any] | None = None,
        *,
        meta: dict[str, Any] | None = None,
        read_timeout_seconds: float | None = None,
    ) -> Any:
        """Invoke an MCP tool. Returns the SDK's ``CallToolResult`` unchanged.

        Adapt to calfkit types via :func:`calfkit.mcp._adapt.adapt_call_tool_result`
        (or :func:`build_retry_prompt_from_error_result` when ``isError`` is
        set) at the bridge boundary.
        """
        self._require_open()
        assert self._session is not None
        timeout = timedelta(seconds=read_timeout_seconds) if read_timeout_seconds is not None else None
        return await self._session.call_tool(
            name,
            args,
            read_timeout_seconds=timeout,
            meta=meta,
        )

    # ----- helpers -----

    def _require_open(self) -> None:
        if self._session is None:
            raise RuntimeError(
                "McpSession is not open. Use `async with McpSession.open(transport) as session:` "
                "or call `await session.aclose()` only after `await session.open(...)` completes."
            )

    @property
    def transport(self) -> McpTransport:
        return self._transport
