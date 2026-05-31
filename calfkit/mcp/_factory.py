"""Module-level ``mcp`` factory + ``McpServers`` mcp.json interop.

The ``mcp`` callable is the README-leading convenience entry point — it
auto-detects transport from its first argument (URL → HTTP, command →
stdio) and dispatches to the appropriate :class:`McpServer` classmethod.

``McpServers.from_file`` / ``from_config`` consume the de facto
``mcp.json`` format used by Claude Desktop / Cursor / Cline. The schemas
are NOT in the config — they're supplied separately via the ``schemas=``
kwarg, typically as ``{"<name>": <codegen-generated-module>.ALL}``.

This module avoids importing typer / SDK details — it's pure user-facing
glue layered on top of ``McpServer`` and the ``_config.py`` parser.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

from calfkit.mcp._config import (
    ParsedHttpSpec,
    ParsedMcpServerSpec,
    ParsedStdioSpec,
    parse_mcp_config,
)
from calfkit.mcp._server import McpServer
from calfkit.mcp.exceptions import McpConfigError

if TYPE_CHECKING:
    from calfkit.mcp._tool_def import McpToolDef


class _McpFactory:
    """Callable + namespace for constructing :class:`McpServer` instances.

    Three call forms:

    - ``mcp(cmd_or_url, *, tools=...)`` — auto-detect transport
    - ``mcp.stdio(command, *args, tools=...)`` — explicit stdio
    - ``mcp.http(url, *, tools=..., token=...)`` — explicit HTTP

    Auto-detection rule:
    - First arg starts with ``http://`` / ``https://`` → HTTP
    - Otherwise → stdio (the string is shell-split on whitespace)

    Examples::

        from calfkit import mcp
        from gmail_schemas import Gmail

        # Auto-detect from command
        gmail = mcp("npx -y @mcp/server-gmail", tools=Gmail.ALL)

        # Auto-detect from URL
        github = mcp("https://api.github.com/mcp", tools=Github.ALL, token="$GH")

        # Explicit (when auto-detect could be ambiguous)
        gmail = mcp.stdio("npx", "-y", "@mcp/server-gmail", tools=Gmail.ALL)
    """

    def __call__(
        self,
        cmd_or_url: str,
        *,
        tools: list[McpToolDef],
        name: str | None = None,
        **kwargs: Any,
    ) -> McpServer:
        if cmd_or_url.startswith("http://") or cmd_or_url.startswith("https://"):
            return self.http(cmd_or_url, tools=tools, name=name, **kwargs)
        # Treat as stdio command line; shlex split for shell-style quoting
        import shlex

        parts = shlex.split(cmd_or_url)
        if not parts:
            raise McpConfigError(f"mcp({cmd_or_url!r}): command is empty after shell-splitting")
        return self.stdio(parts[0], *parts[1:], tools=tools, name=name, **kwargs)

    def stdio(
        self,
        command: str,
        *args: str,
        tools: list[McpToolDef],
        name: str | None = None,
        **kwargs: Any,
    ) -> McpServer:
        """Construct an ``McpServer`` with the stdio transport.

        Delegation to :meth:`McpServer.stdio`. See that method's docstring
        for the full kwargs list.
        """
        return McpServer.stdio(command, *args, tools=tools, name=name, **kwargs)

    def http(
        self,
        url: str,
        *,
        tools: list[McpToolDef],
        name: str | None = None,
        **kwargs: Any,
    ) -> McpServer:
        """Construct an ``McpServer`` with the Streamable HTTP transport.

        Delegation to :meth:`McpServer.http`.
        """
        return McpServer.http(url, tools=tools, name=name, **kwargs)

    def __getattr__(self, name: str) -> Any:
        """Delegate unknown attribute lookups to the ``calfkit.mcp`` submodule.

        ``from calfkit import mcp`` rebinds the ``calfkit.mcp`` attribute on
        the parent package from the submodule object to this factory instance,
        which otherwise breaks ``calfkit.mcp.McpServer`` / ``McpServers`` /
        etc. attribute access. Resolving via ``sys.modules`` recovers the
        real submodule (the import system keeps that binding intact) so both
        idioms keep working.
        """
        if name.startswith("_"):
            raise AttributeError(name)
        import sys

        mod = sys.modules.get("calfkit.mcp")
        # ``mod`` is either the real submodule (the canonical case) or
        # ``None`` during partial-init imports. The defensive ``is self``
        # check covers a pathological case where ``calfkit.mcp`` ends up
        # bound to this factory instance in sys.modules — unreachable in
        # practice but cheap insurance against an infinite-recursion bug.
        if mod is None or mod is self:  # type: ignore[comparison-overlap]
            raise AttributeError(name)
        try:
            return getattr(mod, name)
        except AttributeError:
            raise AttributeError(f"module 'calfkit.mcp' has no attribute {name!r}") from None


mcp = _McpFactory()
"""The README-leading factory. ``mcp("npx ...", tools=...)`` is the
canonical compact form. ``mcp.stdio(...)`` / ``mcp.http(...)`` are the
explicit forms when transport inference would be ambiguous.
"""


# ---------------------------------------------------------------------------
# McpServers — mcp.json interop
# ---------------------------------------------------------------------------


class McpServers(Mapping[str, McpServer]):
    """Dict-like collection of :class:`McpServer` instances loaded from
    an ``mcp.json``-compatible config.

    Schemas are NOT part of the config (they're typically large and
    deserve to live in a generated Python module). Pass them via the
    ``schemas=`` kwarg as ``{<server-name>: <list of McpToolDef>}``.

    Example::

        from calfkit.mcp import McpServers
        from gmail_schemas import Gmail
        from github_schemas import Github

        servers = McpServers.from_file("./mcp.json", schemas={
            "gmail": Gmail.ALL,
            "github": Github.ALL,
        })

        agent  = Agent("scribe", tools=[*servers.values()], ...)
        worker = Worker(client, nodes=[*servers.values(), agent])

    Each server name in the config MUST have a matching entry in
    ``schemas``. Unmatched names raise :class:`McpConfigError` at
    construction time so misconfiguration fails loudly at startup
    rather than at first traffic.
    """

    def __init__(self, servers: dict[str, McpServer]) -> None:
        self._servers = dict(servers)

    @classmethod
    def from_config(
        cls,
        config: dict[str, Any] | str | Path,
        *,
        schemas: dict[str, list[McpToolDef]],
    ) -> McpServers:
        """Parse an in-memory mcp.json dict (or file path) into McpServer instances.

        Raises:
            McpConfigError: parse failures, missing required fields, or a
                server in ``config`` without a matching ``schemas`` entry.
        """
        parsed = parse_mcp_config(config)
        return cls._build_from_parsed(parsed, schemas)

    @classmethod
    def from_file(
        cls,
        path: str | Path,
        *,
        schemas: dict[str, list[McpToolDef]],
    ) -> McpServers:
        """Load an mcp.json file from disk and parse into McpServer instances.

        Thin wrapper around :meth:`from_config` for the most common usage.
        """
        return cls.from_config(path, schemas=schemas)

    @classmethod
    def _build_from_parsed(
        cls,
        parsed: dict[str, ParsedMcpServerSpec],
        schemas: dict[str, list[McpToolDef]],
    ) -> McpServers:
        # Verify every server in the config has matching schemas.
        missing_schemas = set(parsed.keys()) - set(schemas.keys())
        if missing_schemas:
            raise McpConfigError(
                f"mcp.json: server(s) in config without matching schemas: {sorted(missing_schemas)}. "
                f"Supply via from_file(..., schemas={{'name': <list>, ...}}); "
                f"typically McpServers.from_file('./mcp.json', schemas={{'gmail': Gmail.ALL}})."
            )

        unused_schemas = set(schemas.keys()) - set(parsed.keys())
        if unused_schemas:
            # Not an error — user may pass schemas for servers not in this
            # config (e.g. a shared schemas dict across multiple configs).
            # Just log a warning at construction time.
            import logging

            logging.getLogger(__name__).warning(
                "McpServers: schemas supplied for servers not in mcp.json: %s",
                sorted(unused_schemas),
            )

        servers: dict[str, McpServer] = {}
        for name, spec in parsed.items():
            servers[name] = _build_server_from_spec(name, spec, schemas[name])
        return cls(servers)

    # ----- Mapping protocol -----

    def __getitem__(self, key: str) -> McpServer:
        return self._servers[key]

    def __iter__(self) -> Any:
        return iter(self._servers)

    def __len__(self) -> int:
        return len(self._servers)

    def __contains__(self, key: object) -> bool:
        return key in self._servers


def _build_server_from_spec(
    name: str,
    spec: ParsedMcpServerSpec,
    tools: list[McpToolDef],
) -> McpServer:
    """Convert a ``ParsedMcpServerSpec`` + tool schemas into an :class:`McpServer`."""
    if isinstance(spec, ParsedStdioSpec):
        return McpServer.stdio(
            spec.command,
            *spec.args,
            tools=tools,
            name=name,
            env=spec.env,
            cwd=spec.cwd,
        )
    if isinstance(spec, ParsedHttpSpec):
        return McpServer.http(
            spec.url,
            tools=tools,
            name=name,
            headers=spec.headers,
        )
    raise McpConfigError(f"mcp.json: server {name!r} has unknown spec type {type(spec).__name__}")
