"""mcp.json configuration parsing.

Accepts the de facto ``mcp.json`` format used by Claude Desktop, Cursor,
Cline, Gemini CLI, and others. Two shapes are accepted:

1. Wrapped: ``{"mcpServers": {"<name>": {<spec>}}}``
2. Bare inner dict: ``{"<name>": {<spec>}}``

For each server, the spec is one of:

- stdio: ``{"command": "...", "args": [...], "env": {...}, "cwd": "..."}``
- HTTP:  ``{"type": "http", "url": "...", "headers": {...}}``

The parser returns a dict mapping server name → ``ParsedMcpServerSpec``
which is a normalised transport descriptor. ``McpServers.from_file`` /
``from_config`` (Phase 6) wraps the parser to construct ``McpServer``
instances with the user-supplied ``schemas={"<name>": <tools>}`` mapping.

Phase 1 only ships the parser + env expansion; the consumer that turns
``ParsedMcpServerSpec`` into ``McpServer`` lives in Phase 6 because the
``McpServer`` class lands in Phase 2.

Env substitution rules (``$VAR`` and ``${VAR}``):
- Applied to ``command``, every ``args[i]``, every ``env[k]``, ``cwd``,
  ``url``, every ``headers[k]``.
- An unset env var raises :class:`McpConfigError` at parse time (do not
  silently produce empty strings).
- Literal ``$$`` escapes to a single ``$`` (uncommon but supported).

See ``docs/mcp-v1-plan.md`` §2.2 for the user-facing API and
``docs/mcp-adaptor-implementation-plan.md`` §9 for the design rationale.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from calfkit.mcp.exceptions import McpConfigError

# ``$VAR`` and ``${VAR}`` patterns. The escape sequence ``$$`` is matched
# first (and replaced with a literal ``$``) so that ``$$VAR`` resolves to
# a literal ``$VAR`` rather than expanding.
_ENV_PATTERN = re.compile(r"\$\$|\$\{([A-Za-z_][A-Za-z0-9_]*)\}|\$([A-Za-z_][A-Za-z0-9_]*)")


# ---------------------------------------------------------------------------
# Parsed-spec types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ParsedStdioSpec:
    """Normalised stdio MCP server descriptor from mcp.json."""

    kind: Literal["stdio"] = field(default="stdio", init=False)
    command: str = ""
    args: tuple[str, ...] = ()
    env: dict[str, str] | None = None
    cwd: str | None = None


@dataclass(frozen=True)
class ParsedHttpSpec:
    """Normalised HTTP MCP server descriptor from mcp.json."""

    kind: Literal["http"] = field(default="http", init=False)
    url: str = ""
    headers: dict[str, str] | None = None


ParsedMcpServerSpec = ParsedStdioSpec | ParsedHttpSpec


# ---------------------------------------------------------------------------
# Env substitution
# ---------------------------------------------------------------------------


def expand_env(value: Any, *, where: str | None = None) -> Any:
    """Recursively substitute ``$VAR`` / ``${VAR}`` in strings.

    Applied to ``str``, ``dict``, ``list``, and ``tuple`` values; all other
    types pass through unchanged. Unset env vars raise ``McpConfigError``.

    ``where`` is a free-form context string (e.g. ``"McpServer.http(token=)"``)
    prefixed to the error message so multi-field configs are easy to debug.

    >>> expand_env("$HOME")               # doctest: +SKIP
    '/Users/ryan'
    >>> expand_env({"k": "${USER}"})       # doctest: +SKIP
    {'k': 'ryan'}
    >>> expand_env("price: $$5")           # literal $ via $$ escape
    'price: $5'
    """
    if isinstance(value, str):
        return _expand_str(value, where=where)
    if isinstance(value, dict):
        return {k: expand_env(v, where=where) for k, v in value.items()}
    if isinstance(value, list):
        return [expand_env(x, where=where) for x in value]
    if isinstance(value, tuple):
        return tuple(expand_env(x, where=where) for x in value)
    return value


def _expand_str(s: str, *, where: str | None = None) -> str:
    """Expand ``$VAR`` and ``${VAR}`` references in a single string."""

    def _sub(match: re.Match[str]) -> str:
        # $$ → literal $
        if match.group(0) == "$$":
            return "$"
        # ${VAR} → group(1); $VAR → group(2)
        var_name = match.group(1) or match.group(2)
        if var_name is None:
            # Shouldn't happen given the regex, but defensive.
            return match.group(0)
        if var_name not in os.environ:
            prefix = f"{where}: " if where else ""
            raise McpConfigError(f"{prefix}environment variable {var_name!r} is unset (referenced as ${var_name})")
        return os.environ[var_name]

    return _ENV_PATTERN.sub(_sub, s)


# ---------------------------------------------------------------------------
# mcp.json parser
# ---------------------------------------------------------------------------


def parse_mcp_config(config: dict[str, Any] | str | Path) -> dict[str, ParsedMcpServerSpec]:
    """Parse an mcp.json config into a mapping of server-name → spec.

    Accepts:
    - a ``dict`` already in memory (either wrapped or bare inner shape)
    - a ``str`` or ``Path`` pointing at an mcp.json file on disk

    Env substitution is applied to all string values inside the spec.
    Schema-validation errors raise ``McpConfigError`` with the offending
    server name in the message so multi-server configs are easy to debug.

    Returns:
        Dict of ``{server_name: ParsedMcpServerSpec}``.

    Raises:
        McpConfigError: malformed JSON, missing required fields, unset
            env var referenced in the spec, unknown transport type.
    """
    if isinstance(config, (str, Path)):
        config_dict = _load_config_file(config)
    else:
        config_dict = dict(config)

    # Accept both the wrapped {"mcpServers": {...}} and bare inner dict.
    if "mcpServers" in config_dict and isinstance(config_dict["mcpServers"], dict):
        servers_dict = config_dict["mcpServers"]
    else:
        servers_dict = config_dict

    if not isinstance(servers_dict, dict):
        raise McpConfigError(f"mcp.json: top-level value must be an object mapping server names to specs; got {type(servers_dict).__name__}")

    # Expand per-server so unset-var errors name the offending server.
    out: dict[str, ParsedMcpServerSpec] = {}
    for name, raw_spec in servers_dict.items():
        if not isinstance(name, str) or not name:
            raise McpConfigError(f"mcp.json: server name must be a non-empty string; got {name!r}")
        if not isinstance(raw_spec, dict):
            raise McpConfigError(f"mcp.json: spec for server {name!r} must be an object; got {type(raw_spec).__name__}")
        expanded_spec = expand_env(raw_spec, where=f"mcp.json[{name!r}]")
        out[name] = _parse_server_spec(name, expanded_spec)
    return out


def _load_config_file(path: str | Path) -> dict[str, Any]:
    """Read and JSON-parse a config file with a clear error on failure."""
    path = Path(path)
    if not path.exists():
        raise McpConfigError(f"mcp.json: file not found: {path}")
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as e:
        raise McpConfigError(f"mcp.json: cannot read {path}: {e}") from e
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as e:
        raise McpConfigError(f"mcp.json: invalid JSON at {path}: {e}") from e
    if not isinstance(parsed, dict):
        raise McpConfigError(f"mcp.json: top-level value at {path} must be an object; got {type(parsed).__name__}")
    return parsed


def _parse_server_spec(name: str, raw: dict[str, Any]) -> ParsedMcpServerSpec:
    """Parse one server's spec into a ``ParsedMcpServerSpec``.

    Transport detection rule (matches Claude Desktop / Cursor convention):
    - If ``type == "http"`` (or ``type == "sse"``) → HTTP transport.
    - Else if ``url`` is present → HTTP transport (inferred).
    - Else if ``command`` is present → stdio transport.
    - Otherwise → error.
    """
    transport_type = raw.get("type")
    has_url = "url" in raw
    has_command = "command" in raw

    if transport_type == "http" or transport_type == "sse" or (transport_type is None and has_url):
        # SSE is the legacy MCP transport; we treat it as HTTP for the parser
        # here. The runtime will use streamablehttp_client either way.
        return _parse_http_spec(name, raw)
    if transport_type == "stdio" or (transport_type is None and has_command):
        return _parse_stdio_spec(name, raw)

    raise McpConfigError(
        f"mcp.json: server {name!r} has unknown transport. Provide either 'command' (stdio) or 'url' (http); got keys={sorted(raw.keys())}"
    )


def _parse_stdio_spec(name: str, raw: dict[str, Any]) -> ParsedStdioSpec:
    command = raw.get("command")
    if not isinstance(command, str) or not command:
        raise McpConfigError(f"mcp.json: stdio server {name!r} requires a non-empty 'command' string; got {command!r}")

    raw_args = raw.get("args", [])
    if not isinstance(raw_args, list) or not all(isinstance(a, str) for a in raw_args):
        raise McpConfigError(f"mcp.json: stdio server {name!r} 'args' must be a list of strings; got {raw_args!r}")
    args = tuple(raw_args)

    raw_env = raw.get("env")
    env: dict[str, str] | None = None
    if raw_env is not None:
        if not isinstance(raw_env, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in raw_env.items()):
            raise McpConfigError(f"mcp.json: stdio server {name!r} 'env' must be a {{string: string}} object")
        env = dict(raw_env)

    cwd = raw.get("cwd")
    if cwd is not None and not isinstance(cwd, str):
        raise McpConfigError(f"mcp.json: stdio server {name!r} 'cwd' must be a string; got {type(cwd).__name__}")

    return ParsedStdioSpec(command=command, args=args, env=env, cwd=cwd)


def _parse_http_spec(name: str, raw: dict[str, Any]) -> ParsedHttpSpec:
    url = raw.get("url")
    if not isinstance(url, str) or not url:
        raise McpConfigError(f"mcp.json: http server {name!r} requires a non-empty 'url' string; got {url!r}")

    raw_headers = raw.get("headers")
    headers: dict[str, str] | None = None
    if raw_headers is not None:
        if not isinstance(raw_headers, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in raw_headers.items()):
            raise McpConfigError(f"mcp.json: http server {name!r} 'headers' must be a {{string: string}} object")
        headers = dict(raw_headers)

    return ParsedHttpSpec(url=url, headers=headers)
