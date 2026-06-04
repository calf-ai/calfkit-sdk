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
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, NoReturn

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, ValidationError

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
# Input models (validation + schema source of truth)
# ---------------------------------------------------------------------------
#
# These mirror the *input* shape of an mcp.json server spec (as opposed to
# the normalised ``Parsed*`` output above). They drive both per-server
# validation in ``parse_mcp_config`` and the reference schema emitted by
# ``mcp_json_schema``, so the schema can never drift from the parser.


class StdioServerConfig(BaseModel):
    """Input model for a stdio MCP server spec (validation + schema source)."""

    model_config = ConfigDict(extra="ignore")  # drop-in leniency; also suppresses additionalProperties
    # ``type`` is decorative for validation: transport is resolved before we
    # validate (see ``_validate_server``), so this field never drives the
    # stdio-vs-http choice. It exists ONLY to document/autocomplete the key.
    type: Literal["stdio"] | None = Field(default=None, description="Optional; inferred from `command` when omitted.")
    command: str = Field(min_length=1, description="Executable to spawn. Supports $VAR/${VAR} expansion.")
    args: list[str] = Field(default_factory=list, description="CLI args; each supports $VAR.")
    env: dict[str, str] | None = Field(default=None, description="Subprocess env overlay; values support $VAR.")
    cwd: str | None = Field(default=None, description="Subprocess working directory.")


class HttpServerConfig(BaseModel):
    """Input model for an HTTP MCP server spec (validation + schema source)."""

    model_config = ConfigDict(extra="ignore")  # drop-in leniency; also suppresses additionalProperties
    # ``type`` is decorative for validation: transport is resolved before we
    # validate (see ``_validate_server``), so this field never drives the
    # stdio-vs-http choice. It exists ONLY to document/autocomplete the key.
    type: Literal["http", "sse"] | None = Field(default=None, description="`http` (or legacy `sse`); inferred from `url`.")
    url: str = Field(min_length=1, description="Streamable-HTTP endpoint. Supports $VAR.")
    headers: dict[str, str] | None = Field(default=None, description="Request headers; values support $VAR.")


_SERVERS = TypeAdapter(dict[str, StdioServerConfig | HttpServerConfig])


def mcp_json_schema() -> dict[str, Any]:
    """Reference JSON Schema (draft 2020-12) for a calfkit ``mcp.json``.

    Permissive: unknown keys are allowed (drop-in leniency), so the schema
    carries no ``additionalProperties: false``. It is a documentation/editor
    aid only and is NOT used to validate at runtime — the parser remains the
    sole runtime validator.
    """
    servers_map = _SERVERS.json_schema(ref_template="#/$defs/{model}")
    defs = servers_map.pop("$defs")
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "calfkit mcp.json",
        "description": "Reference schema for calfkit McpServers.from_file(...). Unknown keys are ignored.",
        "type": "object",
        "required": ["mcpServers"],
        "properties": {"mcpServers": servers_map},
        "$defs": defs,
    }


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
        out[name] = _to_parsed(_validate_server(name, expanded_spec))
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


def _validate_server(name: str, raw: dict[str, Any]) -> StdioServerConfig | HttpServerConfig:
    """Resolve transport, then validate the spec via the matching input model.

    Transport detection rule (matches Claude Desktop / Cursor convention):
    - If ``type == "http"`` (or legacy ``type == "sse"``) → HTTP transport.
    - Else if ``url`` is present → HTTP transport (inferred).
    - Else if ``type == "stdio"`` or ``command`` is present → stdio transport.
    - Otherwise → ``unknown transport`` error.

    ``url`` takes precedence over ``command`` on ambiguous specs (url-first).
    Resolving transport *before* validating keeps the friendly
    ``unknown transport`` message out of Pydantic and preserves the
    ``loc[0]`` substring contract relied on by ``test_config.py``.
    """
    t, has_url, has_command = raw.get("type"), "url" in raw, "command" in raw
    model: type[StdioServerConfig] | type[HttpServerConfig]
    if t in ("http", "sse") or (t is None and has_url):
        model = HttpServerConfig
    elif t == "stdio" or (t is None and has_command):
        model = StdioServerConfig
    else:
        raise McpConfigError(
            f"mcp.json: server {name!r} has unknown transport. Provide either 'command' (stdio) or 'url' (http); got keys={sorted(raw.keys())}"
        )
    try:
        return model.model_validate(raw)
    except ValidationError as exc:
        _raise_from_validation(name, exc)


def _raise_from_validation(name: str, exc: ValidationError) -> NoReturn:
    """Translate a Pydantic ``ValidationError`` into an ``McpConfigError``.

    Keys on ``loc[0]`` (the offending field name) so the rendered ``{field!r}``
    reproduces the ``'command'`` / ``'args'`` / ``'env'`` / ``'cwd'`` / ``'url'``
    / ``'headers'`` substrings matched by the regression tests.
    """
    err = exc.errors()[0]
    field = err["loc"][0] if err.get("loc") else "<unknown>"
    raise McpConfigError(f"mcp.json: server {name!r}: invalid {field!r} — {err['msg']} (got {err['input']!r})") from exc


def _to_parsed(cfg: StdioServerConfig | HttpServerConfig) -> ParsedMcpServerSpec:
    """Normalise a validated input model into the existing ``Parsed*`` output.

    Hand-maintained field copy (the type system does not enforce input↔output
    congruence — the congruence tests in ``test_config.py`` are the only
    guard). Keep this adjacent to both model definitions.
    """
    if isinstance(cfg, StdioServerConfig):
        return ParsedStdioSpec(command=cfg.command, args=tuple(cfg.args), env=cfg.env, cwd=cfg.cwd)
    return ParsedHttpSpec(url=cfg.url, headers=cfg.headers)
