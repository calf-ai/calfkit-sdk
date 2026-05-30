"""Unit tests for ``calfkit.mcp._config``.

Coverage focus:
- ``expand_env`` handles ``$VAR``, ``${VAR}``, the ``$$`` escape, nested
  dicts/lists/tuples, and raises clearly on unset vars.
- ``parse_mcp_config`` accepts both wrapped and bare-inner-dict shapes.
- Transport detection: explicit ``type`` wins; ``url`` infers HTTP;
  ``command`` infers stdio; missing → clear error.
- All shape-validation paths raise ``McpConfigError`` with the server
  name in the message (so multi-server configs are easy to debug).
- Reading from a file path round-trips via JSON.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from calfkit.mcp._config import (
    ParsedHttpSpec,
    ParsedStdioSpec,
    expand_env,
    parse_mcp_config,
)
from calfkit.mcp.exceptions import McpConfigError

# ---------------------------------------------------------------------------
# expand_env
# ---------------------------------------------------------------------------


def test_expand_env_simple(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MY_TOKEN", "abc123")
    assert expand_env("$MY_TOKEN") == "abc123"
    assert expand_env("${MY_TOKEN}") == "abc123"


def test_expand_env_in_braces_with_surrounding_text(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HOST", "example.com")
    assert expand_env("https://${HOST}/api") == "https://example.com/api"


def test_expand_env_dollar_escape() -> None:
    """``$$`` becomes a literal ``$`` and does not trigger substitution."""
    assert expand_env("price: $$5.00") == "price: $5.00"
    # $$VAR is the literal $ followed by VAR, not a substitution
    assert expand_env("$$NOT_A_VAR") == "$NOT_A_VAR"


def test_expand_env_recursive_dict(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TOK", "tok-value")
    out = expand_env({"headers": {"Authorization": "Bearer $TOK"}})
    assert out == {"headers": {"Authorization": "Bearer tok-value"}}


def test_expand_env_recursive_list(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PORT", "8080")
    out = expand_env(["--port", "$PORT", "--debug"])
    assert out == ["--port", "8080", "--debug"]


def test_expand_env_recursive_tuple(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("X", "x-val")
    out = expand_env(("$X",))
    assert out == ("x-val",)


def test_expand_env_non_string_passthrough() -> None:
    """Non-string values pass through unchanged."""
    assert expand_env(42) == 42
    assert expand_env(None) is None
    assert expand_env(True) is True


def test_expand_env_unset_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("THIS_VAR_IS_UNSET", raising=False)
    with pytest.raises(McpConfigError, match="THIS_VAR_IS_UNSET"):
        expand_env("$THIS_VAR_IS_UNSET")


def test_expand_env_unset_in_braces_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("UNSET_BRACED", raising=False)
    with pytest.raises(McpConfigError, match="UNSET_BRACED"):
        expand_env("${UNSET_BRACED}")


# ---------------------------------------------------------------------------
# parse_mcp_config — shape acceptance
# ---------------------------------------------------------------------------


def test_parse_wrapped_mcp_servers_shape() -> None:
    config = {"mcpServers": {"gmail": {"command": "npx", "args": ["-y", "@mcp/server-gmail"]}}}
    parsed = parse_mcp_config(config)
    assert set(parsed.keys()) == {"gmail"}
    assert isinstance(parsed["gmail"], ParsedStdioSpec)


def test_parse_bare_inner_dict_shape() -> None:
    """No wrapping {"mcpServers": ...} is also accepted."""
    config = {"gmail": {"command": "npx"}}
    parsed = parse_mcp_config(config)
    assert "gmail" in parsed


# ---------------------------------------------------------------------------
# parse_mcp_config — transport detection
# ---------------------------------------------------------------------------


def test_parse_stdio_explicit_type() -> None:
    parsed = parse_mcp_config({"s": {"type": "stdio", "command": "/bin/echo"}})
    assert isinstance(parsed["s"], ParsedStdioSpec)
    assert parsed["s"].command == "/bin/echo"


def test_parse_stdio_inferred_from_command() -> None:
    parsed = parse_mcp_config({"s": {"command": "/bin/echo"}})
    assert isinstance(parsed["s"], ParsedStdioSpec)


def test_parse_http_explicit_type() -> None:
    parsed = parse_mcp_config({"s": {"type": "http", "url": "https://example.com/mcp"}})
    assert isinstance(parsed["s"], ParsedHttpSpec)
    assert parsed["s"].url == "https://example.com/mcp"


def test_parse_http_inferred_from_url() -> None:
    parsed = parse_mcp_config({"s": {"url": "https://example.com/mcp"}})
    assert isinstance(parsed["s"], ParsedHttpSpec)


def test_parse_sse_treated_as_http() -> None:
    """The legacy 'sse' transport is parsed as HTTP (we run streamable_http over both)."""
    parsed = parse_mcp_config({"s": {"type": "sse", "url": "https://example.com/sse"}})
    assert isinstance(parsed["s"], ParsedHttpSpec)
    assert parsed["s"].url == "https://example.com/sse"


def test_parse_missing_transport_raises() -> None:
    with pytest.raises(McpConfigError, match="unknown transport"):
        parse_mcp_config({"s": {"description": "nope"}})


# ---------------------------------------------------------------------------
# parse_mcp_config — stdio shape validation
# ---------------------------------------------------------------------------


def test_parse_stdio_missing_command() -> None:
    with pytest.raises(McpConfigError, match="'command'"):
        parse_mcp_config({"s": {"type": "stdio"}})


def test_parse_stdio_command_must_be_non_empty() -> None:
    with pytest.raises(McpConfigError, match="'command'"):
        parse_mcp_config({"s": {"command": ""}})


def test_parse_stdio_args_must_be_list_of_strings() -> None:
    with pytest.raises(McpConfigError, match="'args'"):
        parse_mcp_config({"s": {"command": "x", "args": [1, 2, 3]}})


def test_parse_stdio_env_must_be_string_dict() -> None:
    with pytest.raises(McpConfigError, match="'env'"):
        parse_mcp_config({"s": {"command": "x", "env": {"k": 123}}})


def test_parse_stdio_cwd_must_be_string() -> None:
    with pytest.raises(McpConfigError, match="'cwd'"):
        parse_mcp_config({"s": {"command": "x", "cwd": 42}})


def test_parse_stdio_all_optional_fields() -> None:
    """Full stdio spec with all optional fields parses cleanly."""
    parsed = parse_mcp_config({"gmail": {"command": "npx", "args": ["-y", "x"], "env": {"K": "V"}, "cwd": "/tmp"}})
    spec = parsed["gmail"]
    assert isinstance(spec, ParsedStdioSpec)
    assert spec.command == "npx"
    assert spec.args == ("-y", "x")
    assert spec.env == {"K": "V"}
    assert spec.cwd == "/tmp"


# ---------------------------------------------------------------------------
# parse_mcp_config — http shape validation
# ---------------------------------------------------------------------------


def test_parse_http_missing_url() -> None:
    with pytest.raises(McpConfigError, match="'url'"):
        parse_mcp_config({"s": {"type": "http"}})


def test_parse_http_headers_must_be_string_dict() -> None:
    with pytest.raises(McpConfigError, match="'headers'"):
        parse_mcp_config({"s": {"url": "https://x", "headers": {"X-Bad": 42}}})


def test_parse_http_all_optional_fields() -> None:
    parsed = parse_mcp_config({"s": {"type": "http", "url": "https://x.com/mcp", "headers": {"Authorization": "Bearer x"}}})
    spec = parsed["s"]
    assert isinstance(spec, ParsedHttpSpec)
    assert spec.url == "https://x.com/mcp"
    assert spec.headers == {"Authorization": "Bearer x"}


# ---------------------------------------------------------------------------
# parse_mcp_config — server-name validation
# ---------------------------------------------------------------------------


def test_parse_empty_server_name_raises() -> None:
    with pytest.raises(McpConfigError, match="non-empty string"):
        parse_mcp_config({"": {"command": "x"}})


def test_parse_non_dict_spec_raises() -> None:
    with pytest.raises(McpConfigError, match="spec for server 's'"):
        parse_mcp_config({"s": "not a dict"})  # type: ignore[dict-item]


def test_parse_non_dict_top_level_raises() -> None:
    with pytest.raises(McpConfigError, match="must be an object"):
        parse_mcp_config({"mcpServers": "not a dict"})  # type: ignore[dict-item]


# ---------------------------------------------------------------------------
# parse_mcp_config — env substitution integration
# ---------------------------------------------------------------------------


def test_parse_applies_env_substitution(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GMAIL_OAUTH", "secret-value")
    parsed = parse_mcp_config({"gmail": {"command": "npx", "env": {"OAUTH_TOKEN": "$GMAIL_OAUTH"}}})
    spec = parsed["gmail"]
    assert isinstance(spec, ParsedStdioSpec)
    assert spec.env == {"OAUTH_TOKEN": "secret-value"}


def test_parse_applies_env_substitution_to_url_and_headers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HOST", "api.example.com")
    monkeypatch.setenv("TOK", "tok-x")
    parsed = parse_mcp_config(
        {
            "s": {
                "type": "http",
                "url": "https://${HOST}/mcp",
                "headers": {"Authorization": "Bearer $TOK"},
            }
        }
    )
    spec = parsed["s"]
    assert isinstance(spec, ParsedHttpSpec)
    assert spec.url == "https://api.example.com/mcp"
    assert spec.headers == {"Authorization": "Bearer tok-x"}


def test_parse_unset_env_raises_at_parse_time(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MISSING", raising=False)
    with pytest.raises(McpConfigError, match="MISSING"):
        parse_mcp_config({"s": {"command": "echo", "env": {"K": "$MISSING"}}})


# ---------------------------------------------------------------------------
# parse_mcp_config — file loading
# ---------------------------------------------------------------------------


def test_parse_from_file(tmp_path: Path) -> None:
    config_path = tmp_path / "mcp.json"
    config_path.write_text(
        json.dumps(
            {
                "mcpServers": {
                    "gmail": {"command": "npx", "args": ["-y", "@mcp/server-gmail"]},
                    "github": {"type": "http", "url": "https://api.github.com/mcp"},
                }
            }
        )
    )
    parsed = parse_mcp_config(config_path)
    assert set(parsed.keys()) == {"gmail", "github"}
    gmail = parsed["gmail"]
    github = parsed["github"]
    assert isinstance(gmail, ParsedStdioSpec)
    assert isinstance(github, ParsedHttpSpec)
    assert gmail.command == "npx"
    assert github.url == "https://api.github.com/mcp"


def test_parse_from_file_missing_path() -> None:
    with pytest.raises(McpConfigError, match="not found"):
        parse_mcp_config("/no/such/path.json")


def test_parse_from_file_invalid_json(tmp_path: Path) -> None:
    config_path = tmp_path / "bad.json"
    config_path.write_text("{not valid json")
    with pytest.raises(McpConfigError, match="invalid JSON"):
        parse_mcp_config(config_path)


def test_parse_from_file_non_object_top_level(tmp_path: Path) -> None:
    config_path = tmp_path / "list.json"
    config_path.write_text("[1, 2, 3]")
    with pytest.raises(McpConfigError, match="must be an object"):
        parse_mcp_config(config_path)


# ---------------------------------------------------------------------------
# parse_mcp_config — frozen output
# ---------------------------------------------------------------------------


def test_parsed_specs_are_immutable() -> None:
    """ParsedStdioSpec and ParsedHttpSpec are frozen dataclasses."""
    parsed = parse_mcp_config({"s": {"command": "x"}})
    stdio = parsed["s"]
    with pytest.raises(Exception):  # FrozenInstanceError
        stdio.command = "y"  # type: ignore[misc]
