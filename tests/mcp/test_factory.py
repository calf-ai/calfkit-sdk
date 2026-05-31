"""Tests for ``calfkit.mcp._factory`` — the ``mcp`` factory + ``McpServers``."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from calfkit import mcp as top_level_mcp
from calfkit.mcp import McpServer, McpServers
from calfkit.mcp import mcp as mcp_factory
from calfkit.mcp._factory import _build_server_from_spec
from calfkit.mcp._session import HttpTransport, StdioTransport
from calfkit.mcp._tool_def import McpToolDef
from calfkit.mcp.exceptions import McpConfigError


def _td(name: str = "t") -> McpToolDef:
    return McpToolDef(name=name, input_schema={"type": "object"})


# ---------------------------------------------------------------------------
# mcp factory — auto-detect
# ---------------------------------------------------------------------------


def test_mcp_factory_is_re_exported_at_top_level() -> None:
    """``from calfkit import mcp`` reaches the same object as ``from calfkit.mcp import mcp``."""
    assert top_level_mcp is mcp_factory


def test_mcp_call_auto_detects_https_url() -> None:
    server = mcp_factory("https://api.example.com/mcp", tools=[_td()])
    assert isinstance(server.transport, HttpTransport)
    assert server.transport.url == "https://api.example.com/mcp"


def test_mcp_call_auto_detects_http_url() -> None:
    server = mcp_factory("http://localhost:8000/mcp", tools=[_td()])
    assert isinstance(server.transport, HttpTransport)


def test_mcp_call_auto_detects_stdio_command() -> None:
    server = mcp_factory("npx -y @mcp/server-x", tools=[_td()])
    assert isinstance(server.transport, StdioTransport)
    assert server.transport.command == "npx"
    assert server.transport.args == ("-y", "@mcp/server-x")


def test_mcp_call_single_word_command() -> None:
    server = mcp_factory("gmail-mcp-server", tools=[_td()])
    assert isinstance(server.transport, StdioTransport)
    assert server.transport.command == "gmail-mcp-server"
    assert server.transport.args == ()


def test_mcp_call_empty_string_raises() -> None:
    with pytest.raises(McpConfigError, match="empty"):
        mcp_factory("", tools=[_td()])


def test_mcp_call_whitespace_only_raises() -> None:
    with pytest.raises(McpConfigError, match="empty"):
        mcp_factory("   ", tools=[_td()])


def test_mcp_call_threads_tools_through() -> None:
    tools = [_td("a"), _td("b")]
    server = mcp_factory("npx x", tools=tools)
    assert server.tools == tuple(tools)


def test_mcp_call_threads_name_kwarg() -> None:
    server = mcp_factory("npx x", tools=[_td()], name="custom")
    assert server.raw_name == "custom"


def test_mcp_call_threads_other_kwargs_to_stdio() -> None:
    """env / cwd kwargs flow through to StdioTransport via .stdio()."""
    server = mcp_factory("npx x", tools=[_td()], env={"K": "V"}, cwd="/tmp")
    assert isinstance(server.transport, StdioTransport)
    assert server.transport.env == {"K": "V"}
    assert server.transport.cwd == "/tmp"


def test_mcp_call_threads_other_kwargs_to_http() -> None:
    """token / headers kwargs flow through to HttpTransport via .http()."""
    server = mcp_factory("https://x.com/mcp", tools=[_td()], token="tok", headers={"X-K": "V"})
    assert isinstance(server.transport, HttpTransport)
    assert server.transport.token == "tok"
    assert server.transport.headers == {"X-K": "V"}


# ---------------------------------------------------------------------------
# mcp.stdio / mcp.http explicit forms
# ---------------------------------------------------------------------------


def test_mcp_stdio_explicit_form() -> None:
    server = mcp_factory.stdio("python", "-m", "my_mcp", tools=[_td()])
    assert isinstance(server.transport, StdioTransport)
    assert server.transport.command == "python"
    assert server.transport.args == ("-m", "my_mcp")


def test_mcp_http_explicit_form() -> None:
    server = mcp_factory.http("https://api.acme.com/mcp", tools=[_td()])
    assert isinstance(server.transport, HttpTransport)
    assert server.transport.url == "https://api.acme.com/mcp"


# ---------------------------------------------------------------------------
# McpServers — direct construction + from_config / from_file
# ---------------------------------------------------------------------------


def test_mcp_servers_mapping_protocol() -> None:
    fake_servers = {
        "a": mcp_factory.stdio("x", tools=[_td()], name="a"),
        "b": mcp_factory.stdio("y", tools=[_td()], name="b"),
    }
    servers = McpServers(fake_servers)
    assert len(servers) == 2
    assert set(servers.keys()) == {"a", "b"}
    assert isinstance(servers["a"], McpServer)
    assert "a" in servers
    assert "missing" not in servers


def test_mcp_servers_from_config_stdio_only() -> None:
    config = {"gmail": {"command": "npx", "args": ["-y", "@mcp/server-gmail"]}}
    servers = McpServers.from_config(config, schemas={"gmail": [_td("search")]})
    assert "gmail" in servers
    gmail = servers["gmail"]
    assert isinstance(gmail.transport, StdioTransport)
    assert gmail.transport.command == "npx"
    assert gmail.tools == (_td("search"),)


def test_mcp_servers_from_config_http_only() -> None:
    config = {"github": {"type": "http", "url": "https://api.github.com/mcp"}}
    servers = McpServers.from_config(config, schemas={"github": [_td()]})
    assert isinstance(servers["github"].transport, HttpTransport)


def test_mcp_servers_from_config_mixed() -> None:
    config = {
        "gmail": {"command": "npx"},
        "github": {"type": "http", "url": "https://x.com/mcp"},
    }
    servers = McpServers.from_config(
        config,
        schemas={"gmail": [_td("g")], "github": [_td("h")]},
    )
    assert isinstance(servers["gmail"].transport, StdioTransport)
    assert isinstance(servers["github"].transport, HttpTransport)


def test_mcp_servers_from_config_wrapped_envelope() -> None:
    """The canonical {"mcpServers": {...}} envelope is accepted."""
    config = {"mcpServers": {"x": {"command": "echo"}}}
    servers = McpServers.from_config(config, schemas={"x": [_td()]})
    assert "x" in servers


def test_mcp_servers_missing_schema_raises() -> None:
    config = {"gmail": {"command": "npx"}, "github": {"command": "npx"}}
    with pytest.raises(McpConfigError, match="without matching schemas"):
        McpServers.from_config(config, schemas={"gmail": [_td()]})
    # The error names the missing one
    try:
        McpServers.from_config(config, schemas={"gmail": [_td()]})
    except McpConfigError as e:
        assert "github" in str(e)


def test_mcp_servers_extra_schema_warns_only(caplog: pytest.LogCaptureFixture) -> None:
    """Schemas for servers not in the config don't raise — they log a warning.

    Use case: a shared schemas dict reused across multiple mcp.json files,
    each subsetting it.
    """
    import logging

    config = {"a": {"command": "x"}}
    with caplog.at_level(logging.WARNING, logger="calfkit.mcp._factory"):
        servers = McpServers.from_config(config, schemas={"a": [_td()], "b": [_td()]})
    assert "a" in servers
    assert "b" not in servers
    assert any("not in mcp.json" in rec.message for rec in caplog.records)


def test_mcp_servers_from_file(tmp_path: Path) -> None:
    config_path = tmp_path / "mcp.json"
    config_path.write_text(
        json.dumps(
            {
                "mcpServers": {
                    "gmail": {"command": "npx", "args": ["-y", "@mcp/server-gmail"]},
                }
            }
        )
    )
    servers = McpServers.from_file(config_path, schemas={"gmail": [_td("search")]})
    assert "gmail" in servers
    assert servers["gmail"].transport.command == "npx"  # type: ignore[union-attr]


def test_mcp_servers_env_substitution(monkeypatch: pytest.MonkeyPatch) -> None:
    """$VAR in mcp.json is expanded via _config.expand_env (Phase 1.3)."""
    monkeypatch.setenv("MY_OAUTH", "secret-tok")
    config = {"gmail": {"command": "npx", "env": {"OAUTH_TOKEN": "$MY_OAUTH"}}}
    servers = McpServers.from_config(config, schemas={"gmail": [_td()]})
    assert servers["gmail"].transport.env == {"OAUTH_TOKEN": "secret-tok"}  # type: ignore[union-attr]


def test_mcp_servers_threads_env_cwd_to_stdio() -> None:
    config = {"x": {"command": "echo", "args": ["hi"], "env": {"K": "V"}, "cwd": "/tmp"}}
    servers = McpServers.from_config(config, schemas={"x": [_td()]})
    transport = servers["x"].transport
    assert isinstance(transport, StdioTransport)
    assert transport.env == {"K": "V"}
    assert transport.cwd == "/tmp"
    assert transport.args == ("hi",)


def test_mcp_servers_threads_headers_to_http() -> None:
    config = {"x": {"type": "http", "url": "https://x", "headers": {"X-K": "V"}}}
    servers = McpServers.from_config(config, schemas={"x": [_td()]})
    transport = servers["x"].transport
    assert isinstance(transport, HttpTransport)
    assert transport.headers == {"X-K": "V"}


# ---------------------------------------------------------------------------
# _build_server_from_spec defensive guard
# ---------------------------------------------------------------------------


def test_build_server_from_unknown_spec_type() -> None:
    """A spec type that's not Stdio or Http should raise McpConfigError (future-proofing)."""

    class _Unknown:
        pass

    with pytest.raises(McpConfigError, match="unknown spec type"):
        _build_server_from_spec("x", _Unknown(), [_td()])  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# README-leading example smoke test
# ---------------------------------------------------------------------------


def test_readme_minimal_example_works() -> None:
    """The 3-line README example must work as advertised:

    from calfkit import mcp
    from gmail_schemas import Gmail
    gmail = mcp("npx -y @mcp/server-gmail", tools=Gmail.ALL)
    """
    tools = [_td("search"), _td("send")]
    gmail = top_level_mcp("npx -y @mcp/server-gmail", tools=tools)
    assert isinstance(gmail, McpServer)
    assert isinstance(gmail.transport, StdioTransport)
    assert gmail.raw_name == "server-gmail"
    assert len(list(gmail)) == 2  # iterates to yield BaseToolNodeSchema


# ---------------------------------------------------------------------------
# mcp() expand-before-routing — $VAR URLs / commands must auto-detect correctly
# ---------------------------------------------------------------------------


def test_mcp_call_expands_var_url_and_routes_to_http(monkeypatch: pytest.MonkeyPatch) -> None:
    """``mcp("$MCP_URL", ...)`` with ``$MCP_URL=https://...`` routes to HTTP.

    Regression: the routing check must run on the expanded value, not on
    the literal ``"$VAR"`` prefix, or env-templated URLs misroute to stdio.
    """
    monkeypatch.setenv("MCP_URL", "https://api.example.com/mcp")
    server = mcp_factory("$MCP_URL", tools=[_td()])
    assert isinstance(server.transport, HttpTransport)
    assert server.transport.url == "https://api.example.com/mcp"


def test_mcp_call_expands_var_command_and_routes_to_stdio(monkeypatch: pytest.MonkeyPatch) -> None:
    """``mcp("$MCP_CMD", ...)`` with ``$MCP_CMD="npx -y @x"`` must shlex-split
    the expanded value so multi-word commands work via env-var indirection.
    """
    monkeypatch.setenv("MCP_CMD", "npx -y @mcp/server-x")
    server = mcp_factory("$MCP_CMD", tools=[_td()])
    assert isinstance(server.transport, StdioTransport)
    assert server.transport.command == "npx"
    assert server.transport.args == ("-y", "@mcp/server-x")


def test_mcp_call_unset_var_raises_with_context(monkeypatch: pytest.MonkeyPatch) -> None:
    """Error message includes the literal ``mcp('$X')`` call form so operators
    can grep the source for the broken call site, not just the var name.
    """
    monkeypatch.delenv("CALFKIT_NONEXISTENT_FACTORY_VAR", raising=False)
    with pytest.raises(McpConfigError, match=r"mcp\('\$CALFKIT_NONEXISTENT_FACTORY_VAR'\)"):
        mcp_factory("$CALFKIT_NONEXISTENT_FACTORY_VAR", tools=[_td()])


def test_mcp_call_non_string_raises_clear_error() -> None:
    """``mcp(None, ...)`` / ``mcp(123, ...)`` must raise a clear McpConfigError
    instead of a confusing ``AttributeError`` deep in expand_env.
    """
    with pytest.raises(McpConfigError, match="non-empty string"):
        mcp_factory(None, tools=[_td()])  # type: ignore[arg-type]
    with pytest.raises(McpConfigError, match="non-empty string"):
        mcp_factory(123, tools=[_td()])  # type: ignore[arg-type]


def test_mcp_call_non_http_scheme_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """A typo like ``ftp://...`` is more likely a wrong-scheme bug than an
    intent to spawn the URL as a binary — fail loudly with guidance.
    """
    with pytest.raises(McpConfigError, match="is not http"):
        mcp_factory("ftp://server/path", tools=[_td()])


def test_mcp_call_non_http_scheme_via_var_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """Same check applies after $VAR expansion."""
    monkeypatch.setenv("BAD_SCHEME_URL", "ftp://server/path")
    with pytest.raises(McpConfigError, match="is not http"):
        mcp_factory("$BAD_SCHEME_URL", tools=[_td()])


def test_mcp_call_command_with_embedded_url_arg_routes_to_stdio() -> None:
    """A stdio command-line that contains a URL inside an arg must route to
    stdio, not raise a scheme-mismatch — the anchored scheme check guards
    against this false positive.
    """
    server = mcp_factory("python -m mymcp --endpoint http://api.example.com", tools=[_td()])
    assert isinstance(server.transport, StdioTransport)
    assert server.transport.command == "python"
    assert server.transport.args == ("-m", "mymcp", "--endpoint", "http://api.example.com")


# ---------------------------------------------------------------------------
# Factory __getattr__ delegation (re-binding submodule shadow)
# ---------------------------------------------------------------------------


def test_factory_getattr_resolves_mcpserver_after_top_level_import() -> None:
    """``from calfkit import mcp; calfkit.mcp.McpServer`` works despite the
    factory rebinding the ``calfkit.mcp`` attribute on the parent package.
    """
    import calfkit  # noqa: F401  triggers the rebound attribute path
    from calfkit.mcp import McpServer as direct_McpServer

    assert top_level_mcp.McpServer is direct_McpServer


def test_factory_getattr_resolves_other_public_names() -> None:
    from calfkit.mcp import McpServers as direct_McpServers
    from calfkit.mcp import McpToolAnnotations as direct_McpToolAnnotations
    from calfkit.mcp import McpToolDef as direct_McpToolDef

    assert top_level_mcp.McpServers is direct_McpServers
    assert top_level_mcp.McpToolDef is direct_McpToolDef
    assert top_level_mcp.McpToolAnnotations is direct_McpToolAnnotations


def test_factory_getattr_private_attribute_raises() -> None:
    with pytest.raises(AttributeError):
        _ = top_level_mcp._not_a_real_attribute


def test_factory_getattr_unknown_public_attribute_message() -> None:
    """Unknown public attribute gets the standard Python module message,
    matching what users see for any other module typo.
    """
    with pytest.raises(AttributeError, match="has no attribute 'NoSuchThing'"):
        _ = top_level_mcp.NoSuchThing


def test_factory_explicit_methods_still_resolve_without_getattr() -> None:
    """``mcp.stdio`` / ``mcp.http`` resolve via the class, NOT __getattr__."""
    # These are class methods on _McpFactory itself — verify they don't
    # accidentally route through __getattr__ and break the factory contract.
    assert callable(top_level_mcp.stdio)
    assert callable(top_level_mcp.http)
    # And the canonical happy path still works
    s = top_level_mcp.stdio("x", tools=[_td()], name="explicit")
    assert isinstance(s.transport, StdioTransport)
