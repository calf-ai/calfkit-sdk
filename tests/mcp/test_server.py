"""Unit tests for ``calfkit.mcp._server``.

Coverage focus:
- Construction via .stdio() / .http() classmethods produces correct transports
- name inference + topic-component normalization (Q13)
- empty tools=[] yields nothing + warns (Q4)
- Filter chains (only/exclude/where) compose correctly with frozen-instance semantics
- Annotation-based filters honour MCP spec defaults via McpToolDef properties
- Rename chains: prefix vs explicit, affect agent-facing name not topic
- __iter__ yields BaseToolNodeSchema with correct topic naming
- _build_schema metadata carries source="mcp" + original tool name for bridge dispatch
"""

from __future__ import annotations

import logging

import pytest

from calfkit._vendor.pydantic_ai.tools import ToolDefinition
from calfkit.mcp._server import McpServer
from calfkit.mcp._session import HttpTransport, StdioTransport
from calfkit.mcp._tool_def import McpToolAnnotations, McpToolDef
from calfkit.mcp.exceptions import McpConfigError
from calfkit.models.node_schema import BaseToolNodeSchema

# ---------------------------------------------------------------------------
# Test fixtures (kept inline for clarity rather than in conftest)
# ---------------------------------------------------------------------------


def _td(
    name: str,
    *,
    description: str | None = None,
    read_only: bool | None = None,
    destructive: bool | None = None,
    idempotent: bool | None = None,
) -> McpToolDef:
    """Build a McpToolDef with a minimal valid input schema and optional annotations."""
    ann = None
    if read_only is not None or destructive is not None or idempotent is not None:
        ann = McpToolAnnotations(
            read_only_hint=read_only,
            destructive_hint=destructive,
            idempotent_hint=idempotent,
        )
    return McpToolDef(
        name=name,
        description=description,
        input_schema={"type": "object", "properties": {}},
        annotations=ann,
    )


# ---------------------------------------------------------------------------
# Construction — .stdio() / .http() classmethods
# ---------------------------------------------------------------------------


def test_stdio_construction_minimal() -> None:
    s = McpServer.stdio("npx", "-y", "@mcp/server-gmail", tools=[_td("search")])
    assert isinstance(s.transport, StdioTransport)
    assert s.transport.command == "npx"
    assert s.transport.args == ("-y", "@mcp/server-gmail")
    assert s.raw_name == "server-gmail"  # inferred from npm-style scoped package
    assert s.name == "server_gmail"  # hyphen normalized to underscore for topic safety
    assert s.tools == (_td("search"),)


def test_stdio_construction_with_all_kwargs() -> None:
    s = McpServer.stdio(
        "echo",
        "hi",
        tools=[_td("t")],
        name="custom-name",
        env={"K": "V"},
        cwd="/tmp",
        shutdown_grace_seconds=10.0,
        safe_env_only=True,
        read_timeout_seconds=60.0,
        client_info_version="0.9-test",
    )
    assert s.raw_name == "custom-name"
    assert s.name == "custom_name"  # normalized: hyphen → underscore
    assert s.transport.env == {"K": "V"}
    assert s.transport.cwd == "/tmp"
    assert s.transport.shutdown_grace_seconds == 10.0
    assert s.transport.safe_env_only is True


def test_http_construction_minimal() -> None:
    s = McpServer.http("https://api.example.com/mcp", tools=[_td("t")])
    assert isinstance(s.transport, HttpTransport)
    assert s.transport.url == "https://api.example.com/mcp"
    assert s.raw_name == "api.example.com"
    assert s.name == "api_example_com"  # normalized


def test_http_construction_with_token() -> None:
    s = McpServer.http(
        "https://x.com/mcp",
        tools=[_td("t")],
        token="bearer-tok",
        headers={"X-Foo": "bar"},
        timeout_seconds=15.0,
        sse_read_timeout_seconds=120.0,
    )
    assert s.transport.token == "bearer-tok"
    assert s.transport.headers == {"X-Foo": "bar"}
    assert s.transport.timeout_seconds == 15.0
    assert s.transport.sse_read_timeout_seconds == 120.0


def test_stdio_tools_coerced_to_tuple() -> None:
    """tools is stored as a tuple internally for immutability."""
    s = McpServer.stdio("x", tools=[_td("a"), _td("b")])
    assert isinstance(s.tools, tuple)
    assert len(s.tools) == 2


def test_empty_name_inference_fails() -> None:
    """If transport can't infer a name and user didn't supply one, raise."""
    # HttpTransport with empty URL → infer_name returns "mcp" (not empty), so
    # this is hard to trigger. Use the bare constructor with a transport
    # whose infer_name returns "".
    from calfkit.mcp._session import McpTransport

    class _NoNameTransport(McpTransport):
        def infer_name(self) -> str:
            return ""

    with pytest.raises(McpConfigError, match="empty"):
        McpServer(_NoNameTransport(), tools=[_td("x")])


# ---------------------------------------------------------------------------
# Topic-component normalization (Q13)
# ---------------------------------------------------------------------------


def test_name_normalization_dots() -> None:
    s = McpServer.stdio("x", tools=[_td("t")], name="my.server.v2")
    assert s.raw_name == "my.server.v2"
    assert s.name == "my_server_v2"


def test_name_normalization_hyphens() -> None:
    s = McpServer.stdio("x", tools=[_td("t")], name="my-mcp-server")
    assert s.raw_name == "my-mcp-server"
    assert s.name == "my_mcp_server"


def test_name_normalization_both() -> None:
    s = McpServer.stdio("x", tools=[_td("t")], name="my-srv.v2.1")
    assert s.name == "my_srv_v2_1"


def test_name_normalization_no_op_for_safe_names() -> None:
    s = McpServer.stdio("x", tools=[_td("t")], name="gmail")
    assert s.raw_name == s.name == "gmail"


# ---------------------------------------------------------------------------
# Empty tools (Q4)
# ---------------------------------------------------------------------------


def test_empty_tools_yields_nothing_and_warns(caplog: pytest.LogCaptureFixture) -> None:
    s = McpServer.stdio("x", tools=[], name="empty")
    with caplog.at_level(logging.WARNING, logger="calfkit.mcp._server"):
        results = list(s)
    assert results == []
    assert any("yielded no tools" in rec.message for rec in caplog.records)


def test_empty_tools_after_filter_warns(caplog: pytest.LogCaptureFixture) -> None:
    """Filtering down to nothing should also warn."""
    s = McpServer.stdio("x", tools=[_td("a"), _td("b")], name="t").only("nonexistent")
    with caplog.at_level(logging.WARNING, logger="calfkit.mcp._server"):
        results = list(s)
    assert results == []
    assert any("yielded no tools" in rec.message for rec in caplog.records)


# ---------------------------------------------------------------------------
# Filters: only / exclude
# ---------------------------------------------------------------------------


def test_only_filter() -> None:
    s = McpServer.stdio("x", tools=[_td("a"), _td("b"), _td("c")], name="t").only("a", "c")
    names = [schema.tool_schema.name for schema in s]
    assert names == ["a", "c"]


def test_exclude_filter() -> None:
    s = McpServer.stdio("x", tools=[_td("a"), _td("b"), _td("c")], name="t").exclude("b")
    names = [schema.tool_schema.name for schema in s]
    assert names == ["a", "c"]


def test_only_intersection_with_existing() -> None:
    """Chained .only() takes the intersection (you can't expand an existing allowlist)."""
    s = McpServer.stdio("x", tools=[_td("a"), _td("b"), _td("c")], name="t").only("a", "b").only("b", "c")
    names = [schema.tool_schema.name for schema in s]
    assert names == ["b"]  # only b is in BOTH allowlists


def test_only_and_exclude_combine() -> None:
    s = McpServer.stdio("x", tools=[_td("a"), _td("b"), _td("c")], name="t").only("a", "b").exclude("a")
    names = [schema.tool_schema.name for schema in s]
    assert names == ["b"]


def test_exclude_accumulates() -> None:
    s = McpServer.stdio("x", tools=[_td("a"), _td("b"), _td("c")], name="t").exclude("a").exclude("c")
    names = [schema.tool_schema.name for schema in s]
    assert names == ["b"]


# ---------------------------------------------------------------------------
# Filters: where (annotation hints)
# ---------------------------------------------------------------------------


def test_where_read_only_hint_true() -> None:
    """only tools with effective read_only=True survive."""
    s = McpServer.stdio(
        "x",
        tools=[
            _td("ro", read_only=True),
            _td("rw", read_only=False),
            _td("default"),  # no annotation → read_only defaults to False
        ],
        name="t",
    ).where(read_only_hint=True)
    names = [schema.tool_schema.name for schema in s]
    assert names == ["ro"]


def test_where_destructive_hint_false_respects_spec_default() -> None:
    """destructive defaults to True per spec, so unannotated tools fail this filter."""
    s = McpServer.stdio(
        "x",
        tools=[
            _td("safe", destructive=False),
            _td("danger", destructive=True),
            _td("default"),  # spec default destructive=True → fails False filter
        ],
        name="t",
    ).where(destructive_hint=False)
    names = [schema.tool_schema.name for schema in s]
    assert names == ["safe"]


def test_where_idempotent_filter() -> None:
    s = McpServer.stdio(
        "x",
        tools=[_td("idem", idempotent=True), _td("non", idempotent=False)],
        name="t",
    ).where(idempotent_hint=True)
    names = [schema.tool_schema.name for schema in s]
    assert names == ["idem"]


def test_where_predicate() -> None:
    """Arbitrary predicate filter on McpToolDef."""
    s = McpServer.stdio(
        "x",
        tools=[_td("alpha"), _td("beta"), _td("gamma")],
        name="t",
    ).where(predicate=lambda t: t.name.startswith("a") or t.name.startswith("g"))
    names = [schema.tool_schema.name for schema in s]
    assert names == ["alpha", "gamma"]


def test_where_combines_with_other_filters() -> None:
    s = (
        McpServer.stdio(
            "x",
            tools=[_td("a", read_only=True), _td("b", read_only=True), _td("c", read_only=False)],
            name="t",
        )
        .only("a", "c")
        .where(read_only_hint=True)
    )
    names = [schema.tool_schema.name for schema in s]
    assert names == ["a"]


def test_multiple_where_compose_with_and() -> None:
    s = (
        McpServer.stdio(
            "x",
            tools=[
                _td("ro_idem", read_only=True, idempotent=True),
                _td("ro_only", read_only=True, idempotent=False),
                _td("idem_only", read_only=False, idempotent=True),
            ],
            name="t",
        )
        .where(read_only_hint=True)
        .where(idempotent_hint=True)
    )
    names = [schema.tool_schema.name for schema in s]
    assert names == ["ro_idem"]


# ---------------------------------------------------------------------------
# Renames: prefix and explicit
# ---------------------------------------------------------------------------


def test_prefix_rename_affects_llm_name() -> None:
    s = McpServer.stdio("x", tools=[_td("search"), _td("send")], name="gmail").prefix("inbox")
    schemas = list(s)
    llm_names = [sch.tool_schema.name for sch in schemas]
    assert llm_names == ["inbox.search", "inbox.send"]


def test_prefix_rename_does_not_affect_topic() -> None:
    """Topic paths use the ORIGINAL tool name (wire identity stable across renames)."""
    s = McpServer.stdio("x", tools=[_td("search")], name="gmail").prefix("inbox")
    schemas = list(s)
    assert schemas[0].subscribe_topics == ["mcp.gmail.search.input"]
    assert schemas[0].publish_topic == "mcp.gmail.search.output"
    assert schemas[0].node_id == "mcp_gmail_search"


def test_explicit_rename() -> None:
    s = McpServer.stdio("x", tools=[_td("search"), _td("send")], name="gmail").rename({"search": "find"})
    llm_names = [sch.tool_schema.name for sch in s]
    assert "find" in llm_names
    assert "search" not in llm_names
    # "send" not renamed → keeps original name
    assert "send" in llm_names


def test_explicit_rename_overrides_prefix() -> None:
    """When both prefix and explicit rename apply to a tool, explicit wins."""
    s = McpServer.stdio("x", tools=[_td("search"), _td("send")], name="gmail").prefix("inbox").rename({"search": "find"})
    llm_names = [sch.tool_schema.name for sch in s]
    assert "find" in llm_names  # explicit rename took precedence
    assert "inbox.send" in llm_names  # only prefix applies


def test_rename_chain_accumulates() -> None:
    s = McpServer.stdio("x", tools=[_td("a"), _td("b")], name="t").rename({"a": "alpha"}).rename({"b": "beta"})
    llm_names = {sch.tool_schema.name for sch in s}
    assert llm_names == {"alpha", "beta"}


# ---------------------------------------------------------------------------
# __iter__ output shape
# ---------------------------------------------------------------------------


def test_iter_yields_base_tool_node_schema() -> None:
    s = McpServer.stdio("x", tools=[_td("search")], name="gmail")
    schemas = list(s)
    assert len(schemas) == 1
    assert isinstance(schemas[0], BaseToolNodeSchema)
    assert isinstance(schemas[0].tool_schema, ToolDefinition)


def test_topic_naming_uses_normalized_server_name() -> None:
    """A server named ``my-srv.v2`` produces ``mcp.my_srv_v2.tool.input``."""
    s = McpServer.stdio("x", tools=[_td("search")], name="my-srv.v2")
    schemas = list(s)
    assert schemas[0].subscribe_topics == ["mcp.my_srv_v2.search.input"]
    assert schemas[0].publish_topic == "mcp.my_srv_v2.search.output"
    assert schemas[0].node_id == "mcp_my_srv_v2_search"


def test_schema_metadata_carries_dispatch_keys() -> None:
    """The BaseToolNodeSchema's tool_schema.metadata carries the original
    tool name + normalised server name + source marker. Phase 3 McpBridge
    reads these to dispatch to the correct MCP method.
    """
    s = McpServer.stdio("x", tools=[_td("search", description="find emails", read_only=True)], name="my-srv")
    schemas = list(s)
    meta = schemas[0].tool_schema.metadata
    assert meta is not None
    assert meta["mcp_tool_name"] == "search"  # original, for SDK dispatch
    assert meta["mcp_server"] == "my-srv"  # original
    assert meta["mcp_server_normalized"] == "my_srv"  # for topic resolution
    assert meta["source"] == "mcp"
    assert meta["annotations"] == {"read_only_hint": True}


# ---------------------------------------------------------------------------
# Immutability — filter / rename methods return new instances
# ---------------------------------------------------------------------------


def test_only_returns_new_instance() -> None:
    s = McpServer.stdio("x", tools=[_td("a"), _td("b")], name="t")
    filtered = s.only("a")
    assert filtered is not s
    # Original is unchanged
    assert len(list(s)) == 2
    assert len(list(filtered)) == 1


def test_prefix_returns_new_instance() -> None:
    s = McpServer.stdio("x", tools=[_td("a")], name="t")
    renamed = s.prefix("p")
    assert renamed is not s
    # Original has no prefix
    assert list(s)[0].tool_schema.name == "a"
    assert list(renamed)[0].tool_schema.name == "p.a"


def test_chained_filters_each_return_new() -> None:
    s = McpServer.stdio("x", tools=[_td("a"), _td("b")], name="t")
    step1 = s.only("a", "b")
    step2 = step1.exclude("a")
    assert s is not step1 is not step2  # all distinct
    assert {sch.tool_schema.name for sch in s} == {"a", "b"}
    assert {sch.tool_schema.name for sch in step1} == {"a", "b"}
    assert {sch.tool_schema.name for sch in step2} == {"b"}


def test_copy_resets_session_state() -> None:
    """A filtered/renamed copy does NOT inherit the parent's _session attr.

    A copy is a new logical server — if deployed, it gets its own session.
    """
    s = McpServer.stdio("x", tools=[_td("a")], name="t")
    # Simulate that the parent has an open session
    s._session = object()  # type: ignore[assignment]
    s._initialize_result = "init"
    copy = s.only("a")
    assert copy._session is None
    assert copy._initialize_result is None


# ---------------------------------------------------------------------------
# Accessors
# ---------------------------------------------------------------------------


def test_meta_hook_property_passes_through() -> None:
    hook = lambda ctx: {"user_id": "x"}  # noqa: E731
    s = McpServer.stdio("x", tools=[_td("t")], name="srv", meta=hook)
    assert s.meta_hook is hook


def test_session_property_initially_none() -> None:
    s = McpServer.stdio("x", tools=[_td("t")], name="srv")
    assert s.session is None


def test_transport_property() -> None:
    s = McpServer.stdio("x", tools=[_td("t")], name="srv")
    assert isinstance(s.transport, StdioTransport)


# ---------------------------------------------------------------------------
# $VAR expansion at construction time (P0 #5 — token/headers/env)
# ---------------------------------------------------------------------------


def test_http_token_var_is_expanded(monkeypatch: pytest.MonkeyPatch) -> None:
    """``$VAR`` substitution applied to ``token=`` at construction.

    Regression: prior to this fix, only the ``mcp.json`` path expanded
    env vars — users following the README's ``token="$CALFKIT_SERVICE_TOKEN"``
    snippet sent the literal string as their Bearer.
    """
    monkeypatch.setenv("CALFKIT_SERVICE_TOKEN", "sk-real-token")
    s = McpServer.http("https://api.example.com/mcp", tools=[_td("t")], token="$CALFKIT_SERVICE_TOKEN")
    assert isinstance(s.transport, HttpTransport)
    assert s.transport.token == "sk-real-token"
    # The assembled header reflects the expanded value
    assert s.transport.build_session_headers() == {"Authorization": "Bearer sk-real-token"}


def test_http_headers_values_are_expanded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_VERSION", "2024-11-25")
    s = McpServer.http(
        "https://api.example.com/mcp",
        tools=[_td("t")],
        headers={"X-Api-Version": "$API_VERSION", "X-Static": "yes"},
    )
    assert isinstance(s.transport, HttpTransport)
    assert s.transport.headers == {"X-Api-Version": "2024-11-25", "X-Static": "yes"}


def test_http_url_is_expanded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MCP_HOST", "api.example.com")
    s = McpServer.http("https://$MCP_HOST/mcp", tools=[_td("t")])
    assert isinstance(s.transport, HttpTransport)
    assert s.transport.url == "https://api.example.com/mcp"


def test_stdio_env_values_are_expanded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GMAIL_API_KEY", "k-from-host")
    s = McpServer.stdio(
        "npx",
        "-y",
        "@m/server-gmail",
        tools=[_td("t")],
        env={"GMAIL_API_KEY": "$GMAIL_API_KEY", "STATIC": "yes"},
    )
    assert s.transport.env == {"GMAIL_API_KEY": "k-from-host", "STATIC": "yes"}


def test_stdio_args_are_expanded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PKG_VERSION", "1.2.3")
    s = McpServer.stdio("npx", "-y", "@m/server-gmail@$PKG_VERSION", tools=[_td("t")])
    assert s.transport.args == ("-y", "@m/server-gmail@1.2.3")


def test_http_unset_token_var_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CALFKIT_NONEXISTENT_TOKEN_VAR", raising=False)
    with pytest.raises(McpConfigError, match="CALFKIT_NONEXISTENT_TOKEN_VAR"):
        McpServer.http("https://api.example.com/mcp", tools=[_td("t")], token="$CALFKIT_NONEXISTENT_TOKEN_VAR")


def test_stdio_no_expansion_when_no_var() -> None:
    """Plain strings without `$VAR` should pass through unchanged."""
    s = McpServer.stdio("npx", "-y", "@m/server", tools=[_td("t")], env={"K": "literal"})
    assert s.transport.command == "npx"
    assert s.transport.args == ("-y", "@m/server")
    assert s.transport.env == {"K": "literal"}


def test_http_braced_var_form_expanded(monkeypatch: pytest.MonkeyPatch) -> None:
    """``${VAR}`` form works through ``McpServer.http``, not just ``mcp.json``."""
    monkeypatch.setenv("MCP_HOST", "api.example.com")
    s = McpServer.http("https://${MCP_HOST}/mcp", tools=[_td("t")])
    assert s.transport.url == "https://api.example.com/mcp"


def test_http_explicit_authorization_beats_expanded_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """Explicit ``headers={'Authorization': ...}`` wins over ``token="$VAR"``.

    Precedence is checked AFTER expansion — both legs need to be honoured.
    """
    monkeypatch.setenv("SECRET", "should-be-ignored")
    s = McpServer.http(
        "https://x.com/mcp",
        tools=[_td("t")],
        token="$SECRET",
        headers={"Authorization": "Basic preset"},
    )
    assert s.transport.build_session_headers()["Authorization"] == "Basic preset"


def test_http_expanded_url_must_have_scheme(monkeypatch: pytest.MonkeyPatch) -> None:
    """A scheme-less expansion result fails loudly at construction."""
    monkeypatch.setenv("BAD_BASE", "no-scheme-here")
    with pytest.raises(McpConfigError, match="does not start with"):
        McpServer.http("$BAD_BASE", tools=[_td("t")])


def test_expand_env_failure_includes_arg_context(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unset-var errors include the field name so multi-field configs are debuggable."""
    monkeypatch.delenv("CALFKIT_TEST_MISSING_ENV", raising=False)
    with pytest.raises(McpConfigError, match=r"McpServer\.stdio\(env=\).*CALFKIT_TEST_MISSING_ENV"):
        McpServer.stdio("x", tools=[_td("t")], env={"K": "$CALFKIT_TEST_MISSING_ENV"})
