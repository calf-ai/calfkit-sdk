"""ToolSelector protocol, MCPToolboxNode-as-selector, and agent resolution.

The toolbox instance is passed to agents like a tool node (``tools=[docs]``);
resolution happens per turn against the Capability View. Post-migration the view
is a :class:`ControlPlaneView` that owns staleness + schema-version filtering, so
the resolver simplifies to "is it here, and does it have the tools I asked for".
The ``strict`` flag and the ``stale_seconds`` / ``skipped_newer_schema``
diagnostics are gone (D5/D6); unresolved selections always warn + degrade.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from calfkit.controlplane import ControlPlaneView
from calfkit.mcp.mcp_toolbox import MCPToolboxNode
from calfkit.mcp.mcp_transport import StreamableHttpParameters
from calfkit.models.capability import (
    CAPABILITY_VIEW_RESOURCE_KEY,
    CapabilityRecord,
    CapabilityToolDef,
    SelectorResult,
)
from calfkit.models.tool_dispatch import ToolBinding, ToolProvider, ToolSelector, split_tool_declarations
from tests.test_controlplane_view import _FakeTable  # the substrate's dict-backed GroupedTableReader fake


def make_toolbox(name: str = "docs_server") -> MCPToolboxNode:
    return MCPToolboxNode(name, connection_params=StreamableHttpParameters(url="http://unused.local/mcp"))


def make_record(**overrides: Any) -> CapabilityRecord:
    now = datetime.now(tz=timezone.utc)
    defaults: dict[str, Any] = dict(
        started_at=now,
        last_heartbeat_at=now,
        heartbeat_interval=30.0,
        dispatch_topic="mcp_server.docs_server",
        tools=[CapabilityToolDef(name=n, parameters_json_schema={"type": "object", "properties": {}}) for n in ("search", "fetch")],
        content_updated_at=now,
    )
    defaults.update(overrides)
    return CapabilityRecord(**defaults)


def make_view(record: CapabilityRecord | None = None, **table_kwargs: Any) -> ControlPlaneView[CapabilityRecord]:
    """A real ControlPlaneView over a one-instance dict-backed fake, keyed docs_server×w1."""
    data: dict[str, dict[str, CapabilityRecord]] = {"docs_server": {"w1": record}} if record is not None else {}
    return ControlPlaneView(_FakeTable(data, **table_kwargs), CapabilityRecord)


class TestToolboxIsNoLongerAProvider:
    def test_tool_bindings_stub_is_gone(self) -> None:
        toolbox = make_toolbox()
        assert not hasattr(toolbox, "tool_bindings")
        assert not isinstance(toolbox, ToolProvider)

    def test_toolbox_satisfies_tool_selector(self) -> None:
        assert isinstance(make_toolbox(), ToolSelector)


class TestResolveTools:
    def test_resolves_all_tools_from_view(self) -> None:
        toolbox = make_toolbox()
        view = {"docs_server": make_record()}
        result = toolbox.resolve_tools(view)
        assert isinstance(result, SelectorResult)
        assert [b.name for b in result.bindings] == ["search", "fetch"]
        assert all(isinstance(b, ToolBinding) and b.validator is None for b in result.bindings)
        assert all(b.dispatch_topic == "mcp_server.docs_server" for b in result.bindings)
        assert not result.missing_toolbox and not result.missing_tools

    def test_missing_toolbox_diagnostic(self) -> None:
        result = make_toolbox().resolve_tools({})
        assert result.missing_toolbox
        assert result.bindings == []
        assert result.toolbox_id == "docs_server"

    def test_include_filter_and_missing_tools(self) -> None:
        toolbox = make_toolbox()
        selector = toolbox.select(include=["search", "nonexistent"])
        result = selector.resolve_tools({"docs_server": make_record()})
        assert [b.name for b in result.bindings] == ["search"]
        assert result.missing_tools == ("nonexistent",)

    def test_scoped_selector_is_frozen(self) -> None:
        selector = make_toolbox().select(include=["search"])
        with pytest.raises(Exception):
            selector.include = ("other",)  # type: ignore[misc]

    def test_malformed_record_is_skipped_with_diagnostic(self) -> None:
        # Tolerant reader admits an empty dispatch_topic; binding expansion
        # must not crash the turn — it surfaces as invalid_record instead.
        record = make_record(dispatch_topic="")
        result = make_toolbox().resolve_tools({"docs_server": record})
        assert result.invalid_record
        assert result.bindings == []

    # -- view-owned filtering (D6): the resolver never sees stale/newer-schema records --

    def test_stale_record_hidden_by_view(self) -> None:
        # age 100s > 3*30 = 90 threshold -> the view collapses it to None, so the
        # resolver reports missing_toolbox (no advisory "use last-known" path).
        view = make_view(make_record(last_heartbeat_at=datetime.now(tz=timezone.utc) - timedelta(seconds=100)))
        result = make_toolbox().resolve_tools(view)
        assert result.missing_toolbox
        assert result.bindings == []

    def test_newer_schema_hidden_by_view(self) -> None:
        # A record from a newer schema major is filtered (and logged) by the view;
        # the resolver just sees None -> missing_toolbox.
        view = make_view(make_record(schema_version=2))
        result = make_toolbox().resolve_tools(view)
        assert result.missing_toolbox
        assert result.bindings == []

    def test_live_record_resolves_through_view(self) -> None:
        view = make_view(make_record())
        result = make_toolbox().resolve_tools(view)
        assert [b.name for b in result.bindings] == ["search", "fetch"]
        assert not result.missing_toolbox


class TestSplitToolDeclarations:
    def test_separates_bindings_providers_and_selectors(self) -> None:
        from calfkit.nodes.tool import agent_tool

        def get_weather(city: str) -> str:
            return city

        tool_node = agent_tool(get_weather)
        toolbox = make_toolbox()
        scoped = toolbox.select(include=["search"])
        raw = tool_node.tool_bindings()[0]

        bindings, selectors = split_tool_declarations([tool_node, toolbox, scoped, raw])
        assert [b.name for b in bindings] == ["get_weather", "get_weather"]
        assert selectors == [toolbox, scoped]

    def test_garbage_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="ToolBinding|ToolProvider|ToolSelector"):
            split_tool_declarations(["nope"])


class TestAgentResolution:
    """Drives the agent's per-turn selector resolution."""

    def make_agent(self, *tools: Any):
        from calfkit.nodes.agent import Agent
        from calfkit.providers.pydantic_ai.model_client import PydanticModelClient

        class FakeModel(PydanticModelClient):
            @property
            def model_name(self) -> str:
                return "fake"

            @property
            def system(self) -> str:
                return "fake"

            async def request(self, *args: object, **kwargs: object) -> object:
                raise NotImplementedError

        return Agent("a", subscribe_topics="a.in", model_client=FakeModel(), tools=list(tools))

    def test_ctor_sets_aside_selectors(self) -> None:
        toolbox = make_toolbox()
        agent = self.make_agent(toolbox)
        assert agent.tools == []
        assert agent._tool_selectors == [toolbox]

    def test_resolution_merges_bindings_into_registry(self) -> None:
        agent = self.make_agent(make_toolbox())
        registry: dict[str, ToolBinding] = {}
        agent._resolve_selector_tools({CAPABILITY_VIEW_RESOURCE_KEY: {"docs_server": make_record()}}, registry)
        assert sorted(registry) == ["fetch", "search"]

    def test_collision_static_wins_and_logs(self, caplog: pytest.LogCaptureFixture) -> None:
        agent = self.make_agent(make_toolbox())
        static = ToolBinding(
            tool_def=__import__("calfkit._vendor.pydantic_ai.tools", fromlist=["ToolDefinition"]).ToolDefinition(name="search"),
            dispatch_topic="static.topic",
        )
        registry = {"search": static}
        with caplog.at_level("ERROR"):
            agent._resolve_selector_tools({CAPABILITY_VIEW_RESOURCE_KEY: {"docs_server": make_record()}}, registry)
        assert registry["search"] is static  # remote never shadows local
        assert any("search" in r.message for r in caplog.records)
        assert "fetch" in registry  # non-colliding remote tool still merged

    def test_missing_view_warns_and_degrades(self, caplog: pytest.LogCaptureFixture) -> None:
        agent = self.make_agent(make_toolbox())
        registry: dict[str, ToolBinding] = {}
        with caplog.at_level("WARNING"):
            agent._resolve_selector_tools({}, registry)  # no view resource at all
        assert registry == {}
        assert any("capability" in r.message.lower() for r in caplog.records)

    def test_unresolved_toolbox_warns_and_degrades(self, caplog: pytest.LogCaptureFixture) -> None:
        # No strict path: an unresolved toolbox always warns + degrades, never raises.
        agent = self.make_agent(make_toolbox())
        registry: dict[str, ToolBinding] = {}
        with caplog.at_level("WARNING"):
            agent._resolve_selector_tools({CAPABILITY_VIEW_RESOURCE_KEY: {}}, registry)
        assert registry == {}
        assert any("docs_server" in r.message for r in caplog.records)

    def test_degraded_view_logs_warning_but_proceeds(self, caplog: pytest.LogCaptureFixture) -> None:
        # CRITICAL-4 (D6, log-only): a degraded reader is surfaced via a warning,
        # but resolution still proceeds against whatever the view serves.
        agent = self.make_agent(make_toolbox())
        registry: dict[str, ToolBinding] = {}
        view = make_view(make_record(), status="degraded")
        with caplog.at_level("WARNING"):
            agent._resolve_selector_tools({CAPABILITY_VIEW_RESOURCE_KEY: view}, registry)
        assert sorted(registry) == ["fetch", "search"]  # turn proceeds
        assert any("degraded" in r.message.lower() for r in caplog.records)

    def test_failed_view_logs_warning_but_proceeds(self, caplog: pytest.LogCaptureFixture) -> None:
        agent = self.make_agent(make_toolbox())
        registry: dict[str, ToolBinding] = {}
        view = make_view(make_record(), status="failed", failure=RuntimeError("reader died"))
        with caplog.at_level("WARNING"):
            agent._resolve_selector_tools({CAPABILITY_VIEW_RESOURCE_KEY: view}, registry)
        assert any("failed" in r.message.lower() or "reader died" in r.message for r in caplog.records)


class TestNoStrictSurface:
    """D5: the strict knob and its exception are gone from the public surface."""

    def test_select_has_no_strict_parameter(self) -> None:
        import inspect

        params = inspect.signature(MCPToolboxNode.select).parameters
        assert "strict" not in params
        assert "include" in params

    def test_selector_result_has_no_strict_or_filtering_diagnostics(self) -> None:
        fields = set(SelectorResult.__dataclass_fields__)
        assert "strict" not in fields
        assert "stale_seconds" not in fields
        assert "skipped_newer_schema" not in fields

    def test_resolution_error_is_gone(self) -> None:
        import calfkit.exceptions as exc

        assert not hasattr(exc, "MCPToolResolutionError")


class TestOverridesSuppressSelectors:
    """Per-run overrides pin the exact tool surface: selectors are skipped."""

    def _ctx(self, overrides=None):
        from calfkit.models.state import OverridesState, State
        from tests.test_tool_errors import _make_ctx

        state = State()
        if overrides is not None:
            state.overrides = OverridesState(override_agent_tools=overrides)
        ctx = _make_ctx(state)
        return ctx

    def test_overridden_turn_skips_selector_resolution(self) -> None:
        from calfkit._vendor.pydantic_ai.tools import ToolDefinition

        agent = TestAgentResolution().make_agent(make_toolbox())
        override = ToolBinding(tool_def=ToolDefinition(name="pinned"), dispatch_topic="pinned.topic")
        registry = {"pinned": override}
        ctx = self._ctx(overrides=[override])
        # A view IS present and would resolve docs_server's tools — but the
        # override gate must short-circuit before resolution.
        ctx._resources = {CAPABILITY_VIEW_RESOURCE_KEY: {"docs_server": make_record()}}
        agent._maybe_resolve_selectors(ctx, registry)
        assert list(registry) == ["pinned"]  # search/fetch NOT merged

    def test_non_overridden_turn_resolves(self) -> None:
        agent = TestAgentResolution().make_agent(make_toolbox())
        ctx = self._ctx()
        ctx._resources = {CAPABILITY_VIEW_RESOURCE_KEY: {"docs_server": make_record()}}
        registry: dict[str, ToolBinding] = {}
        agent._maybe_resolve_selectors(ctx, registry)
        assert sorted(registry) == ["fetch", "search"]
