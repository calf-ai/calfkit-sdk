"""PR C: ToolSelector protocol, MCPToolbox-as-selector, and agent resolution.

Spec §8.4: the toolbox instance is passed to agents like a tool node
(``tools=[docs]``); resolution happens per turn against the Capability View,
typed as a plain ``Mapping[str, CapabilityRecord]`` — these tests use plain
dicts as the view (the agent layer never imports ktables).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from calfkit.exceptions import MCPToolResolutionError
from calfkit.mcp.mcp_toolbox import MCPToolbox
from calfkit.mcp.mcp_transport import StreamableHttpParameters
from calfkit.models.capability import (
    CAPABILITY_SCHEMA_VERSION,
    CAPABILITY_VIEW_RESOURCE_KEY,
    CapabilityRecord,
    CapabilityToolDef,
    SelectorResult,
)
from calfkit.models.tool_dispatch import ToolBinding, ToolProvider, ToolSelector, split_tool_declarations


def make_toolbox(name: str = "docs_server") -> MCPToolbox:
    return MCPToolbox(name, connection_params=StreamableHttpParameters(url="http://unused.local/mcp"))


def make_record(toolbox_id: str = "docs_server", *, tool_names: tuple[str, ...] = ("search", "fetch"), **overrides: Any) -> CapabilityRecord:
    defaults: dict[str, Any] = dict(
        toolbox_id=toolbox_id,
        dispatch_topic=f"mcp_server.{toolbox_id}",
        tools=[CapabilityToolDef(name=n, parameters_json_schema={"type": "object", "properties": {}}) for n in tool_names],
        published_at=datetime.now(tz=timezone.utc),
    )
    defaults.update(overrides)
    return CapabilityRecord(**defaults)


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
        assert not result.strict

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

    def test_strict_flag_propagates(self) -> None:
        selector = make_toolbox().select(strict=True)
        assert selector.resolve_tools({}).strict is True

    def test_scoped_selector_is_frozen(self) -> None:
        selector = make_toolbox().select(include=["search"])
        with pytest.raises(Exception):
            selector.strict = True  # type: ignore[misc]

    def test_newer_schema_is_skipped_with_diagnostic(self) -> None:
        record = make_record(schema_version=CAPABILITY_SCHEMA_VERSION + 1)
        result = make_toolbox().resolve_tools({"docs_server": record})
        assert result.skipped_newer_schema
        assert result.bindings == []

    def test_malformed_record_is_skipped_with_diagnostic(self) -> None:
        # Tolerant reader admits an empty dispatch_topic; binding expansion
        # must not crash the turn (spec §8.4 guard).
        record = make_record(dispatch_topic="")
        result = make_toolbox().resolve_tools({"docs_server": record})
        assert result.invalid_record
        assert result.bindings == []

    def test_stale_seconds_reflects_record_age(self) -> None:
        old = make_record(published_at=datetime.now(tz=timezone.utc) - timedelta(seconds=120))
        result = make_toolbox().resolve_tools({"docs_server": old})
        assert result.stale_seconds is not None and 119 < result.stale_seconds < 130


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
    """Drives the agent's per-turn selector resolution against a plain dict."""

    def make_agent(self, *tools: Any):
        from calfkit.nodes.agent import Agent
        from tests.test_capability_models import __name__ as _  # noqa: F401

        class _FakeModel:
            pass

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

    def test_missing_view_with_strict_raises(self) -> None:
        agent = self.make_agent(make_toolbox().select(strict=True))
        with pytest.raises(MCPToolResolutionError):
            agent._resolve_selector_tools({}, {})

    def test_unresolved_toolbox_warns_or_raises(self, caplog: pytest.LogCaptureFixture) -> None:
        lenient = self.make_agent(make_toolbox())
        with caplog.at_level("WARNING"):
            lenient._resolve_selector_tools({CAPABILITY_VIEW_RESOURCE_KEY: {}}, {})
        assert any("docs_server" in r.message for r in caplog.records)

        strict = self.make_agent(make_toolbox().select(strict=True))
        with pytest.raises(MCPToolResolutionError, match="docs_server"):
            strict._resolve_selector_tools({CAPABILITY_VIEW_RESOURCE_KEY: {}}, {})
