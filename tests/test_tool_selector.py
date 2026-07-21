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
from calfkit.nodes.toolbox import Toolbox, Toolboxes
from tests.test_controlplane_view import _FakeTable  # the substrate's dict-backed GroupedTableReader fake


def make_toolbox(name: str = "docs_server") -> MCPToolboxNode:
    return MCPToolboxNode(name, connection_params=StreamableHttpParameters(url="http://unused.local/mcp"))


def make_record(**overrides: Any) -> CapabilityRecord:
    now = datetime.now(tz=timezone.utc)
    defaults: dict[str, Any] = dict(
        started_at=now,
        last_heartbeat_at=now,
        heartbeat_interval=30.0,
        node_kind="toolbox",
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
        assert isinstance(result.bindings, tuple)  # frozen value: immutable binding sequence
        assert [b.name for b in result.bindings] == ["docs_server__search", "docs_server__fetch"]
        assert all(isinstance(b, ToolBinding) and b.validator is None for b in result.bindings)
        assert all(b.dispatch_topic == "mcp_server.docs_server" for b in result.bindings)
        assert not result.unresolved

    def test_missing_target_diagnostic(self) -> None:
        result = make_toolbox().resolve_tools({})
        assert result.missing_targets == ("docs_server",)
        assert result.bindings == ()
        assert result.unresolved

    def test_include_filter_and_missing_tools(self) -> None:
        selector = Toolboxes(Toolbox("docs_server", include=("search", "nonexistent")))  # include = BARE names (C5)
        result = selector.resolve_tools({"docs_server": make_record()})
        assert [b.name for b in result.bindings] == ["docs_server__search"]
        assert result.missing_tools == ("nonexistent",)  # missing_tools reported bare too

    def test_include_pins_bare_name_containing_double_underscore(self) -> None:
        # C6 at the EXPANSION+include layer: a server tool whose BARE name contains `__` must
        # expand to docs_server__a__b AND stay pinnable by its bare name `a__b` — the include
        # strip must recover `a__b`, not mangle it (removeprefix, not split("__")).
        record = make_record(tools=[CapabilityToolDef(name="a__b", parameters_json_schema={"type": "object", "properties": {}})])
        result = Toolboxes(Toolbox("docs_server", include=("a__b",))).resolve_tools({"docs_server": record})
        assert [b.name for b in result.bindings] == ["docs_server__a__b"]
        assert result.missing_tools == ()

    def test_empty_include_pins_nothing(self) -> None:
        # include=() pins ZERO tools — distinct from include=None (all tools).
        result = Toolboxes(Toolbox("docs_server", include=())).resolve_tools({"docs_server": make_record()})
        assert result.bindings == ()
        assert result.missing_tools == ()

    def test_empty_toolbox_record_resolves_to_no_bindings(self) -> None:
        # A toolbox advertising zero tools expands to zero bindings; an include miss is bare.
        result = Toolboxes(Toolbox("docs_server", include=("search",))).resolve_tools({"docs_server": make_record(tools=[])})
        assert result.bindings == ()
        assert result.missing_tools == ("search",)

    def test_scoped_spec_is_frozen(self) -> None:
        spec = Toolbox("docs_server", include=("search",))
        with pytest.raises(Exception):
            spec.include = ("other",)  # type: ignore[misc]

    def test_malformed_record_is_skipped_with_diagnostic(self) -> None:
        # Tolerant reader admits an empty dispatch_topic; binding expansion
        # must not crash the turn — it surfaces as invalid_targets instead.
        record = make_record(dispatch_topic="")
        result = make_toolbox().resolve_tools({"docs_server": record})
        assert result.invalid_targets == ("docs_server",)
        assert result.bindings == ()

    # -- view-owned filtering (D6): the resolver never sees stale/newer-schema records --

    def test_stale_record_hidden_by_view(self) -> None:
        # age 100s > 3*30 = 90 threshold -> the view collapses it to None, so the
        # resolver reports missing_targets (no advisory "use last-known" path).
        view = make_view(make_record(last_heartbeat_at=datetime.now(tz=timezone.utc) - timedelta(seconds=100)))
        result = make_toolbox().resolve_tools(view)
        assert result.missing_targets == ("docs_server",)
        assert result.bindings == ()

    def test_newer_schema_hidden_by_view(self) -> None:
        # A record from a newer schema major is filtered (and logged) by the view;
        # the resolver just sees None -> missing_targets.
        view = make_view(make_record(schema_version=2))
        result = make_toolbox().resolve_tools(view)
        assert result.missing_targets == ("docs_server",)
        assert result.bindings == ()

    def test_live_record_resolves_through_view(self) -> None:
        view = make_view(make_record())
        result = make_toolbox().resolve_tools(view)
        assert [b.name for b in result.bindings] == ["docs_server__search", "docs_server__fetch"]
        assert not result.unresolved

    def test_wrong_kind_record_is_rejected(self) -> None:
        # Over-pull guard: an MCP toolbox handle must not bind a record of a
        # different node_kind (e.g. a function tool node sharing the name).
        result = make_toolbox().resolve_tools({"docs_server": make_record(node_kind="tool")})
        assert result.wrong_kind_targets == ("docs_server",)
        assert result.bindings == ()
        assert result.unresolved


class TestSplitToolDeclarations:
    def test_separates_bindings_providers_and_selectors(self) -> None:
        from calfkit.nodes.tool import agent_tool

        def get_weather(city: str) -> str:
            return city

        tool_node = agent_tool(get_weather)
        toolbox = make_toolbox()
        scoped = Toolboxes(Toolbox("docs_server", include=("search",)))
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
        from calfkit.nodes.agent import StatelessAgent
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

        return StatelessAgent("a", subscribe_topics="a.in", model_client=FakeModel(), tools=list(tools))

    def test_ctor_sets_aside_selectors(self) -> None:
        toolbox = make_toolbox()
        agent = self.make_agent(toolbox)
        assert agent.tools == []
        assert agent._tool_selectors == [toolbox]

    def test_resolution_merges_bindings_into_registry(self) -> None:
        agent = self.make_agent(make_toolbox())
        registry: dict[str, ToolBinding] = {}
        agent._resolve_selector_tools({CAPABILITY_VIEW_RESOURCE_KEY: {"docs_server": make_record()}}, registry)
        assert sorted(registry) == ["docs_server__fetch", "docs_server__search"]

    def test_namespaced_toolbox_tool_does_not_collide_with_static(self, caplog: pytest.LogCaptureFixture) -> None:
        # C8: namespacing makes a toolbox tool `search` (advertised as docs_server__search)
        # disjoint from a static local tool named `search` — both coexist, no collision.
        from calfkit._vendor.pydantic_ai.tools import ToolDefinition

        agent = self.make_agent(make_toolbox())
        static = ToolBinding(tool_def=ToolDefinition(name="search"), dispatch_topic="static.topic")
        registry = {"search": static}
        with caplog.at_level("ERROR"):
            agent._resolve_selector_tools({CAPABILITY_VIEW_RESOURCE_KEY: {"docs_server": make_record()}}, registry)
        # Both survive under their own keys; the remote tools merge without shadowing the static.
        assert registry["search"] is static
        assert "docs_server__search" in registry and "docs_server__fetch" in registry
        assert not any("collides" in r.message for r in caplog.records)

    def test_collision_static_wins_and_logs(self, caplog: pytest.LogCaptureFixture) -> None:
        # The merge rule is unchanged — a static local binding wins over a discovered one on a
        # genuine key collision. Post-namespacing the collision is at the NAMESPACED name the
        # toolbox actually advertises (docs_server__search).
        from calfkit._vendor.pydantic_ai.tools import ToolDefinition

        agent = self.make_agent(make_toolbox())
        static = ToolBinding(tool_def=ToolDefinition(name="docs_server__search"), dispatch_topic="static.topic")
        registry = {"docs_server__search": static}
        with caplog.at_level("ERROR"):
            agent._resolve_selector_tools({CAPABILITY_VIEW_RESOURCE_KEY: {"docs_server": make_record()}}, registry)
        assert registry["docs_server__search"] is static  # remote never shadows local
        assert any("docs_server__search" in r.message for r in caplog.records)
        assert "docs_server__fetch" in registry  # non-colliding remote tool still merged

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
        assert sorted(registry) == ["docs_server__fetch", "docs_server__search"]  # turn proceeds
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

    def test_selection_surface_has_no_strict_parameter(self) -> None:
        import inspect

        toolboxes_params = inspect.signature(Toolboxes.__init__).parameters
        toolbox_params = inspect.signature(Toolbox.__init__).parameters
        assert "strict" not in toolboxes_params and "strict" not in toolbox_params
        assert "include" in toolbox_params

    def test_selector_result_has_no_strict_or_filtering_diagnostics(self) -> None:
        fields = set(SelectorResult.__dataclass_fields__)
        assert "strict" not in fields
        assert "stale_seconds" not in fields
        assert "skipped_newer_schema" not in fields
        # cardinality-neutral, de-MCP'd diagnostics (no singular toolbox_id)
        assert "toolbox_id" not in fields
        assert {"bindings", "missing_targets", "missing_tools", "invalid_targets", "wrong_kind_targets"} <= fields

    def test_resolution_error_is_gone(self) -> None:
        import calfkit.exceptions as exc

        assert not hasattr(exc, "MCPToolResolutionError")


class TestSelectorsAlwaysResolve:
    """Selector resolution runs on EVERY turn a selector exists (overrides-removal
    spec D1: the only skip gate was the per-run override pin, now removed)."""

    def test_selectors_resolve_on_every_turn(self) -> None:
        from calfkit.models.state import State
        from tests.test_tool_errors import _make_ctx

        agent = TestAgentResolution().make_agent(make_toolbox())
        ctx = _make_ctx(State())
        ctx._resources = {CAPABILITY_VIEW_RESOURCE_KEY: {"docs_server": make_record()}}
        registry: dict[str, ToolBinding] = {}
        agent._maybe_resolve_selectors(ctx, registry)
        assert sorted(registry) == ["docs_server__fetch", "docs_server__search"]
