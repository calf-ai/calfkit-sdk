"""``Tools`` — the identity-only handle to one or more function tool nodes.

The call-side counterpart to a deployed tool node (mirrors ``Toolboxes``): an agent
holds ``Tools("add", "subtract")`` and the schemas are discovered per turn from the
shared capability view. Resolution reuses ``resolve_capability`` with
``expected_kind="tool"`` (the over-pull guard), so ``Tools`` can never bind a toolbox
record. The ``strict`` flag is intentionally absent (mirrors MCP).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from calfkit.controlplane import ControlPlaneView
from calfkit.models.capability import CAPABILITY_VIEW_RESOURCE_KEY, CapabilityRecord, CapabilityToolDef, SelectorResult
from calfkit.models.tool_dispatch import ToolBinding, ToolSelector, split_tool_declarations
from calfkit.nodes.tool import Tools
from tests._capability_fakes import _FakeView  # dict-backed EnumerableCapabilityView (adds snapshot())
from tests.test_controlplane_view import _FakeTable  # the substrate's dict-backed GroupedTableReader fake
from tests.test_tool_binding import make_tool_node  # builds a real ToolNodeDef (eager tool node)


def make_tool_record(name: str = "add", *, dispatch: str | None = None, **overrides: Any) -> CapabilityRecord:
    now = datetime.now(tz=timezone.utc)
    defaults: dict[str, Any] = dict(
        started_at=now,
        last_heartbeat_at=now,
        heartbeat_interval=30.0,
        node_kind="tool",
        dispatch_topic=dispatch or f"tool.{name}.input",
        tools=[CapabilityToolDef(name=name, parameters_json_schema={"type": "object", "properties": {}})],
        content_updated_at=now,
    )
    defaults.update(overrides)
    return CapabilityRecord(**defaults)


def make_agent(*tools: Any) -> Any:
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


class TestToolsConstruction:
    def test_varargs(self) -> None:
        assert Tools("add", "subtract").names == ("add", "subtract")

    def test_names_kwarg(self) -> None:
        assert Tools(names=["add", "subtract"]).names == ("add", "subtract")

    def test_order_preserving_dedupe(self) -> None:
        assert Tools("add", "subtract", "add").names == ("add", "subtract")
        assert Tools("add") == Tools("add", "add")  # dedupe gives clean value semantics

    def test_rejects_mixed_positional_and_kwarg(self) -> None:
        with pytest.raises(ValueError, match="positionally or via names="):
            Tools("a", names=["b"])

    def test_rejects_empty(self) -> None:
        with pytest.raises(ValueError, match="at least one"):
            Tools()
        with pytest.raises(ValueError, match="at least one"):
            Tools(names=[])

    def test_rejects_empty_name_string(self) -> None:
        with pytest.raises(ValueError, match="non-empty"):
            Tools("add", "")

    def test_frozen_and_hashable_value_semantics(self) -> None:
        t = Tools("add", "subtract")
        with pytest.raises(Exception):
            t.names = ("x",)  # type: ignore[misc]
        assert len({Tools("add"), Tools("add")}) == 1


class TestToolsIsASelector:
    def test_satisfies_tool_selector(self) -> None:
        assert isinstance(Tools("add"), ToolSelector)

    def test_splits_as_a_selector(self) -> None:
        bindings, selectors = split_tool_declarations([Tools("add")])
        assert bindings == [] and selectors == [Tools("add")]

    def test_agent_ctor_sets_aside_the_selector(self) -> None:
        agent = make_agent(Tools("add"))
        assert agent.tools == []  # not expanded at construction
        assert agent._tool_selectors == [Tools("add")]  # trips the worker's read-side view gate


class TestToolsResolveTools:
    def test_resolves_single_name_to_one_validatorless_binding(self) -> None:
        result = Tools("add").resolve_tools({"add": make_tool_record("add")})
        assert isinstance(result, SelectorResult)
        assert [b.name for b in result.bindings] == ["add"]
        assert result.bindings[0].dispatch_topic == "tool.add.input"
        assert result.bindings[0].validator is None  # discovered = no local validator (agent validates against the advertised schema at dispatch)
        assert not result.unresolved

    def test_resolves_multiple_names(self) -> None:
        view = {"add": make_tool_record("add"), "sub": make_tool_record("sub")}
        result = Tools("add", "sub").resolve_tools(view)
        assert [b.name for b in result.bindings] == ["add", "sub"]
        assert not result.unresolved

    def test_missing_name_is_a_missing_target_and_others_still_resolve(self) -> None:
        result = Tools("add", "ghost").resolve_tools({"add": make_tool_record("add")})
        assert [b.name for b in result.bindings] == ["add"]
        assert result.missing_targets == ("ghost",)
        assert result.unresolved

    def test_toolbox_record_is_rejected_as_wrong_kind(self) -> None:
        # The over-pull guard: Tools references single-tool nodes; a toolbox record at that
        # key is rejected, so Tools can never silently absorb a whole multi-tool MCP toolbox.
        result = Tools("github").resolve_tools({"github": make_tool_record("github", node_kind="toolbox")})
        assert result.bindings == ()
        assert result.wrong_kind_targets == ("github",)
        assert result.unresolved

    def test_resolves_through_a_real_control_plane_view(self) -> None:
        view = ControlPlaneView(_FakeTable({"add": {"w1": make_tool_record("add")}}), CapabilityRecord)
        result = Tools("add").resolve_tools(view)
        assert [b.name for b in result.bindings] == ["add"]
        assert not result.unresolved


class TestToolsThroughAgent:
    def test_resolution_merges_discovered_bindings_into_the_registry(self) -> None:
        agent = make_agent(Tools("add", "sub"))
        registry: dict[str, ToolBinding] = {}
        view = {"add": make_tool_record("add"), "sub": make_tool_record("sub")}
        agent._resolve_selector_tools({CAPABILITY_VIEW_RESOURCE_KEY: view}, registry)
        assert sorted(registry) == ["add", "sub"]
        assert all(registry[n].validator is None for n in registry)  # discovered = no local validator (schema-checked at dispatch)

    def test_eager_static_tool_wins_on_collision(self, caplog: pytest.LogCaptureFixture) -> None:
        # The per-turn registry merge: when a discovered binding's name is already in the
        # registry (e.g. a pre-seeded static/override binding), the existing one wins and the
        # collision is error-logged. The construction-time contract forbids pairing an eager
        # tool node with a same-named Tools handle — that combination raises before any turn
        # (see TestToolSurfaceContract); this exercises the residual runtime merge path.
        from calfkit._vendor.pydantic_ai.tools import ToolDefinition

        agent = make_agent(Tools("add"))
        static = ToolBinding(tool_def=ToolDefinition(name="add"), dispatch_topic="static.add.input", validator=lambda a: a)
        registry = {"add": static}
        with caplog.at_level("ERROR"):
            agent._resolve_selector_tools({CAPABILITY_VIEW_RESOURCE_KEY: {"add": make_tool_record("add")}}, registry)
        assert registry["add"] is static  # existing (eager) wins; discovered never shadows it
        assert registry["add"].validator is not None  # local fail-fast validation preserved
        assert any("add" in r.message for r in caplog.records)


class TestToolsDiscoverMode:
    """``Tools(discover=True)`` — open-ended discovery of every live tool node (spec §15.1)."""

    def test_discover_constructs_with_no_names(self) -> None:
        t = Tools(discover=True)
        assert t.discover is True
        assert t.names == ()

    def test_discover_with_positional_names_raises(self) -> None:
        with pytest.raises(ValueError, match="no tool names"):
            Tools("add", discover=True)

    def test_discover_with_names_kwarg_raises(self) -> None:
        with pytest.raises(ValueError, match="no tool names"):
            Tools(names=["add"], discover=True)

    def test_named_handle_defaults_to_discover_false(self) -> None:
        assert Tools("add").discover is False

    def test_empty_with_discover_false_raises(self) -> None:
        # The fail-loud rail: a bare/empty handle is never an implicit "everything".
        with pytest.raises(ValueError, match="at least one"):
            Tools(discover=False)

    def test_discover_value_semantics(self) -> None:
        assert Tools(discover=True) == Tools(discover=True)
        assert len({Tools(discover=True), Tools(discover=True)}) == 1
        assert Tools(discover=True) != Tools("add")

    def test_discover_resolves_all_tool_nodes_excluding_toolboxes(self) -> None:
        view = _FakeView(
            {
                "add": make_tool_record("add"),
                "sub": make_tool_record("sub"),
                "github": make_tool_record("search", dispatch="mcp_server.github", node_kind="toolbox"),
            }
        )
        result = Tools(discover=True).resolve_tools(view)
        assert sorted(b.name for b in result.bindings) == ["add", "sub"]  # toolbox record excluded
        assert all(b.validator is None for b in result.bindings)  # discovered = no local validator (schema-checked at dispatch)
        assert not result.unresolved


class TestToolSurfaceContract:
    """The construction-time tool-surface contract (spec §15.3, L18), enforced in both
    ``Agent(tools=...)`` and ``add_tools``:
      (1) no duplicate tool names across eager bindings + named ``Tools``;
      (2) ``Tools(discover=True)`` owns the tool-node surface (no eager tool node or named
          ``Tools`` alongside it; a ``Toolboxes``/eager toolbox node — a different kind — may).
    """

    # --- (1) no duplicate tool names ------------------------------------------------
    def test_duplicate_named_handles_raise(self) -> None:
        with pytest.raises(ValueError, match="duplicate tool name"):
            make_agent(Tools("add"), Tools("add"))

    def test_eager_node_and_named_same_name_raises(self) -> None:
        with pytest.raises(ValueError, match="duplicate tool name"):
            make_agent(make_tool_node("add"), Tools("add"))

    def test_eager_node_and_named_same_name_raises_order_independent(self) -> None:
        with pytest.raises(ValueError, match="duplicate tool name"):
            make_agent(Tools("add"), make_tool_node("add"))

    # --- (2) discover owns the tool-node surface ------------------------------------
    def test_discover_with_eager_tool_node_raises(self) -> None:
        with pytest.raises(ValueError, match="tool-node surface"):
            make_agent(make_tool_node("add"), Tools(discover=True))

    def test_discover_with_named_tools_raises(self) -> None:
        with pytest.raises(ValueError, match="tool-node surface"):
            make_agent(Tools("add"), Tools(discover=True))

    # --- legal combinations ---------------------------------------------------------
    def test_discover_alone_is_legal(self) -> None:
        agent = make_agent(Tools(discover=True))
        assert agent._tool_selectors == [Tools(discover=True)]

    def test_discover_composes_with_toolboxes(self) -> None:
        from calfkit.nodes.toolbox import Toolboxes

        agent = make_agent(Tools(discover=True), Toolboxes("fs"))
        assert Tools(discover=True) in agent._tool_selectors
        assert Toolboxes("fs") in agent._tool_selectors

    def test_distinct_named_handles_are_legal(self) -> None:
        agent = make_agent(Tools("add"), Tools("sub"))
        assert agent._tool_selectors == [Tools("add"), Tools("sub")]

    def test_distinct_eager_nodes_are_legal(self) -> None:
        agent = make_agent(make_tool_node("add"), make_tool_node("sub"))
        assert [b.name for b in agent.tools] == ["add", "sub"]

    # --- add_tools enforces the contract incrementally ------------------------------
    def test_add_tools_discover_onto_eager_node_raises(self) -> None:
        agent = make_agent(make_tool_node("add"))
        with pytest.raises(ValueError, match="tool-node surface"):
            agent.add_tools(Tools(discover=True))

    def test_add_tools_duplicate_onto_named_raises(self) -> None:
        agent = make_agent(Tools("add"))
        with pytest.raises(ValueError, match="duplicate tool name"):
            agent.add_tools(make_tool_node("add"))

    def test_add_tools_discover_onto_named_raises(self) -> None:
        # The contract re-validates the FULL accumulated surface: a discover handle added onto
        # an agent that already holds a named Tools is the discover-exclusivity violation.
        agent = make_agent(Tools("add"))
        with pytest.raises(ValueError, match="tool-node surface"):
            agent.add_tools(Tools(discover=True))

    def test_add_tools_named_onto_discover_raises(self) -> None:
        # ...and the reverse: a named Tools added onto an agent already in discover mode.
        agent = make_agent(Tools(discover=True))
        with pytest.raises(ValueError, match="tool-node surface"):
            agent.add_tools(Tools("add"))

    def test_add_tools_accumulates_disjoint_surfaces(self) -> None:
        agent = make_agent(make_tool_node("add"))
        agent.add_tools(make_tool_node("sub"))
        agent.add_tools(Tools("mul"))
        assert [b.name for b in agent.tools] == ["add", "sub"]
        assert agent._tool_selectors == [Tools("mul")]

    def test_add_tools_raise_leaves_surface_unchanged(self) -> None:
        # validate-before-commit: a failed add must not mutate the live surface — all three
        # pieces of tool-surface state (bindings, selectors, eager tool nodes) stay as they were.
        agent = make_agent(make_tool_node("add"))
        with pytest.raises(ValueError, match="duplicate tool name"):
            agent.add_tools(make_tool_node("add"))
        assert [b.name for b in agent.tools] == ["add"]
        assert agent._tool_selectors == []
        assert [n.name for n in agent._eager_tool_nodes] == ["add"]


class TestDiscoverDiagnostics:
    """Discover-mode diagnostics (spec §15.4): a per-turn DEBUG count; healthy-empty is
    silent by design; a degraded view still warns; per-run overrides skip discover."""

    def test_discover_binds_all_tool_nodes_and_logs_count(self, caplog: pytest.LogCaptureFixture) -> None:
        agent = make_agent(Tools(discover=True))
        registry: dict[str, ToolBinding] = {}
        view = _FakeView(
            {
                "add": make_tool_record("add"),
                "sub": make_tool_record("sub"),
                "github": make_tool_record("search", dispatch="mcp_server.github", node_kind="toolbox"),
            }
        )
        with caplog.at_level("DEBUG"):
            agent._resolve_selector_tools({CAPABILITY_VIEW_RESOURCE_KEY: view}, registry)
        assert sorted(registry) == ["add", "sub"]  # every live tool node; the toolbox is excluded
        assert any("discover mode resolved 2 tool node(s)" in r.getMessage() for r in caplog.records)

    def test_discover_on_healthy_empty_view_is_silent_with_debug_count(self, caplog: pytest.LogCaptureFixture) -> None:
        agent = make_agent(Tools(discover=True))
        registry: dict[str, ToolBinding] = {}
        with caplog.at_level("DEBUG"):
            agent._resolve_selector_tools({CAPABILITY_VIEW_RESOURCE_KEY: _FakeView({})}, registry)
        assert registry == {}
        assert any("discover mode resolved 0 tool node(s)" in r.getMessage() for r in caplog.records)
        # An empty cluster is a legitimate state, not a misconfiguration — no WARNING (don't cry wolf).
        assert not any(r.levelname == "WARNING" for r in caplog.records)

    def test_discover_with_poisoned_record_warns_and_binds_the_healthy(self, caplog: pytest.LogCaptureFixture) -> None:
        # A poisoned tool record (empty dispatch_topic fails ToolBinding expansion) degrades to
        # invalid_targets: the healthy node still binds, the unresolved WARNING names the poisoned
        # one, and the DEBUG count reflects the successfully-resolved nodes.
        agent = make_agent(Tools(discover=True))
        registry: dict[str, ToolBinding] = {}
        # Override dispatch_topic to "" directly: make_tool_record's `dispatch=` param coalesces ""
        # to the default, so poison it via the override (empty topic fails ToolBinding expansion).
        view = _FakeView({"add": make_tool_record("add"), "broken": make_tool_record("broken", dispatch_topic="")})
        with caplog.at_level("DEBUG"):
            agent._resolve_selector_tools({CAPABILITY_VIEW_RESOURCE_KEY: view}, registry)
        assert "add" in registry and "broken" not in registry  # healthy survives, poisoned dropped
        assert any("discover mode resolved 1 tool node(s)" in r.getMessage() for r in caplog.records)
        assert any(r.levelname == "WARNING" and "broken" in r.getMessage() for r in caplog.records)

    def test_discover_against_degraded_view_still_warns_and_binds(self, caplog: pytest.LogCaptureFixture) -> None:
        class _DegradedView(_FakeView):
            status = "degraded"
            failure = None

        agent = make_agent(Tools(discover=True))
        registry: dict[str, ToolBinding] = {}
        view = _DegradedView({"add": make_tool_record("add")})
        with caplog.at_level("WARNING"):
            agent._resolve_selector_tools({CAPABILITY_VIEW_RESOURCE_KEY: view}, registry)
        assert "add" in registry  # binds whatever the frozen view holds
        assert any(r.levelname == "WARNING" and "degraded" in r.getMessage() for r in caplog.records)

    def test_per_run_overrides_skip_discover(self) -> None:
        from types import SimpleNamespace

        from calfkit.models.state import OverridesState

        agent = make_agent(Tools(discover=True))
        registry: dict[str, ToolBinding] = {}
        # Overrides pin the exact surface for the turn; discovery must not widen it.
        ctx = SimpleNamespace(
            state=SimpleNamespace(overrides=OverridesState(override_agent_tools=[])),
            resources={CAPABILITY_VIEW_RESOURCE_KEY: _FakeView({"add": make_tool_record("add")})},
        )
        agent._maybe_resolve_selectors(ctx, registry)  # type: ignore[arg-type]
        assert registry == {}  # discover did not run — the view's "add" was not bound


class TestToolsExport:
    def test_importable_from_calfkit_and_nodes(self) -> None:
        import calfkit
        from calfkit import Tools as TopLevel
        from calfkit.nodes import Tools as FromNodes
        from calfkit.nodes.tool import Tools as FromModule

        assert TopLevel is FromModule is FromNodes
        assert "Tools" in calfkit.__all__


class TestBareStringGuard:
    def test_names_kwarg_rejects_bare_string(self) -> None:
        with pytest.raises(ValueError, match="not a bare string"):
            Tools(names="abc")
