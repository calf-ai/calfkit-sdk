"""``Toolboxes``/``Toolbox`` — the dual-mode family selector for toolboxes (spec D1-D6).

The toolbox counterpart of ``Tools``: ``Toolboxes`` declares the agent's toolbox surface
(named entries XOR discover), and ``Toolbox`` is the frozen per-box entry spec carrying the
``include=`` trust boundary. ``Toolbox`` is deliberately *not* a selector — it exists only
inside ``Toolboxes(...)``.

Phase 1 covers the construction surface; resolution (Phase 2) extends this file.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from calfkit.models.capability import CAPABILITY_VIEW_RESOURCE_KEY, CapabilityRecord, CapabilityToolDef, SelectorResult
from calfkit.models.tool_dispatch import ToolSelector
from calfkit.nodes.tool import Tools
from calfkit.nodes.toolbox import Toolbox, Toolboxes
from tests._capability_fakes import _FakeView
from tests.test_mcp_toolbox import make_toolbox  # deployed MCPToolboxNode builder
from tests.test_tools_selector import make_agent, make_tool_record


class TestToolboxSpec:
    def test_name_and_default_include(self) -> None:
        box = Toolbox("github")
        assert box.name == "github"
        assert box.include is None

    def test_include_coerced_to_tuple(self) -> None:
        assert Toolbox("github", include=["a", "b"]).include == ("a", "b")

    def test_empty_include_is_legal_explicit_exclusion(self) -> None:
        assert Toolbox("github", include=()).include == ()

    def test_rejects_empty_name(self) -> None:
        with pytest.raises(ValueError, match="non-empty"):
            Toolbox("")

    def test_frozen_value_semantics(self) -> None:
        box = Toolbox("github", include=["a"])
        with pytest.raises(Exception):
            box.name = "x"  # type: ignore[misc]
        assert Toolbox("github", include=["a"]) == Toolbox("github", include=("a",))
        assert len({Toolbox("github"), Toolbox("github")}) == 1

    def test_is_not_a_tool_selector(self) -> None:
        assert not isinstance(Toolbox("github"), ToolSelector)


class TestToolboxesConstruction:
    def test_bare_names_desugar_to_entries(self) -> None:
        assert Toolboxes("github", "slack").entries == (Toolbox("github"), Toolbox("slack"))

    def test_mixed_entries_canonical_form(self) -> None:
        got = Toolboxes(Toolbox("github", include=("create_issue",)), "jira")
        assert got.entries == (Toolbox("github", include=("create_issue",)), Toolbox("jira"))

    def test_entries_kwarg(self) -> None:
        got = Toolboxes(entries=["github", Toolbox("slack")])
        assert got.entries == (Toolbox("github"), Toolbox("slack"))

    def test_discover_mode_carries_no_entries(self) -> None:
        handle = Toolboxes(discover=True)
        assert handle.discover is True
        assert handle.entries == ()

    def test_rejects_positional_entries_with_discover(self) -> None:
        with pytest.raises(ValueError, match="takes no"):
            Toolboxes("github", discover=True)

    def test_rejects_kwarg_entries_with_discover(self) -> None:
        # The check must cover BOTH entry channels — never silently prefer discover.
        with pytest.raises(ValueError, match="takes no"):
            Toolboxes(entries=["github"], discover=True)

    def test_rejects_mixed_positional_and_kwarg(self) -> None:
        # The message must direct to entries= (not the siblings' names=).
        with pytest.raises(ValueError, match="entries="):
            Toolboxes("github", entries=["slack"])

    def test_rejects_empty(self) -> None:
        with pytest.raises(ValueError, match="at least one"):
            Toolboxes()
        with pytest.raises(ValueError, match="at least one"):
            Toolboxes(entries=[])

    def test_rejects_blank_name(self) -> None:
        with pytest.raises(ValueError, match="non-empty"):
            Toolboxes("github", "")

    def test_rejects_byte_identical_duplicates(self) -> None:
        # A duplicate is a conflict, never silently collapsed (deliberate divergence
        # from the names rail's dedupe — entries carry policy, not just identity).
        with pytest.raises(ValueError, match="[Dd]uplicate"):
            Toolboxes("github", "github")

    def test_rejects_same_name_different_policy(self) -> None:
        with pytest.raises(ValueError, match="[Dd]uplicate"):
            Toolboxes("github", Toolbox("github", include=("a",)))

    def test_rejects_kwarg_form_duplicates(self) -> None:
        with pytest.raises(ValueError, match="[Dd]uplicate"):
            Toolboxes(entries=[Toolbox("github"), Toolbox("github")])

    def test_frozen_hashable_value_semantics(self) -> None:
        handle = Toolboxes("github", Toolbox("slack", include=["post"]))
        with pytest.raises(Exception):
            handle.discover = True  # type: ignore[misc]
        assert handle == Toolboxes("github", Toolbox("slack", include=("post",)))
        assert len({Toolboxes(discover=True), Toolboxes(discover=True)}) == 1
        # Hashability hinges on the canonical tuple-of-Toolbox form AND include
        # tuple-coercion both holding — a list anywhere breaks it.
        assert hash(Toolboxes("github", Toolbox("slack", include=["post"])))


# ---------------------------------------------------------------------------
# Phase 2 — resolution (spec D7)
# ---------------------------------------------------------------------------


def make_toolbox_record(name: str = "github", tool_names: tuple[str, ...] = ("search", "create_issue"), **overrides: Any) -> CapabilityRecord:
    now = datetime.now(tz=timezone.utc)
    defaults: dict[str, Any] = dict(
        started_at=now,
        last_heartbeat_at=now,
        heartbeat_interval=30.0,
        node_kind="toolbox",
        dispatch_topic=f"mcp_server.{name}",
        tools=[CapabilityToolDef(name=n, parameters_json_schema={"type": "object", "properties": {}}) for n in tool_names],
        content_updated_at=now,
    )
    defaults.update(overrides)
    return CapabilityRecord(**defaults)


class TestToolboxesResolveTools:
    def test_named_entry_resolves_namespaced_bindings(self) -> None:
        result = Toolboxes("github").resolve_tools({"github": make_toolbox_record()})
        assert isinstance(result, SelectorResult)
        assert [b.name for b in result.bindings] == ["github__search", "github__create_issue"]

    def test_include_scopes_bare_names(self) -> None:
        result = Toolboxes(Toolbox("github", include=("search",))).resolve_tools({"github": make_toolbox_record()})
        assert [b.name for b in result.bindings] == ["github__search"]

    def test_multiple_entries_concatenate_in_order(self) -> None:
        view = {"github": make_toolbox_record("github", ("search",)), "slack": make_toolbox_record("slack", ("post",))}
        result = Toolboxes("github", "slack").resolve_tools(view)
        assert [b.name for b in result.bindings] == ["github__search", "slack__post"]

    def test_missing_box_is_missing_target_and_others_still_resolve(self) -> None:
        result = Toolboxes("github", "ghost").resolve_tools({"github": make_toolbox_record("github", ("search",))})
        assert [b.name for b in result.bindings] == ["github__search"]
        assert result.missing_targets == ("ghost",)
        assert result.unresolved

    def test_missing_include_names_aggregate_into_missing_tools(self) -> None:
        # The FIFTH channel — the one Tools named mode has no include= to populate.
        result = Toolboxes(Toolbox("github", include=("search", "nope"))).resolve_tools({"github": make_toolbox_record()})
        assert [b.name for b in result.bindings] == ["github__search"]
        assert result.missing_tools == ("nope",)
        assert result.unresolved

    def test_empty_include_resolves_zero_bindings_silently(self) -> None:
        # include=() is explicit exclusion: binds the box, zero tools, NO degrade signal.
        result = Toolboxes(Toolbox("github", include=())).resolve_tools({"github": make_toolbox_record()})
        assert result.bindings == ()
        assert not result.unresolved

    def test_tool_record_is_rejected_as_wrong_kind(self) -> None:
        result = Toolboxes("add").resolve_tools({"add": make_tool_record("add")})
        assert result.bindings == ()
        assert result.wrong_kind_targets == ("add",)

    def test_poisoned_record_degrades_to_invalid_target(self) -> None:
        result = Toolboxes("github").resolve_tools({"github": make_toolbox_record(dispatch_topic="")})
        assert result.bindings == ()
        assert result.invalid_targets == ("github",)

    def test_discover_binds_every_toolbox_and_skips_other_kinds(self) -> None:
        view = _FakeView(
            github=make_toolbox_record("github", ("search",)),
            slack=make_toolbox_record("slack", ("post",)),
            add=make_tool_record("add"),
        )
        result = Toolboxes(discover=True).resolve_tools(view)
        assert sorted(b.name for b in result.bindings) == ["github__search", "slack__post"]
        assert not result.unresolved

    def test_discover_on_empty_view_resolves_empty_and_silent(self) -> None:
        result = Toolboxes(discover=True).resolve_tools(_FakeView())
        assert result.bindings == ()
        assert not result.unresolved


class TestToolboxesDiscoverDebugLog:
    def test_discover_resolution_logs_debug_count(self, caplog: Any) -> None:
        import logging

        agent = make_agent(Toolboxes(discover=True))
        registry: dict[str, Any] = {}
        view = _FakeView(github=make_toolbox_record("github", ("search", "create_issue")))
        with caplog.at_level(logging.DEBUG, logger="calfkit.nodes.agent"):
            agent._resolve_selector_tools({CAPABILITY_VIEW_RESOURCE_KEY: view}, registry)
        assert any("discover mode resolved 2 toolbox tool(s)" in r.getMessage() for r in caplog.records)
        assert set(registry) == {"github__search", "github__create_issue"}


# ---------------------------------------------------------------------------
# Phase 3 — agent assembly rules (spec D3, D5, D6, §3)
# ---------------------------------------------------------------------------


class TestBareToolboxTeachingError:
    def test_bare_toolbox_in_tools_raises_teaching_valueerror(self) -> None:
        # Must be the teaching ValueError, not split_tool_declarations' generic TypeError.
        with pytest.raises(ValueError, match=r"Toolboxes\("):
            make_agent(Toolbox("github"))

    def test_alias_shaped_mistake_gets_the_same_teaching(self) -> None:
        with pytest.raises(ValueError, match=r"Toolboxes\("):
            make_agent(Toolbox("github", include=("search",)))


class TestDuplicateBoxAcrossDeclarations:
    def test_duplicate_box_across_handles_raises(self) -> None:
        with pytest.raises(ValueError, match="[Dd]uplicate"):
            make_agent(Toolboxes("github"), Toolboxes(Toolbox("github", include=("a",))))

    def test_eager_node_counts_as_a_declaration(self) -> None:
        with pytest.raises(ValueError, match="[Dd]uplicate"):
            make_agent(make_toolbox("github"), Toolboxes("github"))

    def test_eager_node_counts_order_independent(self) -> None:
        with pytest.raises(ValueError, match="[Dd]uplicate"):
            make_agent(Toolboxes("github"), make_toolbox("github"))

    def test_add_tools_after_construction_validates_before_commit(self) -> None:
        agent = make_agent(Toolboxes("github"))
        with pytest.raises(ValueError, match="[Dd]uplicate"):
            agent.add_tools(Toolboxes("github"))
        assert agent._tool_selectors == [Toolboxes("github")]  # surface unchanged

    def test_distinct_boxes_across_handles_are_legal(self) -> None:
        agent = make_agent(Toolboxes("github"), Toolboxes("slack"))
        assert agent._tool_selectors == [Toolboxes("github"), Toolboxes("slack")]


class TestDiscoverExclusivity:
    def test_named_handle_beside_discover_raises(self) -> None:
        with pytest.raises(ValueError, match="exclusive author"):
            make_agent(Toolboxes(discover=True), Toolboxes("github"))

    def test_duplicate_discover_raises(self) -> None:
        # Messaging-style strength: a redundant second discover handle is an error.
        with pytest.raises(ValueError, match="exclusive author"):
            make_agent(Toolboxes(discover=True), Toolboxes(discover=True))

    def test_eager_toolbox_node_beside_discover_raises(self) -> None:
        with pytest.raises(ValueError, match="exclusive author"):
            make_agent(Toolboxes(discover=True), make_toolbox("github"))

    def test_exclusivity_checked_before_duplicates(self) -> None:
        # Multi-violation list: the exclusivity message wins (spec §3 precedence).
        with pytest.raises(ValueError, match="exclusive author"):
            make_agent(Toolboxes(discover=True), Toolboxes("a"), Toolboxes(Toolbox("a", include=())))

    def test_discover_alone_is_legal(self) -> None:
        agent = make_agent(Toolboxes(discover=True))
        assert agent._tool_selectors == [Toolboxes(discover=True)]


class TestCrossKindCoexistence:
    def test_toolbox_discover_beside_tools_discover_is_legal(self) -> None:
        agent = make_agent(Toolboxes(discover=True), Tools(discover=True))
        assert Toolboxes(discover=True) in agent._tool_selectors
        assert Tools(discover=True) in agent._tool_selectors

    def test_named_toolboxes_beside_named_tools_is_legal(self) -> None:
        agent = make_agent(Toolboxes("github"), Tools("add"))
        assert agent._tool_selectors == [Toolboxes("github"), Tools("add")]

    def test_tools_exclusivity_message_names_the_new_surface(self) -> None:
        # The old "(an MCPToolbox may)" parenthetical becomes false post-change.
        with pytest.raises(ValueError, match=r"a Toolboxes\(...\) or eager toolbox node may"):
            make_agent(Tools(discover=True), Tools("add"))


# ---------------------------------------------------------------------------
# Phase 4 — alias, exports, node rewire, worker gate (spec D3, D8, D9)
# ---------------------------------------------------------------------------


class TestAliasAndExports:
    def test_mcptoolbox_is_an_alias_of_toolbox(self) -> None:
        import calfkit
        import calfkit.mcp

        assert calfkit.MCPToolbox is Toolbox
        assert calfkit.mcp.MCPToolbox is Toolbox

    def test_top_level_exports(self) -> None:
        import calfkit

        assert calfkit.Toolboxes is Toolboxes
        assert calfkit.Toolbox is Toolbox
        assert {"Toolboxes", "Toolbox", "MCPToolbox"} <= set(calfkit.__all__)

    def test_nodes_package_exports_beside_tools(self) -> None:
        from calfkit import nodes

        assert nodes.Toolboxes is Toolboxes
        assert nodes.Toolbox is Toolbox
        assert {"Toolboxes", "Toolbox"} <= set(nodes.__all__)

    def test_old_selector_idiom_raises_the_teaching_error(self) -> None:
        # The alias caveat (ADR-0045): old code imports and constructs, then fails
        # loud at agent construction with the wrap-it hint — never silently.
        from calfkit.mcp import MCPToolbox

        with pytest.raises(ValueError, match=r"Toolboxes\("):
            make_agent(MCPToolbox("github"))


class TestNodeRewire:
    def test_select_is_gone(self) -> None:
        assert not hasattr(make_toolbox("github"), "select")

    def test_node_resolves_identically_to_naming_its_toolbox(self) -> None:
        view = {"github": make_toolbox_record()}
        assert make_toolbox("github").resolve_tools(view) == Toolboxes("github").resolve_tools(view)


class TestWorkerTeachingGate:
    def test_add_nodes_rejects_toolbox_entry_with_teaching_message(self) -> None:
        # The reference-detection gate widens to Toolbox so the alias-shaped mistake
        # keeps its "belongs in Agent(tools=[...])" pointer (the framework's literal
        # message, worker.py — spec D3 worker parity).
        from calfkit.client import Client
        from calfkit.worker.worker import Worker

        worker = Worker(Client.connect("kafka:9092"))
        with pytest.raises(TypeError, match=r"MCPToolboxNode\(") as excinfo:
            worker.add_nodes(Toolbox("github"))  # type: ignore[arg-type]
        assert "reference" in str(excinfo.value).lower()

    def test_rejects_garbage_entry_types(self) -> None:
        with pytest.raises(TypeError, match="toolbox names or Toolbox specs"):
            Toolboxes(123)  # type: ignore[arg-type]


class TestAddToolsIncremental:
    """The rules re-validate accumulated state on every add_tools call, each raise
    leaving the committed surface unchanged (validate-before-commit, all raise types)."""

    def test_add_discover_onto_named_raises_surface_unchanged(self) -> None:
        agent = make_agent(Toolboxes("github"))
        with pytest.raises(ValueError, match="exclusive author"):
            agent.add_tools(Toolboxes(discover=True))
        assert agent._tool_selectors == [Toolboxes("github")]
        assert agent.tools == [] and agent._eager_tool_nodes == []

    def test_add_named_onto_discover_raises_surface_unchanged(self) -> None:
        agent = make_agent(Toolboxes(discover=True))
        with pytest.raises(ValueError, match="exclusive author"):
            agent.add_tools(Toolboxes("github"))
        assert agent._tool_selectors == [Toolboxes(discover=True)]

    def test_add_eager_node_onto_discover_raises_surface_unchanged(self) -> None:
        agent = make_agent(Toolboxes(discover=True))
        with pytest.raises(ValueError, match="exclusive author"):
            agent.add_tools(make_toolbox("github"))
        assert agent._tool_selectors == [Toolboxes(discover=True)]

    def test_add_bare_toolbox_raises_teaching_surface_unchanged(self) -> None:
        agent = make_agent(Toolboxes("github"))
        with pytest.raises(ValueError, match=r"Toolboxes\("):
            agent.add_tools(Toolbox("slack"))
        assert agent._tool_selectors == [Toolboxes("github")]


class TestFiveChannelAggregation:
    def test_all_five_channels_aggregate_across_entries_in_one_handle(self) -> None:
        # One handle tripping every channel at once — proves per-channel extend
        # (not overwrite) across entries, independent of the shared loop body.
        view = _FakeView(
            gh=make_toolbox_record("gh", ("ok", "other")),
            add=make_tool_record("add"),
            poisoned=make_toolbox_record("poisoned", ("x",), dispatch_topic=""),
        )
        result = Toolboxes(Toolbox("gh", include=("ok", "miss")), "ghost", "add", "poisoned").resolve_tools(view)
        assert [b.name for b in result.bindings] == ["gh__ok"]
        assert result.missing_tools == ("miss",)
        assert result.missing_targets == ("ghost",)
        assert result.wrong_kind_targets == ("add",)
        assert result.invalid_targets == ("poisoned",)
        assert result.unresolved

    def test_unresolved_named_selection_warns_at_agent_level(self, caplog: Any) -> None:
        import logging

        agent = make_agent(Toolboxes("ghost"))
        registry: dict[str, Any] = {}
        with caplog.at_level(logging.WARNING, logger="calfkit.nodes.agent"):
            agent._resolve_selector_tools({CAPABILITY_VIEW_RESOURCE_KEY: _FakeView()}, registry)
        assert any("partially unresolved" in r.getMessage() for r in caplog.records)


class TestBareStringGuard:
    """Family-wide decision (post-review, 2026-07-19): a bare string where a sequence is
    expected raises instead of silently iterating character-wise."""

    def test_entries_kwarg_rejects_bare_string(self) -> None:
        with pytest.raises(ValueError, match="not a bare string"):
            Toolboxes(entries="jira")

    def test_include_rejects_bare_string(self) -> None:
        with pytest.raises(ValueError, match="not a bare string"):
            Toolbox("github", include="search")
