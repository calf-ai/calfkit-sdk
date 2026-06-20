"""``Tools`` — the identity-only handle to one or more function tool nodes.

The call-side counterpart to a deployed tool node (mirrors ``MCPToolbox``): an agent
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
from tests.test_controlplane_view import _FakeTable  # the substrate's dict-backed GroupedTableReader fake


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
        assert result.bindings[0].validator is None  # discovered = schema-only (node validates on receipt)
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
        assert all(registry[n].validator is None for n in registry)  # discovered = schema-only

    def test_eager_static_tool_wins_on_collision(self, caplog: pytest.LogCaptureFixture) -> None:
        # Tools + an eager tool of the same name: the eager (locally validated) binding wins.
        from calfkit._vendor.pydantic_ai.tools import ToolDefinition

        agent = make_agent(Tools("add"))
        static = ToolBinding(tool_def=ToolDefinition(name="add"), dispatch_topic="static.add.input", validator=lambda a: a)
        registry = {"add": static}
        with caplog.at_level("ERROR"):
            agent._resolve_selector_tools({CAPABILITY_VIEW_RESOURCE_KEY: {"add": make_tool_record("add")}}, registry)
        assert registry["add"] is static  # existing (eager) wins; discovered never shadows it
        assert registry["add"].validator is not None  # local fail-fast validation preserved
        assert any("add" in r.message for r in caplog.records)


class TestToolsExport:
    def test_importable_from_calfkit_and_nodes(self) -> None:
        import calfkit
        from calfkit import Tools as TopLevel
        from calfkit.nodes import Tools as FromNodes
        from calfkit.nodes.tool import Tools as FromModule

        assert TopLevel is FromModule is FromNodes
        assert "Tools" in calfkit.__all__
