"""``resolve_all_capabilities`` — the discover-mode bulk kernel (spec §15.2).

Binds EVERY live record of a given ``node_kind`` from the capability view's
``snapshot()`` — the discover analogue of the single-target ``resolve_capability``.
A POSITIVE FILTER, not the over-pull guard: a record of another kind is out of
scope (skipped), not ``wrong_kind_targets``; only a poisoned record of the right
kind degrades to ``invalid_targets``.
"""

from __future__ import annotations

from datetime import datetime, timezone

from calfkit.models.capability import CapabilityRecord, CapabilityToolDef, resolve_all_capabilities
from tests._capability_fakes import _FakeView


def _record(name: str, *, node_kind: str = "tool", dispatch: str | None = None) -> CapabilityRecord:
    now = datetime.now(tz=timezone.utc)
    return CapabilityRecord(
        started_at=now,
        last_heartbeat_at=now,
        heartbeat_interval=30.0,
        node_kind=node_kind,
        dispatch_topic=f"tool.{name}.input" if dispatch is None else dispatch,
        tools=[CapabilityToolDef(name=name, parameters_json_schema={"type": "object", "properties": {}})],
        content_updated_at=now,
    )


class TestResolveAllCapabilities:
    def test_binds_only_records_of_the_requested_kind(self) -> None:
        view = _FakeView(
            {
                "add": _record("add", node_kind="tool"),
                "sub": _record("sub", node_kind="tool"),
                "github": _record("search", node_kind="toolbox", dispatch="mcp_server.github"),
            }
        )
        result = resolve_all_capabilities(view, node_kind="tool")
        assert sorted(b.name for b in result.bindings) == ["add", "sub"]  # toolbox record excluded
        # Nothing was named, so the named-path diagnostics are always empty (positive filter).
        assert result.missing_targets == ()
        assert result.missing_tools == ()
        assert result.wrong_kind_targets == ()
        assert not result.unresolved

    def test_discovered_bindings_are_validatorless(self) -> None:
        result = resolve_all_capabilities(_FakeView({"add": _record("add")}), node_kind="tool")
        assert [b.name for b in result.bindings] == ["add"]
        assert result.bindings[0].validator is None  # discovered = schema-only (node validates on receipt)

    def test_poisoned_record_of_the_right_kind_degrades_to_invalid_targets(self) -> None:
        # An empty dispatch_topic survives the tolerant reader but fails ToolBinding's
        # min_length in record_to_bindings -> the node lands in invalid_targets; others still bind.
        view = _FakeView({"add": _record("add"), "broken": _record("broken", dispatch="")})
        result = resolve_all_capabilities(view, node_kind="tool")
        assert [b.name for b in result.bindings] == ["add"]
        assert result.invalid_targets == ("broken",)
        assert result.unresolved

    def test_empty_view_binds_nothing_and_is_not_unresolved(self) -> None:
        result = resolve_all_capabilities(_FakeView({}), node_kind="tool")
        assert result.bindings == ()
        assert not result.unresolved
