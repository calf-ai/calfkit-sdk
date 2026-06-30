"""Unit tests for the caller-side mesh view (``calfkit.client.mesh``).

The offline projection, health, and cache/cancel-safety tests are added across this
feature's commits; this module holds them (the URL-resolver tests moved to
``test_mesh_url.py``).
"""

from __future__ import annotations

import copy
import logging
from dataclasses import FrozenInstanceError
from datetime import datetime, timezone
from typing import Any

import pytest
from pydantic import ValidationError

from calfkit import MeshUnavailableError
from calfkit.client.mesh import (
    AgentInfo,
    MeshViewConfig,
    ToolboxInfo,
    ToolInfo,
    ToolNodeInfo,
    ToolSpec,
    _project_agent,
    _project_tool,
    _Projector,
)
from calfkit.models.agents import AgentCard
from calfkit.models.capability import CapabilityRecord, CapabilityToolDef
from calfkit.tuning import KTableReaderTuning

_NOW = datetime(2026, 6, 29, 12, 0, 0, tzinfo=timezone.utc)


def _agent_card(*, description: str | None = "A test agent") -> AgentCard:
    return AgentCard(started_at=_NOW, last_heartbeat_at=_NOW, heartbeat_interval=30.0, node_kind="agent", description=description)


def _tool_def(name: str, *, description: str | None = None, schema: dict[str, Any] | None = None) -> CapabilityToolDef:
    return CapabilityToolDef(name=name, description=description, parameters_json_schema=schema if schema is not None else {"type": "object"})


def _capability_record(*, node_kind: str, tools: list[CapabilityToolDef]) -> CapabilityRecord:
    return CapabilityRecord(
        started_at=_NOW,
        last_heartbeat_at=_NOW,
        heartbeat_interval=30.0,
        node_kind=node_kind,
        dispatch_topic="some.dispatch.topic",
        tools=tools,
        content_updated_at=_NOW,
    )


# -- DTOs: construction + immutability (frozen dataclasses, the RunEvent idiom) ----


def test_agent_info_carries_name_description_and_last_seen() -> None:
    info = AgentInfo(name="billing", description="Handles invoices", last_seen=_NOW)
    assert (info.name, info.description, info.last_seen) == ("billing", "Handles invoices", _NOW)
    with pytest.raises(FrozenInstanceError):
        info.name = "other"  # type: ignore[misc]


def test_agent_info_description_may_be_absent() -> None:
    assert AgentInfo(name="billing", description=None, last_seen=_NOW).description is None


def test_tool_spec_carries_bare_name_description_and_schema() -> None:
    spec = ToolSpec(name="search", description="Find things", parameters_schema={"type": "object"})
    assert (spec.name, spec.description, spec.parameters_schema) == ("search", "Find things", {"type": "object"})
    with pytest.raises(FrozenInstanceError):
        spec.name = "other"  # type: ignore[misc]


def test_tool_node_info_inlines_a_single_tool() -> None:
    info = ToolNodeInfo(name="add", description="Add two numbers", parameters_schema={"type": "object"}, last_seen=_NOW)
    assert (info.name, info.description, info.parameters_schema, info.last_seen) == (
        "add",
        "Add two numbers",
        {"type": "object"},
        _NOW,
    )
    with pytest.raises(FrozenInstanceError):
        info.name = "other"  # type: ignore[misc]


def test_toolbox_info_carries_a_tuple_of_tool_specs() -> None:
    spec = ToolSpec(name="search", description=None, parameters_schema={})
    info = ToolboxInfo(name="docs", tools=(spec,), last_seen=_NOW)
    assert info.name == "docs"
    assert info.tools == (spec,)
    assert info.last_seen == _NOW
    with pytest.raises(FrozenInstanceError):
        info.tools = ()  # type: ignore[misc]


def test_toolbox_info_has_no_description_field() -> None:
    # The wire CapabilityRecord carries no toolbox-level description (spec §5.3); descriptions
    # live per ToolSpec.
    assert "description" not in ToolboxInfo.__dataclass_fields__


def test_toolbox_info_may_carry_zero_tools() -> None:
    assert ToolboxInfo(name="empty", tools=(), last_seen=_NOW).tools == ()


# -- ToolInfo: closed, match-friendly union over the two advertiser shapes ---------


def test_tool_info_branches_by_isinstance() -> None:
    node: ToolInfo = ToolNodeInfo(name="add", description=None, parameters_schema={}, last_seen=_NOW)
    box: ToolInfo = ToolboxInfo(name="docs", tools=(), last_seen=_NOW)
    assert isinstance(node, ToolNodeInfo) and not isinstance(node, ToolboxInfo)
    assert isinstance(box, ToolboxInfo) and not isinstance(box, ToolNodeInfo)


def test_tool_info_dispatches_via_structural_match() -> None:
    # The spec's public surface example matches structurally on the union (§1).
    results: list[str] = []
    for info in (
        ToolNodeInfo(name="add", description=None, parameters_schema={}, last_seen=_NOW),
        ToolboxInfo(name="docs", tools=(), last_seen=_NOW),
    ):
        got = "other"
        match info:
            case ToolNodeInfo():
                got = "node"
            case ToolboxInfo():
                got = "toolbox"
        results.append(got)
    assert results == ["node", "toolbox"]


# -- MeshViewConfig: reader-scoped, frozen, extra=forbid ---------------------------


def test_mesh_view_config_defaults() -> None:
    config = MeshViewConfig()
    assert config.stale_after is None
    assert config.catchup_timeout == 30.0
    assert config.reader_tuning == KTableReaderTuning()


def test_mesh_view_config_is_frozen() -> None:
    config = MeshViewConfig()
    with pytest.raises(ValidationError):
        config.catchup_timeout = 5.0


@pytest.mark.parametrize("field", ["bootstrap_servers", "heartbeat_interval", "ensure_topic", "probe_interval"])
def test_mesh_view_config_forbids_foreign_fields(field: str) -> None:
    # No bootstrap_servers (the client supplies it), no heartbeat_interval (publisher-only),
    # no ensure_topic (the observer opens naively), no probe_interval (no probe) — spec §5.4.
    with pytest.raises(ValidationError):
        MeshViewConfig(**{field: 1.0})


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), -float("inf"), 0.0, -1.0])
def test_mesh_view_config_rejects_non_positive_or_non_finite_catchup(bad: float) -> None:
    with pytest.raises(ValidationError):
        MeshViewConfig(catchup_timeout=bad)


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), 0.0, -1.0])
def test_mesh_view_config_rejects_non_positive_or_non_finite_stale_after(bad: float) -> None:
    with pytest.raises(ValidationError):
        MeshViewConfig(stale_after=bad)


def test_mesh_view_config_accepts_explicit_tuning() -> None:
    tuning = KTableReaderTuning(poll_timeout_ms=20)
    config = MeshViewConfig(stale_after=10.0, catchup_timeout=5.0, reader_tuning=tuning)
    assert config.stale_after == 10.0
    assert config.catchup_timeout == 5.0
    assert config.reader_tuning is tuning


# -- MeshUnavailableError: typed, reason-carrying, cross-process-faithful -----------


def test_mesh_unavailable_error_is_a_distinct_typed_signal() -> None:
    err = MeshUnavailableError("agents directory still establishing", reason="establishing")
    assert isinstance(err, Exception)
    assert err.reason == "establishing"
    assert "establishing" in str(err)


def test_mesh_unavailable_error_is_flat_no_artificial_base() -> None:
    # spec §6.4 family: flat typed signals (cf. ClientTimeoutError / ClientClosedError).
    assert MeshUnavailableError.__bases__ == (Exception,)


def test_mesh_unavailable_error_carries_each_reason() -> None:
    assert MeshUnavailableError("m", reason="establishing").reason == "establishing"
    assert MeshUnavailableError("m", reason="reader_dead").reason == "reader_dead"
    assert MeshUnavailableError("m", reason="open_failed").reason == "open_failed"


def test_mesh_unavailable_error_is_exported_from_calfkit() -> None:
    from calfkit import MeshUnavailableError as Exported
    from calfkit.exceptions import MeshUnavailableError as Defined

    assert Exported is Defined


def test_mesh_unavailable_error_reconstructs_from_its_fields() -> None:
    # The keyword-only `reason` breaks the default reduction (which would replay only the
    # message string through __init__ and drop `reason`); a custom __reduce__ rebuilds both,
    # so the error survives cross-process serialization (the ClientTimeoutError precedent,
    # exercised here via copy.deepcopy).
    restored = copy.deepcopy(MeshUnavailableError("could not open the agents directory", reason="open_failed"))
    assert isinstance(restored, MeshUnavailableError)
    assert restored.reason == "open_failed"
    assert str(restored) == "could not open the agents directory"


# -- Projections: wire record -> public DTO (spec §6.5) ---------------------------


def test_project_agent_maps_card_to_agent_info() -> None:
    info = _project_agent("billing", _agent_card(description="Handles invoices"))
    assert info == AgentInfo(name="billing", description="Handles invoices", last_seen=_NOW)


def test_project_agent_carries_absent_description() -> None:
    assert _project_agent("billing", _agent_card(description=None)).description is None


def test_project_tool_maps_a_function_tool_node_to_tool_node_info() -> None:
    record = _capability_record(node_kind="tool", tools=[_tool_def("add", description="Add two numbers", schema={"type": "object", "x": 1})])
    assert _project_tool("add", record) == ToolNodeInfo(
        name="add", description="Add two numbers", parameters_schema={"type": "object", "x": 1}, last_seen=_NOW
    )


def test_project_tool_maps_a_toolbox_to_toolbox_info_with_bare_names() -> None:
    record = _capability_record(node_kind="toolbox", tools=[_tool_def("search", description="Find"), _tool_def("fetch")])
    info = _project_tool("docs", record)
    assert isinstance(info, ToolboxInfo)
    assert info.name == "docs"
    assert info.last_seen == _NOW
    assert [s.name for s in info.tools] == ["search", "fetch"]  # BARE names, not docs__search
    assert info.tools[0] == ToolSpec(name="search", description="Find", parameters_schema={"type": "object"})


def test_project_tool_toolbox_with_zero_tools_projects_to_empty_tuple() -> None:
    assert _project_tool("empty", _capability_record(node_kind="toolbox", tools=[])) == ToolboxInfo(name="empty", tools=(), last_seen=_NOW)


def test_project_tool_unknown_kind_is_skipped() -> None:
    assert _project_tool("weird", _capability_record(node_kind="gadget", tools=[_tool_def("x")])) is None


@pytest.mark.parametrize("count", [0, 2])
def test_project_tool_node_with_not_exactly_one_tool_is_skipped(count: int) -> None:
    # A "tool" advertises exactly one tool by producer convention; a foreign/malformed record
    # with 0 or >1 tools is skipped, not blind-indexed (spec §6.5).
    tools = [_tool_def(f"t{i}") for i in range(count)]
    assert _project_tool("add", _capability_record(node_kind="tool", tools=tools)) is None


def test_project_tool_deep_copies_parameters_schema() -> None:
    # A caller mutating info.parameters_schema must not reach the live cached record (spec §6.5).
    record = _capability_record(node_kind="tool", tools=[_tool_def("add", schema={"type": "object", "nested": {"k": "v"}})])
    info = _project_tool("add", record)
    assert isinstance(info, ToolNodeInfo)
    info.parameters_schema["nested"]["k"] = "MUTATED"
    assert record.tools[0].parameters_json_schema["nested"]["k"] == "v"


# -- _Projector: snapshot -> DTO dict, with deduped skip-logging -------------------


def test_projector_projects_a_snapshot_skipping_non_projectable_records() -> None:
    projector = _Projector(_project_tool, label="tools")
    snapshot = {
        "add": _capability_record(node_kind="tool", tools=[_tool_def("add")]),
        "docs": _capability_record(node_kind="toolbox", tools=[_tool_def("search")]),
        "weird": _capability_record(node_kind="gadget", tools=[]),
    }
    out = projector(snapshot)
    assert set(out) == {"add", "docs"}  # "weird" skipped, uniformly absent
    assert isinstance(out["add"], ToolNodeInfo)
    assert isinstance(out["docs"], ToolboxInfo)


def test_projector_handles_agents_which_never_skip() -> None:
    projector = _Projector(_project_agent, label="agents")
    out = projector({"billing": _agent_card(description="Invoices")})
    assert out == {"billing": AgentInfo(name="billing", description="Invoices", last_seen=_NOW)}


def test_projector_dedupes_the_skip_log_across_repeated_projections(caplog: pytest.LogCaptureFixture) -> None:
    projector = _Projector(_project_tool, label="tools")
    snapshot = {"weird": _capability_record(node_kind="gadget", tools=[])}
    with caplog.at_level(logging.WARNING, logger="calfkit.client.mesh"):
        projector(snapshot)
        projector(snapshot)  # the same skip again — must not re-log
    skip_warnings = [r for r in caplog.records if "weird" in r.getMessage()]
    assert len(skip_warnings) == 1
