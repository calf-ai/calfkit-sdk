"""Unit tests for the caller-side mesh view (``calfkit.client.mesh``).

The offline projection, health, and cache/cancel-safety tests are added across this
feature's commits; this module holds them (the URL-resolver tests moved to
``test_mesh_url.py``).
"""

from __future__ import annotations

import copy
from dataclasses import FrozenInstanceError
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from calfkit import MeshUnavailableError
from calfkit.client.mesh import AgentInfo, MeshViewConfig, ToolboxInfo, ToolInfo, ToolNodeInfo, ToolSpec
from calfkit.tuning import KTableReaderTuning

_NOW = datetime(2026, 6, 29, 12, 0, 0, tzinfo=timezone.utc)


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
