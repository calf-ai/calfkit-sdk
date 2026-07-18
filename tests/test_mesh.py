"""Unit tests for the caller-side mesh view (``calfkit.client.mesh``).

The offline projection, health, and cache/cancel-safety tests are added across this
feature's commits; this module holds them (the URL-resolver tests moved to
``test_mesh_url.py``).
"""

from __future__ import annotations

import asyncio
import contextlib
import copy
import logging
import subprocess
import sys
from collections.abc import Callable
from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
from typing import Any, cast
from unittest.mock import Mock

import pytest
from pydantic import ValidationError

from calfkit import ClientClosedError, MeshUnavailableError
from calfkit.client.caller import Client
from calfkit.client.mesh import (
    AgentInfo,
    Mesh,
    MeshViewConfig,
    ToolboxInfo,
    ToolInfo,
    ToolNodeInfo,
    ToolSpec,
    _Cell,
    _project_agent,
    _project_tool,
    _Projector,
)
from calfkit.controlplane.view import ControlPlaneView
from calfkit.models.agents import AgentCard
from calfkit.models.capability import CapabilityRecord, CapabilityToolDef
from calfkit.provisioning import ProvisioningConfig
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


# === Mesh cache: stubs + helpers =================================================


class _StubClient:
    """A minimal stand-in for Client — the Mesh reads only ``_connection_profile`` (spec §7.3;
    ``None`` = the directly-built, unconnected posture)."""

    def __init__(self, server_urls: str | None = "localhost:9092") -> None:
        from calfkit.client._connection import ConnectionProfile

        self.server_urls = server_urls
        self._connection_profile = (
            None if server_urls is None else ConnectionProfile(bootstrap_servers=server_urls, security_opts={}, max_message_bytes=5 * 1024 * 1024)
        )


def _make_mesh(*, server_urls: str | None = "localhost:9092", config: MeshViewConfig | None = None) -> Mesh:
    return Mesh(cast(Client, _StubClient(server_urls)), config)


class _StubView:
    """A controllable stand-in for ControlPlaneView, driving the Mesh cache offline.

    ``snapshot`` holds real wire records (so the kind's projector runs); ``failure`` /
    ``is_caught_up`` drive the health checks; ``start_error`` makes ``start()`` raise;
    ``start_gate`` makes ``start()`` block until released (to exercise cancel-safety).
    """

    def __init__(
        self,
        *,
        snapshot: dict[str, Any] | None = None,
        failure: BaseException | None = None,
        is_caught_up: bool = True,
        start_error: BaseException | None = None,
        start_gate: asyncio.Event | None = None,
    ) -> None:
        self._snapshot = snapshot if snapshot is not None else {}
        self.failure = failure
        self.is_caught_up = is_caught_up
        self._start_error = start_error
        self._start_gate = start_gate
        self.start_count = 0
        self.stopped = False
        self.entered = asyncio.Event()

    async def start(self) -> None:
        self.start_count += 1
        self.entered.set()
        if self._start_gate is not None:
            await self._start_gate.wait()
        if self._start_error is not None:
            raise self._start_error

    async def stop(self) -> None:
        self.stopped = True

    def snapshot(self) -> dict[str, Any]:
        return dict(self._snapshot)


def _install_opener(monkeypatch: pytest.MonkeyPatch, view_for: Callable[..., _StubView]) -> list[dict[str, Any]]:
    """Replace ``ControlPlaneView.open`` (as the mesh imports it) with a factory; record each open."""
    calls: list[dict[str, Any]] = []

    def _open(**kwargs: Any) -> _StubView:
        calls.append(kwargs)
        return view_for(**kwargs)

    monkeypatch.setattr(ControlPlaneView, "open", staticmethod(_open))
    return calls


# === Mesh cache: basic open / reuse / config ====================================


async def test_get_agents_opens_once_and_reuses_the_cached_view(monkeypatch: pytest.MonkeyPatch) -> None:
    view = _StubView(snapshot={"billing": _agent_card(description="Invoices")})
    calls = _install_opener(monkeypatch, lambda **kw: view)
    mesh = _make_mesh()
    first = await mesh.get_agents()
    second = await mesh.get_agents()
    assert first == {"billing": AgentInfo(name="billing", description="Invoices", last_seen=_NOW)}
    assert second == first
    assert len(calls) == 1  # one open, reused
    assert view.start_count == 1
    await mesh.aclose()


async def test_get_tools_projects_the_tools_view(monkeypatch: pytest.MonkeyPatch) -> None:
    record = _capability_record(node_kind="tool", tools=[_tool_def("add", description="Add")])
    view = _StubView(snapshot={"add": record})
    _install_opener(monkeypatch, lambda **kw: view)
    mesh = _make_mesh()
    tools = await mesh.get_tools()
    assert isinstance(tools["add"], ToolNodeInfo)
    assert tools["add"].description == "Add"
    await mesh.aclose()


async def test_open_passes_ensure_topic_false_and_the_kind_topic(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _install_opener(monkeypatch, lambda **kw: _StubView())
    mesh = _make_mesh()
    await mesh.get_agents()
    assert calls[0]["ensure_topic"] is False  # the observer never creates a topic
    assert calls[0]["topic"] == "calf.agents"
    assert calls[0]["record_type"] is AgentCard
    await mesh.aclose()


async def test_reads_return_a_read_only_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    view = _StubView(snapshot={"billing": _agent_card()})
    _install_opener(monkeypatch, lambda **kw: view)
    mesh = _make_mesh()
    agents = await mesh.get_agents()
    with pytest.raises(TypeError):
        agents["x"] = agents["billing"]  # type: ignore[index]
    await mesh.aclose()


async def test_unconnected_client_raises_value_error_synchronously(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _install_opener(monkeypatch, lambda **kw: _StubView())
    mesh = _make_mesh(server_urls=None)
    with pytest.raises(ValueError, match="connected client"):
        await mesh.get_agents()
    assert calls == []  # never even attempted an open


async def test_config_is_threaded_into_the_open(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _install_opener(monkeypatch, lambda **kw: _StubView())
    tuning = KTableReaderTuning(poll_timeout_ms=15)
    mesh = _make_mesh(config=MeshViewConfig(stale_after=12.0, catchup_timeout=7.0, reader_tuning=tuning))
    await mesh.get_agents()
    assert calls[0]["catchup_timeout"] == 7.0
    assert calls[0]["stale_after"] == 12.0
    assert calls[0]["reader_tuning"] is tuning
    await mesh.aclose()


# === Mesh cache: concurrency + single-flight ====================================


async def test_concurrent_first_calls_of_one_kind_open_once(monkeypatch: pytest.MonkeyPatch) -> None:
    gate = asyncio.Event()
    view = _StubView(start_gate=gate)
    calls = _install_opener(monkeypatch, lambda **kw: view)
    mesh = _make_mesh()
    waiters = asyncio.gather(mesh.get_agents(), mesh.get_agents(), mesh.get_agents())
    await asyncio.wait_for(view.entered.wait(), timeout=1.0)  # the single shared open is in-flight
    gate.set()
    results = await waiters
    assert results == [{}, {}, {}]
    assert len(calls) == 1  # single-flight: one open for three concurrent callers
    await mesh.aclose()


async def test_the_two_kinds_open_concurrently_without_cross_serialization(monkeypatch: pytest.MonkeyPatch) -> None:
    gate = asyncio.Event()
    agents_view = _StubView(start_gate=gate)
    tools_view = _StubView(start_gate=gate)
    views = {"calf.agents": agents_view, "calf.capabilities": tools_view}
    _install_opener(monkeypatch, lambda **kw: views[kw["topic"]])
    mesh = _make_mesh()
    waiters = asyncio.gather(mesh.get_agents(), mesh.get_tools())
    # both opens reach start() — the lock is NOT held across start(), so neither blocks the other
    await asyncio.wait_for(asyncio.gather(agents_view.entered.wait(), tools_view.entered.wait()), timeout=1.0)
    assert agents_view.start_count == 1 and tools_view.start_count == 1
    gate.set()
    await waiters
    await mesh.aclose()


# === Mesh cache: cancel-safety (the round-3 CRITICAL) ===========================


async def test_a_waiter_cancel_does_not_kill_the_shared_open(monkeypatch: pytest.MonkeyPatch) -> None:
    gate = asyncio.Event()
    view = _StubView(start_gate=gate)
    _install_opener(monkeypatch, lambda **kw: view)
    mesh = _make_mesh()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(mesh.get_agents(), timeout=0.05)  # waiter cancelled mid-open
    gate.set()  # release the shared open, which must have survived the waiter's cancel
    result = await mesh.get_agents()  # a fresh read reuses the SAME open
    assert result == {}
    assert view.start_count == 1  # not re-opened — shield absorbed the waiter's cancel
    assert not view.stopped  # not torn down
    await mesh.aclose()


async def test_aclose_cancels_an_in_flight_open_and_tears_it_down(monkeypatch: pytest.MonkeyPatch) -> None:
    gate = asyncio.Event()  # never released → the open stays in-flight
    view = _StubView(start_gate=gate)
    _install_opener(monkeypatch, lambda **kw: view)
    mesh = _make_mesh()
    waiter = asyncio.ensure_future(mesh.get_agents())
    await asyncio.wait_for(view.entered.wait(), timeout=1.0)
    await mesh.aclose()  # cancels the in-flight open
    assert view.stopped  # the teardown-guard stopped the half-built view (no leaked consumer)
    with pytest.raises(ClientClosedError):
        await waiter


async def test_get_after_aclose_raises_client_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_opener(monkeypatch, lambda **kw: _StubView())
    mesh = _make_mesh()
    await mesh.aclose()
    with pytest.raises(ClientClosedError) as e:
        await mesh.get_agents()
    assert e.value.correlation_id is None  # a non-run wait — the generalized ClientClosedError form


# === Mesh cache: aclose teardown ================================================


async def test_aclose_stops_a_completed_view(monkeypatch: pytest.MonkeyPatch) -> None:
    view = _StubView()
    _install_opener(monkeypatch, lambda **kw: view)
    mesh = _make_mesh()
    await mesh.get_agents()
    await mesh.aclose()
    assert view.stopped


async def test_aclose_logs_a_failing_view_stop_and_still_completes(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    class _FailStopView(_StubView):
        async def stop(self) -> None:
            raise RuntimeError("stop boom")

    view = _FailStopView()
    _install_opener(monkeypatch, lambda **kw: view)
    mesh = _make_mesh()
    await mesh.get_agents()
    with caplog.at_level(logging.ERROR, logger="calfkit.client.mesh"):
        await mesh.aclose()  # best-effort: must not raise
    assert any("stop failed" in r.getMessage() for r in caplog.records)


# === Mesh cache: health by reason (spec §6.4) ===================================


async def test_reader_dead_raises_reader_dead_and_retains_the_cell(monkeypatch: pytest.MonkeyPatch) -> None:
    boom = RuntimeError("authorization failed")
    # failure set AND is_caught_up True (the sticky latch) -> reader_dead, NOT establishing:
    # the load-bearing check order tests failure before is_caught_up (spec §6.4).
    view = _StubView(failure=boom, is_caught_up=True)
    calls = _install_opener(monkeypatch, lambda **kw: view)
    mesh = _make_mesh()
    with pytest.raises(MeshUnavailableError) as first:
        await mesh.get_agents()
    assert first.value.reason == "reader_dead"
    assert first.value.__cause__ is boom
    with pytest.raises(MeshUnavailableError) as second:
        await mesh.get_agents()  # the same dead view, not re-opened (terminal)
    assert second.value.reason == "reader_dead"
    assert len(calls) == 1
    await mesh.aclose()


async def test_not_caught_up_raises_establishing(monkeypatch: pytest.MonkeyPatch) -> None:
    view = _StubView(is_caught_up=False)
    _install_opener(monkeypatch, lambda **kw: view)
    mesh = _make_mesh()
    with pytest.raises(MeshUnavailableError) as e:
        await mesh.get_agents()
    assert e.value.reason == "establishing"
    assert e.value.__cause__ is None  # establishing has no cause — just an unlatched event (spec §6.4)
    await mesh.aclose()


async def test_establishing_self_heals_on_the_same_cached_view(monkeypatch: pytest.MonkeyPatch) -> None:
    view = _StubView(is_caught_up=False)
    calls = _install_opener(monkeypatch, lambda **kw: view)
    mesh = _make_mesh()
    with pytest.raises(MeshUnavailableError):
        await mesh.get_agents()
    view.is_caught_up = True  # the background reader latched
    assert await mesh.get_agents() == {}  # same cached view now returns
    assert len(calls) == 1  # no re-open — self-healed on the same view
    await mesh.aclose()


async def test_caught_up_empty_returns_an_empty_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_opener(monkeypatch, lambda **kw: _StubView(snapshot={}))
    mesh = _make_mesh()
    assert await mesh.get_agents() == {}  # empty is a value, not an error
    await mesh.aclose()


async def test_open_failure_raises_open_failed_then_is_evicted_and_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    failing = _StubView(start_error=RuntimeError("topic missing"))
    healthy = _StubView()
    seq = iter([failing, healthy])
    calls = _install_opener(monkeypatch, lambda **kw: next(seq))
    mesh = _make_mesh()
    with pytest.raises(MeshUnavailableError) as e:
        await mesh.get_agents()
    assert e.value.reason == "open_failed"
    assert "directory" in str(e.value) and "provision" in str(e.value)  # actionable message (spec §6.3)
    assert isinstance(e.value.__cause__, RuntimeError)
    assert failing.stopped  # the teardown-guard tore down the half-built view
    await asyncio.sleep(0)  # let the done-callback eviction run
    assert "agents" not in mesh._cells  # the failed open was evicted
    assert await mesh.get_agents() == {}  # a retry re-opens (the healthy view)
    assert len(calls) == 2
    await mesh.aclose()


# === Mesh module: lazy import (no eager ktables) ================================


def test_importing_mesh_pulls_no_ktables() -> None:
    # mesh.py must not eagerly import ktables (it stays lazy inside ControlPlaneView.open).
    # A subprocess avoids prior in-process imports polluting sys.modules.
    code = (
        "import sys, calfkit.client.mesh\n"
        "leaked = sorted(m for m in sys.modules if m == 'ktables' or m.startswith('ktables.'))\n"
        "assert not leaked, leaked\n"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


# === Client.mesh wiring (spec §5.1 / §6.2) ======================================


def test_client_mesh_is_a_zero_io_identity_stable_singleton() -> None:
    client = Client.connect("localhost:9092")
    assert client._mesh is None  # __init__ initialized it to None (created lazily)
    first = client.mesh
    second = client.mesh
    assert first is second and isinstance(first, Mesh)  # one cached singleton per client
    assert not client._broker._connection  # zero I/O — accessing client.mesh brings up no broker


def test_connect_threads_mesh_config_into_the_mesh() -> None:
    config = MeshViewConfig(catchup_timeout=9.0)
    client = Client.connect("localhost:9092", mesh_config=config)
    assert client.mesh._config is config


def test_client_mesh_defaults_config_when_unset() -> None:
    client = Client.connect("localhost:9092")
    assert client.mesh._config == MeshViewConfig()


async def test_opening_a_mesh_view_does_not_start_the_client_broker(monkeypatch: pytest.MonkeyPatch) -> None:
    client = Client.connect("localhost:9092")
    _install_opener(monkeypatch, lambda **kw: _StubView())
    await client.mesh.get_agents()
    assert not client._broker._connection  # the mesh consumer is independent of the inbox/broker (§7.3)
    await client.aclose()


async def test_client_aclose_tears_down_the_mesh(monkeypatch: pytest.MonkeyPatch) -> None:
    client = Client.connect("localhost:9092")
    view = _StubView()
    _install_opener(monkeypatch, lambda **kw: view)
    await client.mesh.get_agents()
    await client.aclose()
    assert view.stopped  # client.aclose() stopped the cached mesh view


async def test_client_aclose_runs_the_broker_close_path_even_if_mesh_teardown_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = Client.connect("localhost:9092")
    _install_opener(monkeypatch, lambda **kw: _StubView())
    await client.mesh.get_agents()

    async def _boom() -> None:
        raise RuntimeError("mesh teardown boom")

    monkeypatch.setattr(client.mesh, "aclose", _boom)
    client._started = True  # so we can observe the finally resetting it
    with pytest.raises(RuntimeError, match="mesh teardown boom"):
        await client.aclose()
    assert client._started is False  # the broker-close path (finally) ran despite the mesh raise


# === Public exports (Commit 5) ==================================================

_PUBLIC_MESH_NAMES = ("Mesh", "MeshViewConfig", "AgentInfo", "ToolInfo", "ToolNodeInfo", "ToolboxInfo", "ToolSpec")


def test_mesh_surface_is_exported_from_calfkit_top_level() -> None:
    import calfkit

    for name in _PUBLIC_MESH_NAMES:
        assert name in calfkit.__all__, f"{name} missing from calfkit.__all__"
        assert hasattr(calfkit, name), f"{name} not importable from calfkit"


def test_mesh_surface_is_exported_from_calfkit_client() -> None:
    import calfkit.client

    for name in _PUBLIC_MESH_NAMES:
        assert name in calfkit.client.__all__, f"{name} missing from calfkit.client.__all__"
        assert hasattr(calfkit.client, name)


def test_wire_records_and_topics_stay_unexported() -> None:
    # ADR-0028: callers depend only on the projected DTOs; the wire records and topic names
    # stay internal so the records stay free to change behind them.
    import calfkit
    import calfkit.client

    for name in ("AgentCard", "CapabilityRecord", "AGENTS_TOPIC", "CAPABILITY_TOPIC"):
        assert name not in calfkit.__all__
        assert name not in calfkit.client.__all__


# === Review-round additions (deeper health-order / cancel / aclose / projection coverage) =======


async def test_reader_dead_takes_precedence_over_not_caught_up(monkeypatch: pytest.MonkeyPatch) -> None:
    # A reader that died BEFORE catching up has failure set AND is_caught_up=False. The load-bearing
    # check order (failure BEFORE is_caught_up) must yield reader_dead, not establishing — a swapped
    # check would diverge only in exactly this state (spec §6.4).
    boom = RuntimeError("authorization failed during cold start")
    view = _StubView(failure=boom, is_caught_up=False)
    _install_opener(monkeypatch, lambda **kw: view)
    mesh = _make_mesh()
    with pytest.raises(MeshUnavailableError) as e:
        await mesh.get_agents()
    assert e.value.reason == "reader_dead"  # NOT "establishing"
    assert e.value.__cause__ is boom
    await mesh.aclose()


async def test_a_waiter_cancel_does_not_poison_a_co_waiter(monkeypatch: pytest.MonkeyPatch) -> None:
    # Two concurrent waiters share one open; cancelling one must not poison the other (spec §8, the
    # round-3 CRITICAL): the survivor resolves on the SAME open, with no re-open and no teardown.
    gate = asyncio.Event()
    view = _StubView(start_gate=gate)
    _install_opener(monkeypatch, lambda **kw: view)
    mesh = _make_mesh()
    victim = asyncio.ensure_future(asyncio.wait_for(mesh.get_agents(), timeout=0.05))
    survivor = asyncio.ensure_future(mesh.get_agents())
    await asyncio.wait_for(view.entered.wait(), timeout=1.0)  # the single shared open is in-flight; both waiters are on it
    with pytest.raises(asyncio.TimeoutError):
        await victim  # the victim's wait_for cancels its waiter
    gate.set()  # release the shared open
    assert await survivor == {}  # the co-waiter still resolves on the shared open
    assert view.start_count == 1  # one open — neither cancel re-opened
    assert not view.stopped  # the shared open was never torn down
    await mesh.aclose()


async def test_aclose_isolates_a_failing_view_stop_from_the_other_kind(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    # One kind's stop() raising must not skip the other kind's stop() (per-view isolation), and the
    # ERROR log names which view failed.
    class _FailStopView(_StubView):
        async def stop(self) -> None:
            raise RuntimeError("agents stop boom")

    agents_view = _FailStopView()
    tools_view = _StubView()
    views: dict[str, _StubView] = {"calf.agents": agents_view, "calf.capabilities": tools_view}
    _install_opener(monkeypatch, lambda **kw: views[kw["topic"]])
    mesh = _make_mesh()
    await mesh.get_agents()
    await mesh.get_tools()
    with caplog.at_level(logging.ERROR, logger="calfkit.client.mesh"):
        await mesh.aclose()  # agents' stop() raises; tools' must still be stopped
    assert tools_view.stopped  # the other kind's view was still stopped
    assert any("agents" in r.getMessage() for r in caplog.records)  # the failing view is named


def test_project_tool_deep_copies_toolbox_tool_schemas() -> None:
    # The toolbox path deep-copies each tool's schema too (mirrors the tool-node path, spec §6.5).
    record = _capability_record(node_kind="toolbox", tools=[_tool_def("search", schema={"type": "object", "nested": {"k": "v"}})])
    info = _project_tool("docs", record)
    assert isinstance(info, ToolboxInfo)
    info.tools[0].parameters_schema["nested"]["k"] = "MUTATED"
    assert record.tools[0].parameters_json_schema["nested"]["k"] == "v"


async def test_evict_keeps_a_retrys_newer_cell(monkeypatch: pytest.MonkeyPatch) -> None:
    # The identity guard: a stale failed-open's done-callback must NOT drop a newer cell a retry
    # installed under the same key (the §7.2 "never drop a retry's newer cell" guarantee).
    _install_opener(monkeypatch, lambda **kw: _StubView())
    mesh = _make_mesh()
    await mesh.get_agents()
    current = mesh._cells["agents"]  # the live cell a retry would have installed

    async def _boom() -> None:
        raise RuntimeError("a prior open that failed")

    stale_task: asyncio.Task[Any] = asyncio.ensure_future(_boom())
    with contextlib.suppress(RuntimeError):
        await stale_task
    stale_cell = _Cell(task=stale_task, project=_Projector(_project_agent, label="agents"))
    mesh._evict("agents", stale_cell, stale_task)  # the stale callback fires after the retry installed `current`
    assert mesh._cells["agents"] is current  # the newer cell was NOT dropped
    await mesh.aclose()


def _agent_card_at(*, age_s: float) -> AgentCard:
    hb = datetime.now(tz=timezone.utc) - timedelta(seconds=age_s)
    return AgentCard(started_at=hb, last_heartbeat_at=hb, heartbeat_interval=30.0, node_kind="agent", description=None)


def test_projection_over_a_real_view_drops_stale_nodes() -> None:
    # The spec §8 strategy: drive the projection through a REAL ControlPlaneView over a dict-backed
    # fake table, so the view's staleness filter (a stale node -> absent) is exercised end to end.
    from tests.test_controlplane_view import _FakeTable

    fresh = _agent_card_at(age_s=0.0)
    stale = _agent_card_at(age_s=10_000.0)
    view = ControlPlaneView(_FakeTable({"fresh": {"w1": fresh}, "stale": {"w1": stale}}), AgentCard, stale_after=30.0)
    projected = _Projector(_project_agent, label="agents")(view.snapshot())
    assert set(projected) == {"fresh"}  # the stale node is filtered out before projection
    assert projected["fresh"].name == "fresh"


async def test_get_agents_mapping_supports_membership_get_len_iteration(monkeypatch: pytest.MonkeyPatch) -> None:
    # The documented Mapping surface (spec §5.2): membership, .get()->None, len, iteration.
    view = _StubView(snapshot={"billing": _agent_card(description="X")})
    _install_opener(monkeypatch, lambda **kw: view)
    mesh = _make_mesh()
    agents = await mesh.get_agents()
    assert "billing" in agents and "ghost" not in agents
    assert agents.get("ghost") is None
    assert len(agents) == 1
    assert [name for name in agents] == ["billing"]
    assert list(agents.items()) == [("billing", agents["billing"])]
    await mesh.aclose()


def test_directly_built_client_mesh_does_not_attribute_error() -> None:
    # A Client built directly (no connect()) still has a working client.mesh — __init__ unconditionally
    # inits _mesh=None, so the accessor never AttributeErrors (plan §4 Commit 4).
    client = Client(
        Mock(),
        Mock(),
        "inbox",
        emitter_id="e",
        firehose_buffer_size=1024,
        deps_factory=None,
        provisioning=ProvisioningConfig(),
        startup_ensurer=Mock(),
        server_urls=None,
    )
    assert client._mesh is None
    assert isinstance(client.mesh, Mesh)
