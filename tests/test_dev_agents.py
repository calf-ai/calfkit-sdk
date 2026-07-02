"""Offline tests for the ``ck dev`` agent-daemon supervisor (``calfkit.cli._dev_agents``).

The agent-layer sibling of ``test_dev_broker.py``: preflight classification, the stateless
argv-marker scan, the bounded readiness poll, connect-or-spawn, and the stop/status data — all at
the supervisor seams (fake psutil / fake mesh views / FakePopen), no broker, no subprocesses.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import typer

from calfkit.cli import _dev_agents
from calfkit.cli._dev_agents import DevAgentError
from calfkit.cli._dev_broker import MeshExtraMissingError
from tests._dev_fakes import FAKE_CREATE_TIME, FakeProc, install_fake_psutil

_NODES = "tests.dev_agents_nodes"


@pytest.fixture(autouse=True)
def _isolated_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """No test touches the real ``~/.calfkit`` or inherits a mesh URL from the session env."""
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("CALFKIT_MESH_URL", raising=False)


def _daemon_argv(
    *,
    targets: tuple[str, ...] = ("app:agent",),
    host: str | None = "127.0.0.1:9092",
    marker: str | None = "--dev-daemon=general",
) -> list[str]:
    """The exact ``-d`` spawn argv shape (plan §3.1)."""
    argv = [sys.executable, "-m", "calfkit.cli", "run", *targets]
    if host is not None:
        argv += ["--host", host]
    argv += ["--provision", "--reload"]
    if marker is not None:
        argv.append(marker)
    argv += ["--heartbeat-interval", "5.0"]
    return argv


# --- preflight (spec §5.1): per-target names by kind, fail-fast usage errors --------------------------


def test_preflight_classifies_agent_and_tool_names_per_target() -> None:
    plan = _dev_agents.preflight([f"{_NODES}:general", f"{_NODES}:get_weather"])
    assert [t.spec for t in plan] == [f"{_NODES}:general", f"{_NODES}:get_weather"]
    assert plan[0].agent_names == ("general",)
    assert plan[0].tool_names == ()
    assert plan[1].agent_names == ()
    assert plan[1].tool_names == ("get_weather",)


def test_preflight_names_is_the_union_of_both_kinds() -> None:
    """The readiness set is the union of agent + tool names (spec §5.1) — a mixed target
    contributes both kinds."""
    (target,) = _dev_agents.preflight([f"{_NODES}:support_team"])
    assert target.agent_names == ("support",)
    assert target.tool_names == ("get_time",)
    assert target.names == ("support", "get_time")


def test_preflight_carries_the_node_objects() -> None:
    """``chat TARGET`` hosts the (non-reused) targets in-process — preflight must yield the
    resolved node defs themselves, not just their names."""
    (target,) = _dev_agents.preflight([f"{_NODES}:general"])
    assert [node.node_id for node in target.nodes] == ["general"]


def test_preflight_duplicate_names_across_targets_is_a_usage_error() -> None:
    """Never a silent dedupe (spec §5.1): the same advertised name arriving via two targets is
    ambiguous and errors naming the collision."""
    with pytest.raises(DevAgentError, match=r"general"):
        _dev_agents.preflight([f"{_NODES}:general", f"{_NODES}:general_dupe"])


def test_preflight_same_target_listed_twice_is_a_usage_error() -> None:
    with pytest.raises(DevAgentError, match=r"general"):
        _dev_agents.preflight([f"{_NODES}:general", f"{_NODES}:general"])


def test_preflight_zero_advert_target_is_a_usage_error() -> None:
    """Ryan's ruling (2026-07-02): a target with no presence-advertising node would be an
    invisible, unstoppable daemon (no status row, no name for stop) — fail fast instead and
    point at foreground ``ck dev run``."""
    with pytest.raises(DevAgentError, match=r"no agents or tools"):
        _dev_agents.preflight([f"{_NODES}:audit_log"])


def test_preflight_zero_advert_error_names_the_target() -> None:
    with pytest.raises(DevAgentError, match=r"audit_log"):
        _dev_agents.preflight([f"{_NODES}:audit_log"])


def test_preflight_consumer_co_hosted_with_an_advertising_node_is_fine() -> None:
    """A consumer node co-hosted WITH an advertising node rides along (the daemon is visible and
    stoppable via the advertised names); only an all-consumer target errors."""
    (target,) = _dev_agents.preflight([f"{_NODES}:desk_with_audit"])
    assert target.agent_names == ("desk",)
    assert target.names == ("desk",)  # the consumer contributes no presence name


def test_preflight_zero_advert_target_among_advertising_targets_still_errors() -> None:
    """The rule is per target: a consumer-only TARGET errors even when another target in the same
    invocation advertises fine."""
    with pytest.raises(DevAgentError, match=r"audit_log"):
        _dev_agents.preflight([f"{_NODES}:general", f"{_NODES}:audit_log"])


def test_preflight_bad_spec_exits_2_via_the_shipped_loader() -> None:
    """Import/spec failures keep the shipped ``load_nodes`` fail-fast contract (typer.Exit 2)."""
    with pytest.raises(typer.Exit) as exc:
        _dev_agents.preflight(["no_colon_here"])
    assert exc.value.exit_code == 2


# --- the stateless argv-marker scan (spec §5.4) -------------------------------------------------------


def test_scan_finds_a_marker_daemon_with_names_host_and_targets(monkeypatch: pytest.MonkeyPatch) -> None:
    argv = _daemon_argv(targets=("app:agent", "tools:all"), marker="--dev-daemon=general,get_weather")
    install_fake_psutil(monkeypatch, [FakeProc(4055, argv)])
    (hit,) = _dev_agents.scan_daemons()
    assert hit.pid == 4055
    assert hit.names == ("general", "get_weather")
    assert hit.host_key == "127.0.0.1:9092"
    assert hit.targets == ("app:agent", "tools:all")
    assert hit.started_at  # iso from create_time — the SINCE column source


def test_scan_started_at_comes_from_process_create_time(monkeypatch: pytest.MonkeyPatch) -> None:
    from datetime import datetime, timezone

    install_fake_psutil(monkeypatch, [FakeProc(4055, _daemon_argv())])
    (hit,) = _dev_agents.scan_daemons()
    assert hit.started_at == datetime.fromtimestamp(FAKE_CREATE_TIME, tz=timezone.utc).isoformat(timespec="seconds")


def test_scan_matches_only_the_supervisor_never_spawn_main_descendants(monkeypatch: pytest.MonkeyPatch) -> None:
    """The C3 test (spec §5.4): the reload/multiprocessing descendants carry ``spawn_main``-shaped
    argv without the marker — exactly one process per daemon matches."""
    supervisor = FakeProc(4055, _daemon_argv())
    worker_child = FakeProc(
        4056,
        [sys.executable, "-c", "from multiprocessing.spawn import spawn_main; spawn_main(tracker_fd=6, pipe_handle=8)", "--multiprocessing-fork"],
    )
    install_fake_psutil(monkeypatch, [supervisor, worker_child])
    hits = _dev_agents.scan_daemons()
    assert [hit.pid for hit in hits] == [4055]


def test_scan_accepts_the_space_separated_flag_form(monkeypatch: pytest.MonkeyPatch) -> None:
    """A hand-run ``ck run … --dev-daemon general`` is managed too (R3: whoever started it)."""
    argv = [sys.executable, "-m", "calfkit.cli", "run", "app:agent", "--dev-daemon", "general"]
    install_fake_psutil(monkeypatch, [FakeProc(4055, argv)])
    (hit,) = _dev_agents.scan_daemons()
    assert hit.names == ("general",)


def test_scan_without_host_scopes_to_the_default_address(monkeypatch: pytest.MonkeyPatch) -> None:
    """Spec §3.4: a marker hit whose argv lacks ``--host`` scopes to the default address."""
    install_fake_psutil(monkeypatch, [FakeProc(4055, _daemon_argv(host=None))])
    (hit,) = _dev_agents.scan_daemons()
    assert hit.host_key == "127.0.0.1:9092"


def test_scan_normalizes_the_host_for_the_address_join(monkeypatch: pytest.MonkeyPatch) -> None:
    """A hand-run ``--host localhost`` must compare equal to the normalized ``127.0.0.1:9092``."""
    install_fake_psutil(monkeypatch, [FakeProc(4055, _daemon_argv(host="localhost"))])
    (hit,) = _dev_agents.scan_daemons()
    assert hit.host_key == "127.0.0.1:9092"


def test_scan_skips_an_unparseable_host(monkeypatch: pytest.MonkeyPatch) -> None:
    """An address that does not parse cannot be joined with presence — not a daemon we can manage."""
    install_fake_psutil(monkeypatch, [FakeProc(4055, _daemon_argv(host="bad:port:extra:99999999"))])
    assert _dev_agents.scan_daemons() == []


def test_scan_skips_an_empty_marker_value(monkeypatch: pytest.MonkeyPatch) -> None:
    """A hand-run empty ``--dev-daemon=`` names nothing: invisible to status-by-name and
    unstoppable by name — not a daemon we can manage."""
    install_fake_psutil(monkeypatch, [FakeProc(4055, _daemon_argv(marker="--dev-daemon="))])
    assert _dev_agents.scan_daemons() == []


def test_scan_skips_unmarked_processes(monkeypatch: pytest.MonkeyPatch) -> None:
    tansu = FakeProc(4021, ["tansu", "broker", "--storage-engine=memory://tansu/", "--kafka-listener-url=tcp://127.0.0.1:9092"])
    install_fake_psutil(monkeypatch, [tansu, FakeProc(1, ["init"])])
    assert _dev_agents.scan_daemons() == []


def test_scan_skips_processes_that_vanish_or_deny_mid_scan(monkeypatch: pytest.MonkeyPatch) -> None:
    install_fake_psutil(
        monkeypatch,
        [FakeProc(1, raises="nosuch"), FakeProc(2, raises="denied"), FakeProc(3, raises="zombie"), FakeProc(4055, _daemon_argv())],
    )
    assert [hit.pid for hit in _dev_agents.scan_daemons()] == [4055]


def test_scan_without_psutil_raises_the_mesh_extra_hint(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "psutil", None)  # forces ModuleNotFoundError on import
    with pytest.raises(MeshExtraMissingError, match=r"\[mesh\]"):
        _dev_agents.scan_daemons()


def test_find_daemons_filters_by_host_key(monkeypatch: pytest.MonkeyPatch) -> None:
    here = FakeProc(4055, _daemon_argv(marker="--dev-daemon=general"))
    elsewhere = FakeProc(4102, _daemon_argv(host="127.0.0.1:19092", marker="--dev-daemon=finance"))
    install_fake_psutil(monkeypatch, [here, elsewhere])
    assert [hit.pid for hit in _dev_agents.find_daemons("127.0.0.1:9092")] == [4055]
    assert [hit.pid for hit in _dev_agents.find_daemons(None)] == [4055, 4102]


# --- the log slug: derived identically at spawn and scan time (handoff rule) --------------------------


def test_scan_derives_the_same_log_path_the_spawn_writes(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    targets = ("general_help:general",)
    spawn_side = _dev_agents.daemon_log_path("127.0.0.1:9092", targets)
    install_fake_psutil(monkeypatch, [FakeProc(4055, _daemon_argv(targets=targets))])
    (hit,) = _dev_agents.scan_daemons()
    assert hit.log_path == str(spawn_side)
    assert str(tmp_path) in hit.log_path  # under the isolated HOME's ~/.calfkit/logs
    assert Path(hit.log_path).name.startswith("agents-")


def test_daemon_log_path_sanitizes_path_hostile_characters() -> None:
    path = _dev_agents.daemon_log_path("127.0.0.1:9092", ("pkg.mod:attr", "other:one"))
    name = Path(path).name
    assert name.endswith(".log")
    assert ":" not in name and "/" not in name and "," not in name
