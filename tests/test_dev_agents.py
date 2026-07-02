"""Offline tests for the ``ck dev`` agent-daemon supervisor (``calfkit.cli._dev_agents``).

The agent-layer sibling of ``test_dev_broker.py``: preflight classification, the stateless
argv-marker scan, the bounded readiness poll, connect-or-spawn, and the stop/status data — all at
the supervisor seams (fake psutil / fake mesh views / FakePopen), no broker, no subprocesses.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
import typer

import calfkit.cli._dev_broker as dev_broker_module
from calfkit.cli import _dev_agents
from calfkit.cli._dev_agents import DevAgentError
from calfkit.cli._dev_broker import MeshExtraMissingError, normalize
from calfkit.client.mesh import AgentInfo, ToolNodeInfo
from calfkit.exceptions import MeshUnavailableError
from tests._dev_fakes import FAKE_CREATE_TIME, FakeMesh, FakePopen, FakeProc, install_fake_psutil

_NODES = "tests.dev_agents_nodes"


def _agent_info(name: str, *, age: float = 0.0) -> AgentInfo:
    return AgentInfo(name=name, description=None, last_seen=datetime.now(tz=timezone.utc) - timedelta(seconds=age))


def _tool_info(name: str, *, age: float = 0.0) -> ToolNodeInfo:
    return ToolNodeInfo(name=name, description=None, parameters_schema={}, last_seen=datetime.now(tz=timezone.utc) - timedelta(seconds=age))


def _unavailable() -> MeshUnavailableError:
    return MeshUnavailableError("no plane yet", reason="open_failed")


def _record_killpg(monkeypatch: pytest.MonkeyPatch) -> list[tuple[int, int]]:
    """Never let a test signal a real process group: replace the killpg seam with a recorder."""
    calls: list[tuple[int, int]] = []
    monkeypatch.setattr(_dev_agents, "_killpg", lambda pgid, sig: calls.append((pgid, sig)))
    return calls


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


def test_scan_requires_the_emitted_marker_form_never_the_bare_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """Review round 1 (Ryan-approved): the marker matches ONLY its emitted ``--dev-daemon=<names>``
    form — a bare ``--dev-daemon`` token appearing as *data* in some other process's argv (e.g.
    ``rg -- --dev-daemon docs/``) must never be claimed as a daemon, or ``ck dev down`` would
    group-kill an innocent process."""
    searcher = FakeProc(6001, ["rg", "--", "--dev-daemon", "docs/"])
    space_form = FakeProc(6002, [sys.executable, "-m", "calfkit.cli", "run", "app:agent", "--dev-daemon", "general"])
    install_fake_psutil(monkeypatch, [searcher, space_form])
    assert _dev_agents.scan_daemons() == []


def test_scan_requires_a_run_token_alongside_the_marker(monkeypatch: pytest.MonkeyPatch) -> None:
    """Second anchor (review round 1, Ryan-approved): the emitted marker only ever rides a
    ``run`` command line, so a process merely *mentioning* the ``=``-form (e.g. a grep of a log
    line) is still not a daemon."""
    grepper = FakeProc(6003, ["rg", "--", "--dev-daemon=general", "notes.md"])
    hand_run_ck = FakeProc(6004, ["/venv/bin/ck", "run", "app:agent", "--dev-daemon=general"])
    install_fake_psutil(monkeypatch, [grepper, hand_run_ck])
    (hit,) = _dev_agents.scan_daemons()
    assert hit.pid == 6004  # the hand-run `ck run` form stays managed (R3: whoever started it)
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


def test_scan_skips_a_marker_on_a_non_run_command(monkeypatch: pytest.MonkeyPatch) -> None:
    """The ``run`` anchor is required (review round 1): a marker on an argv with no ``run`` verb
    is data, not a daemon — unmatchable, so unmanageable, so never signalled."""
    argv = [sys.executable, "-m", "calfkit.cli", "--dev-daemon=general", "--host", "127.0.0.1:9092"]
    install_fake_psutil(monkeypatch, [FakeProc(4055, argv)])
    assert _dev_agents.scan_daemons() == []


def test_killpg_seam_delivers_via_os_killpg(monkeypatch: pytest.MonkeyPatch) -> None:
    """The seam's body is the one real ``os.killpg`` call site (call-time lookup so module import
    stays platform-safe); everything else in the suite replaces the seam itself."""
    import os as os_module

    calls: list[tuple[int, int]] = []
    monkeypatch.setattr(os_module, "killpg", lambda pgid, sig: calls.append((pgid, sig)))
    dev_broker_module._killpg(123, 15)
    assert calls == [(123, 15)]


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


# --- the bounded readiness poll (spec §5.2): the broker loop, one layer up ----------------------------


def _fast_wait(**kwargs: Any) -> dict[str, Any]:
    """Tight loop parameters so deadline tests run in real milliseconds."""
    return {"timeout": kwargs.pop("timeout", 0.25), "poll_interval": 0.01, **kwargs}


async def test_wait_ready_returns_once_every_name_is_online(tmp_path: Path) -> None:
    mesh = FakeMesh(agents_frames=[{}, {"general": _agent_info("general")}])
    await _dev_agents.wait_agents_ready(None, ("general",), (), mesh, log_path=None, **_fast_wait())


async def test_wait_ready_child_death_is_checked_first_and_wins_ties(tmp_path: Path) -> None:
    """The supervisor died: even with the mesh reporting the names online, the child-exit arm
    fires first (spec §5.2 arm 1) with the log tail."""
    log = tmp_path / "dead.log"
    log.write_text("boom: broker rejected the connection")
    proc = FakePopen(["cmd"], stdout=None)
    proc.returncode = 2
    mesh = FakeMesh(agents_frames=[{"general": _agent_info("general")}])
    with pytest.raises(DevAgentError, match=r"(?s)exited.*boom: broker rejected") as exc:
        await _dev_agents.wait_agents_ready(proc, ("general",), (), mesh, log_path=str(log), **_fast_wait())
    assert "exit code 2" in str(exc.value)


async def test_wait_ready_open_failed_counts_as_not_ready_and_retries(tmp_path: Path) -> None:
    """The fresh-broker race (spec §5.5): an unopenable presence view is 'not ready yet', absorbed
    inside the deadline — the spawned worker's provisioning creates the topics meanwhile."""
    mesh = FakeMesh(agents_frames=[_unavailable(), _unavailable(), {"general": _agent_info("general")}])
    await _dev_agents.wait_agents_ready(None, ("general",), (), mesh, log_path=None, **_fast_wait())


async def test_wait_ready_mixed_kinds_need_both_views(tmp_path: Path) -> None:
    """The readiness set is the union across kinds: an online agent alone must not satisfy a
    target that also advertises a tool."""
    mesh = FakeMesh(
        agents_frames=[{"support": _agent_info("support")}],
        tools_frames=[{}, {}, {"get_time": _tool_info("get_time")}],
    )
    await _dev_agents.wait_agents_ready(None, ("support",), ("get_time",), mesh, log_path=None, **_fast_wait())


async def test_wait_ready_deadline_group_kills_the_spawn_and_reports_the_log_tail(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    killed = _record_killpg(monkeypatch)
    log = tmp_path / "slow.log"
    log.write_text("still importing torch...")
    proc = FakePopen(["cmd"], stdout=None)  # never exits (returncode None)
    mesh = FakeMesh(agents_frames=[{}])  # never online
    with pytest.raises(DevAgentError, match=r"(?s)did not come online.*still importing torch"):
        await _dev_agents.wait_agents_ready(proc, ("general",), (), mesh, log_path=str(log), **_fast_wait())
    import signal

    assert killed == [(proc.pid, signal.SIGKILL)]


async def test_wait_ready_deadline_without_a_process_raises_without_killing(monkeypatch: pytest.MonkeyPatch) -> None:
    """The chat variant (proc=None): the same loop minus the process arms — a deadline raises,
    nothing is signalled (the in-process worker is the caller's to stop)."""
    killed = _record_killpg(monkeypatch)
    mesh = FakeMesh(agents_frames=[{}])
    with pytest.raises(DevAgentError, match=r"did not come online"):
        await _dev_agents.wait_agents_ready(None, ("general",), (), mesh, log_path=None, **_fast_wait())
    assert killed == []


async def test_wait_ready_checks_names_against_one_captured_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    """The raced-record protocol: each iteration captures the snapshot once and checks every name
    against that capture — alternating single-name reads must never add up to 'ready'."""
    killed = _record_killpg(monkeypatch)
    frames: list[Any] = [{"a": _agent_info("a")}, {"b": _agent_info("b")}] * 40
    mesh = FakeMesh(agents_frames=frames)
    with pytest.raises(DevAgentError, match=r"did not come online"):
        await _dev_agents.wait_agents_ready(None, ("a", "b"), (), mesh, log_path=None, **_fast_wait())
    assert killed == []


async def test_wait_ready_reads_only_the_kinds_the_targets_advertise() -> None:
    """An agents-only launch must not open/read the tools view at all (plan §3.4: per the
    target's kinds)."""
    mesh = FakeMesh(agents_frames=[{"general": _agent_info("general")}])
    await _dev_agents.wait_agents_ready(None, ("general",), (), mesh, log_path=None, **_fast_wait())
    assert set(mesh.reads) == {"agents"}


async def test_wait_ready_reader_dead_fails_fast_naming_the_client_side_failure() -> None:
    """Review round 1 (Ryan-approved): ``reader_dead`` is terminal for the view's lifetime —
    polling it for 15s and then blaming the daemon's healthy log misdiagnoses a client-side
    failure. Fail fast, naming the real culprit."""
    dead = MeshUnavailableError("the agents reader died", reason="reader_dead")
    mesh = FakeMesh(agents_frames=[dead])
    with pytest.raises(DevAgentError, match=r"(?s)presence view died.*not the agents") as exc:
        await _dev_agents.wait_agents_ready(None, ("general",), (), mesh, log_path=None, **_fast_wait())
    assert exc.value.__cause__ is dead


async def test_wait_ready_reader_dead_still_tears_down_the_spawn(monkeypatch: pytest.MonkeyPatch) -> None:
    """The fail-fast arm must not strand a half-started spawn: the group is killed exactly as on
    the deadline path."""
    import signal

    killed = _record_killpg(monkeypatch)
    proc = FakePopen(["cmd"], stdout=None)
    mesh = FakeMesh(agents_frames=[MeshUnavailableError("died", reason="reader_dead")])
    with pytest.raises(DevAgentError, match="presence view died"):
        await _dev_agents.wait_agents_ready(proc, ("general",), (), mesh, log_path=None, **_fast_wait())
    assert killed == [(proc.pid, signal.SIGKILL)]


async def test_wait_ready_deadline_names_the_unreadable_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    """A deadline whose LAST read failed must say so — 'did not come online' would point the user
    at their agent when the presence read itself was the problem. Round-2 minor 2: the message
    claims only what is known (the last read), never 'never became readable' — the view may have
    been readable for most of the wait before a late failure."""
    _record_killpg(monkeypatch)
    establishing = MeshUnavailableError("catching up", reason="establishing")
    mesh = FakeMesh(agents_frames=[establishing])
    with pytest.raises(DevAgentError, match=r"(?s)did not come online.*last presence read failed.*establishing"):
        await _dev_agents.wait_agents_ready(None, ("general",), (), mesh, log_path=None, **_fast_wait())


async def test_wait_ready_deadline_after_a_late_read_failure_stays_accurate(monkeypatch: pytest.MonkeyPatch) -> None:
    """Readable for most of the wait (names just offline), then the broker dies at the end: the
    message must not claim the view was 'never readable'."""
    _record_killpg(monkeypatch)
    frames: list[Any] = [{}, {}, {}, MeshUnavailableError("gone", reason="open_failed")]
    mesh = FakeMesh(agents_frames=frames)
    with pytest.raises(DevAgentError, match=r"(?s)did not come online.*last presence read failed \(open_failed") as exc:
        await _dev_agents.wait_agents_ready(None, ("general",), (), mesh, log_path=None, **_fast_wait())
    assert "never" not in str(exc.value)


# --- connect-or-spawn evaluation (spec §5.5) ----------------------------------------------------------


def _target(spec_attr: str) -> _dev_agents.TargetNodes:
    (target,) = _dev_agents.preflight([f"{_NODES}:{spec_attr}"])
    return target


_KEY = "127.0.0.1:9092"


async def test_evaluate_all_names_online_reuses_with_heartbeat_ages(monkeypatch: pytest.MonkeyPatch) -> None:
    install_fake_psutil(monkeypatch, [])
    mesh = FakeMesh(agents_frames=[{"general": _agent_info("general", age=3.0)}])
    reused, to_launch = await _dev_agents.evaluate_targets([_target("general")], _KEY, mesh)
    assert to_launch == []
    (outcome,) = reused
    assert outcome.target.spec == f"{_NODES}:general"
    assert 2.5 <= outcome.ages["general"] <= 10.0


async def test_evaluate_none_online_no_marker_spawns(monkeypatch: pytest.MonkeyPatch) -> None:
    install_fake_psutil(monkeypatch, [])
    mesh = FakeMesh()
    reused, to_launch = await _dev_agents.evaluate_targets([_target("general")], _KEY, mesh)
    assert reused == []
    assert [t.spec for t in to_launch] == [f"{_NODES}:general"]


async def test_evaluate_unopenable_view_counts_as_none_online(monkeypatch: pytest.MonkeyPatch) -> None:
    install_fake_psutil(monkeypatch, [])
    mesh = FakeMesh(agents_frames=[_unavailable()])
    _, to_launch = await _dev_agents.evaluate_targets([_target("general")], _KEY, mesh)
    assert [t.spec for t in to_launch] == [f"{_NODES}:general"]


async def test_evaluate_non_open_failed_read_failures_error_instead_of_classifying(monkeypatch: pytest.MonkeyPatch) -> None:
    """Spec §5.5 pins only ``open_failed`` as none-online. ``establishing``/``reader_dead`` are a
    different class: classifying on them could brand a HEALTHY daemon 'broken' (and advise
    'ck dev stop' against it) or double-spawn — error naming the read failure instead (review
    round 1, Ryan-approved)."""
    daemon = FakeProc(4055, _daemon_argv(marker="--dev-daemon=general"))
    install_fake_psutil(monkeypatch, [daemon])
    for reason in ("establishing", "reader_dead"):
        mesh = FakeMesh(agents_frames=[MeshUnavailableError("unreadable", reason=reason)])
        with pytest.raises(DevAgentError, match=rf"(?s)could not be read.*{reason}"):
            await _dev_agents.evaluate_targets([_target("general")], _KEY, mesh)


async def test_evaluate_broken_daemon_errors_instead_of_a_second_spawn(monkeypatch: pytest.MonkeyPatch) -> None:
    """Never a second daemon over a broken first (Ryan's ruling, spec §5.5): offline names owned
    by a live marker daemon error with pid, the offline explanation, logs, and the stop hint."""
    daemon = FakeProc(4055, _daemon_argv(targets=("general_help:general",), marker="--dev-daemon=general"))
    install_fake_psutil(monkeypatch, [daemon])
    mesh = FakeMesh()  # nothing online
    with pytest.raises(DevAgentError, match=r"already exists \(pid 4055.*offline.*ck dev stop general") as exc:
        await _dev_agents.evaluate_targets([_target("general")], _KEY, mesh)
    assert "agents-" in str(exc.value)  # the log path is named


async def test_evaluate_broken_daemon_on_another_address_does_not_block(monkeypatch: pytest.MonkeyPatch) -> None:
    """Address scoping: a marker daemon for the same names on a DIFFERENT mesh is unrelated."""
    elsewhere = FakeProc(4055, _daemon_argv(host="127.0.0.1:19092", marker="--dev-daemon=general"))
    install_fake_psutil(monkeypatch, [elsewhere])
    mesh = FakeMesh()
    _, to_launch = await _dev_agents.evaluate_targets([_target("general")], _KEY, mesh)
    assert [t.spec for t in to_launch] == [f"{_NODES}:general"]


async def test_evaluate_partial_online_within_one_target_errors_naming_the_collision(monkeypatch: pytest.MonkeyPatch) -> None:
    install_fake_psutil(monkeypatch, [])
    mesh = FakeMesh(agents_frames=[{"support": _agent_info("support")}])  # get_time offline
    with pytest.raises(DevAgentError, match=r"support.*online.*get_time"):
        await _dev_agents.evaluate_targets([_target("support_team")], _KEY, mesh)


async def test_evaluate_mixed_invocation_splits_reuse_and_launch(monkeypatch: pytest.MonkeyPatch) -> None:
    install_fake_psutil(monkeypatch, [])
    mesh = FakeMesh(agents_frames=[{"general": _agent_info("general")}])
    reused, to_launch = await _dev_agents.evaluate_targets([_target("general"), _target("get_weather")], _KEY, mesh)
    assert [o.target.spec for o in reused] == [f"{_NODES}:general"]
    assert [t.spec for t in to_launch] == [f"{_NODES}:get_weather"]


async def test_evaluate_tool_targets_reuse_via_the_tools_view(monkeypatch: pytest.MonkeyPatch) -> None:
    install_fake_psutil(monkeypatch, [])
    mesh = FakeMesh(tools_frames=[{"get_weather": _tool_info("get_weather", age=2.0)}])
    reused, to_launch = await _dev_agents.evaluate_targets([_target("get_weather")], _KEY, mesh)
    assert to_launch == []
    assert "get_weather" in reused[0].ages


# --- ensure_agents: the -d orchestration under the agent-layer flock (spec §5.1) ----------------------


def _capture_spawns(monkeypatch: pytest.MonkeyPatch) -> list[FakePopen]:
    spawned: list[FakePopen] = []

    def factory(cmd: list[str], **kwargs: object) -> FakePopen:
        proc = FakePopen(cmd, **kwargs)
        spawned.append(proc)
        return proc

    monkeypatch.setattr(_dev_agents, "Popen", factory)
    return spawned


def _run_args(**overrides: Any) -> _dev_agents.RunOptions:
    return _dev_agents.RunOptions(**overrides)


async def test_ensure_spawns_one_daemon_for_all_offline_targets(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """One worker per invocation (spec §4): both targets co-host in a single spawn whose argv is
    the pinned §5.1 shape — targets, normalized --host, presets, the marker, the 5s heartbeat."""
    install_fake_psutil(monkeypatch, [])
    spawned = _capture_spawns(monkeypatch)
    mesh = FakeMesh(
        agents_frames=[{}, {"general": _agent_info("general")}],
        tools_frames=[{}, {}, {"get_weather": _tool_info("get_weather")}],
    )
    report = await _dev_agents.ensure_agents(
        [_target("general"), _target("get_weather")],
        normalize([_KEY]),
        mesh,
        run_args=_run_args(),
        **_fast_wait(timeout=5.0),
    )
    (proc,) = spawned
    assert proc.cmd[:2] == [sys.executable, "-m"]
    assert proc.cmd[2:4] == ["calfkit.cli", "run"]
    assert proc.cmd[4:6] == [f"{_NODES}:general", f"{_NODES}:get_weather"]
    host_index = proc.cmd.index("--host")
    assert proc.cmd[host_index + 1] == _KEY
    assert "--provision" in proc.cmd
    assert "--reload" in proc.cmd
    assert "--dev-daemon=general,get_weather" in proc.cmd
    heartbeat_index = proc.cmd.index("--heartbeat-interval")
    assert proc.cmd[heartbeat_index + 1] == "5.0"
    # The detach + log-fd contract (the broker Popen pattern verbatim): stdout and stderr share
    # the REAL log-file fd (never PIPE — an unread pipe would deadlock a chatty worker), stdin is
    # detached, and the parent closes its copy of the fd after the spawn.
    import subprocess as subprocess_module

    assert proc.kwargs["start_new_session"] is True
    assert proc.kwargs["stdin"] is subprocess_module.DEVNULL
    assert proc.kwargs["stdout"] is proc.kwargs["stderr"]
    assert Path(proc.kwargs["stdout"].name) == Path(str(report.log_path))
    assert proc.kwargs["stdout"].closed  # the parent's copy — the daemon holds its own
    assert report.pid == proc.pid
    assert report.log_path is not None and Path(report.log_path).exists()
    assert [o.reused for o in report.outcomes] == [False, False]


async def test_ensure_all_reused_spawns_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    install_fake_psutil(monkeypatch, [])
    spawned = _capture_spawns(monkeypatch)
    mesh = FakeMesh(agents_frames=[{"general": _agent_info("general", age=1.0)}])
    report = await _dev_agents.ensure_agents([_target("general")], normalize([_KEY]), mesh, run_args=_run_args(), **_fast_wait())
    assert spawned == []
    assert report.pid is None and report.log_path is None
    assert [o.reused for o in report.outcomes] == [True]


async def test_ensure_forwards_the_users_run_options_into_the_daemon_argv(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """'Everything ck dev run is today' (spec §3.1): the daemon child receives the caller's
    non-default options; --no-provision / --no-reload drop their presets."""
    install_fake_psutil(monkeypatch, [])
    spawned = _capture_spawns(monkeypatch)
    mesh = FakeMesh(agents_frames=[{}, {"general": _agent_info("general")}])
    await _dev_agents.ensure_agents(
        [_target("general")],
        normalize([_KEY]),
        mesh,
        run_args=_run_args(
            provision=False,
            reload=False,
            reload_dir=["src", "lib"],
            app_dir=str(tmp_path),
            group_id="g1",
            env_file="custom.env",
            enable_idempotence=True,
        ),
        **_fast_wait(timeout=5.0),
    )
    (proc,) = spawned
    assert "--provision" not in proc.cmd
    assert "--reload" not in proc.cmd
    assert proc.cmd[proc.cmd.index("--app-dir") + 1] == str(tmp_path)
    assert proc.cmd[proc.cmd.index("--group-id") + 1] == "g1"
    assert proc.cmd[proc.cmd.index("--env-file") + 1] == "custom.env"
    assert "--enable-idempotence" in proc.cmd
    reload_dirs = [proc.cmd[i + 1] for i, tok in enumerate(proc.cmd) if tok == "--reload-dir"]
    assert reload_dirs == ["src", "lib"]


async def test_ensure_overwrites_the_log_per_spawn(monkeypatch: pytest.MonkeyPatch) -> None:
    install_fake_psutil(monkeypatch, [])
    monkeypatch.setattr(_dev_agents, "Popen", FakePopen)
    log_path = _dev_agents.daemon_log_path(_KEY, (f"{_NODES}:general",))
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text("old contents from the previous spawn")
    mesh = FakeMesh(agents_frames=[{}, {"general": _agent_info("general")}])
    await _dev_agents.ensure_agents([_target("general")], normalize([_KEY]), mesh, run_args=_run_args(), **_fast_wait(timeout=5.0))
    assert log_path.read_text() == ""  # truncated by the fresh open


async def test_ensure_readiness_failure_propagates_after_the_group_kill(monkeypatch: pytest.MonkeyPatch) -> None:
    install_fake_psutil(monkeypatch, [])
    killed = _record_killpg(monkeypatch)
    spawned = _capture_spawns(monkeypatch)
    mesh = FakeMesh()  # never online
    with pytest.raises(DevAgentError, match=r"did not come online"):
        await _dev_agents.ensure_agents([_target("general")], normalize([_KEY]), mesh, run_args=_run_args(), **_fast_wait())
    assert len(spawned) == 1
    assert killed and killed[0][0] == spawned[0].pid


# --- stop: whole-daemon group signalling (spec §3.4) --------------------------------------------------


def _hit(proc: FakeProc, names: tuple[str, ...] = ("general",), host_key: str = _KEY) -> _dev_agents.DaemonHit:
    return _dev_agents.DaemonHit(
        proc=proc,
        pid=proc.pid,
        names=names,
        host_key=host_key,
        targets=("app:agent",),
        log_path="/tmp/agents-x.log",
        started_at="2026-07-02T00:00:00+00:00",
    )


def _group_killer(monkeypatch: pytest.MonkeyPatch, procs: list[FakeProc], *, dies_on: int | None = None) -> list[tuple[int, int]]:
    """Replace the killpg seam with a recorder that flips the leader's FakeProc state, modelling
    the group signal reaching the tree. ``dies_on`` = the signal that actually kills (None = both)."""
    import signal as signal_module

    calls: list[tuple[int, int]] = []

    def fake_killpg(pgid: int, sig: int) -> None:
        calls.append((pgid, sig))
        for proc in procs:
            if proc.pid == pgid and (dies_on is None or sig == dies_on):
                proc.alive = False

    monkeypatch.setattr(dev_broker_module, "_killpg", fake_killpg)
    assert signal_module.SIGTERM  # keep the import obviously used
    return calls


def test_stop_daemon_signals_the_group_term_then_a_final_kill_sweep(monkeypatch: pytest.MonkeyPatch) -> None:
    """Review round 1 (Ryan-approved): leader death alone must not declare the GROUP dead — a
    SIGTERM-surviving grandchild (a node's own subprocess) would silently outlive the 'stopped'
    report, markerless and unmanageable. After the leader is gone, one unconditional group
    SIGKILL sweeps the now-headless tree."""
    import signal as signal_module

    install_fake_psutil(monkeypatch, [])
    proc = FakeProc(4055)
    calls = _group_killer(monkeypatch, [proc])
    assert _dev_agents.stop_daemon(_hit(proc), grace=0.3) is True
    assert calls == [(4055, signal_module.SIGTERM), (4055, signal_module.SIGKILL)]


def test_stop_daemon_final_sweep_tolerates_an_already_empty_group(monkeypatch: pytest.MonkeyPatch) -> None:
    """The final sweep is best-effort: a fully-dead group raises ProcessLookupError — the goal
    state, not a failure."""
    import signal as signal_module

    install_fake_psutil(monkeypatch, [])
    proc = FakeProc(4055)
    calls: list[tuple[int, int]] = []

    def killpg_then_gone(pgid: int, sig: int) -> None:
        calls.append((pgid, sig))
        if sig == signal_module.SIGTERM:
            proc.alive = False
        else:
            raise ProcessLookupError("group already empty")

    monkeypatch.setattr(dev_broker_module, "_killpg", killpg_then_gone)
    assert _dev_agents.stop_daemon(_hit(proc), grace=0.3) is True
    assert [sig for _, sig in calls] == [signal_module.SIGTERM, signal_module.SIGKILL]


def test_stop_daemon_never_signals_a_recycled_pid(monkeypatch: pytest.MonkeyPatch) -> None:
    """Review round 1 (Ryan-approved): the group path must keep psutil's identity guard — if the
    scanned daemon exited between scan and signal (its pid possibly recycled by an unrelated
    process group), no raw killpg may ever be sent; the exit IS the goal state."""
    install_fake_psutil(monkeypatch, [])
    proc = FakeProc(4055)
    proc.alive = False  # exited after the scan; pid may now belong to an innocent group
    calls = _group_killer(monkeypatch, [proc])
    assert _dev_agents.stop_daemon(_hit(proc), grace=0.3) is True
    assert calls == []  # no signal was ever delivered to the (possibly recycled) pgid


def test_stop_daemon_escalates_to_group_sigkill_after_grace(monkeypatch: pytest.MonkeyPatch) -> None:
    import signal as signal_module

    install_fake_psutil(monkeypatch, [])
    proc = FakeProc(4055, survives_sigterm=True)
    calls = _group_killer(monkeypatch, [proc], dies_on=signal_module.SIGKILL)
    assert _dev_agents.stop_daemon(_hit(proc), grace=0.3) is True
    assert calls == [(4055, signal_module.SIGTERM), (4055, signal_module.SIGKILL)]


def test_stop_daemon_denied_group_signal_returns_false(monkeypatch: pytest.MonkeyPatch) -> None:
    install_fake_psutil(monkeypatch, [])
    proc = FakeProc(4055)

    def deny(pgid: int, sig: int) -> None:
        raise PermissionError("not yours")

    monkeypatch.setattr(dev_broker_module, "_killpg", deny)
    assert _dev_agents.stop_daemon(_hit(proc), grace=0.3) is False


def test_stop_daemon_vanished_group_is_the_goal_state(monkeypatch: pytest.MonkeyPatch) -> None:
    install_fake_psutil(monkeypatch, [])
    proc = FakeProc(4055)

    def gone(pgid: int, sig: int) -> None:
        raise ProcessLookupError("already gone")

    monkeypatch.setattr(dev_broker_module, "_killpg", gone)
    assert _dev_agents.stop_daemon(_hit(proc), grace=0.3) is True


def test_stop_daemon_zombie_leader_counts_as_stopped(monkeypatch: pytest.MonkeyPatch) -> None:
    """The broker's zombie discipline holds one layer up: a dead-but-unreaped leader (its
    spawning shell still alive) IS the stopped state, not a failure."""
    import signal as signal_module

    install_fake_psutil(monkeypatch, [])
    proc = FakeProc(4055)
    calls: list[tuple[int, int]] = []

    def zombify(pgid: int, sig: int) -> None:
        calls.append((pgid, sig))
        proc.zombie = True

    monkeypatch.setattr(dev_broker_module, "_killpg", zombify)
    assert _dev_agents.stop_daemon(_hit(proc), grace=0.3) is True
    # The zombie leader is the stopped state; the final group sweep still fires (a zombie-only
    # group absorbs it harmlessly, a straggler member gets reaped).
    assert calls == [(4055, signal_module.SIGTERM), (4055, signal_module.SIGKILL)]


def test_stop_daemon_sweeps_when_the_leader_dies_mid_escalation(monkeypatch: pytest.MonkeyPatch) -> None:
    """Round-2 minor 1: leader survives TERM through the grace, then dies just before the KILL
    send — the identity gate raises NoSuchProcess, but a group signal WAS already delivered this
    call, so descendants may remain: the final sweep must still fire (the pgid stays reserved by
    any survivor, so it is safe)."""
    import signal as signal_module

    install_fake_psutil(monkeypatch, [])

    class _DiesBeforeKill(FakeProc):
        def __init__(self, pid: int) -> None:
            super().__init__(pid, survives_sigterm=True)
            self._running_answers = [True, False]  # TERM gate: alive; KILL gate: just died

        def is_running(self) -> bool:
            return self._running_answers.pop(0) if self._running_answers else False

    proc = _DiesBeforeKill(4055)
    calls: list[tuple[int, int]] = []
    monkeypatch.setattr(dev_broker_module, "_killpg", lambda pgid, sig: calls.append((pgid, sig)))
    assert _dev_agents.stop_daemon(_hit(proc), grace=0.3) is True
    assert calls == [(4055, signal_module.SIGTERM), (4055, signal_module.SIGKILL)]


def test_stop_daemon_no_sweep_when_no_signal_was_ever_delivered(monkeypatch: pytest.MonkeyPatch) -> None:
    """The counter-case pinning the guard: the identity gate refusing the FIRST send (recycled
    pid) must not be followed by a sweep — no signal was delivered, and killpg could hit an
    innocent recycled group. (Same assertion as the recycled-pid test, restated against the
    sweep specifically.)"""
    install_fake_psutil(monkeypatch, [])
    proc = FakeProc(4055)
    proc.alive = False
    calls = _group_killer(monkeypatch, [proc])
    assert _dev_agents.stop_daemon(_hit(proc), grace=0.3) is True
    assert calls == []


def test_daemon_grace_is_at_least_eight_seconds() -> None:
    """Spec §3.4: the reload supervisor's own teardown budget is ~6s (SIGINT→join(5)→SIGKILL→
    join(1)); a shorter grace would SIGKILL it mid-shutdown and orphan the worker. The broker's
    own 5s default stays untouched (R4: parameterized per call site)."""
    import inspect

    assert _dev_agents.DAEMON_GRACE >= 8.0
    assert inspect.signature(_dev_agents.stop_daemon).parameters["grace"].default == _dev_agents.DAEMON_GRACE
    assert dev_broker_module.DEFAULT_GRACE == 5.0


# --- stop resolution: names -> daemons at one address (spec §3.4) -------------------------------------


def test_resolve_stop_dedupes_co_hosted_names_to_one_daemon(monkeypatch: pytest.MonkeyPatch) -> None:
    daemon = _hit(FakeProc(4055), names=("general", "finance"))
    hits = _dev_agents.resolve_stop(["general", "finance"], [daemon], host_key=_KEY, online=set())
    assert [h.pid for h in hits] == [4055]


def test_resolve_stop_multiple_names_across_daemons(monkeypatch: pytest.MonkeyPatch) -> None:
    first = _hit(FakeProc(4055), names=("general",))
    second = _hit(FakeProc(4102), names=("finance",))
    hits = _dev_agents.resolve_stop(["finance", "general"], [first, second], host_key=_KEY, online=set())
    assert [h.pid for h in hits] == [4102, 4055]


def test_resolve_stop_unknown_name_lists_the_address_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exit-2 material naming what IS running at the target address and how to look elsewhere."""
    daemon = _hit(FakeProc(4055), names=("general",))
    with pytest.raises(DevAgentError, match=r"(?s)'nope'.*running at 127\.0\.0\.1:9092: general.*--host"):
        _dev_agents.resolve_stop(["nope"], [daemon], host_key=_KEY, online=set())


def test_resolve_stop_unknown_name_with_no_daemons_says_none(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(DevAgentError, match=r"(?s)running at 127\.0\.0\.1:9092: none.*--host"):
        _dev_agents.resolve_stop(["nope"], [], host_key=_KEY, online=set())


def test_resolve_stop_online_but_unmanaged_name_explains_why(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(DevAgentError, match=r"'support' is not a ck dev daemon — stop it where it runs"):
        _dev_agents.resolve_stop(["support"], [], host_key=_KEY, online={"support"})


def test_resolve_stop_two_same_named_hits_lists_both_pids_never_guesses(monkeypatch: pytest.MonkeyPatch) -> None:
    """Hand-run markers / dead-broker windows can leave two same-named daemons at one address —
    exit 2 listing both pids, never a guess (spec §3.4)."""
    first = _hit(FakeProc(4055), names=("general",))
    second = _hit(FakeProc(4102), names=("general",))
    with pytest.raises(DevAgentError, match=r"(?s)4055.*4102"):
        _dev_agents.resolve_stop(["general"], [first, second], host_key=_KEY, online=set())


async def test_ensure_unwritable_log_location_is_exit_2_material(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    install_fake_psutil(monkeypatch, [])
    blocker = tmp_path / "blocker"
    blocker.write_text("a file where a directory must go")
    monkeypatch.setattr(_dev_agents, "daemon_log_path", lambda key, targets: blocker / "logs" / "agents-x.log")
    with pytest.raises(DevAgentError, match=r"cannot write the agent daemon log"):
        await _dev_agents.ensure_agents([_target("general")], normalize([_KEY]), FakeMesh(), run_args=_run_args(), **_fast_wait())


async def test_ensure_exec_failure_is_exit_2_material(monkeypatch: pytest.MonkeyPatch) -> None:
    install_fake_psutil(monkeypatch, [])

    def boom(cmd: list[str], **kwargs: object) -> FakePopen:
        raise OSError("exec format error")

    monkeypatch.setattr(_dev_agents, "Popen", boom)
    with pytest.raises(DevAgentError, match=r"failed to launch the agent daemon"):
        await _dev_agents.ensure_agents([_target("general")], normalize([_KEY]), FakeMesh(), run_args=_run_args(), **_fast_wait())


async def test_ensure_holds_the_agents_flock_across_its_critical_section(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """The double-spawn race (spec §5.1): the whole check→spawn→readiness section holds
    ~/.calfkit/dev-agents.lock exclusively; a concurrent ensure would block until release."""
    import fcntl

    install_fake_psutil(monkeypatch, [])
    monkeypatch.setattr(_dev_agents, "Popen", FakePopen)
    lock_state: dict[str, Any] = {}

    class ProbingMesh(FakeMesh):
        async def get_agents(self) -> Any:
            if "held" not in lock_state:
                fd = (tmp_path / ".calfkit" / "dev-agents.lock").open("rb")
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    lock_state["held"] = True  # the supervisor holds it mid-section
                else:
                    lock_state["held"] = False
                    fcntl.flock(fd, fcntl.LOCK_UN)
                finally:
                    fd.close()
            return await super().get_agents()

    mesh = ProbingMesh(agents_frames=[{}, {"general": _agent_info("general")}])
    await _dev_agents.ensure_agents([_target("general")], normalize([_KEY]), mesh, run_args=_run_args(), **_fast_wait(timeout=5.0))
    assert lock_state["held"] is True
    # Released after: an exclusive claim now succeeds.
    with (tmp_path / ".calfkit" / "dev-agents.lock").open("rb") as fd:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(fd, fcntl.LOCK_UN)
