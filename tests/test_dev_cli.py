"""CliRunner tests for the ``ck dev`` command group (spec §4).

The supervisor is monkeypatched at its seams (``calfkit.cli._dev_broker.*`` for the broker
commands, ``calfkit.cli.dev._run_command``/``_chat_command`` for delegation), so everything here
is offline. The wrapper contract under test: ``.env`` loads **before** host resolution, the broker
is ensured once in the parent with a lazy ``resolve_bin`` thunk, the managed-vs-reused line prints,
and the delegate receives every argument explicitly with the **normalized** host.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

import calfkit.cli._dev_agents as dev_agents
import calfkit.cli._dev_broker as dev_broker
import calfkit.cli.dev as dev_cli
from calfkit.cli._dev_agents import DevAgentError, EnsureReport, TargetNodes, TargetOutcome
from calfkit.cli._dev_broker import BrokerInfo, DevBrokerError, MeshStatus

_ANSI = re.compile(r"\x1b\[[0-9;]*m")
_KEY = "127.0.0.1:9092"


def _plain(text: str) -> str:
    return _ANSI.sub("", text)


def _info(**kw: object) -> BrokerInfo:
    defaults: dict[str, object] = dict(
        listener=_KEY,
        pid=4242,
        managed=True,
        started_at="2026-07-01T00:00:00+00:00",
    )
    defaults.update(kw)
    return BrokerInfo(**defaults)  # type: ignore[arg-type]


def _invoke(args: list[str]) -> Any:
    from calfkit.cli import _build_app

    return CliRunner().invoke(_build_app(), args)


@pytest.fixture(autouse=True)
def _isolated_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """No test touches the real ``~/.calfkit`` or inherits a mesh URL from the session env."""
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("CALFKIT_MESH_URL", raising=False)


@pytest.fixture
def ensure_calls(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Monkeypatch ``ensure_broker`` at the supervisor seam, recording each call."""
    calls: list[dict[str, Any]] = []

    def fake_ensure(target: Any, *, resolve_bin: Any, timeout: float) -> BrokerInfo:
        calls.append({"target": target, "resolve_bin": resolve_bin, "timeout": timeout})
        return _info(listener=target.key)

    monkeypatch.setattr(dev_broker, "ensure_broker", fake_ensure)
    return calls


@pytest.fixture
def run_calls(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(dev_cli, "_run_command", lambda **kw: calls.append(kw))
    return calls


@pytest.fixture
def chat_calls(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(dev_cli, "_chat_command", lambda **kw: calls.append(kw))
    return calls


# --- mounting & help ---------------------------------------------------------------------------------


def test_dev_is_mounted_on_the_root_app() -> None:
    result = _invoke(["--help"])
    assert result.exit_code == 0, result.stdout
    assert "dev" in _plain(result.stdout)


def test_dev_help_lists_the_subcommands() -> None:
    result = _invoke(["dev", "--help"])
    assert result.exit_code == 0, result.stdout
    out = _plain(result.stdout)
    for sub in ("run", "chat", "broker"):
        assert sub in out


def test_dev_broker_help_lists_lifecycle_commands() -> None:
    result = _invoke(["dev", "broker", "--help"])
    assert result.exit_code == 0, result.stdout
    out = _plain(result.stdout)
    for sub in ("start", "stop", "status", "restart"):
        assert sub in out


def test_dev_run_help_shows_two_sided_preset_flags() -> None:
    out = _plain(_invoke(["dev", "run", "--help"]).stdout)
    assert "--no-provision" in out
    assert "--no-reload" in out


def test_dev_chat_help_has_no_reload_flag() -> None:
    out = _plain(_invoke(["dev", "chat", "--help"]).stdout)
    assert "--no-provision" in out
    assert "--reload" not in out


# --- dev run: ensure-then-delegate (spec §4.1, §4.3) --------------------------------------------------


def test_dev_run_ensures_once_and_delegates_with_presets(ensure_calls: list[dict[str, Any]], run_calls: list[dict[str, Any]]) -> None:
    result = _invoke(["dev", "run", "app:agent"])
    assert result.exit_code == 0, result.stdout + str(result.exception)
    (ensure,) = ensure_calls
    assert ensure["target"].listener == _KEY  # bare default resolved + normalized
    assert callable(ensure["resolve_bin"])
    (call,) = run_calls
    assert call["targets"] == ["app:agent"]
    assert call["host"] == _KEY, "the delegate must receive the NORMALIZED listener as host"
    assert call["provision"] is True, "provisioning is preset ON (Tansu has no topic auto-create)"
    assert call["reload"] is True, "reload is preset ON for dev run"
    assert call["enable_idempotence"] is False
    assert call["group_id"] is None
    assert call["env_file"] is None
    assert call["app_dir"] == "."


def test_dev_run_presets_are_overridable(ensure_calls: list[dict[str, Any]], run_calls: list[dict[str, Any]]) -> None:
    result = _invoke(["dev", "run", "app:agent", "--no-provision", "--no-reload", "--enable-idempotence"])
    assert result.exit_code == 0, result.stdout + str(result.exception)
    (call,) = run_calls
    assert call["provision"] is False
    assert call["reload"] is False
    assert call["enable_idempotence"] is True


def test_dev_run_loads_env_before_resolving_the_host(
    monkeypatch: pytest.MonkeyPatch, ensure_calls: list[dict[str, Any]], run_calls: list[dict[str, Any]]
) -> None:
    """A .env-set CALFKIT_MESH_URL must be visible when the address is normalized (spec §4.3)."""

    def fake_load_env(env_file: str | None) -> None:
        monkeypatch.setenv("CALFKIT_MESH_URL", "127.0.0.1:7777")

    monkeypatch.setattr(dev_cli, "_load_env", fake_load_env)
    result = _invoke(["dev", "run", "app:agent"])
    assert result.exit_code == 0, result.stdout + str(result.exception)
    assert ensure_calls[0]["target"].listener == "127.0.0.1:7777"
    assert run_calls[0]["host"] == "127.0.0.1:7777"


def test_dev_run_prints_the_managed_line_in_the_parent(ensure_calls: list[dict[str, Any]], run_calls: list[dict[str, Any]]) -> None:
    result = _invoke(["dev", "run", "app:agent"])
    out = _plain(result.stdout)
    assert "managed broker" in out
    assert _KEY in out
    assert "4242" in out


def test_dev_run_borrow_prints_the_reused_line_and_never_imports_calfkit_mesh(
    monkeypatch: pytest.MonkeyPatch, run_calls: list[dict[str, Any]]
) -> None:
    """A reachable remote --host is a pure borrow: no spawn, and the locator is never imported."""
    monkeypatch.setattr(dev_broker, "is_reachable", lambda servers, *, timeout: True)
    monkeypatch.delitem(sys.modules, "calfkit_mesh", raising=False)
    result = _invoke(["dev", "run", "app:agent", "--host", "203.0.113.7:9092"])
    assert result.exit_code == 0, result.stdout + str(result.exception)
    out = _plain(result.stdout)
    assert "not managed by calfkit" in out
    assert "calfkit_mesh" not in sys.modules
    (call,) = run_calls
    assert call["host"] == "203.0.113.7:9092"


def test_dev_run_multi_address_forwards_the_resolved_list_unchanged(monkeypatch: pytest.MonkeyPatch, run_calls: list[dict[str, Any]]) -> None:
    monkeypatch.setattr(dev_broker, "is_reachable", lambda servers, *, timeout: True)
    result = _invoke(["dev", "run", "app:agent", "--host", "b.example:2,a.example:1"])
    assert result.exit_code == 0, result.stdout + str(result.exception)
    (call,) = run_calls
    assert call["host"] == "b.example:2,a.example:1", "a multi-address borrow forwards the user's list unchanged"


def test_dev_run_unreachable_remote_exits_2_without_spawning(monkeypatch: pytest.MonkeyPatch, run_calls: list[dict[str, Any]]) -> None:
    monkeypatch.setattr(dev_broker, "is_reachable", lambda servers, *, timeout: False)
    monkeypatch.delitem(sys.modules, "calfkit_mesh", raising=False)
    result = _invoke(["dev", "run", "app:agent", "--host", "203.0.113.7:9092"])
    assert result.exit_code == 2
    assert "calfkit_mesh" not in sys.modules
    assert run_calls == []


def test_dev_run_broker_ensure_failure_exits_2(monkeypatch: pytest.MonkeyPatch, run_calls: list[dict[str, Any]]) -> None:
    def boom(target: Any, *, resolve_bin: Any, timeout: float) -> BrokerInfo:
        raise DevBrokerError("did not become ready")

    monkeypatch.setattr(dev_broker, "ensure_broker", boom)
    result = _invoke(["dev", "run", "app:agent"])
    assert result.exit_code == 2
    assert "did not become ready" in _plain(result.stdout) + _plain(result.output)
    assert run_calls == []


def test_dev_run_invalid_address_exits_2(run_calls: list[dict[str, Any]]) -> None:
    result = _invoke(["dev", "run", "app:agent", "--host", ":9092"])
    assert result.exit_code == 2
    assert run_calls == []


# --- dev chat (spec §4.1) ------------------------------------------------------------------------------


def test_dev_chat_ensures_then_delegates_with_presets(ensure_calls: list[dict[str, Any]], chat_calls: list[dict[str, Any]]) -> None:
    result = _invoke(["dev", "chat"])
    assert result.exit_code == 0, result.stdout + str(result.exception)
    assert len(ensure_calls) == 1
    (call,) = chat_calls
    assert call["name"] is None
    assert call["host"] == _KEY
    assert call["provision"] is True
    assert call["timeout"] is None
    assert call["env_file"] is None


def test_dev_chat_forwards_name_timeout_and_no_provision(ensure_calls: list[dict[str, Any]], chat_calls: list[dict[str, Any]]) -> None:
    result = _invoke(["dev", "chat", "helpdesk", "--no-provision", "--timeout", "12.5"])
    assert result.exit_code == 0, result.stdout + str(result.exception)
    (call,) = chat_calls
    assert call["name"] == "helpdesk"
    assert call["provision"] is False
    assert call["timeout"] == 12.5


def test_dev_chat_broker_ensure_failure_exits_2(monkeypatch: pytest.MonkeyPatch, chat_calls: list[dict[str, Any]]) -> None:
    def boom(target: Any, *, resolve_bin: Any, timeout: float) -> BrokerInfo:
        raise DevBrokerError("nope")

    monkeypatch.setattr(dev_broker, "ensure_broker", boom)
    result = _invoke(["dev", "chat"])
    assert result.exit_code == 2
    assert chat_calls == []


# --- dev broker start/stop/status/restart (spec §4.2) ---------------------------------------------------


def test_broker_start_ensures_and_reports(ensure_calls: list[dict[str, Any]]) -> None:
    result = _invoke(["dev", "broker", "start"])
    assert result.exit_code == 0, result.stdout + str(result.exception)
    assert len(ensure_calls) == 1
    assert "managed broker" in _plain(result.stdout)


def test_broker_start_failure_exits_2(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(target: Any, *, resolve_bin: Any, timeout: float) -> BrokerInfo:
        raise DevBrokerError("no binary")

    monkeypatch.setattr(dev_broker, "ensure_broker", boom)
    assert _invoke(["dev", "broker", "start"]).exit_code == 2


def test_broker_stop_reports_a_stop(monkeypatch: pytest.MonkeyPatch) -> None:
    stopped: list[Any] = []

    def fake_stop(target: Any, *, grace: float = 5.0) -> bool:
        stopped.append(target.key)
        return True

    monkeypatch.setattr(dev_broker, "stop", fake_stop)
    result = _invoke(["dev", "broker", "stop", "--host", "127.0.0.1:19092"])
    assert result.exit_code == 0, result.stdout + str(result.exception)
    assert stopped == ["127.0.0.1:19092"]
    assert "stopped" in _plain(result.stdout).lower()


def test_broker_stop_noop_reports_nothing_managed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dev_broker, "stop", lambda target, *, grace=5.0: False)
    result = _invoke(["dev", "broker", "stop"])
    assert result.exit_code == 0, result.stdout + str(result.exception)
    assert "no managed broker" in _plain(result.stdout).lower()


def test_broker_stop_all_stops_everything(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(dev_broker, "stop_all", lambda *, grace=5.0: calls.append("all") or ["a:1", "b:2"])
    monkeypatch.setattr(dev_broker, "stop", lambda *a, **kw: pytest.fail("stop --all must call stop_all, not stop"))
    result = _invoke(["dev", "broker", "stop", "--all"])
    assert result.exit_code == 0, result.stdout + str(result.exception)
    assert calls == ["all"]
    out = _plain(result.stdout)
    assert "a:1" in out
    assert "b:2" in out


def test_broker_stop_all_with_nothing_to_stop(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dev_broker, "stop_all", lambda *, grace=5.0: [])
    result = _invoke(["dev", "broker", "stop", "--all"])
    assert result.exit_code == 0, result.stdout + str(result.exception)
    assert "no managed brokers" in _plain(result.stdout).lower()


def test_broker_stop_without_mesh_extra_exits_2(monkeypatch: pytest.MonkeyPatch) -> None:
    # The scan needs psutil (the [mesh] extra); the supervisor raises an actionable DevBrokerError.
    def boom(target: Any, *, grace: float = 5.0) -> bool:
        raise DevBrokerError("needs the `calfkit[mesh]` extra")

    monkeypatch.setattr(dev_broker, "stop", boom)
    result = _invoke(["dev", "broker", "stop"])
    assert result.exit_code == 2
    assert "calfkit[mesh]" in _plain(result.stdout) + _plain(result.output)


def test_broker_status_without_mesh_extra_exits_2(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(target: Any, *, timeout: float = 5.0) -> MeshStatus:
        raise DevBrokerError("needs the `calfkit[mesh]` extra")

    monkeypatch.setattr(dev_broker, "status", boom)
    assert _invoke(["dev", "broker", "status"]).exit_code == 2


def test_broker_status_reports_managed_and_reachable(monkeypatch: pytest.MonkeyPatch) -> None:
    report = MeshStatus(
        target_key=_KEY,
        reachable=True,
        brokers=(_info(),),
    )
    monkeypatch.setattr(dev_broker, "status", lambda target, *, timeout=5.0: report)
    out = _plain(_invoke(["dev", "broker", "status"]).stdout)
    assert _KEY in out
    assert "4242" in out
    assert "running" in out.lower()


def test_broker_status_reports_reachable_not_managed(monkeypatch: pytest.MonkeyPatch) -> None:
    report = MeshStatus(target_key=_KEY, reachable=True, brokers=())
    monkeypatch.setattr(dev_broker, "status", lambda target, *, timeout=5.0: report)
    out = _plain(_invoke(["dev", "broker", "status"]).stdout)
    assert "reachable" in out.lower()
    assert "not managed by calfkit" in out.lower()


def test_broker_status_reports_nothing_reachable(monkeypatch: pytest.MonkeyPatch) -> None:
    report = MeshStatus(target_key=_KEY, reachable=False, brokers=())
    monkeypatch.setattr(dev_broker, "status", lambda target, *, timeout=5.0: report)
    out = _plain(_invoke(["dev", "broker", "status"]).stdout)
    assert "no broker" in out.lower()


def test_broker_restart_reports_the_new_broker(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    def fake_restart(target: Any, *, resolve_bin: Any, timeout: float, grace: float = 5.0) -> BrokerInfo:
        calls.append(target.key)
        return _info()

    monkeypatch.setattr(dev_broker, "restart", fake_restart)
    result = _invoke(["dev", "broker", "restart"])
    assert result.exit_code == 0, result.stdout + str(result.exception)
    assert calls == [_KEY]
    assert "managed broker" in _plain(result.stdout)


def test_broker_restart_failure_exits_2(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(target: Any, *, resolve_bin: Any, timeout: float, grace: float = 5.0) -> BrokerInfo:
        raise DevBrokerError("no binary")

    monkeypatch.setattr(dev_broker, "restart", boom)
    assert _invoke(["dev", "broker", "restart"]).exit_code == 2


# --- the resolve_bin thunk ------------------------------------------------------------------------------


def test_resolve_bin_honors_calf_tansu_bin_without_the_extra(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """The escape hatch must work on a core install: CALF_TANSU_BIN is read BEFORE calfkit_mesh
    is imported (the upstream locator that also reads it may not exist)."""
    binary = tmp_path / "tansu"
    binary.write_text("#!/bin/sh\n")
    binary.chmod(0o755)
    monkeypatch.setenv("CALF_TANSU_BIN", str(binary))
    monkeypatch.setitem(sys.modules, "calfkit_mesh", None)  # an import would raise
    assert dev_cli._resolve_bin() == str(binary)


def test_resolve_bin_rejects_a_bad_calf_tansu_bin(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CALF_TANSU_BIN", "/nonexistent/tansu")
    with pytest.raises(DevBrokerError, match="CALF_TANSU_BIN"):
        dev_cli._resolve_bin()


def test_resolve_bin_thunk_lazily_imports_calfkit_mesh(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CALF_TANSU_BIN", raising=False)
    import types

    fake = types.ModuleType("calfkit_mesh")
    fake.resolve_broker_bin = lambda: "/from/the/wheel/tansu"  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "calfkit_mesh", fake)
    assert dev_cli._resolve_bin() == "/from/the/wheel/tansu"


def test_resolve_bin_thunk_raises_module_not_found_without_the_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CALF_TANSU_BIN", raising=False)
    monkeypatch.setitem(sys.modules, "calfkit_mesh", None)  # forces ModuleNotFoundError on import
    with pytest.raises(ModuleNotFoundError):
        dev_cli._resolve_bin()


def test_dev_run_forwards_every_parameter_of_run(ensure_calls: list[dict[str, Any]], run_calls: list[dict[str, Any]]) -> None:
    """Drift guard for the §4.3 delegation contract: a parameter added to run() but not forwarded
    would silently receive a typer OptionInfo sentinel instead of its real default."""
    import inspect

    from calfkit.cli.run import run as run_command

    result = _invoke(["dev", "run", "app:agent"])
    assert result.exit_code == 0, result.stdout + str(result.exception)
    assert set(run_calls[0]) == set(inspect.signature(run_command).parameters)


# --- dev run --detach: launch agent daemons (agent-lifecycle spec §3.1) --------------------------------


def _plan(agent_names: tuple[str, ...] = ("general",), tool_names: tuple[str, ...] = (), spec: str = "app:agent") -> list[TargetNodes]:
    return [TargetNodes(spec=spec, nodes=(), agent_names=agent_names, tool_names=tool_names)]


class _FakeMeshHandle:
    pass


class _FakeDetachClient:
    """Stands in for the short-lived readiness Client the -d path opens."""

    instances: list[_FakeDetachClient] = []

    def __init__(self) -> None:
        self.mesh = _FakeMeshHandle()
        self.closed = False
        _FakeDetachClient.instances.append(self)

    @classmethod
    def connect(cls, server_urls: object = None, **kwargs: object) -> _FakeDetachClient:
        client = cls()
        client.server_urls_arg = server_urls  # type: ignore[attr-defined]
        return client

    async def aclose(self) -> None:
        self.closed = True


@pytest.fixture
def detach_seams(monkeypatch: pytest.MonkeyPatch, ensure_calls: list[dict[str, Any]]) -> dict[str, Any]:
    """Fake the supervisor seams for the -d path; records call order and arguments."""
    seams: dict[str, Any] = {"order": [], "plan": _plan()}
    _FakeDetachClient.instances = []

    def fake_preflight(targets: list[str], *, app_dir: str | None = None) -> list[TargetNodes]:
        seams["order"].append("preflight")
        seams["preflight"] = {"targets": targets, "app_dir": app_dir}
        plan: list[TargetNodes] = seams["plan"]
        return plan

    async def fake_ensure(plan: list[TargetNodes], target: Any, mesh: Any, *, run_args: Any, **kwargs: Any) -> EnsureReport:
        seams["order"].append("ensure_agents")
        seams["ensure"] = {"plan": plan, "target": target, "mesh": mesh, "run_args": run_args}
        report: EnsureReport = seams.get("report") or EnsureReport(
            outcomes=tuple(TargetOutcome(target=t, reused=False) for t in plan),
            pid=4055,
            log_path="/tmp/agents-x.log",
        )
        return report

    original_ensure_broker = dev_broker.ensure_broker

    def ordered_ensure_broker(*args: Any, **kwargs: Any) -> BrokerInfo:
        seams["order"].append("ensure_broker")
        result: BrokerInfo = original_ensure_broker(*args, **kwargs)
        return result

    monkeypatch.setattr(dev_broker, "ensure_broker", ordered_ensure_broker)
    monkeypatch.setattr(dev_agents, "preflight", fake_preflight)
    monkeypatch.setattr(dev_agents, "ensure_agents", fake_ensure)
    monkeypatch.setattr("calfkit.client.Client", _FakeDetachClient)
    return seams


def test_dev_run_detach_prints_the_launched_line(detach_seams: dict[str, Any], run_calls: list[dict[str, Any]]) -> None:
    """§3.1: per launched agent — the SUPERVISOR pid (what stop signals), the lifetime statement,
    and the log path; the foreground delegation never runs."""
    result = _invoke(["dev", "run", "-d", "app:agent"])
    assert result.exit_code == 0, result.stdout + str(result.exception)
    out = _plain(result.stdout)
    assert "ck dev: launched agent 'general' (pid 4055) — runs until 'ck dev stop general' — logs: /tmp/agents-x.log" in out
    assert run_calls == []


def test_dev_run_detach_prints_the_reusing_line_with_age(detach_seams: dict[str, Any]) -> None:
    plan = detach_seams["plan"]
    detach_seams["report"] = EnsureReport(outcomes=(TargetOutcome(target=plan[0], reused=True, ages={"general": 3.2}),), pid=None, log_path=None)
    result = _invoke(["dev", "run", "--detach", "app:agent"])
    assert result.exit_code == 0, result.stdout + str(result.exception)
    assert "ck dev: reusing agent 'general' (online, last seen 3s ago)" in _plain(result.stdout)


def test_dev_run_detach_tool_names_use_the_tool_word(detach_seams: dict[str, Any]) -> None:
    """Tools-only targets are legitimate daemons (spec §5.1) — their lines say what they are."""
    detach_seams["plan"] = _plan(agent_names=(), tool_names=("get_weather",), spec="tools:get_weather")
    result = _invoke(["dev", "run", "-d", "tools:get_weather"])
    assert result.exit_code == 0, result.stdout + str(result.exception)
    assert "ck dev: launched tool 'get_weather' (pid 4055)" in _plain(result.stdout)


def test_dev_run_detach_preflights_before_the_broker_ensure(detach_seams: dict[str, Any]) -> None:
    """Spec §5.1 order: preflight fails fast at the prompt BEFORE any broker work."""
    result = _invoke(["dev", "run", "-d", "app:agent"])
    assert result.exit_code == 0, result.stdout + str(result.exception)
    assert detach_seams["order"] == ["preflight", "ensure_broker", "ensure_agents"]


def test_dev_run_detach_supervisor_error_exits_2(detach_seams: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    async def broken(*args: Any, **kwargs: Any) -> EnsureReport:
        raise DevAgentError("a daemon for 'general' already exists (pid 4055)")

    monkeypatch.setattr(dev_agents, "ensure_agents", broken)
    result = _invoke(["dev", "run", "-d", "app:agent"])
    assert result.exit_code == 2
    assert "Error: a daemon for 'general' already exists" in _plain(result.stderr)
    assert _FakeDetachClient.instances[0].closed is True  # the readiness client never leaks


def test_dev_run_detach_interrupt_leaves_the_daemon_and_hints(detach_seams: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    """§3.4: Ctrl-C during the readiness wait leaves the daemon running (recoverable) and says so."""

    async def interrupted(*args: Any, **kwargs: Any) -> EnsureReport:
        raise KeyboardInterrupt

    monkeypatch.setattr(dev_agents, "ensure_agents", interrupted)
    result = _invoke(["dev", "run", "-d", "app:agent"])
    assert result.exit_code == 130
    assert "ck dev status" in _plain(result.stderr)


def test_dev_run_detach_forwards_the_run_options(detach_seams: dict[str, Any], tmp_path: Path) -> None:
    result = _invoke(
        [
            "dev",
            "run",
            "-d",
            "app:agent",
            "--no-reload",
            "--no-provision",
            "--group-id",
            "g1",
            "--app-dir",
            str(tmp_path),
            "--reload-dir",
            "src",
            "--enable-idempotence",
        ]
    )
    assert result.exit_code == 0, result.stdout + str(result.exception)
    run_args = detach_seams["ensure"]["run_args"]
    assert run_args.reload is False
    assert run_args.provision is False
    assert run_args.group_id == "g1"
    assert run_args.app_dir == str(tmp_path)
    assert list(run_args.reload_dir or []) == ["src"]
    assert run_args.enable_idempotence is True
    assert detach_seams["preflight"]["app_dir"] == str(tmp_path)


def test_dev_run_detach_closes_the_readiness_client(detach_seams: dict[str, Any]) -> None:
    result = _invoke(["dev", "run", "-d", "app:agent"])
    assert result.exit_code == 0, result.stdout + str(result.exception)
    (client,) = _FakeDetachClient.instances
    assert client.closed is True
    assert detach_seams["ensure"]["mesh"] is client.mesh


def test_dev_run_forwards_the_hidden_internals_explicitly(ensure_calls: list[dict[str, Any]], run_calls: list[dict[str, Any]]) -> None:
    """The parity-guard contract (impl plan CG-B): `dev run` forwards the hidden internals with
    their preset values — the 5s dev heartbeat (spec §5.6 covers foreground dev runs too) and no
    ownership marker (a foreground run is not a daemon) — so the forwards-every-parameter guard
    above keeps its simple equality contract."""
    result = _invoke(["dev", "run", "app:agent"])
    assert result.exit_code == 0, result.stdout + str(result.exception)
    assert run_calls[0]["heartbeat_interval"] == 5.0
    assert run_calls[0]["dev_daemon"] is None


def test_dev_run_help_hides_the_internal_flags() -> None:
    """R1 guard, dev side: the hidden internals must not leak into ck dev run --help either."""
    out = _plain(_invoke(["dev", "run", "--help"]).stdout)
    assert "--dev-daemon" not in out
    assert "--heartbeat-interval" not in out


def test_dev_chat_forwards_every_parameter_of_chat(ensure_calls: list[dict[str, Any]], chat_calls: list[dict[str, Any]]) -> None:
    import inspect

    from calfkit.cli.chat import chat as chat_command

    result = _invoke(["dev", "chat"])
    assert result.exit_code == 0, result.stdout + str(result.exception)
    assert set(chat_calls[0]) == set(inspect.signature(chat_command).parameters)


def test_dev_run_forwards_every_option_value(ensure_calls: list[dict[str, Any]], run_calls: list[dict[str, Any]]) -> None:
    result = _invoke(
        [
            "dev",
            "run",
            "app:agent",
            "--reload-dir",
            "dir_one",
            "--reload-dir",
            "dir_two",
            "--app-dir",
            "some/dir",
            "--group-id",
            "my-group",
            "--env-file",
            "missing.env",  # warns (not found) and continues
        ]
    )
    assert result.exit_code == 0, result.stdout + str(result.exception)
    (call,) = run_calls
    assert call["reload_dir"] == ["dir_one", "dir_two"]
    assert call["app_dir"] == "some/dir"
    assert call["group_id"] == "my-group"
    assert call["env_file"] == "missing.env"


def test_broker_commands_load_dotenv_before_resolving(monkeypatch: pytest.MonkeyPatch) -> None:
    """A .env-set CALFKIT_MESH_URL must target the SAME address across every ck dev command,
    or `broker stop` would miss the broker `dev run` spawned."""
    loaded: list[str | None] = []
    monkeypatch.setattr(dev_cli, "_load_env", lambda env_file: loaded.append(env_file))
    monkeypatch.setattr(dev_broker, "status", lambda target, *, timeout=5.0: MeshStatus(target.key, False, ()))
    monkeypatch.setattr(dev_broker, "stop", lambda target, *, grace=5.0: False)
    _invoke(["dev", "broker", "status"])
    _invoke(["dev", "broker", "stop"])
    assert loaded == [None, None]


def test_broker_commands_forward_an_explicit_env_file(monkeypatch: pytest.MonkeyPatch) -> None:
    loaded: list[str | None] = []
    monkeypatch.setattr(dev_cli, "_load_env", lambda env_file: loaded.append(env_file))
    monkeypatch.setattr(dev_broker, "status", lambda target, *, timeout=5.0: MeshStatus(target.key, False, ()))
    result = _invoke(["dev", "broker", "status", "--env-file", "custom.env"])
    assert result.exit_code == 0, result.stdout + str(result.exception)
    assert loaded == ["custom.env"]


def test_broker_status_multi_target_with_all_elements_managed(monkeypatch: pytest.MonkeyPatch) -> None:
    # A multi-address target whose every element hosts a dev broker must not also print the
    # contradictory "reachable, not managed" line for the joined key.
    report = MeshStatus(
        target_key="127.0.0.1:9092,127.0.0.1:19092",
        reachable=True,
        brokers=(_info(), _info(listener="127.0.0.1:19092", pid=4343)),
    )
    monkeypatch.setattr(dev_broker, "status", lambda target, *, timeout=5.0: report)
    out = _plain(_invoke(["dev", "broker", "status", "--host", "127.0.0.1:9092,127.0.0.1:19092"]).stdout)
    assert "not managed" not in out
    assert "pid 4242" in out
    assert "pid 4343" in out


def test_missing_env_file_warns_only_once_per_process(tmp_path: Path) -> None:
    # dev run loads the env in the wrapper AND in the delegated command; a typo'd --env-file
    # must not print the same warning twice.
    import contextlib
    import io

    from calfkit.cli._common import _load_env

    missing = str(tmp_path / "definitely-missing.env")
    stderr = io.StringIO()
    with contextlib.redirect_stderr(stderr):
        _load_env(missing)
        _load_env(missing)
    assert stderr.getvalue().count("not found") == 1
