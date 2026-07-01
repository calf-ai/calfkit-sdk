"""Tests for ``calfkit.cli._dev_broker`` — the ``ck dev`` connect-or-spawn supervisor (spec §5).

All offline: the reachability probe, ``Popen``, and ``psutil`` are faked. The real-broker spawn path
lives in the kafka lane (``tests/integration/test_dev_broker_kafka.py``).
"""

from __future__ import annotations

from pathlib import Path

import pytest

import calfkit.cli._dev_broker as dev_broker
from calfkit.cli._dev_broker import DEFAULT_PORT, DevBrokerError, Target, ensure_broker, normalize
from calfkit.cli._dev_state import BrokerRecord, Registry
from tests._dev_fakes import CountingResolveBin, FakePopen, FakeProc, MustNotCall, install_fake_psutil, scripted_probe

_BIN = "/fake/bin/tansu"
_KEY = "127.0.0.1:9092"
_OUR_ARGV = [_BIN, "broker", "--storage-engine=memory://tansu/", f"--kafka-listener-url=tcp://{_KEY}"]


def _rec(**kw: object) -> BrokerRecord:
    defaults: dict[str, object] = dict(
        pid=4242,
        bootstrap="localhost",
        listener=_KEY,
        binary_path=_BIN,
        started_at="2026-07-01T00:00:00+00:00",
        log_path="/tmp/tansu.log",
        spawned_by_calfkit=True,
    )
    defaults.update(kw)
    return BrokerRecord(**defaults)  # type: ignore[arg-type]


@pytest.fixture
def reg(tmp_path: Path) -> Registry:
    return Registry(root=tmp_path)


def _capture_popen(monkeypatch: pytest.MonkeyPatch, **proc_kw: object) -> list[FakePopen]:
    spawned: list[FakePopen] = []

    def factory(cmd: list[str], **kwargs: object) -> FakePopen:
        proc = FakePopen(cmd, **kwargs)
        for name, value in proc_kw.items():
            setattr(proc, name, value)
        spawned.append(proc)
        return proc

    monkeypatch.setattr(dev_broker, "Popen", factory)
    return spawned


# --- normalize (spec §5.2) -------------------------------------------------------------------------


def test_bare_localhost_normalizes_to_ipv4_default_port() -> None:
    target = normalize(["localhost"])
    assert target.listen_ip == "127.0.0.1"
    assert target.port == DEFAULT_PORT == 9092
    assert target.listener == "127.0.0.1:9092"
    assert target.registry_key == "127.0.0.1:9092"
    assert target.servers == ("127.0.0.1:9092",)
    assert target.is_loopback is True
    assert target.is_single is True


def test_explicit_port_is_kept() -> None:
    target = normalize(["localhost:19092"])
    assert target.listener == "127.0.0.1:19092"
    assert target.port == 19092


def test_bootstrap_preserves_the_raw_resolved_input() -> None:
    assert normalize(["localhost"]).bootstrap == "localhost"
    assert normalize(["a:1", "b:2"]).bootstrap == "a:1,b:2"


def test_loopback_range_is_loopback() -> None:
    # Anywhere in 127.0.0.0/8, not just 127.0.0.1.
    assert normalize(["127.0.0.5:9092"]).is_loopback is True


def test_ipv6_loopback_is_loopback_and_bracketed() -> None:
    target = normalize(["::1"])
    assert target.is_loopback is True
    assert target.listener == "[::1]:9092"


def test_bracketed_ipv6_with_port_parses() -> None:
    target = normalize(["[::1]:19092"])
    assert target.listen_ip == "::1"
    assert target.port == 19092
    assert target.listener == "[::1]:19092"


def test_wildcard_bind_is_not_loopback() -> None:
    # 0.0.0.0 must be connect-only: a spawn there would bind a non-local address (spec §5.2).
    assert normalize(["0.0.0.0"]).is_loopback is False
    assert normalize(["0.0.0.0:9092"]).is_loopback is False


def test_hostname_is_not_loopback() -> None:
    assert normalize(["kafka.internal:9092"]).is_loopback is False


def test_non_local_ip_is_not_loopback() -> None:
    assert normalize(["192.168.1.50:9092"]).is_loopback is False


def test_multi_address_gets_canonical_key_and_never_spawns() -> None:
    target = normalize(["b.example:2", "a.example:1"])
    assert target.is_single is False
    assert target.is_loopback is False
    assert target.registry_key == "a.example:1,b.example:2"
    assert target.servers == ("b.example:2", "a.example:1")


def test_multi_address_key_normalizes_each_element() -> None:
    target = normalize(["localhost", "other.example:9093"])
    assert target.registry_key == "127.0.0.1:9092,other.example:9093"


def test_multi_address_has_no_single_listener() -> None:
    target = normalize(["a.example:1", "b.example:2"])
    assert target.listen_ip is None
    assert target.port is None
    with pytest.raises(ValueError, match="single-address"):
        _ = target.listener


def test_invalid_port_raises_value_error() -> None:
    with pytest.raises(ValueError, match="port"):
        normalize(["host:notaport"])


def test_empty_host_raises_value_error() -> None:
    with pytest.raises(ValueError, match="invalid bootstrap address"):
        normalize([":9092"])


def test_log_tail_of_an_unreadable_log_is_empty() -> None:
    assert dev_broker._log_tail(Path("/nonexistent/tansu.log")) == ""


def test_empty_servers_raises_value_error() -> None:
    with pytest.raises(ValueError):
        normalize([])


def test_target_is_frozen() -> None:
    target = normalize(["localhost"])
    with pytest.raises(AttributeError):
        target.port = 1  # type: ignore[misc]


def test_normalize_accepts_target_roundtrip_servers() -> None:
    # The normalized single-address servers tuple is itself a valid normalize() input (idempotent).
    target = normalize(["localhost"])
    again = normalize(list(target.servers))
    assert again.registry_key == target.registry_key
    assert again.listener == target.listener


def test_target_type_is_exported() -> None:
    assert isinstance(normalize(["localhost"]), Target)


# --- ensure_broker: reuse / borrow / connect-only (spec §5.2, §5.7) --------------------------------


def test_reuses_a_reachable_broker_we_own(reg: Registry, monkeypatch: pytest.MonkeyPatch) -> None:
    rec = _rec()
    reg.put(_KEY, rec)
    install_fake_psutil(monkeypatch, {4242: FakeProc(4242, _OUR_ARGV)})
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(True))
    out = ensure_broker(normalize(["localhost"]), resolve_bin=MustNotCall(), registry=reg)
    assert out == rec
    assert reg.get(_KEY) == rec


def test_borrows_a_reachable_broker_we_did_not_spawn(reg: Registry, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(True))
    out = ensure_broker(normalize(["localhost"]), resolve_bin=MustNotCall(), registry=reg)
    assert out.spawned_by_calfkit is False
    assert out.listener == _KEY
    assert out.bootstrap == "localhost"
    assert reg.read() == {}, "a borrowed broker must never be persisted"


def test_reachable_with_stale_record_cleans_and_borrows(reg: Registry, monkeypatch: pytest.MonkeyPatch) -> None:
    # Our record's pid now holds a foreign process, yet SOMETHING answers at the address: whatever
    # answers is not ours (a live tansu of ours would pass is_ours) — clean the record, borrow.
    reg.put(_KEY, _rec())
    install_fake_psutil(monkeypatch, {4242: FakeProc(4242, ["/usr/bin/python", "-m", "http.server"])})
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(True))
    out = ensure_broker(normalize(["localhost"]), resolve_bin=MustNotCall(), registry=reg)
    assert out.spawned_by_calfkit is False
    assert reg.read() == {}


def test_multi_address_borrows_when_reachable(reg: Registry, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(True))
    out = ensure_broker(normalize(["a.example:1", "b.example:2"]), resolve_bin=MustNotCall(), registry=reg)
    assert out.spawned_by_calfkit is False


def test_errors_for_unreachable_non_loopback(reg: Registry, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(False))
    with pytest.raises(DevBrokerError, match="connect-only|not a loopback"):
        ensure_broker(normalize(["kafka.internal:9092"]), resolve_bin=MustNotCall(), registry=reg)


def test_errors_for_unreachable_wildcard_bind(reg: Registry, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(False))
    with pytest.raises(DevBrokerError):
        ensure_broker(normalize(["0.0.0.0:9092"]), resolve_bin=MustNotCall(), registry=reg)


def test_errors_for_unreachable_multi_address(reg: Registry, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(False))
    with pytest.raises(DevBrokerError):
        ensure_broker(normalize(["a.example:1", "b.example:2"]), resolve_bin=MustNotCall(), registry=reg)


def test_detection_probe_targets_the_normalized_servers(reg: Registry, monkeypatch: pytest.MonkeyPatch) -> None:
    probe = scripted_probe(True)
    monkeypatch.setattr(dev_broker, "is_reachable", probe)
    ensure_broker(normalize(["localhost"]), resolve_bin=MustNotCall(), timeout=3.0, registry=reg)
    assert probe.calls == [(["127.0.0.1:9092"], 3.0)]


# --- ensure_broker: the spawn branch (spec §5.2, §5.5) ---------------------------------------------


def test_spawns_detached_when_loopback_unreachable(reg: Registry, monkeypatch: pytest.MonkeyPatch) -> None:
    import subprocess

    spawned = _capture_popen(monkeypatch)
    resolve_bin = CountingResolveBin(_BIN)
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(False, True))  # detect: absent; ready: yes

    out = ensure_broker(normalize(["localhost"]), resolve_bin=resolve_bin, registry=reg)

    assert resolve_bin.calls == 1
    (proc,) = spawned
    assert proc.cmd == [
        _BIN,
        "broker",
        "--storage-engine=memory://tansu/",
        f"--kafka-listener-url=tcp://{_KEY}",
        f"--kafka-advertised-listener-url=tcp://{_KEY}",
    ]
    assert proc.kwargs["start_new_session"] is True
    assert proc.kwargs["stdin"] is subprocess.DEVNULL
    assert out.pid == proc.pid
    assert out.spawned_by_calfkit is True
    assert out.listener == _KEY
    assert out.binary_path == _BIN
    assert out.started_at  # a real timestamp, not empty
    assert reg.get(_KEY) == out, "a spawned broker must be persisted under its registry key"


def test_spawn_logs_to_a_real_file_overwritten_each_spawn(reg: Registry, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    log_path = tmp_path / "logs" / f"tansu-{_KEY}.log"
    log_path.parent.mkdir(parents=True)
    log_path.write_bytes(b"stale output from a previous spawn\n")
    spawned: list[FakePopen] = []

    def factory(cmd: list[str], **kwargs: object) -> FakePopen:
        proc = FakePopen(cmd, **kwargs)
        proc.write_log(b"ready in 20ms\n")  # what a real child writes while it holds the fd
        spawned.append(proc)
        return proc

    monkeypatch.setattr(dev_broker, "Popen", factory)
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(False, True))

    out = ensure_broker(normalize(["localhost"]), resolve_bin=CountingResolveBin(_BIN), registry=reg)

    (proc,) = spawned
    assert out.log_path == str(log_path)
    assert log_path.read_bytes() == b"ready in 20ms\n", "the log must be truncated on each spawn"
    assert proc.kwargs["stdout"] is proc.kwargs["stderr"], "stdout and stderr share the log fd"
    assert proc.kwargs["stdout"].closed, "the parent must close its copy of the log fd after spawn"


def test_spawn_waits_for_readiness_via_the_probe(reg: Registry, monkeypatch: pytest.MonkeyPatch) -> None:
    _capture_popen(monkeypatch)
    probe = scripted_probe(False, False, False, True)
    monkeypatch.setattr(dev_broker, "is_reachable", probe)
    ensure_broker(normalize(["localhost"]), resolve_bin=CountingResolveBin(_BIN), registry=reg)
    # 1 detection probe + 3 readiness probes; readiness targets the single bind address.
    assert len(probe.calls) == 4
    assert probe.calls[-1][0] == _KEY


def test_readiness_timeout_kills_the_spawn_and_surfaces_the_log_tail(reg: Registry, monkeypatch: pytest.MonkeyPatch) -> None:
    spawned: list[FakePopen] = []

    def factory(cmd: list[str], **kwargs: object) -> FakePopen:
        proc = FakePopen(cmd, **kwargs)
        proc.write_log(b"error: address already in use\n")
        spawned.append(proc)
        return proc

    monkeypatch.setattr(dev_broker, "Popen", factory)
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(False))
    with pytest.raises(DevBrokerError, match="address already in use"):
        ensure_broker(normalize(["localhost"]), resolve_bin=CountingResolveBin(_BIN), timeout=0.05, registry=reg)
    (proc,) = spawned
    assert proc.killed is True
    assert reg.read() == {}, "a never-ready spawn must not be recorded"


def test_spawn_that_exits_during_startup_fails_fast_with_the_log_tail(reg: Registry, monkeypatch: pytest.MonkeyPatch) -> None:
    spawned: list[FakePopen] = []

    def factory(cmd: list[str], **kwargs: object) -> FakePopen:
        proc = FakePopen(cmd, **kwargs)
        proc.write_log(b"panic: bind failed\n")
        proc.returncode = 1  # died immediately (spec §7.2)
        spawned.append(proc)
        return proc

    monkeypatch.setattr(dev_broker, "Popen", factory)
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(False))
    with pytest.raises(DevBrokerError, match="bind failed"):
        ensure_broker(normalize(["localhost"]), resolve_bin=CountingResolveBin(_BIN), timeout=30.0, registry=reg)
    assert reg.read() == {}


def test_wrong_arch_binary_surfaces_a_distinct_error(reg: Registry, monkeypatch: pytest.MonkeyPatch) -> None:
    def factory(cmd: list[str], **kwargs: object) -> FakePopen:
        raise OSError(8, "Exec format error")

    monkeypatch.setattr(dev_broker, "Popen", factory)
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(False))
    with pytest.raises(DevBrokerError, match="Exec format error"):
        ensure_broker(normalize(["localhost"]), resolve_bin=CountingResolveBin(_BIN), registry=reg)
    assert reg.read() == {}


def test_unreachable_stale_record_is_cleaned_then_respawned(reg: Registry, monkeypatch: pytest.MonkeyPatch) -> None:
    reg.put(_KEY, _rec(pid=9999))  # a record whose process is long gone (spec §7.7)
    _capture_popen(monkeypatch)
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(False, True))
    out = ensure_broker(normalize(["localhost"]), resolve_bin=CountingResolveBin(_BIN), registry=reg)
    assert out.pid == 5555
    assert reg.get(_KEY) == out


def test_missing_mesh_extra_yields_an_actionable_error(reg: Registry, monkeypatch: pytest.MonkeyPatch) -> None:
    def resolve_bin() -> str:
        raise ModuleNotFoundError("No module named 'calfkit_mesh'")

    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(False))
    with pytest.raises(DevBrokerError, match=r"calfkit\[mesh\].*CALF_TANSU_BIN"):
        ensure_broker(normalize(["localhost"]), resolve_bin=resolve_bin, registry=reg)


def test_lock_is_held_across_probe_spawn_and_readiness(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Spec §5.3: releasing before readiness would let a second invocation double-spawn."""
    depth_at: dict[str, int] = {}

    class SpyRegistry(Registry):
        @property
        def depth(self) -> int:
            return self._lock_depth

    spy = SpyRegistry(root=tmp_path)

    def factory(cmd: list[str], **kwargs: object) -> FakePopen:
        depth_at["popen"] = spy.depth
        return FakePopen(cmd, **kwargs)

    probe = scripted_probe(False, True)

    def probing(bootstrap: object, *, timeout: float) -> bool:
        depth_at.setdefault("first_probe", spy.depth)
        depth_at["last_probe"] = spy.depth
        result: bool = probe(bootstrap, timeout=timeout)
        return result

    monkeypatch.setattr(dev_broker, "Popen", factory)
    monkeypatch.setattr(dev_broker, "is_reachable", probing)
    ensure_broker(normalize(["localhost"]), resolve_bin=CountingResolveBin(_BIN), registry=spy)
    assert depth_at["first_probe"] >= 1, "detection probe must run under the lock"
    assert depth_at["popen"] >= 1, "spawn must run under the lock"
    assert depth_at["last_probe"] >= 1, "readiness probe must run under the lock"


def test_detach_kwargs_per_platform(monkeypatch: pytest.MonkeyPatch) -> None:
    import os
    import subprocess

    monkeypatch.setattr(os, "name", "posix")
    assert dev_broker._detach_kwargs() == {"start_new_session": True}
    monkeypatch.setattr(os, "name", "nt")
    expected = getattr(subprocess, "DETACHED_PROCESS", 0)
    assert dev_broker._detach_kwargs() == {"creationflags": expected}


# --- stop / stop_all (spec §5.4, §5.6) --------------------------------------------------------------


def test_stop_terminates_an_owned_broker(reg: Registry, monkeypatch: pytest.MonkeyPatch) -> None:
    reg.put(_KEY, _rec())
    proc = FakeProc(4242, _OUR_ARGV)
    install_fake_psutil(monkeypatch, {4242: proc})
    assert dev_broker.stop(normalize(["localhost"]), registry=reg) is True
    assert proc.events == ["terminate", "wait"]
    assert reg.read() == {}


def test_stop_escalates_to_sigkill_after_grace(reg: Registry, monkeypatch: pytest.MonkeyPatch) -> None:
    reg.put(_KEY, _rec())
    proc = FakeProc(4242, _OUR_ARGV, survives_sigterm=True)
    install_fake_psutil(monkeypatch, {4242: proc})
    assert dev_broker.stop(normalize(["localhost"]), registry=reg, grace=0.01) is True
    assert proc.events == ["terminate", "wait", "kill", "wait"]
    assert reg.read() == {}


def test_stop_is_a_noop_without_a_record(reg: Registry) -> None:
    assert dev_broker.stop(normalize(["localhost"]), registry=reg) is False


def test_stop_never_signals_a_foreign_pid(reg: Registry, monkeypatch: pytest.MonkeyPatch) -> None:
    """C3: a recycled pid holding a foreign process is cleaned, NEVER signalled (spec §5.4)."""
    reg.put(_KEY, _rec())
    foreign = FakeProc(4242, ["/usr/bin/postgres", "-D", "/data"])
    install_fake_psutil(monkeypatch, {4242: foreign})
    assert dev_broker.stop(normalize(["localhost"]), registry=reg) is False
    assert foreign.events == [], "a foreign process must never be signalled"
    assert reg.read() == {}, "the stale record must be cleaned"


def test_stop_cleans_the_record_when_the_pid_is_gone(reg: Registry, monkeypatch: pytest.MonkeyPatch) -> None:
    reg.put(_KEY, _rec())
    install_fake_psutil(monkeypatch, {})
    assert dev_broker.stop(normalize(["localhost"]), registry=reg) is False
    assert reg.read() == {}


def test_stop_never_signals_a_record_not_spawned_by_calfkit(reg: Registry, monkeypatch: pytest.MonkeyPatch) -> None:
    # Borrowed records are never persisted; if one ever were, the ownership guard still holds.
    reg.put(_KEY, _rec(spawned_by_calfkit=False))
    proc = FakeProc(4242, _OUR_ARGV)
    install_fake_psutil(monkeypatch, {4242: proc})
    assert dev_broker.stop(normalize(["localhost"]), registry=reg) is False
    assert proc.events == []


def test_stop_signals_outside_the_lock(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Spec §5.3: the SIGTERM grace wait must not stall other invocations on the registry lock."""

    class SpyRegistry(Registry):
        @property
        def depth(self) -> int:
            return self._lock_depth

    spy = SpyRegistry(root=tmp_path)
    spy.put(_KEY, _rec())
    depth_at: dict[str, int] = {}

    class DepthProc(FakeProc):
        def terminate(self) -> None:
            depth_at["terminate"] = spy.depth
            super().terminate()

    install_fake_psutil(monkeypatch, {4242: DepthProc(4242, _OUR_ARGV)})
    assert dev_broker.stop(normalize(["localhost"]), registry=spy) is True
    assert depth_at["terminate"] == 0, "signalling must happen outside the registry lock"


def test_stop_all_stops_every_owned_broker(reg: Registry, monkeypatch: pytest.MonkeyPatch) -> None:
    reg.put(_KEY, _rec())
    other_key = "127.0.0.1:19092"
    reg.put(other_key, _rec(pid=4343, listener=other_key))
    proc_a = FakeProc(4242, _OUR_ARGV)
    proc_b = FakeProc(4343, [_BIN, "broker", f"--kafka-listener-url=tcp://{other_key}"])
    install_fake_psutil(monkeypatch, {4242: proc_a, 4343: proc_b})
    stopped = dev_broker.stop_all(registry=reg)
    assert sorted(stopped) == sorted([_KEY, other_key])
    assert proc_a.events == ["terminate", "wait"]
    assert proc_b.events == ["terminate", "wait"]
    assert reg.read() == {}


def test_stop_all_with_empty_registry(reg: Registry) -> None:
    assert dev_broker.stop_all(registry=reg) == []


# --- status (spec §4.2) -----------------------------------------------------------------------------


def test_status_reports_managed_records_and_target_probe(reg: Registry, monkeypatch: pytest.MonkeyPatch) -> None:
    reg.put(_KEY, _rec())
    install_fake_psutil(monkeypatch, {4242: FakeProc(4242, _OUR_ARGV)})
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(True))
    report = dev_broker.status(normalize(["localhost"]), registry=reg)
    assert report.target_key == _KEY
    assert report.reachable is True
    (broker,) = report.brokers
    assert broker.key == _KEY
    assert broker.record == _rec()
    assert broker.running is True


def test_status_flags_a_dead_managed_record(reg: Registry, monkeypatch: pytest.MonkeyPatch) -> None:
    reg.put(_KEY, _rec())
    install_fake_psutil(monkeypatch, {})
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(False))
    report = dev_broker.status(normalize(["localhost"]), registry=reg)
    (broker,) = report.brokers
    assert broker.running is False


def test_status_reachable_but_not_managed(reg: Registry, monkeypatch: pytest.MonkeyPatch) -> None:
    # A borrowed/pre-existing broker answers, but calfkit spawned nothing: reachable, no record.
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(True))
    report = dev_broker.status(normalize(["localhost"]), registry=reg)
    assert report.reachable is True
    assert report.brokers == ()


def test_status_probes_outside_the_lock(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    class SpyRegistry(Registry):
        @property
        def depth(self) -> int:
            return self._lock_depth

    spy = SpyRegistry(root=tmp_path)
    depth_at: dict[str, int] = {}

    def probing(bootstrap: object, *, timeout: float) -> bool:
        depth_at["probe"] = spy.depth
        return False

    monkeypatch.setattr(dev_broker, "is_reachable", probing)
    dev_broker.status(normalize(["localhost"]), registry=spy)
    assert depth_at["probe"] == 0, "the status probe must run outside the registry lock"


# --- restart (spec §4.2) ----------------------------------------------------------------------------


def test_restart_stops_the_owned_broker_then_spawns_fresh(reg: Registry, monkeypatch: pytest.MonkeyPatch) -> None:
    reg.put(_KEY, _rec())
    old = FakeProc(4242, _OUR_ARGV)
    install_fake_psutil(monkeypatch, {4242: old})
    spawned = _capture_popen(monkeypatch)
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(False, True))  # post-stop: absent, then ready
    out = dev_broker.restart(normalize(["localhost"]), resolve_bin=CountingResolveBin(_BIN), registry=reg)
    assert old.events == ["terminate", "wait"]
    assert len(spawned) == 1
    assert out.pid == 5555
    assert reg.get(_KEY) == out


def test_restart_of_a_borrowed_broker_just_reuses_it(reg: Registry, monkeypatch: pytest.MonkeyPatch) -> None:
    # stop is a no-op on a broker we don't own, so restart re-reuses the still-running one.
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(True))
    out = dev_broker.restart(normalize(["localhost"]), resolve_bin=MustNotCall(), registry=reg)
    assert out.spawned_by_calfkit is False
