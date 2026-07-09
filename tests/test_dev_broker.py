"""Tests for ``calfkit.cli._dev_broker`` — the ``ck dev`` connect-or-spawn supervisor (spec §5).

All offline: the reachability probe, ``Popen``, and ``psutil`` are faked. Ownership is the
**stateless process-table scan** (spec §5.4): a dev broker is any live process whose argv carries
the memory-engine anchor and the target listener — no persisted registry. The real-broker spawn
path lives in the kafka lane (``tests/integration/test_dev_broker_kafka.py``).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

import calfkit.cli._dev_broker as dev_broker
from calfkit.cli._dev_broker import (
    DEFAULT_PORT,
    BrokerInfo,
    DevBrokerError,
    Target,
    ensure_broker,
    normalize,
)
from tests._dev_fakes import CountingResolveBin, FakePopen, FakeProc, MustNotCall, install_fake_psutil, scripted_probe

_BIN = "/fake/bin/tansu"
_KEY = "127.0.0.1:9092"
_STARTED = "2026-07-01T00:00:00+00:00"  # FAKE_CREATE_TIME rendered as UTC ISO


def _broker_argv(listener: str = _KEY, engine: str = "memory://tansu/") -> list[str]:
    return [
        _BIN,
        "broker",
        f"--storage-engine={engine}",
        f"--kafka-listener-url=tcp://{listener}",
        f"--kafka-advertised-listener-url=tcp://{listener}",
    ]


@pytest.fixture(autouse=True)
def _home(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Keep the spawn lock + logs out of the real ``~/.calfkit``."""
    monkeypatch.setenv("HOME", str(tmp_path))
    return tmp_path


def _capture_popen(monkeypatch: pytest.MonkeyPatch) -> list[FakePopen]:
    spawned: list[FakePopen] = []

    def factory(cmd: list[str], **kwargs: object) -> FakePopen:
        proc = FakePopen(cmd, **kwargs)
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
    assert target.key == "127.0.0.1:9092"
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
    assert target.key == "a.example:1,b.example:2"
    assert target.servers == ("b.example:2", "a.example:1")


def test_multi_address_key_normalizes_each_element() -> None:
    target = normalize(["localhost", "other.example:9093"])
    assert target.key == "127.0.0.1:9092,other.example:9093"


def test_comma_joined_element_is_flattened() -> None:
    # resolve_mesh_url does not comma-split an env-provided CALFKIT_MESH_URL, so a comma-joined
    # value reaches normalize as ONE element; without flattening, "a:1,b:2" would silently
    # misparse as host "a:1,b" port 2.
    flattened = normalize(["a.example:1,b.example:2"])
    assert flattened.is_single is False
    assert flattened.key == normalize(["a.example:1", "b.example:2"]).key


def test_multi_address_has_no_single_listener() -> None:
    target = normalize(["a.example:1", "b.example:2"])
    assert target.listen_ip is None
    assert target.port is None
    with pytest.raises(ValueError, match="single-address"):
        _ = target.listener


def test_invalid_port_raises_value_error() -> None:
    with pytest.raises(ValueError, match="port"):
        normalize(["host:notaport"])


def test_out_of_range_ports_are_rejected() -> None:
    # Port 0 would bind an ephemeral port the probe can never hit; >65535 crashes deep in the
    # socket layer. Both are config errors that must surface immediately.
    for bad in ("host:0", "host:65536", "host:99999"):
        with pytest.raises(ValueError, match="port"):
            normalize([bad])


def test_aliased_duplicates_collapse_to_a_single_address() -> None:
    # localhost:9092,127.0.0.1:9092 is semantically ONE loopback address — it must stay
    # spawn-eligible, not fall into the borrow-or-error multi branch.
    target = normalize(["localhost:9092", "127.0.0.1:9092"])
    assert target.is_single is True
    assert target.listener == "127.0.0.1:9092"


def test_empty_host_raises_value_error() -> None:
    with pytest.raises(ValueError, match="invalid bootstrap address"):
        normalize([":9092"])


def test_log_tail_of_an_unreadable_log_says_so() -> None:
    # An empty tail would be indistinguishable from "the broker printed nothing".
    assert "could not read the log" in dev_broker._log_tail(Path("/nonexistent/tansu.log"))


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
    assert again.key == target.key
    assert again.listener == target.listener


def test_target_type_is_exported() -> None:
    assert isinstance(normalize(["localhost"]), Target)


# --- ensure_broker: reuse / borrow / connect-only (spec §5.2, §5.4, §5.7) ---------------------------


def test_reuses_a_reachable_dev_broker_as_managed(monkeypatch: pytest.MonkeyPatch) -> None:
    install_fake_psutil(monkeypatch, [FakeProc(4242, _broker_argv())])
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(True))
    out = ensure_broker(normalize(["localhost"]), resolve_bin=MustNotCall())
    assert out == BrokerInfo(listener=_KEY, pid=4242, managed=True, started_at=_STARTED)


def test_borrows_a_reachable_broker_that_is_not_a_dev_broker(monkeypatch: pytest.MonkeyPatch) -> None:
    # Something answers on the loopback address, but no memory-engine tansu is bound there
    # (e.g. the developer's own Redpanda): reused, not managed.
    install_fake_psutil(monkeypatch, [FakeProc(4242, ["/usr/local/bin/redpanda", "--kafka-addr", _KEY])])
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(True))
    out = ensure_broker(normalize(["localhost"]), resolve_bin=MustNotCall())
    assert out == BrokerInfo(listener=_KEY, pid=None, managed=False, started_at=None)


def test_borrows_a_reachable_remote_without_scanning(monkeypatch: pytest.MonkeyPatch) -> None:
    # A non-loopback address can never host a local dev broker, so the scan (and psutil) is
    # skipped entirely — the pure-client path works on a core install. No fake psutil is
    # installed here: an import would fail loudly.
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(True))
    out = ensure_broker(normalize(["203.0.113.7:9092"]), resolve_bin=MustNotCall())
    assert out.managed is False
    assert out.listener == "203.0.113.7:9092"


def test_reachable_loopback_without_psutil_degrades_to_reused(monkeypatch: pytest.MonkeyPatch) -> None:
    # Core install (no [mesh]): the managed-vs-reused classification cannot scan, and managing is
    # impossible without the extra anyway — report the broker as reused rather than failing.
    monkeypatch.setitem(sys.modules, "psutil", None)
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(True))
    out = ensure_broker(normalize(["localhost"]), resolve_bin=MustNotCall())
    assert out.managed is False


def test_multi_address_borrows_when_reachable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(True))
    out = ensure_broker(normalize(["a.example:1", "b.example:2"]), resolve_bin=MustNotCall())
    assert out.managed is False
    assert out.listener == "a.example:1,b.example:2"


def test_errors_for_unreachable_non_loopback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(False))
    with pytest.raises(DevBrokerError, match="connect-only|not a single loopback"):
        ensure_broker(normalize(["kafka.internal:9092"]), resolve_bin=MustNotCall())


def test_errors_for_unreachable_wildcard_bind(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(False))
    with pytest.raises(DevBrokerError):
        ensure_broker(normalize(["0.0.0.0:9092"]), resolve_bin=MustNotCall())


def test_errors_for_unreachable_multi_address(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(False))
    with pytest.raises(DevBrokerError):
        ensure_broker(normalize(["a.example:1", "b.example:2"]), resolve_bin=MustNotCall())


def test_detection_probe_targets_the_normalized_servers(monkeypatch: pytest.MonkeyPatch) -> None:
    probe = scripted_probe(True)
    monkeypatch.setattr(dev_broker, "is_reachable", probe)
    ensure_broker(normalize(["203.0.113.7:9092"]), resolve_bin=MustNotCall(), timeout=3.0)
    assert probe.calls == [(["203.0.113.7:9092"], 3.0)]


# --- ensure_broker: the spawn branch (spec §5.2, §5.5) ---------------------------------------------


def test_spawns_detached_when_loopback_unreachable(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import subprocess

    spawned = _capture_popen(monkeypatch)
    resolve_bin = CountingResolveBin(_BIN)
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(False, True))  # detect: absent; ready: yes

    out = ensure_broker(normalize(["localhost"]), resolve_bin=resolve_bin)

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
    assert out.managed is True
    assert out.listener == _KEY
    assert out.started_at  # a real timestamp, not empty
    assert out.log_path == str(tmp_path / ".calfkit" / "logs" / f"tansu-{_KEY}.log")


def test_spawn_logs_to_a_real_file_overwritten_each_spawn(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    log_path = tmp_path / ".calfkit" / "logs" / f"tansu-{_KEY}.log"
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

    out = ensure_broker(normalize(["localhost"]), resolve_bin=CountingResolveBin(_BIN))

    (proc,) = spawned
    assert out.log_path == str(log_path)
    assert log_path.read_bytes() == b"ready in 20ms\n", "the log must be truncated on each spawn"
    assert proc.kwargs["stdout"] is proc.kwargs["stderr"], "stdout and stderr share the log fd"
    assert proc.kwargs["stdout"].closed, "the parent must close its copy of the log fd after spawn"


def test_spawn_waits_for_readiness_via_the_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    _capture_popen(monkeypatch)
    probe = scripted_probe(False, False, False, True)
    monkeypatch.setattr(dev_broker, "is_reachable", probe)
    ensure_broker(normalize(["localhost"]), resolve_bin=CountingResolveBin(_BIN))
    # 1 detection probe + 3 readiness probes; readiness targets the single bind address.
    assert len(probe.calls) == 4
    assert probe.calls[-1][0] == _KEY


def test_readiness_timeout_kills_the_spawn_and_surfaces_the_log_tail(monkeypatch: pytest.MonkeyPatch) -> None:
    import subprocess

    spawned: list[FakePopen] = []

    class UnreapablePopen(FakePopen):
        def wait(self, timeout: float | None = None) -> int | None:
            # The post-kill reap must never let a slow exit escape as a raw exception.
            raise subprocess.TimeoutExpired(cmd=self.cmd, timeout=timeout or 0)

    def factory(cmd: list[str], **kwargs: object) -> FakePopen:
        proc = UnreapablePopen(cmd, **kwargs)
        proc.write_log(b"error: address already in use\n")
        spawned.append(proc)
        return proc

    monkeypatch.setattr(dev_broker, "Popen", factory)
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(False))
    with pytest.raises(DevBrokerError, match="address already in use"):
        ensure_broker(normalize(["localhost"]), resolve_bin=CountingResolveBin(_BIN), timeout=0.05)
    (proc,) = spawned
    assert proc.killed is True


def test_spawn_that_exits_during_startup_fails_fast_with_the_log_tail(monkeypatch: pytest.MonkeyPatch) -> None:
    def factory(cmd: list[str], **kwargs: object) -> FakePopen:
        proc = FakePopen(cmd, **kwargs)
        proc.write_log(b"panic: bind failed\n")
        proc.returncode = 1  # died immediately (spec §7.2)
        return proc

    monkeypatch.setattr(dev_broker, "Popen", factory)
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(False))
    with pytest.raises(DevBrokerError, match="bind failed"):
        ensure_broker(normalize(["localhost"]), resolve_bin=CountingResolveBin(_BIN), timeout=30.0)


def test_spawn_killed_by_a_signal_hints_at_a_concurrent_stop(monkeypatch: pytest.MonkeyPatch) -> None:
    # A lock-free `ck dev mesh stop/restart` can race an in-flight readiness wait (spec §5.3);
    # the negative returncode gets a hint instead of a bare "exit code -15".
    def factory(cmd: list[str], **kwargs: object) -> FakePopen:
        proc = FakePopen(cmd, **kwargs)
        proc.returncode = -15
        return proc

    monkeypatch.setattr(dev_broker, "Popen", factory)
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(False))
    with pytest.raises(DevBrokerError, match="killed by a signal"):
        ensure_broker(normalize(["localhost"]), resolve_bin=CountingResolveBin(_BIN))


def test_wrong_arch_binary_surfaces_a_distinct_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def factory(cmd: list[str], **kwargs: object) -> FakePopen:
        raise OSError(8, "Exec format error")

    monkeypatch.setattr(dev_broker, "Popen", factory)
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(False))
    with pytest.raises(DevBrokerError, match="Exec format error"):
        ensure_broker(normalize(["localhost"]), resolve_bin=CountingResolveBin(_BIN))


def test_missing_mesh_extra_yields_an_actionable_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def resolve_bin() -> str:
        raise ModuleNotFoundError("No module named 'calfkit_mesh'")

    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(False))
    with pytest.raises(DevBrokerError, match=r"calfkit\[mesh\].*CALF_TANSU_BIN"):
        ensure_broker(normalize(["localhost"]), resolve_bin=resolve_bin)


def test_locator_failure_maps_to_a_clean_error(monkeypatch: pytest.MonkeyPatch) -> None:
    # calfkit_mesh.TansuBinaryNotFound is a RuntimeError: exit-2 material, never a raw traceback.
    def resolve_bin() -> str:
        raise RuntimeError("No tansu binary: install a platform wheel or set CALF_TANSU_BIN.")

    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(False))
    with pytest.raises(DevBrokerError, match="No tansu binary"):
        ensure_broker(normalize(["localhost"]), resolve_bin=resolve_bin)


def test_readiness_probe_attempts_are_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    # Per-attempt budget ≤ 1s (so proc.poll() keeps being re-checked) and ≤ the remaining
    # deadline (so the total wait never overshoots `timeout`).
    _capture_popen(monkeypatch)
    probe = scripted_probe(False, False, False, True)
    monkeypatch.setattr(dev_broker, "is_reachable", probe)
    ensure_broker(normalize(["localhost"]), resolve_bin=CountingResolveBin(_BIN), timeout=5.0)
    readiness_timeouts = [timeout for _, timeout in probe.calls[1:]]
    assert readiness_timeouts, "expected readiness probes"
    assert all(t <= 1.0 for t in readiness_timeouts)


def test_unwritable_state_dir_is_a_clean_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    (tmp_path / ".calfkit").write_text("a file where the state dir should be")
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(False))
    with pytest.raises(DevBrokerError, match="calfkit state dir"):
        ensure_broker(normalize(["localhost"]), resolve_bin=MustNotCall())


def test_unwritable_log_dir_is_a_clean_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calfkit_dir = tmp_path / ".calfkit"
    calfkit_dir.mkdir()
    (calfkit_dir / "logs").write_text("a file where the log dir should be")
    _capture_popen(monkeypatch)
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(False))
    with pytest.raises(DevBrokerError, match="dev broker log"):
        ensure_broker(normalize(["localhost"]), resolve_bin=CountingResolveBin(_BIN))


def test_contended_spawn_lock_announces_itself(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """Spec §5.3: a blocked second invocation prints 'waiting for the dev broker to start…'."""
    import types

    real_fcntl = __import__("fcntl")
    fake = types.ModuleType("fcntl")
    fake.LOCK_EX = real_fcntl.LOCK_EX  # type: ignore[attr-defined]
    fake.LOCK_NB = real_fcntl.LOCK_NB  # type: ignore[attr-defined]
    fake.LOCK_UN = real_fcntl.LOCK_UN  # type: ignore[attr-defined]
    state = {"nb_attempted": False}

    def flock(fd: int, flags: int) -> None:
        if flags & real_fcntl.LOCK_NB and not state["nb_attempted"]:
            state["nb_attempted"] = True
            raise BlockingIOError  # someone else is mid-spawn
        # the message must precede the blocking acquire; the lock is then granted

    fake.flock = flock  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "fcntl", fake)
    with dev_broker._spawn_lock():
        pass
    assert "waiting for the dev broker to start" in capsys.readouterr().err


def test_flock_failure_is_a_clean_error(monkeypatch: pytest.MonkeyPatch) -> None:
    # An flock that fails outright (e.g. ENOLCK on an NFS home) is exit-2 material, not a crash.
    import errno
    import types

    real_fcntl = __import__("fcntl")
    fake = types.ModuleType("fcntl")
    fake.LOCK_EX = real_fcntl.LOCK_EX  # type: ignore[attr-defined]
    fake.LOCK_NB = real_fcntl.LOCK_NB  # type: ignore[attr-defined]
    fake.LOCK_UN = real_fcntl.LOCK_UN  # type: ignore[attr-defined]

    def flock(fd: int, flags: int) -> None:
        raise OSError(errno.ENOLCK, "No locks available")

    fake.flock = flock  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "fcntl", fake)
    with pytest.raises(DevBrokerError, match="cannot lock"), dev_broker._spawn_lock():
        pass  # pragma: no cover — the lock acquire raises before the body runs


def test_missing_locator_symbol_is_the_same_actionable_error(monkeypatch: pytest.MonkeyPatch) -> None:
    # Version skew: a calfkit_mesh module without resolve_broker_bin raises a bare ImportError.
    def resolve_bin() -> str:
        raise ImportError("cannot import name 'resolve_broker_bin' from 'calfkit_mesh'")

    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(False))
    with pytest.raises(DevBrokerError, match=r"calfkit\[mesh\]"):
        ensure_broker(normalize(["localhost"]), resolve_bin=resolve_bin)


def test_restart_without_the_mesh_extra_reuses_a_borrowed_broker(monkeypatch: pytest.MonkeyPatch) -> None:
    # §4.2: on a borrowed broker restart just re-reuses it — including on a core install,
    # where the ownership scan (psutil) cannot run at all.
    monkeypatch.setitem(sys.modules, "psutil", None)
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(True))
    out = dev_broker.restart(normalize(["localhost"]), resolve_bin=MustNotCall())
    assert out.managed is False


def test_detach_kwargs_per_platform(monkeypatch: pytest.MonkeyPatch) -> None:
    import os
    import subprocess

    monkeypatch.setattr(os, "name", "posix")
    assert dev_broker._detach_kwargs() == {"start_new_session": True}
    monkeypatch.setattr(os, "name", "nt")
    expected = getattr(subprocess, "DETACHED_PROCESS", 0)
    assert dev_broker._detach_kwargs() == {"creationflags": expected}


# --- spawn_foreground: the attached spawn (the `ck dev mesh start` foreground path) ----------------


def test_foreground_spawns_attached_when_loopback_unreachable(monkeypatch: pytest.MonkeyPatch) -> None:
    import subprocess

    spawned = _capture_popen(monkeypatch)
    resolve_bin = CountingResolveBin(_BIN)
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(False, True))  # absent, then ready

    proc = dev_broker.spawn_foreground(normalize(["localhost"]), resolve_bin=resolve_bin)

    assert resolve_bin.calls == 1
    (spawned_proc,) = spawned
    assert spawned_proc is proc, "the live attached process is returned for the caller to wait on"
    # Byte-identical argv to the detached spawn, so the §5.4 ownership scan recognizes it too.
    assert proc.cmd == _broker_argv()
    # Attached: shares our session (Ctrl-C reaches it) and inherits the terminal (no log redirect).
    assert "start_new_session" not in proc.kwargs
    assert proc.kwargs["stdin"] is subprocess.DEVNULL
    assert "stdout" not in proc.kwargs and "stderr" not in proc.kwargs


def test_foreground_errors_when_a_broker_is_already_running(monkeypatch: pytest.MonkeyPatch) -> None:
    # Foreground cannot attach to an already-detached daemon — refuse (no spawn, no binary resolve).
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(True))
    with pytest.raises(DevBrokerError, match="already running"):
        dev_broker.spawn_foreground(normalize(["localhost"]), resolve_bin=MustNotCall())


def test_foreground_refuses_a_non_loopback_address(monkeypatch: pytest.MonkeyPatch) -> None:
    # Same connect-only rule as ensure_broker (spec §5.2): never spawn at a non-loopback address.
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(False))
    with pytest.raises(DevBrokerError, match="connect-only"):
        dev_broker.spawn_foreground(normalize(["203.0.113.7:9092"]), resolve_bin=MustNotCall())


def test_foreground_readiness_timeout_kills_the_spawn(monkeypatch: pytest.MonkeyPatch) -> None:
    spawned = _capture_popen(monkeypatch)
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(False))  # absent, then never ready
    with pytest.raises(DevBrokerError, match="did not become ready"):
        dev_broker.spawn_foreground(normalize(["localhost"]), resolve_bin=CountingResolveBin(_BIN), timeout=0.05)
    (proc,) = spawned
    assert proc.killed is True


def test_foreground_spawn_that_exits_during_startup_fails_fast(monkeypatch: pytest.MonkeyPatch) -> None:
    def factory(cmd: list[str], **kwargs: object) -> FakePopen:
        proc = FakePopen(cmd, **kwargs)
        proc.returncode = 1  # bind failure — died immediately
        return proc

    monkeypatch.setattr(dev_broker, "Popen", factory)
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(False))
    with pytest.raises(DevBrokerError, match="exited during startup"):
        dev_broker.spawn_foreground(normalize(["localhost"]), resolve_bin=CountingResolveBin(_BIN))


def test_foreground_wrong_arch_binary_surfaces_a_distinct_error(monkeypatch: pytest.MonkeyPatch) -> None:
    # The exec-time failure twin of test_wrong_arch_binary_surfaces_a_distinct_error (detached path).
    def factory(cmd: list[str], **kwargs: object) -> FakePopen:
        raise OSError(8, "Exec format error")

    monkeypatch.setattr(dev_broker, "Popen", factory)
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(False))
    with pytest.raises(DevBrokerError, match="failed to launch"):
        dev_broker.spawn_foreground(normalize(["localhost"]), resolve_bin=CountingResolveBin(_BIN))


def test_foreground_holds_the_lock_across_probe_and_readiness(monkeypatch: pytest.MonkeyPatch) -> None:
    """The attached spawn takes the same §5.3 lock and releases it once ready, before the caller
    blocks on the process — a concurrent invocation must never see the not-yet-ready broker."""
    from contextlib import contextmanager

    events: list[str] = []

    @contextmanager
    def spy_lock():  # type: ignore[no-untyped-def]
        events.append("lock")
        yield
        events.append("unlock")

    def factory(cmd: list[str], **kwargs: object) -> FakePopen:
        events.append("popen")
        return FakePopen(cmd, **kwargs)

    probe = scripted_probe(False, True)

    def probing(bootstrap: object, *, timeout: float) -> bool:
        events.append("probe")
        result: bool = probe(bootstrap, timeout=timeout)
        return result

    monkeypatch.setattr(dev_broker, "_spawn_lock", spy_lock)
    monkeypatch.setattr(dev_broker, "Popen", factory)
    monkeypatch.setattr(dev_broker, "is_reachable", probing)
    dev_broker.spawn_foreground(normalize(["localhost"]), resolve_bin=CountingResolveBin(_BIN))
    assert events[0] == "lock"
    assert events[-1] == "unlock"
    assert events[1:-1] == ["probe", "popen", "probe"], "probe→spawn→readiness must all run under the lock"


# --- the spawn lock (spec §5.3) ---------------------------------------------------------------------


def test_lock_is_held_across_probe_spawn_and_readiness(monkeypatch: pytest.MonkeyPatch) -> None:
    """Spec §5.3: releasing before readiness would let a second invocation double-spawn."""
    from contextlib import contextmanager

    events: list[str] = []

    @contextmanager
    def spy_lock():  # type: ignore[no-untyped-def]
        events.append("lock")
        yield
        events.append("unlock")

    def factory(cmd: list[str], **kwargs: object) -> FakePopen:
        events.append("popen")
        return FakePopen(cmd, **kwargs)

    probe = scripted_probe(False, True)

    def probing(bootstrap: object, *, timeout: float) -> bool:
        events.append("probe")
        result: bool = probe(bootstrap, timeout=timeout)
        return result

    monkeypatch.setattr(dev_broker, "_spawn_lock", spy_lock)
    monkeypatch.setattr(dev_broker, "Popen", factory)
    monkeypatch.setattr(dev_broker, "is_reachable", probing)
    ensure_broker(normalize(["localhost"]), resolve_bin=CountingResolveBin(_BIN))
    assert events[0] == "lock"
    assert events[-1] == "unlock"
    assert events[1:-1] == ["probe", "popen", "probe"], "probe→spawn→readiness must all run under the lock"


def test_spawn_lock_excludes_other_processes(tmp_path: Path) -> None:
    """The flock must be held for real — a second *process* has to block (probed with LOCK_NB)."""
    import subprocess
    import sys as _sys

    lock_file = tmp_path / ".calfkit" / "dev-mesh.lock"
    probe = (
        "import fcntl, sys\n"
        f"fd = open({str(lock_file)!r}, 'w')\n"
        "try:\n"
        "    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)\n"
        "except BlockingIOError:\n"
        "    sys.exit(42)\n"
        "sys.exit(0)\n"
    )
    with dev_broker._spawn_lock():
        held = subprocess.run([_sys.executable, "-c", probe])
    released = subprocess.run([_sys.executable, "-c", probe])
    assert held.returncode == 42, "lock file was not exclusively held while inside _spawn_lock()"
    assert released.returncode == 0


def test_stop_takes_no_lock(monkeypatch: pytest.MonkeyPatch) -> None:
    """Spec §5.3: the lock exists only for the spawn race — stop must never contend on it."""
    monkeypatch.setattr(dev_broker, "_spawn_lock", lambda: pytest.fail("stop must not take the spawn lock"))
    install_fake_psutil(monkeypatch, [FakeProc(4242, _broker_argv())])
    assert dev_broker.stop(normalize(["localhost"])) is True


# --- stop / stop_all — the memory-engine tansu stopper (spec §5.4, §5.6) ----------------------------


def test_stop_terminates_the_dev_broker_at_the_listener(monkeypatch: pytest.MonkeyPatch) -> None:
    proc = FakeProc(4242, _broker_argv())
    install_fake_psutil(monkeypatch, [proc])
    assert dev_broker.stop(normalize(["localhost"])) is True
    assert proc.events == ["terminate", "wait"]


def test_stop_escalates_to_sigkill_after_grace(monkeypatch: pytest.MonkeyPatch) -> None:
    proc = FakeProc(4242, _broker_argv(), survives_sigterm=True)
    install_fake_psutil(monkeypatch, [proc])
    assert dev_broker.stop(normalize(["localhost"]), grace=0.01) is True
    assert proc.events == ["terminate", "wait", "kill", "wait"]


def test_stop_is_a_noop_when_no_dev_broker_matches(monkeypatch: pytest.MonkeyPatch) -> None:
    install_fake_psutil(monkeypatch, [])
    assert dev_broker.stop(normalize(["localhost"])) is False


def test_stop_never_signals_non_matching_processes(monkeypatch: pytest.MonkeyPatch) -> None:
    """C3: only a memory-engine tansu bound to the target listener may ever be signalled."""
    durable_tansu = FakeProc(1111, _broker_argv(engine="s3://bucket/"))  # durable engine — not a dev broker
    other_listener = FakeProc(2222, _broker_argv(listener="127.0.0.1:19092"))
    postgres = FakeProc(3333, ["/usr/bin/postgres", "-D", "/data"])
    unreadable = FakeProc(4444, raises="denied")
    install_fake_psutil(monkeypatch, [durable_tansu, other_listener, postgres, unreadable])
    assert dev_broker.stop(normalize(["localhost"])) is False
    for proc in (durable_tansu, other_listener, postgres, unreadable):
        assert proc.events == [], "a non-matching process must never be signalled"


def test_scan_matches_the_space_separated_argv_form(monkeypatch: pytest.MonkeyPatch) -> None:
    # clap accepts both `--flag=value` and `--flag value`; a hand-started dev broker may use either.
    proc = FakeProc(
        4242,
        [_BIN, "broker", "--storage-engine", "memory://tansu/", "--kafka-listener-url", "tcp://127.0.0.1:9092"],
    )
    install_fake_psutil(monkeypatch, [proc])
    assert dev_broker.stop(normalize(["localhost"])) is True
    assert proc.events == ["terminate", "wait"]


def test_scan_normalizes_the_hand_started_listener(monkeypatch: pytest.MonkeyPatch) -> None:
    # A hand-started `tcp://localhost:9092` must compare equal to the normalized 127.0.0.1:9092.
    proc = FakeProc(4242, [_BIN, "broker", "--storage-engine=memory://x/", "--kafka-listener-url=tcp://localhost:9092"])
    install_fake_psutil(monkeypatch, [proc])
    assert dev_broker.stop(normalize(["localhost"])) is True


def test_scan_skips_an_unparseable_listener(monkeypatch: pytest.MonkeyPatch) -> None:
    proc = FakeProc(4242, [_BIN, "broker", "--storage-engine=memory://x/", "--kafka-listener-url=tcp://127.0.0.1:bad"])
    install_fake_psutil(monkeypatch, [proc])
    assert dev_broker.stop(normalize(["localhost"])) is False
    assert proc.events == []


def test_scan_skips_an_empty_cmdline(monkeypatch: pytest.MonkeyPatch) -> None:
    # A Linux zombie reads back an empty cmdline (no exception) — by definition not manageable.
    proc = FakeProc(4242, [])
    install_fake_psutil(monkeypatch, [proc])
    assert dev_broker.stop(normalize(["localhost"])) is False


def test_memory_tansu_without_a_listener_arg_never_matches(monkeypatch: pytest.MonkeyPatch) -> None:
    # A memory-engine tansu whose listener came from env/defaults has no listener arg to match —
    # the scan cannot prove which address it is bound to, so it is never managed or signalled.
    proc = FakeProc(4242, [_BIN, "broker", "--storage-engine=memory://tansu/"])
    install_fake_psutil(monkeypatch, [proc])
    assert dev_broker.stop(normalize(["localhost"])) is False
    assert proc.events == []


def test_stop_handles_a_process_that_vanishes_mid_signal(monkeypatch: pytest.MonkeyPatch) -> None:
    # The broker exits between the scan and the SIGTERM: the goal state (stopped) is reached.
    proc = FakeProc(4242, _broker_argv(), vanishes_on_signal=True)
    install_fake_psutil(monkeypatch, [proc])
    assert dev_broker.stop(normalize(["localhost"])) is True


def test_stop_treats_an_unreaped_zombie_as_stopped(monkeypatch: pytest.MonkeyPatch) -> None:
    """The blessed two-terminal flow: the broker's spawning `ck dev run` is still alive, so the
    SIGTERM'd broker becomes an unreaped zombie — psutil's non-child wait never returns for it
    (spec §5.8). That IS the stopped state; it must not crash or escalate to SIGKILL."""
    proc = FakeProc(4242, _broker_argv(), zombie_on_term=True)
    install_fake_psutil(monkeypatch, [proc])
    assert dev_broker.stop(normalize(["localhost"]), grace=0.01) is True
    assert proc.events == ["terminate", "wait"], "a zombie needs no SIGKILL"


def test_stop_treats_a_zombie_after_sigkill_as_stopped(monkeypatch: pytest.MonkeyPatch) -> None:
    # SIGTERM ignored (slow shutdown), SIGKILL lands but the parent hasn't reaped yet.
    proc = FakeProc(4242, _broker_argv(), survives_sigterm=True, zombie_on_kill=True)
    install_fake_psutil(monkeypatch, [proc])
    assert dev_broker.stop(normalize(["localhost"]), grace=0.01) is True
    assert proc.events == ["terminate", "wait", "kill", "wait"]


def test_stop_treats_a_vanish_during_the_status_check_as_stopped(monkeypatch: pytest.MonkeyPatch) -> None:
    # The process disappears between the wait timeout and the zombie check.
    proc = FakeProc(4242, _broker_argv(), survives_sigterm=True, nosuch_on_status=True)
    install_fake_psutil(monkeypatch, [proc])
    assert dev_broker.stop(normalize(["localhost"]), grace=0.01) is True
    assert proc.events == ["terminate", "wait"], "a vanished process needs no SIGKILL"


def test_stop_surfaces_a_sigkill_survivor_actionably(monkeypatch: pytest.MonkeyPatch) -> None:
    # A D-state/unkillable process: exit-2 material (DevBrokerError), never a raw traceback.
    proc = FakeProc(4242, _broker_argv(), survives_sigterm=True, survives_sigkill=True)
    install_fake_psutil(monkeypatch, [proc])
    with pytest.raises(DevBrokerError, match="survived SIGKILL"):
        dev_broker.stop(normalize(["localhost"]), grace=0.01)


def test_stop_of_another_users_broker_is_actionable(monkeypatch: pytest.MonkeyPatch) -> None:
    # Linux cmdlines are world-readable, so the scan can match a broker we cannot signal.
    proc = FakeProc(4242, _broker_argv(), denied_on_signal=True)
    install_fake_psutil(monkeypatch, [proc])
    with pytest.raises(DevBrokerError, match="another user"):
        dev_broker.stop(normalize(["localhost"]))


def test_stop_all_skips_a_denied_broker_and_continues(monkeypatch: pytest.MonkeyPatch) -> None:
    denied = FakeProc(4242, _broker_argv(), denied_on_signal=True)
    ours = FakeProc(4343, _broker_argv(listener="127.0.0.1:19092"))
    install_fake_psutil(monkeypatch, [denied, ours])
    assert dev_broker.stop_all() == ["127.0.0.1:19092"]
    assert ours.events == ["terminate", "wait"], "one denied broker must not abort the sweep"


def test_stop_all_warns_and_continues_past_a_sigkill_survivor(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    unkillable = FakeProc(4242, _broker_argv(), survives_sigterm=True, survives_sigkill=True)
    ours = FakeProc(4343, _broker_argv(listener="127.0.0.1:19092"))
    install_fake_psutil(monkeypatch, [unkillable, ours])
    assert dev_broker.stop_all(grace=0.01) == ["127.0.0.1:19092"]
    assert ours.events == ["terminate", "wait"], "one unkillable broker must not abort the sweep"
    assert "survived SIGKILL" in capsys.readouterr().err


def test_stop_multi_address_is_a_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    # A multi-address target is never a spawn target, so there is nothing local to stop —
    # and psutil is never consulted (no fake installed: an import would fail loudly).
    assert dev_broker.stop(normalize(["a.example:1", "b.example:2"])) is False


def test_stop_without_the_mesh_extra_is_actionable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "psutil", None)
    with pytest.raises(DevBrokerError, match=r"calfkit\[mesh\]"):
        dev_broker.stop(normalize(["localhost"]))


def test_stop_all_stops_every_dev_broker(monkeypatch: pytest.MonkeyPatch) -> None:
    proc_a = FakeProc(4242, _broker_argv())
    proc_b = FakeProc(4343, _broker_argv(listener="127.0.0.1:19092"))
    not_a_broker = FakeProc(3333, ["/usr/bin/postgres"])
    install_fake_psutil(monkeypatch, [proc_a, proc_b, not_a_broker])
    assert dev_broker.stop_all() == [_KEY, "127.0.0.1:19092"]
    assert proc_a.events == ["terminate", "wait"]
    assert proc_b.events == ["terminate", "wait"]
    assert not_a_broker.events == []


def test_stop_all_with_nothing_running(monkeypatch: pytest.MonkeyPatch) -> None:
    install_fake_psutil(monkeypatch, [])
    assert dev_broker.stop_all() == []


# --- status (spec §4.2) -----------------------------------------------------------------------------


def test_status_reports_scan_hits_and_target_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    install_fake_psutil(monkeypatch, [FakeProc(4242, _broker_argv())])
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(True))
    report = dev_broker.status(normalize(["localhost"]))
    assert report.target_key == _KEY
    assert report.reachable is True
    (broker,) = report.brokers
    assert broker == BrokerInfo(listener=_KEY, pid=4242, managed=True, started_at=_STARTED)


def test_status_reachable_but_not_managed(monkeypatch: pytest.MonkeyPatch) -> None:
    # A borrowed/pre-existing broker answers, but no dev broker is bound there.
    install_fake_psutil(monkeypatch, [])
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(True))
    report = dev_broker.status(normalize(["localhost"]))
    assert report.reachable is True
    assert report.brokers == ()


def test_status_nothing_reachable(monkeypatch: pytest.MonkeyPatch) -> None:
    install_fake_psutil(monkeypatch, [])
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(False))
    report = dev_broker.status(normalize(["localhost"]))
    assert report.reachable is False


def test_status_without_the_mesh_extra_is_actionable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "psutil", None)
    with pytest.raises(DevBrokerError, match=r"calfkit\[mesh\]"):
        dev_broker.status(normalize(["localhost"]))


# --- restart (spec §4.2) ----------------------------------------------------------------------------


def test_restart_stops_the_dev_broker_then_spawns_fresh(monkeypatch: pytest.MonkeyPatch) -> None:
    old = FakeProc(4242, _broker_argv())
    install_fake_psutil(monkeypatch, [old])
    spawned = _capture_popen(monkeypatch)
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(False, True))  # post-stop: absent, then ready
    out = dev_broker.restart(normalize(["localhost"]), resolve_bin=CountingResolveBin(_BIN))
    assert old.events == ["terminate", "wait"]
    assert len(spawned) == 1
    assert out.pid == 5555
    assert out.managed is True


def test_restart_of_a_borrowed_broker_just_reuses_it(monkeypatch: pytest.MonkeyPatch) -> None:
    # stop is a no-op when no dev broker matches, so restart re-reuses the still-running one.
    install_fake_psutil(monkeypatch, [])
    monkeypatch.setattr(dev_broker, "is_reachable", scripted_probe(True))
    out = dev_broker.restart(normalize(["localhost"]), resolve_bin=MustNotCall())
    assert out.managed is False
