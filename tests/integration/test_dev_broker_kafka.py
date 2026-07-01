"""Real-broker tests for the ``ck dev`` supervisor (kafka lane, ADR-0007).

Two slices: the reachability probe against the lane's real broker, and the full connect-or-spawn
lifecycle against the **bundled Tansu binary** — spawn → ready → reuse → stop (spec §5.7 end to
end). The spawn slice needs the ``[mesh]`` extra (``calfkit_mesh`` + ``psutil``) and skips cleanly
when it is not installed.
"""

from __future__ import annotations

import socket
from pathlib import Path

import pytest

from calfkit.cli._dev_broker import ensure_broker, normalize, stop
from calfkit.cli._dev_probe import is_reachable
from tests._dev_fakes import MustNotCall

pytestmark = pytest.mark.kafka


def _free_loopback_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        assert isinstance(port, int)
        return port


def test_probe_is_reachable_against_a_real_broker(kafka_bootstrap: str) -> None:
    assert is_reachable(kafka_bootstrap, timeout=10.0) is True


def test_probe_is_unreachable_on_a_closed_port() -> None:
    assert is_reachable(f"127.0.0.1:{_free_loopback_port()}", timeout=2.0) is False


def test_spawn_reuse_stop_roundtrip(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Spawn the bundled Tansu, see it become ready, reuse it via the ownership scan, tear it
    down — spec §5.2–§5.7 end to end, with no persisted state anywhere."""
    calfkit_mesh = pytest.importorskip("calfkit_mesh", reason="the [mesh] extra is not installed")
    pytest.importorskip("psutil", reason="the [mesh] extra is not installed")
    monkeypatch.setenv("HOME", str(tmp_path))  # keep the spawn lock + log out of the real ~/.calfkit

    target = normalize([f"127.0.0.1:{_free_loopback_port()}"])
    spawned = ensure_broker(target, resolve_bin=calfkit_mesh.resolve_broker_bin, timeout=30.0)
    try:
        assert spawned.managed is True
        assert spawned.listener == target.listener
        assert spawned.pid is not None
        assert spawned.log_path is not None and Path(spawned.log_path).exists()

        # A second ensure finds the running broker via the process scan — same pid, and the
        # locator is never touched (spec §5.7).
        again = ensure_broker(target, resolve_bin=MustNotCall(), timeout=10.0)
        assert again.managed is True
        assert again.pid == spawned.pid
    finally:
        stopped = stop(target)

    assert stopped is True
    assert is_reachable(target.listener, timeout=2.0) is False
