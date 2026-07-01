"""Tests for ``calfkit.cli._dev_state`` — the dev broker registry + ``is_ours`` ownership (spec §5.4).

The registry uses a ``tmp_path`` root (no real ``~/.calfkit``). ``is_ours`` is exercised with an injected
fake ``psutil`` so the tests stay offline and control exactly what the process argv looks like.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

from calfkit.cli._dev_state import BrokerRecord, Registry, is_ours

_BIN = "/home/x/.calfkit/bin/tansu-v0.6.0"


def _rec(**kw: object) -> BrokerRecord:
    defaults: dict[str, object] = dict(
        pid=4242,
        bootstrap="localhost",
        listener="127.0.0.1:9092",
        binary_path=_BIN,
        started_at="2026-07-01T00:00:00Z",
        log_path="/tmp/tansu.log",
        spawned_by_calfkit=True,
    )
    defaults.update(kw)
    return BrokerRecord(**defaults)  # type: ignore[arg-type]


# --- registry -------------------------------------------------------------------------------------


def test_put_get_read_remove_roundtrip(tmp_path: object) -> None:
    reg = Registry(root=tmp_path)  # type: ignore[arg-type]
    assert reg.read() == {}
    assert reg.get("127.0.0.1:9092") is None
    rec = _rec()
    reg.put("127.0.0.1:9092", rec)
    assert reg.get("127.0.0.1:9092") == rec
    assert reg.read() == {"127.0.0.1:9092": rec}
    reg.remove("127.0.0.1:9092")
    assert reg.get("127.0.0.1:9092") is None
    assert reg.read() == {}


def test_remove_missing_key_is_noop(tmp_path: object) -> None:
    reg = Registry(root=tmp_path)  # type: ignore[arg-type]
    reg.remove("nope")
    assert reg.read() == {}


def test_corrupt_registry_reads_as_empty(tmp_path: object) -> None:
    reg = Registry(root=tmp_path)  # type: ignore[arg-type]
    (tmp_path / "dev-mesh.json").write_text("{ not json")  # type: ignore[operator]
    assert reg.read() == {}


def test_atomic_write_leaves_no_temp(tmp_path: object) -> None:
    reg = Registry(root=tmp_path)  # type: ignore[arg-type]
    reg.put("a:1", _rec())
    assert list(tmp_path.glob(".dev-mesh.*.tmp")) == []  # type: ignore[attr-defined]


def test_multiple_records(tmp_path: object) -> None:
    reg = Registry(root=tmp_path)  # type: ignore[arg-type]
    reg.put("a:1", _rec(pid=1, listener="a:1"))
    reg.put("b:2", _rec(pid=2, listener="b:2"))
    assert set(reg.read()) == {"a:1", "b:2"}
    reg.remove("a:1")
    assert set(reg.read()) == {"b:2"}


# --- is_ours (fake psutil) -----------------------------------------------------------------------


def _install_fake_psutil(
    monkeypatch: pytest.MonkeyPatch,
    *,
    cmdline: list[str] | None = None,
    raises: str | None = None,
) -> None:
    fake = types.ModuleType("psutil")

    class NoSuchProcess(Exception): ...  # noqa: N818 -- mirrors psutil's real exception name

    class AccessDenied(Exception): ...  # noqa: N818

    class ZombieProcess(Exception): ...  # noqa: N818

    fake.NoSuchProcess = NoSuchProcess  # type: ignore[attr-defined]
    fake.AccessDenied = AccessDenied  # type: ignore[attr-defined]
    fake.ZombieProcess = ZombieProcess  # type: ignore[attr-defined]

    class Process:
        def __init__(self, pid: int) -> None:
            if raises == "nosuch":
                raise NoSuchProcess()
            self.pid = pid

        def cmdline(self) -> list[str]:
            if raises == "zombie":
                raise ZombieProcess()
            if raises == "denied":
                raise AccessDenied()
            return list(cmdline or [])

    fake.Process = Process  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "psutil", fake)


def test_is_ours_true_for_our_tansu(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_psutil(monkeypatch, cmdline=[_BIN, "broker", "--kafka-listener-url=tcp://127.0.0.1:9092"])
    assert is_ours(_rec()) is True


def test_is_ours_false_when_pid_gone(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_psutil(monkeypatch, raises="nosuch")
    assert is_ours(_rec()) is False


def test_is_ours_false_for_foreign_binary(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_psutil(monkeypatch, cmdline=["/usr/bin/python", "-m", "http.server"])
    assert is_ours(_rec()) is False


def test_is_ours_false_when_listener_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    # Same binary, but bound to a different port than the record's listener.
    _install_fake_psutil(monkeypatch, cmdline=[_BIN, "broker", "--kafka-listener-url=tcp://127.0.0.1:5555"])
    assert is_ours(_rec()) is False


def test_is_ours_false_on_zombie(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_psutil(monkeypatch, raises="zombie")
    assert is_ours(_rec()) is False


def test_is_ours_false_on_access_denied(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_psutil(monkeypatch, raises="denied")
    assert is_ours(_rec()) is False


def test_is_ours_false_on_empty_cmdline(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_psutil(monkeypatch, cmdline=[])
    assert is_ours(_rec()) is False


def test_default_root_is_dot_calfkit() -> None:
    from calfkit.cli._dev_state import _state_dir

    expected = Path.home() / ".calfkit"
    assert _state_dir() == expected
    assert Registry()._root == expected
