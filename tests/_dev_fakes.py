"""Shared fakes for the ``ck dev`` supervisor tests (offline lane).

``psutil`` ships only in the ``[mesh]`` extra and is lazily imported by the supervisor, so tests
inject a fake module into ``sys.modules``. The fake models the surface the stateless ownership
scan uses: ``process_iter()`` over :class:`FakeProc` objects with scripted argv, ``create_time``,
and signal bookkeeping. ``FakePopen`` stands in for ``subprocess.Popen`` via the
``calfkit.cli._dev_broker.Popen`` seam.
"""

from __future__ import annotations

import sys
import types
from typing import IO, Any

import pytest

# 2026-07-01T00:00:00Z — a fixed, timezone-stable create_time for scripted processes.
FAKE_CREATE_TIME = 1_782_864_000.0


class FakeProc:
    """A fake ``psutil.Process`` with scripted argv and signal bookkeeping.

    Models the non-child ``wait`` semantics the supervisor relies on: a process that dies into an
    **unreaped zombie** (its spawning ``ck dev run`` is still alive) keeps timing ``wait`` out —
    psutil polls ``pid_exists``, which a zombie passes — while ``status()`` reports ``"zombie"``.
    """

    def __init__(
        self,
        pid: int,
        cmdline: list[str] | None = None,
        *,
        raises: str | None = None,
        survives_sigterm: bool = False,
        survives_sigkill: bool = False,
        vanishes_on_signal: bool = False,
        zombie_on_term: bool = False,
        zombie_on_kill: bool = False,
        denied_on_signal: bool = False,
        nosuch_on_status: bool = False,
    ) -> None:
        self.pid = pid
        self._cmdline = cmdline or []
        self._raises = raises
        self._survives_sigterm = survives_sigterm
        self._survives_sigkill = survives_sigkill
        self._vanishes_on_signal = vanishes_on_signal
        self._zombie_on_term = zombie_on_term
        self._zombie_on_kill = zombie_on_kill
        self._denied_on_signal = denied_on_signal
        self._nosuch_on_status = nosuch_on_status
        self.alive = True
        self.zombie = False
        self.events: list[str] = []

    def cmdline(self) -> list[str]:
        psutil = sys.modules["psutil"]
        if self._raises == "nosuch":
            raise psutil.NoSuchProcess()  # type: ignore[attr-defined]
        if self._raises == "zombie":
            raise psutil.ZombieProcess()  # type: ignore[attr-defined]
        if self._raises == "denied":
            raise psutil.AccessDenied()  # type: ignore[attr-defined]
        return list(self._cmdline)

    def create_time(self) -> float:
        return FAKE_CREATE_TIME

    def status(self) -> str:
        if self._nosuch_on_status:
            psutil = sys.modules["psutil"]
            raise psutil.NoSuchProcess()  # type: ignore[attr-defined]
        return "zombie" if self.zombie else "running"

    def terminate(self) -> None:
        self.events.append("terminate")
        psutil = sys.modules["psutil"]
        if self._denied_on_signal:
            raise psutil.AccessDenied()  # type: ignore[attr-defined]
        if self._vanishes_on_signal:
            raise psutil.NoSuchProcess()  # type: ignore[attr-defined]
        if self._zombie_on_term:
            self.zombie = True  # dead but unreaped: wait keeps timing out, status says zombie
        elif not self._survives_sigterm:
            self.alive = False

    def kill(self) -> None:
        self.events.append("kill")
        if self._zombie_on_kill:
            self.zombie = True
        elif not self._survives_sigkill:
            self.alive = False

    def wait(self, timeout: float | None = None) -> None:
        self.events.append("wait")
        if self.alive or self.zombie:
            psutil = sys.modules["psutil"]
            raise psutil.TimeoutExpired(timeout, self.pid)  # type: ignore[attr-defined]


def install_fake_psutil(monkeypatch: pytest.MonkeyPatch, procs: list[FakeProc]) -> types.ModuleType:
    """Install a fake ``psutil`` module whose ``process_iter()`` yields *procs*."""
    fake = types.ModuleType("psutil")

    class NoSuchProcess(Exception): ...  # noqa: N818 -- mirrors psutil's real exception name

    class AccessDenied(Exception): ...  # noqa: N818

    class ZombieProcess(Exception): ...  # noqa: N818

    class TimeoutExpired(Exception): ...  # noqa: N818

    fake.NoSuchProcess = NoSuchProcess  # type: ignore[attr-defined]
    fake.AccessDenied = AccessDenied  # type: ignore[attr-defined]
    fake.ZombieProcess = ZombieProcess  # type: ignore[attr-defined]
    fake.TimeoutExpired = TimeoutExpired  # type: ignore[attr-defined]
    fake.STATUS_ZOMBIE = "zombie"  # type: ignore[attr-defined]
    fake.process_iter = lambda: list(procs)  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "psutil", fake)
    return fake


class FakePopen:
    """A fake ``subprocess.Popen``: records the spawn call, optionally writes scripted log output,
    optionally exits immediately (a bind failure), never launches anything."""

    def __init__(self, cmd: list[str], **kwargs: Any) -> None:
        self.cmd = cmd
        self.kwargs = kwargs
        self.pid = 5555
        self.returncode: int | None = None
        self.killed = False

    def write_log(self, data: bytes) -> None:
        stdout: IO[bytes] = self.kwargs["stdout"]
        stdout.write(data)
        stdout.flush()

    def poll(self) -> int | None:
        return self.returncode

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9

    def wait(self, timeout: float | None = None) -> int | None:
        return self.returncode


class MustNotCall:
    """A ``resolve_bin`` thunk for paths that must stay locator-free (reuse/borrow/connect-only)."""

    def __call__(self) -> str:
        raise AssertionError("resolve_bin must not be called on this path")


class CountingResolveBin:
    """A ``resolve_bin`` thunk returning a fixed path and counting invocations."""

    def __init__(self, path: str = "/fake/bin/tansu") -> None:
        self.path = path
        self.calls = 0

    def __call__(self) -> str:
        self.calls += 1
        return self.path


def scripted_probe(*values: bool) -> Any:
    """A fake ``is_reachable``: yields *values* in order, repeating the last one; records calls."""

    calls: list[tuple[Any, float]] = []
    remaining = list(values)

    def probe(bootstrap: Any, *, timeout: float) -> bool:
        calls.append((bootstrap, timeout))
        return remaining.pop(0) if len(remaining) > 1 else remaining[0]

    probe.calls = calls  # type: ignore[attr-defined]
    return probe
