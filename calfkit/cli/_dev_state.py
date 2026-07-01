"""On-disk state for the ``ck dev`` broker daemon (spec §5.4).

A registry at ``~/.calfkit/dev-mesh.json`` keyed by the normalized bootstrap address, guarded by a
**blocking advisory file lock** (``~/.calfkit/dev-mesh.lock``) and written **atomically** (temp file +
``os.replace``). Plus the ownership rule: is the live process at a record's ``pid`` still *our* Tansu?

Ownership is checked by **argv**, not the bare pid (the OS recycles pids): via ``psutil`` the process's
command line must start with our ``binary_path`` and contain our ``tcp://<listener>``. ``psutil`` ships
only in the ``[mesh]`` extra, so it is imported **lazily** inside :func:`is_ours` — a core ``ck`` install
never needs it. ``fcntl`` is POSIX; Windows locking is best-effort and not wired here.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


def _state_dir() -> Path:
    return Path.home() / ".calfkit"


@dataclass(frozen=True)
class BrokerRecord:
    """A managed dev broker we spawned. ``listener`` is the normalized ``listen_ip:port`` the spawn
    binds and the ownership check compares against; ``bootstrap`` is the raw resolved input (for display)."""

    pid: int
    bootstrap: str
    listener: str
    binary_path: str
    started_at: str
    log_path: str
    spawned_by_calfkit: bool = True


class Registry:
    """The ``~/.calfkit/dev-mesh.json`` registry, keyed by ``registry_key`` (a normalized address)."""

    def __init__(self, root: Path | None = None) -> None:
        self._root = root if root is not None else _state_dir()
        self._json = self._root / "dev-mesh.json"
        self._lock_path = self._root / "dev-mesh.lock"
        self._lock_depth = 0

    @property
    def root(self) -> Path:
        """The state directory this registry lives in (``~/.calfkit`` by default) — the supervisor
        derives sibling paths (the ``logs/`` dir) from it."""
        return self._root

    @contextmanager
    def lock(self) -> Iterator[None]:
        """Hold a blocking, exclusive advisory lock over the registry.

        Released by the OS if the holder dies, so a crashed process never deadlocks the file.
        Reentrant **per instance**: ``put``/``remove`` take the lock themselves, and the supervisor
        also calls them while already holding ``lock()`` across its probe→spawn→record critical
        section (spec §5.3) — a second ``flock`` on a fresh fd would self-deadlock, so a nested
        enter on the same instance just rides the outer hold.
        """
        import fcntl

        if self._lock_depth > 0:
            self._lock_depth += 1
            try:
                yield
            finally:
                self._lock_depth -= 1
            return

        self._root.mkdir(parents=True, exist_ok=True)
        fd = os.open(self._lock_path, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            self._lock_depth = 1
            try:
                yield
            finally:
                self._lock_depth = 0
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def _read_raw(self) -> dict[str, dict[str, Any]]:
        # The registry is advisory (the reachability probe is authoritative), so a missing or corrupt
        # file reads as empty rather than crashing `ck dev`.
        try:
            loaded: dict[str, dict[str, Any]] = json.loads(self._json.read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            return {}
        return loaded

    def _write_raw(self, data: dict[str, dict[str, Any]]) -> None:
        self._root.mkdir(parents=True, exist_ok=True)
        tmp = self._root / f".dev-mesh.{os.getpid()}.tmp"
        tmp.write_text(json.dumps(data, indent=2))
        os.replace(tmp, self._json)  # atomic within the same directory

    def read(self) -> dict[str, BrokerRecord]:
        """Return all records (not lock-guarded — a point-in-time snapshot)."""
        return {key: BrokerRecord(**raw) for key, raw in self._read_raw().items()}

    def get(self, key: str) -> BrokerRecord | None:
        raw = self._read_raw().get(key)
        return BrokerRecord(**raw) if raw is not None else None

    def put(self, key: str, record: BrokerRecord) -> None:
        with self.lock():
            data = self._read_raw()
            data[key] = asdict(record)
            self._write_raw(data)

    def remove(self, key: str) -> None:
        with self.lock():
            data = self._read_raw()
            if data.pop(key, None) is not None:
                self._write_raw(data)


def owned_process(record: BrokerRecord) -> Any | None:
    """Return the live ``psutil.Process`` at ``record.pid`` iff it is the Tansu **we** spawned
    (spec §5.4), else ``None``.

    Verified by argv rather than the bare pid: ``psutil.cmdline()`` (a list) must have our
    ``binary_path`` as ``argv[0]`` **and** contain ``tcp://<listener>`` as a substring of some argument
    (the listener is one ``=``-joined arg, e.g. ``--kafka-listener-url=tcp://127.0.0.1:9092``). Anything
    else — a dead pid, a recycled pid holding a foreign process, a zombie, an inaccessible process — is
    *not ours*, so the caller cleans the stale record and never signals it. Returning the verified
    ``Process`` handle (typed ``Any`` — psutil ships no type stubs) lets the supervisor signal the
    *same* process it just verified, rather than re-resolving the pid.
    """
    import psutil  # type: ignore[import-untyped]

    try:
        proc = psutil.Process(record.pid)
        cmd = proc.cmdline()
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
        return None
    if bool(cmd) and cmd[0] == record.binary_path and any(f"tcp://{record.listener}" in arg for arg in cmd):
        return proc
    return None


def is_ours(record: BrokerRecord) -> bool:
    """Return ``True`` iff the live process at ``record.pid`` is the Tansu **we** spawned (spec §5.4)."""
    return owned_process(record) is not None
