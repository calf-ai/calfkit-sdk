"""Connect-or-spawn supervisor for the ``ck dev`` broker (spec §5).

Normalizes the resolved bootstrap address to a :class:`Target` (spec §5.2): bare ``localhost``
becomes an explicit ``127.0.0.1`` (a bare hostname would make Tansu bind ``[::]`` while the probe
resolves elsewhere), a missing port becomes ``9092``, and a multi-element address gets a canonical
key and is borrow-or-error — never a spawn. Spawning is reserved for **loopback** single
addresses; ``0.0.0.0``, non-local IPs, and hostnames are connect-only.

Ownership is a **deterministic process-table scan — no saved state** (spec §5.4): a *dev broker*
is any live process whose argv carries both the memory-engine anchor
(``--storage-engine=memory://…``) and the target listener (``--kafka-listener-url=tcp://<addr>``).
``stop`` is "the memory-engine tansu stopper" for the target address — it signals scan hits and
nothing else, whoever started them; a durable tansu, a Kafka/Redpanda, or any non-tansu process
can never match and is never signalled.

:func:`ensure_broker` holds a spawn-race file lock across its whole ``probe → spawn → readiness``
critical section (spec §5.3) — releasing before readiness would let a concurrent invocation see
the not-yet-ready broker as absent and double-spawn into a port-bind fight. The binary locator is
a **zero-arg thunk called only in the spawn branch**, so reuse/borrow/connect-only paths never
import ``calfkit_mesh`` or materialize the binary. ``psutil`` (the scan) ships only in the
``[mesh]`` extra and is imported lazily.
"""

from __future__ import annotations

import ipaddress
import os
import subprocess
import time
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from subprocess import Popen
from typing import Any

from calfkit.cli._dev_probe import is_reachable

DEFAULT_PORT = 9092
DEFAULT_TIMEOUT = 5.0
DEFAULT_GRACE = 5.0
_READINESS_POLL_INTERVAL = 0.05
_LOG_TAIL_BYTES = 2000

# The argv anchors that identify a dev broker (spec §5.4): an ephemeral memory-engine Tansu,
# bound to a specific listener. Anything not matching BOTH is never managed, never signalled.
_MEMORY_ENGINE_PREFIX = "--storage-engine=memory://"
_LISTENER_PREFIX = "--kafka-listener-url=tcp://"

_MESH_EXTRA_HINT = (
    "the dev broker supervisor needs the `calfkit[mesh]` extra (psutil). Install it with `pip install 'calfkit[mesh]'` (or `uv add 'calfkit[mesh]'`)."
)


class DevBrokerError(Exception):
    """A broker-ensure/management failure (missing binary, readiness timeout, connect-only
    address, missing ``[mesh]`` extra, …).

    The ``ck dev`` commands surface it and exit ``2``, for parity with config errors (spec §7.8).
    """


@dataclass(frozen=True)
class Target:
    """A normalized bootstrap target: what to probe, its canonical key, whether a spawn is allowed
    there. ``listen_ip``/``port`` are set only for a single-address target — the one case a spawn
    can bind."""

    bootstrap: str
    servers: tuple[str, ...]
    key: str
    is_loopback: bool
    is_single: bool
    listen_ip: str | None
    port: int | None

    @property
    def listener(self) -> str:
        """The one address a spawn binds, the readiness probe targets, the ownership scan matches
        against, and the worker connects to."""
        if self.listen_ip is None or self.port is None:
            raise ValueError("listener is only defined for a single-address target")
        return _format_address(self.listen_ip, self.port)


@dataclass(frozen=True)
class BrokerInfo:
    """What ``ensure_broker``/``status`` report about one broker: where it is, whether it is a
    managed dev broker (a §5.4 scan hit or a fresh spawn), and — when managed — its pid, start
    time, and spawn log."""

    listener: str
    pid: int | None
    managed: bool
    started_at: str | None = None
    log_path: str | None = None


@dataclass(frozen=True)
class MeshStatus:
    """What ``ck dev broker status`` reports: every running dev broker found by the scan, and
    whether *anything* answers at the target address (reachable with no scan hit = a broker
    calfkit does not manage — spec §4.2)."""

    target_key: str
    reachable: bool
    brokers: tuple[BrokerInfo, ...]


def _parse_element(element: str) -> tuple[str, int]:
    """Split one bootstrap element into ``(host, port)``, defaulting the port and mapping
    ``localhost`` to an explicit ``127.0.0.1``."""
    element = element.strip()
    if element.startswith("["):  # bracketed IPv6, e.g. [::1]:9092
        host, _, rest = element[1:].partition("]")
        port = _parse_port(rest[1:], element) if rest.startswith(":") else DEFAULT_PORT
    else:
        try:
            ipaddress.ip_address(element)  # a whole-string IP literal (IPv4 or IPv6) has no port
        except ValueError:
            host, sep, suffix = element.rpartition(":")
            if sep:
                port = _parse_port(suffix, element)
            else:
                host, port = element, DEFAULT_PORT
        else:
            host, port = element, DEFAULT_PORT
    if not host:
        raise ValueError(f"invalid bootstrap address: {element!r}")
    if host == "localhost":
        host = "127.0.0.1"
    return host, port


def _parse_port(raw: str, element: str) -> int:
    if not raw.isdigit():
        raise ValueError(f"invalid port in bootstrap address {element!r}")
    return int(raw)


def _format_address(host: str, port: int) -> str:
    return f"[{host}]:{port}" if ":" in host else f"{host}:{port}"


def _is_loopback(host: str) -> bool:
    """Loopback per spec §5.2: ``127.0.0.0/8`` or ``::1``. A hostname (not an IP literal) or a
    wildcard/non-local IP is NOT loopback — those are connect-only, never spawn targets."""
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def normalize(servers: Sequence[str]) -> Target:
    """Normalize the resolved bootstrap list (from ``resolve_mesh_url``) to a :class:`Target`.

    Each element is comma-flattened first: ``resolve_mesh_url`` does not split a comma-joined
    ``$CALFKIT_MESH_URL``, and without flattening ``"a:1,b:2"`` would misparse as host ``a:1,b``.
    """
    servers = [part.strip() for element in servers for part in element.split(",") if part.strip()]
    if not servers:
        raise ValueError("no bootstrap address to normalize")
    parsed = [_parse_element(element) for element in servers]
    normalized = tuple(_format_address(host, port) for host, port in parsed)
    if len(parsed) == 1:
        host, port = parsed[0]
        return Target(
            bootstrap=",".join(servers),
            servers=normalized,
            key=normalized[0],
            is_loopback=_is_loopback(host),
            is_single=True,
            listen_ip=host,
            port=port,
        )
    return Target(
        bootstrap=",".join(servers),
        # The raw elements in their given order: a multi-address target is borrow-or-error, and the
        # caller forwards the user's list unchanged (spec §4.3) — only the KEY is canonicalized.
        servers=tuple(servers),
        key=",".join(sorted(normalized)),
        is_loopback=False,
        is_single=False,
        listen_ip=None,
        port=None,
    )


# --- the ownership scan (spec §5.4) -----------------------------------------------------------------


@dataclass(frozen=True)
class _ScanHit:
    """A running dev broker found in the process table, with its live psutil handle (typed ``Any``
    — psutil ships no type stubs) so callers signal the same process the scan just verified."""

    proc: Any
    listener: str
    pid: int
    started_at: str


def _dev_broker_listener(cmd: list[str]) -> str | None:
    """Return the listener a dev broker's argv binds, or ``None`` if *cmd* is not a dev broker."""
    if not any(arg.startswith(_MEMORY_ENGINE_PREFIX) for arg in cmd):
        return None
    for arg in cmd:
        if arg.startswith(_LISTENER_PREFIX):
            return arg[len(_LISTENER_PREFIX) :]
    return None


def _scan_dev_brokers() -> list[_ScanHit]:
    """Scan the live process table for dev brokers (spec §5.4). Raises :class:`DevBrokerError`
    with an actionable message when ``psutil`` (the ``[mesh]`` extra) is not installed."""
    try:
        import psutil  # type: ignore[import-untyped]
    except ModuleNotFoundError as exc:
        raise DevBrokerError(_MESH_EXTRA_HINT) from exc

    hits: list[_ScanHit] = []
    for proc in psutil.process_iter():
        try:
            listener = _dev_broker_listener(proc.cmdline())
            if listener is None:
                continue
            started = datetime.fromtimestamp(proc.create_time(), tz=timezone.utc)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue  # vanished or unreadable mid-scan — by definition not a broker we can manage
        hits.append(_ScanHit(proc=proc, listener=listener, pid=proc.pid, started_at=started.isoformat(timespec="seconds")))
    return hits


def _find_dev_broker(listener: str) -> _ScanHit | None:
    return next((hit for hit in _scan_dev_brokers() if hit.listener == listener), None)


# --- the spawn-race lock (spec §5.3) ----------------------------------------------------------------


def _calfkit_dir() -> Path:
    return Path.home() / ".calfkit"


@contextmanager
def _spawn_lock() -> Iterator[None]:
    """Hold a blocking, exclusive advisory lock over the spawn critical section.

    Exists ONLY for the double-spawn race (spec §5.3); ``stop``/``status`` never take it. Released
    by the OS if the holder dies, so a crashed process never deadlocks the file.
    """
    import fcntl

    root = _calfkit_dir()
    root.mkdir(parents=True, exist_ok=True)
    fd = os.open(root / "dev-mesh.lock", os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


# --- connect-or-spawn (spec §5.2, §5.5) -------------------------------------------------------------


def ensure_broker(
    target: Target,
    *,
    resolve_bin: Callable[[], str],
    timeout: float = DEFAULT_TIMEOUT,
) -> BrokerInfo:
    """Connect-or-spawn: return the broker serving *target*, spawning one only when the address is
    a single loopback address with nothing reachable there (spec §5.2).

    Reachable → reuse: managed if the §5.4 scan finds a dev broker bound to the listener, else a
    plain borrow. Unreachable loopback → resolve the binary via the *resolve_bin* thunk, spawn
    detached, wait for readiness, return managed. Unreachable anywhere else →
    :class:`DevBrokerError` (connect-only — a down remote must never trigger a local bind).
    """
    with _spawn_lock():
        if is_reachable(list(target.servers), timeout=timeout):
            if target.is_single and target.is_loopback:
                try:
                    hit = _find_dev_broker(target.listener)
                except DevBrokerError:
                    # Core install (no [mesh]): the scan cannot run, so the managed-vs-reused
                    # CLASSIFICATION degrades to "reused" — honest, since without the extra
                    # nothing can be managed anyway. Display-only; no behavior depends on it.
                    hit = None
                if hit is not None:
                    return BrokerInfo(listener=hit.listener, pid=hit.pid, managed=True, started_at=hit.started_at)
            return BrokerInfo(listener=target.key, pid=None, managed=False)
        if not (target.is_single and target.is_loopback):
            raise DevBrokerError(
                f"no broker is reachable at {target.bootstrap!r} and it is not a single loopback "
                "address, so `ck dev` will not spawn one there (connect-only; spec §5.2). Start the "
                "broker at that address, or point --host at a loopback address to get a managed one."
            )
        binary = _resolve_binary(resolve_bin)
        return _spawn_and_wait(target, binary, timeout)


def _resolve_binary(resolve_bin: Callable[[], str]) -> str:
    try:
        return resolve_bin()
    except ModuleNotFoundError as exc:
        raise DevBrokerError(
            "the bundled dev broker is not installed. Install it with `pip install 'calfkit[mesh]'` "
            "(or `uv add 'calfkit[mesh]'`), or point CALF_TANSU_BIN at a tansu binary."
        ) from exc


def _detach_kwargs() -> dict[str, object]:
    """Detach the spawn from our session so Ctrl-C / terminal close never reaches it (spec §5.8)."""
    if os.name == "nt":
        # Moot today — Tansu is Unix-only (spec §7.6) — but the branch stays cheap and correct.
        return {"creationflags": getattr(subprocess, "DETACHED_PROCESS", 0)}
    return {"start_new_session": True}


def _log_tail(log_path: Path) -> str:
    try:
        return log_path.read_text(errors="replace")[-_LOG_TAIL_BYTES:]
    except OSError:
        return ""


def _spawn_and_wait(target: Target, binary: str, timeout: float) -> BrokerInfo:
    listener = target.listener
    log_path = _calfkit_dir() / "logs" / f"tansu-{target.key}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        binary,
        "broker",
        "--storage-engine=memory://tansu/",
        f"--kafka-listener-url=tcp://{listener}",
        f"--kafka-advertised-listener-url=tcp://{listener}",
    ]
    # Overwritten each spawn — bounded logs, one daemon per address (spec §5.2). A real file fd,
    # never PIPE: an unread pipe would deadlock Tansu once its buffer fills.
    with log_path.open("wb") as log_file:
        try:
            proc = Popen(cmd, stdin=subprocess.DEVNULL, stdout=log_file, stderr=log_file, **_detach_kwargs())  # type: ignore[call-overload]
        except OSError as exc:
            # Wrong-arch / corrupt binary fails at exec time (spec §7.3) — distinct from a timeout.
            raise DevBrokerError(f"failed to launch the dev broker binary {binary!r}: {exc}") from exc
    deadline = time.monotonic() + timeout
    while True:
        if proc.poll() is not None:
            raise DevBrokerError(
                f"the dev broker exited during startup (exit code {proc.returncode}); log tail from {log_path}:\n{_log_tail(log_path)}"
            )
        if is_reachable(listener, timeout=min(1.0, timeout)):
            break
        if time.monotonic() >= deadline:
            proc.kill()
            raise DevBrokerError(f"the dev broker did not become ready within {timeout:.1f}s; log tail from {log_path}:\n{_log_tail(log_path)}")
        time.sleep(_READINESS_POLL_INTERVAL)
    return BrokerInfo(
        listener=listener,
        pid=proc.pid,
        managed=True,
        started_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        log_path=str(log_path),
    )


# --- stop / status / restart (spec §4.2, §5.6) ------------------------------------------------------


def stop(target: Target, *, grace: float = DEFAULT_GRACE) -> bool:
    """*The memory-engine tansu stopper*: SIGTERM → *grace* → SIGKILL the dev broker bound to the
    target listener — the §5.4 scan hit, whoever started it — and nothing else (spec §5.6).

    ``False`` (a no-op) when nothing matches: a borrowed broker, a durable tansu, the developer's
    own Kafka, or a multi-address target (never a spawn target) are never signalled.
    """
    if not target.is_single:
        return False
    hit = _find_dev_broker(target.listener)
    if hit is None:
        return False
    return _signal(hit, grace)


def stop_all(*, grace: float = DEFAULT_GRACE) -> list[str]:
    """Stop every running local dev broker; return the listeners that were actually stopped."""
    return [hit.listener for hit in _scan_dev_brokers() if _signal(hit, grace)]


def _signal(hit: _ScanHit, grace: float) -> bool:
    import psutil  # already importable: the scan produced the hit (mypy flags the module once, on the scan's import)

    try:
        hit.proc.terminate()
        try:
            hit.proc.wait(grace)
        except psutil.TimeoutExpired:
            hit.proc.kill()
            hit.proc.wait(grace)
    except psutil.NoSuchProcess:
        pass  # exited between the scan and the signal — the goal state (stopped) is reached
    return True


def status(target: Target, *, timeout: float = DEFAULT_TIMEOUT) -> MeshStatus:
    """Report every running dev broker (the §5.4 scan) and probe the target address."""
    brokers = tuple(BrokerInfo(listener=hit.listener, pid=hit.pid, managed=True, started_at=hit.started_at) for hit in _scan_dev_brokers())
    return MeshStatus(
        target_key=target.key,
        reachable=is_reachable(list(target.servers), timeout=timeout),
        brokers=brokers,
    )


def restart(
    target: Target,
    *,
    resolve_bin: Callable[[], str],
    timeout: float = DEFAULT_TIMEOUT,
    grace: float = DEFAULT_GRACE,
) -> BrokerInfo:
    """``stop`` then ``ensure_broker`` — the clean slate (the memory engine is ephemeral, spec §4.2).

    Only truly restarts a dev broker the scan finds; on a borrowed broker ``stop`` is a no-op, so
    this just re-reuses the still-running one.
    """
    stop(target, grace=grace)
    return ensure_broker(target, resolve_bin=resolve_bin, timeout=timeout)
