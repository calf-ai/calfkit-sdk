"""Connect-or-spawn supervisor for the ``ck dev`` broker (spec §5).

Normalizes the resolved bootstrap address to a :class:`Target` (spec §5.2): bare ``localhost``
becomes an explicit ``127.0.0.1`` (a bare hostname would make Tansu bind ``[::]`` while the probe
resolves elsewhere), a missing port becomes ``9092``, and a multi-element address gets a canonical
registry key and is borrow-or-error — never a spawn. Spawning is reserved for **loopback**
single addresses; ``0.0.0.0``, non-local IPs, and hostnames are connect-only.

:func:`ensure_broker` holds the registry lock across its whole ``probe → spawn → readiness →
record`` critical section (spec §5.3) — releasing before readiness would let a concurrent
invocation see the not-yet-ready broker as absent and double-spawn into a port-bind fight. The
binary locator is a **zero-arg thunk called only in the spawn branch**, so reuse/borrow/
connect-only paths never import ``calfkit_mesh`` or materialize the binary.
"""

from __future__ import annotations

import ipaddress
import os
import subprocess
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from subprocess import Popen

from calfkit.cli._dev_probe import is_reachable
from calfkit.cli._dev_state import BrokerRecord, Registry, is_ours, owned_process

DEFAULT_PORT = 9092
DEFAULT_TIMEOUT = 5.0
_READINESS_POLL_INTERVAL = 0.05
_LOG_TAIL_BYTES = 2000


class DevBrokerError(Exception):
    """A broker-ensure failure (missing binary, readiness timeout, connect-only address, …).

    The ``ck dev`` commands surface it and exit ``2``, for parity with config errors (spec §7.8).
    """


@dataclass(frozen=True)
class Target:
    """A normalized bootstrap target: what to probe, what key it registers under, whether a spawn
    is allowed there. ``listen_ip``/``port`` are set only for a single-address target — the one
    case a spawn can bind."""

    bootstrap: str
    servers: tuple[str, ...]
    registry_key: str
    is_loopback: bool
    is_single: bool
    listen_ip: str | None
    port: int | None

    @property
    def listener(self) -> str:
        """The one address a spawn binds, the readiness probe targets, and the worker connects to."""
        if self.listen_ip is None or self.port is None:
            raise ValueError("listener is only defined for a single-address target")
        return _format_address(self.listen_ip, self.port)


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
            registry_key=normalized[0],
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
        registry_key=",".join(sorted(normalized)),
        is_loopback=False,
        is_single=False,
        listen_ip=None,
        port=None,
    )


def ensure_broker(
    target: Target,
    *,
    resolve_bin: Callable[[], str],
    timeout: float = DEFAULT_TIMEOUT,
    registry: Registry | None = None,
) -> BrokerRecord:
    """Connect-or-spawn: return the broker serving *target*, spawning one only when the address is
    a single loopback address with nothing reachable there (spec §5.2).

    Reachable + our live record → the managed record. Reachable otherwise → a **borrowed** record
    (``spawned_by_calfkit=False``, never persisted). Unreachable loopback → resolve the binary via
    the *resolve_bin* thunk, spawn detached, wait for readiness, persist, return. Unreachable
    anywhere else → :class:`DevBrokerError` (connect-only — a down remote must never trigger a
    local bind).
    """
    registry = registry if registry is not None else Registry()
    with registry.lock():
        if is_reachable(list(target.servers), timeout=timeout):
            record = registry.get(target.registry_key)
            if record is not None and is_ours(record):
                return record
            if record is not None:
                # Something answers, but not the process we recorded (its live counterpart would
                # have passed is_ours) — the record is stale (spec §7.7); whatever answers is
                # borrowed, not managed.
                registry.remove(target.registry_key)
            return _borrowed(target)
        record = registry.get(target.registry_key)
        if record is not None:
            registry.remove(target.registry_key)  # dead process behind the record (spec §7.7)
        if not (target.is_single and target.is_loopback):
            raise DevBrokerError(
                f"no broker is reachable at {target.bootstrap!r} and it is not a single loopback "
                "address, so `ck dev` will not spawn one there (connect-only; spec §5.2). Start the "
                "broker at that address, or point --host at a loopback address to get a managed one."
            )
        binary = _resolve_binary(resolve_bin)
        return _spawn_and_wait(target, binary, timeout, registry)


def _borrowed(target: Target) -> BrokerRecord:
    """A record for a broker we merely connected to: reported, never persisted, never torn down."""
    return BrokerRecord(
        pid=0,
        bootstrap=target.bootstrap,
        listener=target.registry_key,
        binary_path="",
        started_at="",
        log_path="",
        spawned_by_calfkit=False,
    )


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


def _spawn_and_wait(target: Target, binary: str, timeout: float, registry: Registry) -> BrokerRecord:
    listener = target.listener
    log_path = registry.root / "logs" / f"tansu-{target.registry_key}.log"
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
    record = BrokerRecord(
        pid=proc.pid,
        bootstrap=target.bootstrap,
        listener=listener,
        binary_path=binary,
        started_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        log_path=str(log_path),
        spawned_by_calfkit=True,
    )
    registry.put(target.registry_key, record)
    return record


DEFAULT_GRACE = 5.0


def stop(target: Target, *, registry: Registry | None = None, grace: float = DEFAULT_GRACE) -> bool:
    """Stop the broker **calfkit spawned** for *target* and clear its record (spec §5.6).

    SIGTERM → *grace* → SIGKILL, and only for a ``spawned_by_calfkit`` record whose live process
    still passes the ownership check — a borrowed broker, a foreign process on a recycled pid, or
    no record at all is a no-op (``False``). The lock guards only the registry read; signalling
    and the grace wait run outside it (spec §5.3).
    """
    registry = registry if registry is not None else Registry()
    with registry.lock():
        record = registry.get(target.registry_key)
    if record is None or not record.spawned_by_calfkit:
        return False
    return _stop_record(target.registry_key, record, registry, grace)


def stop_all(*, registry: Registry | None = None, grace: float = DEFAULT_GRACE) -> list[str]:
    """Stop every calfkit-spawned broker; return the registry keys that were actually stopped."""
    registry = registry if registry is not None else Registry()
    with registry.lock():
        records = registry.read()
    return [key for key, record in sorted(records.items()) if record.spawned_by_calfkit and _stop_record(key, record, registry, grace)]


def _stop_record(key: str, record: BrokerRecord, registry: Registry, grace: float) -> bool:
    proc = owned_process(record)  # the ownership check + signal handle, resolved once (spec §5.4)
    if proc is None:
        registry.remove(key)  # dead or foreign — clean the stale record, NEVER signal
        return False
    import psutil  # type: ignore[import-untyped]

    proc.terminate()
    try:
        proc.wait(grace)
    except psutil.TimeoutExpired:
        proc.kill()
        proc.wait(grace)
    registry.remove(key)
    return True


@dataclass(frozen=True)
class ManagedBroker:
    """One registry record plus its liveness (does its process still pass the ownership check?)."""

    key: str
    record: BrokerRecord
    running: bool


@dataclass(frozen=True)
class MeshStatus:
    """What ``ck dev broker status`` reports: every managed record, and whether *anything* answers
    at the target address (a reachable address with no managed record is a broker calfkit does not
    manage — spec §4.2)."""

    target_key: str
    reachable: bool
    brokers: tuple[ManagedBroker, ...]


def status(target: Target, *, registry: Registry | None = None, timeout: float = DEFAULT_TIMEOUT) -> MeshStatus:
    """Report the managed broker(s) and probe the target address (probe outside the lock)."""
    registry = registry if registry is not None else Registry()
    with registry.lock():
        records = registry.read()
    reachable = is_reachable(list(target.servers), timeout=timeout)
    brokers = tuple(ManagedBroker(key, record, is_ours(record)) for key, record in sorted(records.items()))
    return MeshStatus(target_key=target.registry_key, reachable=reachable, brokers=brokers)


def restart(
    target: Target,
    *,
    resolve_bin: Callable[[], str],
    timeout: float = DEFAULT_TIMEOUT,
    registry: Registry | None = None,
    grace: float = DEFAULT_GRACE,
) -> BrokerRecord:
    """``stop`` then ``ensure_broker`` — the clean slate (the memory engine is ephemeral, spec §4.2).

    Only truly restarts a broker calfkit owns; on a borrowed broker ``stop`` is a no-op, so this
    just re-reuses the still-running one.
    """
    registry = registry if registry is not None else Registry()
    stop(target, registry=registry, grace=grace)
    return ensure_broker(target, resolve_bin=resolve_bin, timeout=timeout, registry=registry)
