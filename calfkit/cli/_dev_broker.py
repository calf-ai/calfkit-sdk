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
import sys
import time
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager, suppress
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


class MeshExtraMissingError(DevBrokerError):
    """The ownership scan needs ``psutil`` (the ``[mesh]`` extra), which is not installed.

    A dedicated type so ``ensure_broker`` can degrade its managed-vs-reused *classification*
    on exactly this failure — and nothing else — while ``stop``/``status`` surface it.
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
    """What ``ck dev mesh status`` reports: every running dev broker found by the scan, and
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
    if not (raw.isascii() and raw.isdigit()) or not 0 < int(raw) <= 65535:
        raise ValueError(f"invalid port in bootstrap address {element!r} (expected 1-65535)")
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
    # Dedup post-normalization: "localhost:9092,127.0.0.1:9092" is semantically ONE loopback
    # address and must stay spawn-eligible, not fall into the borrow-or-error multi branch.
    parsed = list(dict.fromkeys(_parse_element(element) for element in servers))
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
    """Return the **normalized** listener a dev broker's argv binds, or ``None`` if *cmd* is not
    a dev broker.

    Both clap argv forms match (``--flag=value`` and ``--flag value``), and the extracted address
    is normalized through the same parser as the target (a hand-started
    ``--kafka-listener-url tcp://localhost:9092`` must compare equal to the normalized target
    ``127.0.0.1:9092``). An address that does not parse is not a broker we can manage.
    """
    if not _flag_value(cmd, "--storage-engine", "").startswith("memory://"):
        return None
    listener_url = _flag_value(cmd, "--kafka-listener-url", "")
    if not listener_url.startswith("tcp://"):
        return None
    try:
        host, port = _parse_element(listener_url[len("tcp://") :])
    except ValueError:
        return None
    return _format_address(host, port)


def _flag_value(cmd: list[str], flag: str, default: str) -> str:
    """The value of *flag* in argv, accepting both ``--flag=value`` and ``--flag value``."""
    prefix = f"{flag}="
    for index, arg in enumerate(cmd):
        if arg.startswith(prefix):
            return arg[len(prefix) :]
        if arg == flag and index + 1 < len(cmd):
            return cmd[index + 1]
    return default


def _scan_dev_brokers() -> list[_ScanHit]:
    """Scan the live process table for dev brokers (spec §5.4). Raises :class:`DevBrokerError`
    with an actionable message when ``psutil`` (the ``[mesh]`` extra) is not installed."""
    try:
        import psutil  # type: ignore[import-untyped]
    except ModuleNotFoundError as exc:
        raise MeshExtraMissingError(_MESH_EXTRA_HINT) from exc

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
def _spawn_lock(
    filename: str = "dev-mesh.lock",
    waiting_message: str = "waiting for the dev broker to start…",
) -> Iterator[None]:
    """Hold a blocking, exclusive advisory lock over a spawn critical section.

    Exists ONLY for the double-spawn race (spec §5.3); ``stop``/``status`` never take it. Released
    by the OS if the holder dies, so a crashed process never deadlocks the file. A contended lock
    announces itself (*waiting_message*) before blocking. The defaults are the broker's; the
    agent-daemon layer reuses the pattern verbatim with its own lock file (dev-agent-lifecycle
    spec §5.1).
    """
    import fcntl

    root = _calfkit_dir()
    try:
        root.mkdir(parents=True, exist_ok=True)
        fd = os.open(root / filename, os.O_CREAT | os.O_RDWR, 0o644)
    except OSError as exc:
        raise DevBrokerError(f"cannot access the calfkit state dir {root}: {exc}") from exc
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(waiting_message, file=sys.stderr)
            fcntl.flock(fd, fcntl.LOCK_EX)
        except OSError as exc:  # e.g. ENOLCK on an NFS home — exit-2 material, not a crash
            raise DevBrokerError(f"cannot lock the calfkit state dir {root}: {exc}") from exc
        yield
    finally:
        with suppress(OSError):  # belt-and-braces: closing the fd releases the flock anyway
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

    Reachable → reuse: managed if the §5.4 scan finds a dev broker bound to the listener (the
    scan runs only for single loopback targets — a remote address can never host a local dev
    broker, and skipping it keeps the pure-client path free of the ``[mesh]`` deps), else a plain
    borrow. Unreachable loopback → resolve the binary via the *resolve_bin* thunk, spawn
    detached, wait for readiness, return managed. Unreachable anywhere else →
    :class:`DevBrokerError` (connect-only — a down remote must never trigger a local bind).
    """
    with _spawn_lock():
        if is_reachable(list(target.servers), timeout=timeout):
            if target.is_single and target.is_loopback:
                try:
                    hit = _find_dev_broker(target.listener)
                except MeshExtraMissingError:
                    # Core install (no [mesh]): the scan cannot run, so the managed-vs-reused
                    # CLASSIFICATION degrades to "reused" — honest, since without the extra
                    # nothing can be managed anyway. Display-only; no behavior depends on it.
                    hit = None
                if hit is not None:
                    return BrokerInfo(listener=hit.listener, pid=hit.pid, managed=True, started_at=hit.started_at)
            # Borrowed: `listener` carries the display address — for a multi-address borrow
            # that is the canonical key, not any single listener.
            return BrokerInfo(listener=target.key, pid=None, managed=False)
        if not (target.is_single and target.is_loopback):
            raise _connect_only_error(target)
        binary = _resolve_binary(resolve_bin)
        return _spawn_and_wait(target, binary, timeout)


def _resolve_binary(resolve_bin: Callable[[], str]) -> str:
    try:
        return resolve_bin()
    except ImportError as exc:
        # ModuleNotFoundError = the [mesh] extra is absent; a bare ImportError = version skew
        # (a calfkit_mesh without resolve_broker_bin). Same remedy either way.
        raise DevBrokerError(
            "the bundled dev broker is not installed. Install it with `pip install 'calfkit[mesh]'` "
            "(or `uv add 'calfkit[mesh]'`), or point CALF_TANSU_BIN at a tansu binary."
        ) from exc
    except RuntimeError as exc:
        # The locator's own failure (calfkit_mesh.TansuBinaryNotFound is a RuntimeError): no
        # bundled binary for this platform, nothing on PATH, … — exit-2 material, not a crash.
        raise DevBrokerError(str(exc)) from exc


def _detach_kwargs() -> dict[str, object]:
    """Detach the spawn from our session so Ctrl-C / terminal close never reaches it (spec §5.8)."""
    if os.name == "nt":
        # Moot today — Tansu is Unix-only (spec §7.6) — but the branch stays cheap and correct.
        return {"creationflags": getattr(subprocess, "DETACHED_PROCESS", 0)}
    return {"start_new_session": True}


def _log_tail(log_path: Path) -> str:
    try:
        return log_path.read_text(errors="replace")[-_LOG_TAIL_BYTES:]
    except OSError as exc:
        return f"(could not read the log: {exc})"


def _broker_cmd(binary: str, listener: str) -> list[str]:
    """The dev broker argv — the single source of truth for both the detached and the foreground
    spawn, so the §5.4 ownership scan recognizes either by the same memory-engine + listener
    anchors."""
    return [
        binary,
        "broker",
        "--storage-engine=memory://tansu/",
        f"--kafka-listener-url=tcp://{listener}",
        f"--kafka-advertised-listener-url=tcp://{listener}",
    ]


def _connect_only_error(target: Target) -> DevBrokerError:
    """Spec §5.2's refusal to bind anywhere but a single loopback address — shared by the detached
    :func:`ensure_broker` and the foreground :func:`spawn_foreground`."""
    return DevBrokerError(
        f"no broker is reachable at {target.bootstrap!r} and it is not a single loopback "
        "address, so `ck dev` will not spawn one there (connect-only; spec §5.2). Start the "
        "broker at that address, or point --host at a loopback address to get a managed one."
    )


def _startup_detail(log_path: Path | None) -> str:
    """The tail appended to a startup-failure message: the spawn log's last bytes for a detached
    broker (its output went to a file), or a pointer to the terminal for a foreground broker (its
    output already streamed there)."""
    if log_path is None:
        return "; see its output above."
    return f"; log tail from {log_path}:\n{_log_tail(log_path)}"


def _await_ready(proc: Any, listener: str, timeout: float, *, log_path: Path | None) -> None:
    """Block until *proc* answers on *listener*, or raise (spec §5.2): a startup exit fails fast, a
    readiness timeout kills the spawn first. Shared by the detached and foreground spawns; *log_path*
    selects where a failure points the reader — a log file, or the terminal (``None``)."""
    deadline = time.monotonic() + timeout
    while True:
        if proc.poll() is not None:
            code = proc.returncode
            if code is not None and code < 0:
                cause = f"killed by a signal ({code}) — a concurrent `ck dev mesh stop/restart`?"
            else:
                cause = f"exit code {code}"
            raise DevBrokerError(f"the dev broker exited during startup ({cause})" + _startup_detail(log_path))
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            proc.kill()
            try:
                proc.wait(timeout=DEFAULT_GRACE)  # reap our own child — no zombie for long-lived callers
            except subprocess.TimeoutExpired:
                pass  # kill was delivered; the CLI exits next and init reaps
            raise DevBrokerError(f"the dev broker did not become ready within {timeout:.1f}s" + _startup_detail(log_path))
        # Per-attempt probe budget: capped at 1s so proc.poll() keeps getting re-checked, and
        # bounded by the remaining deadline so the total wait never overshoots `timeout`.
        if is_reachable(listener, timeout=min(1.0, max(0.1, remaining))):
            break
        time.sleep(_READINESS_POLL_INTERVAL)


def _spawn_and_wait(target: Target, binary: str, timeout: float) -> BrokerInfo:
    listener = target.listener
    log_path = _calfkit_dir() / "logs" / f"tansu-{target.key}.log"
    cmd = _broker_cmd(binary, listener)
    # Overwritten each spawn — bounded logs, one daemon per address (spec §5.2). A real file fd,
    # never PIPE: an unread pipe would deadlock Tansu once its buffer fills.
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_file = log_path.open("wb")
    except OSError as exc:
        raise DevBrokerError(f"cannot write the dev broker log at {log_path}: {exc}") from exc
    with log_file:
        try:
            proc = Popen(cmd, stdin=subprocess.DEVNULL, stdout=log_file, stderr=log_file, **_detach_kwargs())  # type: ignore[call-overload]
        except OSError as exc:
            # Wrong-arch / corrupt binary fails at exec time (spec §7.3) — distinct from a timeout.
            raise DevBrokerError(f"failed to launch the dev broker binary {binary!r}: {exc}") from exc
    _await_ready(proc, listener, timeout, log_path=log_path)
    return BrokerInfo(
        listener=listener,
        pid=proc.pid,
        managed=True,
        started_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        log_path=str(log_path),
    )


def spawn_foreground(target: Target, *, resolve_bin: Callable[[], str], timeout: float = DEFAULT_TIMEOUT) -> Popen[bytes]:
    """Spawn an **attached** dev broker and return the live process once it is ready — the foreground
    counterpart of :func:`ensure_broker`'s detached spawn (spec §5.2).

    The process stays in the caller's session (no ``start_new_session``, so Ctrl-C reaches Tansu
    directly — the deliberate inverse of §5.8's detach) and inherits the terminal (no log redirect;
    its output streams live). The caller blocks on the returned process. Raises
    :class:`DevBrokerError` when a broker is already reachable (foreground cannot attach to an
    already-running broker — stop it, or use ``-d``), when the address is not a single loopback
    (connect-only), or when the spawn fails or never becomes ready.
    """
    with _spawn_lock():
        if is_reachable(list(target.servers), timeout=timeout):
            raise DevBrokerError(
                f"a broker is already running at {target.key} — stop it with `ck dev mesh stop`, "
                "or start a detached daemon with `ck dev mesh start -d`."
            )
        if not (target.is_single and target.is_loopback):
            raise _connect_only_error(target)
        binary = _resolve_binary(resolve_bin)
        listener = target.listener
        try:
            # Attached: inherit stdout/stderr (the terminal) and stay in our session so Ctrl-C
            # reaches Tansu directly. stdin is DEVNULL — Tansu never reads it.
            proc: Popen[bytes] = Popen(_broker_cmd(binary, listener), stdin=subprocess.DEVNULL)
        except OSError as exc:
            # Wrong-arch / corrupt binary fails at exec time (spec §7.3) — distinct from a timeout.
            raise DevBrokerError(f"failed to launch the dev broker binary {binary!r}: {exc}") from exc
        _await_ready(proc, listener, timeout, log_path=None)
        return proc


# --- stop / status / restart (spec §4.2, §5.6) ------------------------------------------------------


def stop(target: Target, *, grace: float = DEFAULT_GRACE) -> bool:
    """*The memory-engine tansu stopper*: SIGTERM → *grace* → SIGKILL the dev broker bound to the
    target listener — the §5.4 scan hit, whoever started it — and nothing else (spec §5.6).

    ``False`` (a no-op) when nothing matches: a borrowed broker, a durable tansu, the developer's
    own Kafka, or a multi-address target (never a spawn target) are never signalled. A matching
    broker that cannot be signalled (owned by another user) raises :class:`DevBrokerError`.
    """
    if not target.is_single:
        return False
    hit = _find_dev_broker(target.listener)
    if hit is None:
        return False
    if not _signal(hit.proc, pid=hit.pid, label=f"the dev broker at {hit.listener} (pid {hit.pid})", grace=grace):
        raise DevBrokerError(f"cannot stop the dev broker at {hit.listener} (pid {hit.pid}) — it is owned by another user.")
    return True


def stop_all(*, grace: float = DEFAULT_GRACE) -> list[str]:
    """Stop every running local dev broker; return the listeners that were actually stopped.

    A broker that cannot be stopped — owned by another user, or surviving SIGKILL — is skipped
    with a warning (it is absent from the returned list) so one bad process never aborts the
    sweep.
    """
    stopped: list[str] = []
    for hit in _scan_dev_brokers():
        try:
            if _signal(hit.proc, pid=hit.pid, label=f"the dev broker at {hit.listener} (pid {hit.pid})", grace=grace):
                stopped.append(hit.listener)
        except DevBrokerError as exc:
            print(f"warning: {exc}", file=sys.stderr)
    return stopped


def _killpg(pgid: int, sig: int) -> None:
    """Signal a whole process group — the agent-daemon ``-d`` spawn is a session leader, so its
    pid is the pgid and one signal reaps supervisor + worker + multiprocessing helpers together
    (dev-agent-lifecycle spec §3.4). A call-time ``os`` attribute lookup (``killpg`` does not
    exist on Windows, and module import must stay platform-safe) and the test seam — tests
    replace this, never deliver a live group signal."""
    os.killpg(pgid, sig)


def _signal(proc: Any, *, pid: int, label: str, grace: float, group: bool = False) -> bool:
    """SIGTERM → *grace* → SIGKILL one scanned process — or, with ``group=True``, its whole
    process group (the agent-daemon path: the leader's pid IS the pgid). ``True`` = stopped,
    ``False`` = access denied; *label* names the process in the survived-SIGKILL error.

    A process whose spawning parent is still alive dies into an **unreaped zombie** (the parent
    reaps only at its own exit), and psutil's non-child ``wait`` never returns for a zombie — so
    a post-wait timeout with ``status() == zombie`` IS the stopped state, not a failure.
    """
    import signal as signal_module

    import psutil  # already importable: the scan produced the process handle (mypy flags the module once, on the scan's import)

    def _is_gone() -> bool:
        try:
            zombie: bool = proc.status() == psutil.STATUS_ZOMBIE
        except psutil.NoSuchProcess:
            return True
        return zombie

    def _wait_or_gone() -> bool:
        # psutil's non-child wait polls pid_exists, which a zombie passes — it would eat the
        # whole grace for an already-dead process. Wait in short slices, checking the zombie
        # status between them, so the common case (stop while the spawner is alive) is instant.
        deadline = time.monotonic() + grace
        while True:
            remaining = deadline - time.monotonic()
            try:
                proc.wait(min(0.1, max(0.01, remaining)))
                return True
            except psutil.TimeoutExpired:
                if _is_gone():
                    return True
                if remaining <= 0.1:
                    return False

    group_signal_delivered = False

    def _send(sig: int) -> None:
        # The group path signals via killpg (ProcessLookupError/PermissionError are the OS-level
        # twins of psutil's NoSuchProcess/AccessDenied, handled below); the single path keeps
        # psutil's own calls so the scan-verified handle is what gets signalled. Before a raw
        # killpg, the identity-checked handle must still be running (live or unreaped-zombie —
        # either way the pgid cannot have been recycled): a daemon that exited between scan and
        # signal may have had its pid reused by an innocent process GROUP (review round 1).
        nonlocal group_signal_delivered
        if group:
            if not proc.is_running():
                raise psutil.NoSuchProcess(pid)
            _killpg(pid, sig)
            group_signal_delivered = True
        elif sig == signal_module.SIGTERM:
            proc.terminate()
        else:
            proc.kill()

    def _final_group_sweep() -> None:
        # Leader death says nothing about the rest of the group: a SIGTERM-surviving descendant
        # (a node's own subprocess) would otherwise outlive the 'stopped' report — markerless,
        # so unmanageable forever (review round 1). One unconditional SIGKILL sweeps the
        # now-headless tree; an already-empty or zombie-only group absorbs it as a no-op.
        with suppress(ProcessLookupError, PermissionError):
            _killpg(pid, signal_module.SIGKILL)

    try:
        _send(signal_module.SIGTERM)
        if _wait_or_gone():
            if group:
                _final_group_sweep()
            return True
        _send(signal_module.SIGKILL)  # group mode: this already swept the whole group
        if not _wait_or_gone():
            raise DevBrokerError(f"{label} survived SIGKILL for {grace:.1f}s.")
    except (psutil.NoSuchProcess, ProcessLookupError):
        # Exited between the scan and the signal — the goal state (stopped) is reached. Group
        # mode, round 2: if a group signal WAS already delivered this call (the leader died
        # mid-ladder), descendants may remain and any survivor keeps the pgid reserved — sweep.
        # If nothing was ever delivered (the identity gate refused the first send), the pid may
        # belong to a recycled, innocent group: never sweep.
        if group and group_signal_delivered:
            _final_group_sweep()
    except (psutil.AccessDenied, PermissionError):
        return False  # someone else's process — never signalled further; callers decide how to report
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
    this just re-reuses the still-running one — including on a core install, where the scan
    cannot run at all (spec §4.2's borrowed-broker promise outranks the ``[mesh]`` requirement).
    """
    try:
        stop(target, grace=grace)
    except MeshExtraMissingError:
        pass  # no [mesh] extra → nothing can be stopped anyway; the ensure below reuses/borrows
    return ensure_broker(target, resolve_bin=resolve_bin, timeout=timeout)
