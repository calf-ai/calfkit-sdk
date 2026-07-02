"""Agent-daemon supervisor for ``ck dev`` (dev-agent-lifecycle spec §5).

The agent-layer sibling of :mod:`calfkit.cli._dev_broker`, one layer up: preflight ``module:attr``
targets, connect-or-spawn detached agent daemons (identified statelessly by the ``--dev-daemon``
argv marker), gate on presence-plane readiness, and provide the stop/status data the ``ck dev``
management commands render.

Import hygiene (load-bearing, the ``_dev_broker`` rule): ``ck dev`` imports this module on every
invocation, so nothing here may import ``psutil`` at module top — it ships only in the ``[mesh]``
extra and is imported lazily by the process scan.
"""

from __future__ import annotations

import asyncio
import os
import re
import signal
import subprocess
import sys
import time
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from subprocess import Popen
from typing import Any, Protocol

from calfkit.cli._dev_broker import (
    _MESH_EXTRA_HINT,
    DEFAULT_PORT,
    MeshExtraMissingError,
    Target,
    _calfkit_dir,
    _detach_kwargs,
    _flag_value,
    _format_address,
    _log_tail,
    _spawn_lock,
    normalize,
)
from calfkit.cli._loader import load_nodes
from calfkit.exceptions import MeshUnavailableError
from calfkit.models.agents import AGENTS_TOPIC
from calfkit.models.capability import CAPABILITY_TOPIC

MARKER_FLAG = "--dev-daemon"
"""The argv ownership marker (spec §5.4): an *agent daemon* is any live process whose cmdline
carries this flag. Internal — humans launch daemons via ``ck dev run -d``, never by hand-setting
the marker (a hand-set marker IS managed, though: "whoever started it", the broker rule)."""

READY_TIMEOUT = 15.0
"""The readiness gate's deadline (spec §5.2, Ryan-confirmed): how long a launch waits for every
preflighted name to be online on the presence plane."""

_POLL_INTERVAL = 0.25
"""The readiness gate's sample cadence: a bounded one-shot startup gate over the in-memory mesh
snapshot (a local dict read — sanctioned by spec N4), never steady-state polling."""

_DEFAULT_HOST_KEY = _format_address("127.0.0.1", DEFAULT_PORT)


class PresenceReader(Protocol):
    """The slice of ``client.mesh`` the supervisor reads: fresh name-keyed snapshots per kind."""

    async def get_agents(self) -> Mapping[str, Any]: ...

    async def get_tools(self) -> Mapping[str, Any]: ...


def _killpg(pgid: int, sig: int) -> None:
    """Signal a whole process group — the ``-d`` spawn is a session leader, so its pid is the
    pgid and one signal reaps supervisor + worker + multiprocessing helpers together (spec §3.4).
    A call-time ``os`` lookup (``killpg`` does not exist on Windows, and module import must stay
    platform-safe) and the test seam — tests replace this, never a live signal."""
    os.killpg(pgid, sig)


@contextmanager
def agents_lock() -> Iterator[None]:
    """The agent-layer flock (spec §5.1): ``run -d`` holds it across check→spawn→readiness and
    ``chat TARGET`` across evaluate→start→readiness (released before the REPL opens) — without
    it, two concurrent launches of one target both see "none online" and double-spawn (the exact
    hazard the broker lock exists for; agents have no port-bind exclusivity to save them)."""
    with _spawn_lock(filename="dev-agents.lock", waiting_message="waiting for another ck dev agent launch to finish…"):
        yield


DEV_HEARTBEAT_INTERVAL = 5.0
"""The dev heartbeat preset (spec §5.6): every worker ``ck dev`` launches — foreground runs,
``-d`` daemons, and in-process chat session workers alike — heartbeats every 5s instead of the
30s production default, so crash-staleness detection (3 × interval) is ~15s. Writer-side only:
the reader's ``stale_after`` derives from the interval stamped on each record and must never be
shortened below the writer's cadence (live agents would flap offline between heartbeats)."""


class DevAgentError(Exception):
    """An agent-daemon supervise/manage failure (usage errors at preflight, spawn/readiness
    failures, stop-resolution errors, …).

    The ``ck dev`` commands surface it and exit ``2`` — the :class:`DevBrokerError` contract, one
    layer up.
    """


# --- preflight (spec §5.1): per-target names by kind, fail-fast at the prompt ------------------------


@dataclass(frozen=True)
class TargetNodes:
    """One preflighted ``module:attr`` target: its resolved node defs and their advertised names
    by presence kind. ``chat TARGET`` hosts ``nodes`` in-process; ``run -d`` re-imports the spec
    in the daemon child and uses only the names."""

    spec: str
    nodes: tuple[Any, ...]
    agent_names: tuple[str, ...]
    tool_names: tuple[str, ...]

    @property
    def names(self) -> tuple[str, ...]:
        """The readiness/marker set: the union of both advertising kinds (spec §5.1)."""
        return self.agent_names + self.tool_names


def preflight(targets: Sequence[str], *, app_dir: str | None = None) -> list[TargetNodes]:
    """Resolve and classify ``targets``, fail-fast at the prompt (spec §5.1).

    Each target resolves through the shipped ``load_nodes`` (its import/validation failures keep
    their ``typer.Exit(2)`` contract). Per node, agent-ness is the shipped predicate —
    ``AGENTS_TOPIC in control_plane_adverts()`` — tools advertise on ``CAPABILITY_TOPIC``, and the
    advertised name is ``node_id``.

    Raises:
        DevAgentError: on a target with zero presence-advertising nodes (an invisible,
            name-unstoppable daemon — Ryan's ruling, 2026-07-02), or a node name arriving via two
            targets (never a silent dedupe).
        typer.Exit: (code 2) on the loader's own failures (bad spec, import error, non-node,
            zero nodes).
    """
    plan: list[TargetNodes] = []
    seen: dict[str, str] = {}  # advertised-or-not node name -> the spec that brought it
    for spec in targets:
        nodes = load_nodes([spec], app_dir=app_dir, source_label="target")
        agent_names: list[str] = []
        tool_names: list[str] = []
        for node in nodes:
            name = str(node.node_id)
            owner = seen.get(name)
            if owner is not None:
                raise DevAgentError(
                    f"duplicate node name {name!r} across targets {owner!r} and {spec!r} — one invocation "
                    "launches each name once, and same-named nodes from different files are ambiguous. "
                    "Launch them separately or rename one."
                )
            seen[name] = spec
            adverts = node.control_plane_adverts()
            if AGENTS_TOPIC in adverts:
                agent_names.append(name)
            elif CAPABILITY_TOPIC in adverts:
                tool_names.append(name)
        if not agent_names and not tool_names:
            raise DevAgentError(
                f"target {spec!r} resolves to no agents or tools — 'ck dev' launch targets need at least "
                "one presence-advertising node (a daemon with no presence names would be invisible to "
                "'ck dev status' and unstoppable by name). Run plain consumer nodes with foreground "
                "'ck dev run' instead."
            )
        plan.append(TargetNodes(spec=spec, nodes=tuple(nodes), agent_names=tuple(agent_names), tool_names=tuple(tool_names)))
    return plan


# --- the stateless argv-marker scan (spec §5.4) -------------------------------------------------------


@dataclass(frozen=True)
class DaemonHit:
    """A running agent daemon found in the process table, with its live psutil handle (typed
    ``Any`` — psutil ships no type stubs) so callers signal the same process the scan verified.

    The scan matches exactly one process per daemon — the supervisor (the session leader
    ``stop`` group-signals): watchfiles/multiprocessing descendants carry ``spawn_main``-shaped
    argv without the marker. Everything here derives from that one argv: ``names`` from the
    marker value, ``host_key`` from the adjacent ``--host`` (normalized for the address join;
    absent = the default address), ``targets`` from the positional block, and ``log_path``
    re-derived through :func:`daemon_log_path` — the same function the spawn writes with.
    """

    proc: Any
    pid: int
    names: tuple[str, ...]
    host_key: str
    targets: tuple[str, ...]
    log_path: str
    started_at: str


def daemon_log_path(host_key: str, targets: Sequence[str]) -> Path:
    """The daemon's log file (spec §5.1: ``~/.calfkit/logs/agents-<slug>.log``, overwritten per
    spawn). The slug is the normalized address key plus the launched targets, each sanitized —
    derived identically at spawn and scan time so management commands can point at the log
    without any saved state."""
    slug = f"{_sanitize(host_key)}-{_sanitize('-'.join(targets))}"
    return _calfkit_dir() / "logs" / f"agents-{slug}.log"


def _sanitize(part: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "_", part)


def _daemon_hit(proc: Any, cmd: list[str]) -> DaemonHit | None:
    """Project one process's argv to a :class:`DaemonHit`, or ``None`` if it is not a manageable
    agent daemon (no marker, an empty marker, or an address that cannot be normalized for the
    join)."""
    marker = _flag_value(cmd, MARKER_FLAG, "")
    names = tuple(name for name in marker.split(",") if name)
    if not names:
        return None
    host = _flag_value(cmd, "--host", "")
    if host:
        try:
            host_key = normalize([host]).key
        except ValueError:
            return None  # an unjoinable address is not a daemon we can manage
    else:
        host_key = _DEFAULT_HOST_KEY
    targets = _positional_targets(cmd)
    started = datetime.fromtimestamp(proc.create_time(), tz=timezone.utc)
    return DaemonHit(
        proc=proc,
        pid=proc.pid,
        names=names,
        host_key=host_key,
        targets=targets,
        log_path=str(daemon_log_path(host_key, targets)),
        started_at=started.isoformat(timespec="seconds"),
    )


def _positional_targets(cmd: list[str]) -> tuple[str, ...]:
    """The ``module:attr`` block of a daemon argv: the contiguous non-flag tokens after ``run``.

    The ``-d`` spawn always emits ``run *targets --host …``, so this is exact for managed
    daemons; for a hand-run marker with interleaved flags it is best-effort (R3)."""
    try:
        start = cmd.index("run") + 1
    except ValueError:
        return ()
    targets: list[str] = []
    for token in cmd[start:]:
        if token.startswith("-"):
            break
        targets.append(token)
    return tuple(targets)


def scan_daemons() -> list[DaemonHit]:
    """Scan the live process table for agent daemons (spec §5.4) — the broker scan's discipline,
    one layer up. Raises :class:`MeshExtraMissingError` when ``psutil`` (the ``[mesh]`` extra) is
    not installed."""
    try:
        import psutil  # type: ignore[import-untyped]
    except ModuleNotFoundError as exc:
        raise MeshExtraMissingError(_MESH_EXTRA_HINT) from exc

    hits: list[DaemonHit] = []
    for proc in psutil.process_iter():
        try:
            hit = _daemon_hit(proc, proc.cmdline())
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue  # vanished or unreadable mid-scan — by definition not a daemon we can manage
        if hit is not None:
            hits.append(hit)
    return hits


def find_daemons(host_key: str | None) -> list[DaemonHit]:
    """The scan, host-scoped: hits whose argv-derived address key equals *host_key* (``None`` =
    every address — the ``stop --all``/``down`` sweep scope)."""
    return [hit for hit in scan_daemons() if host_key is None or hit.host_key == host_key]


# --- the bounded readiness poll (spec §5.2): the broker loop, one layer up ----------------------------


@dataclass(frozen=True)
class _Presence:
    """One captured point-in-time snapshot, kinds kept apart (a name is checked against its own
    kind's view, never a cross-kind union)."""

    agents: Mapping[str, Any]
    tools: Mapping[str, Any]


async def _read_presence(mesh: PresenceReader, *, want_agents: bool, want_tools: bool) -> _Presence | None:
    """One capture of the requested kinds, or ``None`` while the mesh is not usable yet.

    Any ``MeshUnavailableError`` is *not ready yet*: ``open_failed`` is the fresh-broker race the
    loop is specified to absorb (spec §5.5), and the other reasons are the same transient class —
    a bounded deadline turns a persistent one into the honest exit-2 anyway.
    """
    try:
        agents = dict(await mesh.get_agents()) if want_agents else {}
        tools = dict(await mesh.get_tools()) if want_tools else {}
    except MeshUnavailableError:
        return None
    return _Presence(agents=agents, tools=tools)


def _missing_names(agent_names: Sequence[str], tool_names: Sequence[str], presence: _Presence) -> list[str]:
    """The names not yet online, each checked against ITS kind's view within the one capture."""
    return [n for n in agent_names if n not in presence.agents] + [n for n in tool_names if n not in presence.tools]


async def wait_agents_ready(
    proc: Any | None,
    agent_names: Sequence[str],
    tool_names: Sequence[str],
    mesh: PresenceReader,
    *,
    log_path: str | None,
    timeout: float = READY_TIMEOUT,
    poll_interval: float = _POLL_INTERVAL,
) -> None:
    """Wait until every name is online, bounded (spec §5.2) — the broker readiness loop's shape.

    Each iteration: (1) ``proc.poll()`` FIRST, so child death always wins a tie (fires mainly for
    ``--no-reload``/supervisor-level failures — under reload-ON the supervisor survives worker
    crashes and the deadline is the normal failure path); (2) one captured mesh snapshot, checked
    whole (an unusable mesh = not ready); (3) all online → return; (4) deadline → group-kill the
    spawn and raise with the log tail. *proc* ``None`` is the chat variant: the same loop minus
    the process arms (a failed in-process ``Worker.start()`` raises directly instead).

    Raises:
        DevAgentError: the supervisor died, or the deadline passed.
    """
    deadline = time.monotonic() + timeout
    while True:
        if proc is not None and proc.poll() is not None:
            code = proc.returncode
            cause = f"killed by a signal ({code})" if code is not None and code < 0 else f"exit code {code}"
            raise DevAgentError(f"the agent daemon exited during startup ({cause}){_tail_suffix(log_path)}")
        presence = await _read_presence(mesh, want_agents=bool(agent_names), want_tools=bool(tool_names))
        if presence is not None and not _missing_names(agent_names, tool_names, presence):
            return
        if time.monotonic() >= deadline:
            if proc is not None:
                _kill_spawn_group(proc)
            raise DevAgentError(f"the launched agents did not come online within {timeout:.1f}s{_tail_suffix(log_path)}")
        await asyncio.sleep(poll_interval)


def _tail_suffix(log_path: str | None) -> str:
    return f"; log tail from {log_path}:\n{_log_tail(Path(log_path))}" if log_path else ""


def _kill_spawn_group(proc: Any) -> None:
    """Tear down a spawn that never became ready: SIGKILL its whole process group (the spawn is a
    session leader) and reap the leader — best-effort, the readiness error is what surfaces."""
    with suppress(ProcessLookupError, PermissionError):
        _killpg(proc.pid, signal.SIGKILL)
    with suppress(Exception):
        proc.wait(timeout=5.0)


# --- connect-or-spawn at the agent layer (spec §5.5) --------------------------------------------------


@dataclass(frozen=True)
class TargetOutcome:
    """One target's connect-or-spawn verdict: reused (never owned; ``ages`` carries the
    heartbeat age per name for the honesty line) or launched by this invocation."""

    target: TargetNodes
    reused: bool
    ages: dict[str, float] = field(default_factory=dict)


async def evaluate_targets(
    plan: Sequence[TargetNodes],
    host_key: str,
    mesh: PresenceReader,
) -> tuple[list[TargetOutcome], list[TargetNodes]]:
    """Classify every preflighted target against one presence capture (spec §5.5): all names
    online → reuse (name-identity, never code identity); none online → launch, unless a live
    marker daemon at this address owns overlapping names (a broken daemon is an error, never
    silently doubled); partially online → error naming the collision.

    An unopenable presence view counts as **none online** — the launched worker's provisioning
    then creates the topics and the readiness gate absorbs the race.

    Returns:
        ``(reused, to_launch)``, both in plan order.
    """
    presence = await _read_presence(
        mesh,
        want_agents=any(t.agent_names for t in plan),
        want_tools=any(t.tool_names for t in plan),
    )
    now = datetime.now(tz=timezone.utc)
    reused: list[TargetOutcome] = []
    to_launch: list[TargetNodes] = []
    daemons: list[DaemonHit] | None = None  # scanned lazily: only an offline target needs it
    for target in plan:
        missing = list(target.names) if presence is None else _missing_names(target.agent_names, target.tool_names, presence)
        if not missing:
            assert presence is not None  # not missing anything implies a usable capture
            ages = {name: (now - presence.agents[name].last_seen).total_seconds() for name in target.agent_names}
            ages |= {name: (now - presence.tools[name].last_seen).total_seconds() for name in target.tool_names}
            reused.append(TargetOutcome(target=target, reused=True, ages=ages))
        elif len(missing) == len(target.names):
            if daemons is None:
                daemons = find_daemons(host_key)
            owner = next((hit for hit in daemons if set(hit.names) & set(target.names)), None)
            if owner is not None:
                name = next(n for n in target.names if n in owner.names)
                raise DevAgentError(
                    f"a daemon for '{name}' already exists (pid {owner.pid}; its agents are offline — broken "
                    f"code or mid-restart). Logs: {owner.log_path}. Use 'ck dev stop {name}' to replace it."
                )
            to_launch.append(target)
        else:
            online = [n for n in target.names if n not in missing]
            raise DevAgentError(
                f"target {target.spec!r} is partially online: {', '.join(online)} already online while "
                f"{', '.join(missing)} is not — a worker hosts all its targets' nodes together, so this "
                "invocation can neither reuse nor launch it. Stop the partial set or rename the collision."
            )
    return reused, to_launch


# --- the -d daemon spawn + ensure orchestration (spec §5.1) -------------------------------------------


@dataclass(frozen=True)
class RunOptions:
    """The ``ck dev run`` options a ``-d`` daemon child inherits (spec §3.1: everything
    ``ck dev run`` is today, with the attachment cut). Defaults mirror the ``dev run`` presets."""

    provision: bool = True
    reload: bool = True
    reload_dir: Sequence[str] | None = None
    app_dir: str = "."
    group_id: str | None = None
    env_file: str | None = None
    enable_idempotence: bool = False


@dataclass(frozen=True)
class EnsureReport:
    """What one ``run -d`` ensured: the per-target verdicts (plan order), and — when something
    was spawned — the supervisor pid (the process ``stop`` signals) and its log."""

    outcomes: tuple[TargetOutcome, ...]
    pid: int | None
    log_path: str | None


async def ensure_agents(
    plan: Sequence[TargetNodes],
    target: Target,
    mesh: PresenceReader,
    *,
    run_args: RunOptions,
    timeout: float = READY_TIMEOUT,
    poll_interval: float = _POLL_INTERVAL,
) -> EnsureReport:
    """Connect-or-spawn the plan's targets as ONE detached daemon (spec §5.1), returning only
    once its names are online. The whole check→spawn→readiness section runs under the
    agent-layer flock."""
    with agents_lock():
        reused, to_launch = await evaluate_targets(plan, target.key, mesh)
        if not to_launch:
            return EnsureReport(outcomes=tuple(reused), pid=None, log_path=None)
        log_path = daemon_log_path(target.key, tuple(t.spec for t in to_launch))
        proc = _spawn_daemon(to_launch, target, log_path, run_args)
        await wait_agents_ready(
            proc,
            [n for t in to_launch for n in t.agent_names],
            [n for t in to_launch for n in t.tool_names],
            mesh,
            log_path=str(log_path),
            timeout=timeout,
            poll_interval=poll_interval,
        )
        by_spec = {outcome.target.spec: outcome for outcome in reused}
        outcomes = tuple(by_spec.get(t.spec, TargetOutcome(target=t, reused=False)) for t in plan)
        return EnsureReport(outcomes=outcomes, pid=proc.pid, log_path=str(log_path))


def _spawn_daemon(to_launch: Sequence[TargetNodes], target: Target, log_path: Path, options: RunOptions) -> Any:
    """Spawn the daemon tree — the broker's shipped ``Popen`` pattern verbatim (detached session
    leader, real log-file fd, ``DEVNULL`` stdin) around the §5.1 argv: the child re-invokes the
    CLI (``-m calfkit.cli run``) with the normalized host, the caller's options, the ownership
    marker, and the 5s heartbeat preset."""
    cmd = [sys.executable, "-m", "calfkit.cli", "run", *(t.spec for t in to_launch)]
    cmd += ["--host", target.listener if target.is_single else target.bootstrap]
    if options.provision:
        cmd.append("--provision")
    if options.reload:
        cmd.append("--reload")
    for directory in options.reload_dir or ():
        cmd += ["--reload-dir", directory]
    if options.app_dir != ".":
        cmd += ["--app-dir", options.app_dir]
    if options.group_id:
        cmd += ["--group-id", options.group_id]
    if options.env_file:
        cmd += ["--env-file", options.env_file]
    if options.enable_idempotence:
        cmd.append("--enable-idempotence")
    names = [name for t in to_launch for name in t.names]
    cmd.append(f"{MARKER_FLAG}={','.join(names)}")
    cmd += ["--heartbeat-interval", str(DEV_HEARTBEAT_INTERVAL)]
    # Overwritten each spawn — bounded logs, consult them BEFORE relaunching (spec §9). A real
    # file fd, never PIPE: an unread pipe would deadlock the worker once its buffer fills.
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_file = log_path.open("wb")
    except OSError as exc:
        raise DevAgentError(f"cannot write the agent daemon log at {log_path}: {exc}") from exc
    with log_file:
        try:
            return Popen(cmd, stdin=subprocess.DEVNULL, stdout=log_file, stderr=log_file, **_detach_kwargs())  # type: ignore[call-overload]
        except OSError as exc:
            raise DevAgentError(f"failed to launch the agent daemon: {exc}") from exc
