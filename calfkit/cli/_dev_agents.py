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

import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from calfkit.cli._dev_broker import (
    _MESH_EXTRA_HINT,
    DEFAULT_PORT,
    MeshExtraMissingError,
    _calfkit_dir,
    _flag_value,
    _format_address,
    normalize,
)
from calfkit.cli._loader import load_nodes
from calfkit.models.agents import AGENTS_TOPIC
from calfkit.models.capability import CAPABILITY_TOPIC

MARKER_FLAG = "--dev-daemon"
"""The argv ownership marker (spec §5.4): an *agent daemon* is any live process whose cmdline
carries this flag. Internal — humans launch daemons via ``ck dev run -d``, never by hand-setting
the marker (a hand-set marker IS managed, though: "whoever started it", the broker rule)."""

_DEFAULT_HOST_KEY = _format_address("127.0.0.1", DEFAULT_PORT)

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
