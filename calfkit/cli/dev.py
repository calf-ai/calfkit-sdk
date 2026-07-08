"""The ``ck dev`` command group — a calfkit project against a zero-setup local mesh (spec §4).

Every command loads ``.env`` **first** (so a ``.env``-set ``CALFKIT_MESH_URL`` is visible when
the address is normalized), resolves + normalizes the host, and **ensures** a broker there via
the connect-or-spawn supervisor with the managed-vs-reused line printed — with the **normalized**
``listen_ip:port`` as ``host`` (a multi-address borrow forwards the user's list unchanged). The
preset: provisioning ON (Tansu has no topic auto-create), reload ON (``dev run`` only),
idempotence OFF.

Four surfaces (agent-lifecycle spec §3): foreground ``dev run`` delegates to the top-level
``run()`` command function forwarding every argument explicitly; ``dev run -d`` connect-or-spawns
a detached agent daemon via :mod:`calfkit.cli._dev_agents`; ``dev chat`` attaches (or, given
targets, runs an in-process session worker) through ``run_chat_session`` directly; and
``status``/``stop``/``down`` manage the daemons the argv-marker scan owns. ``ck dev mesh``
controls the broker daemon directly.

Import hygiene (load-bearing): ``_build_app()`` imports this module on **every** ``ck``
invocation, so nothing here may import ``psutil`` or ``calfkit_mesh`` at module top — both belong
to the ``[mesh]`` extra. The locator thunk imports ``calfkit_mesh`` lazily and only ever runs in
the supervisor's spawn branch.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import TYPE_CHECKING, Any

import typer

from calfkit.cli import _dev_agents, _dev_broker
from calfkit.cli._common import _load_env, _parse_host
from calfkit.cli._dev_broker import BrokerInfo, DevBrokerError, Target, normalize
from calfkit.cli.run import run as _run_command
from calfkit.client._mesh_url import resolve_mesh_url

if TYPE_CHECKING:
    from calfkit.client.mesh import AgentInfo, ToolInfo

dev_app = typer.Typer(
    name="dev",
    help="Run against a zero-setup local mesh: connect to (or spawn) a managed dev broker.",
    no_args_is_help=True,
)
mesh_app = typer.Typer(
    name="mesh",
    help="Control the local dev mesh — the broker your agents run on.",
    no_args_is_help=True,
)
dev_app.add_typer(mesh_app, name="mesh")

_HOST_HELP = "Kafka bootstrap server(s), comma-separated. Precedence: this flag > $CALFKIT_MESH_URL > localhost."


def _resolve_bin() -> str:
    """The binary locator thunk, called only when a spawn is imminent.

    ``CALF_TANSU_BIN`` is honored HERE, before ``calfkit_mesh`` is imported — the escape hatch
    must work on a core install (no ``[mesh]`` extra), where the upstream locator that also reads
    it does not exist. Unset, the bundled locator resolves (bundled binary → ``tansu`` on PATH).
    """
    import os

    override = os.environ.get("CALF_TANSU_BIN")
    if override:
        if not (os.path.isfile(override) and os.access(override, os.X_OK)):
            raise DevBrokerError(f"CALF_TANSU_BIN={override!r} does not point at an executable file.")
        return override
    from calfkit_mesh import resolve_broker_bin

    path: str = resolve_broker_bin()
    return path


def _normalize_or_exit(servers: list[str]) -> Target:
    try:
        return normalize(servers)
    except ValueError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(2) from exc


def _echo_broker_line(broker: BrokerInfo) -> None:
    if broker.managed:
        typer.echo(f"ck dev: managed broker at {broker.listener} (pid {broker.pid})")
    else:
        typer.echo(f"ck dev: reusing broker at {broker.listener} — not managed by calfkit")


def _ensure_or_exit(target: Target) -> BrokerInfo:
    try:
        broker = _dev_broker.ensure_broker(target, resolve_bin=_resolve_bin, timeout=_dev_broker.DEFAULT_TIMEOUT)
    except DevBrokerError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(2) from exc
    _echo_broker_line(broker)
    return broker


def _forward_host(target: Target) -> str:
    """The host the delegated command connects to: the normalized listener, so the worker targets
    the exact address the broker bound (never bare ``localhost``/``::1`` while Tansu is on
    ``127.0.0.1``). A multi-address borrow forwards the user's elements unchanged (spec §4.3) —
    ``target.bootstrap`` preserves them verbatim."""
    return target.listener if target.is_single else target.bootstrap


def _session_servers(target: Target) -> str | list[str]:
    """The bootstrap value for a client/session opened DIRECTLY by this module (review round 2,
    R2-M1): ``resolve_mesh_url`` never comma-splits a bare string, so a multi-address borrow must
    hand over the user's elements as a list (``target.servers`` — the same value the reachability
    probe uses); handing it the comma-joined ``bootstrap`` would dial one malformed element. The
    single-address case stays the normalized listener string, byte-identical to before.

    Distinct from :func:`_forward_host`, which feeds delegated COMMANDS (``ck run``'s ``--host``
    string / the daemon argv) whose own ``_parse_host`` does the splitting."""
    return target.listener if target.is_single else list(target.servers)


@dev_app.command(name="run")
def dev_run(
    targets: list[str] = typer.Argument(
        ...,
        help="One or more 'module:attr' targets. Each attr is a node or an iterable of nodes.",
    ),
    detach: bool = typer.Option(
        False,
        "--detach",
        "-d",
        help="Launch as a detached agent daemon: return once the agents/tools are online; they run until 'ck dev stop'.",
    ),
    host: str | None = typer.Option(None, "--host", "-H", help=_HOST_HELP),
    provision: bool = typer.Option(
        True,
        "--provision/--no-provision",
        help="Dev topic auto-creation (preset ON — the bundled broker has no topic auto-create).",
    ),
    enable_idempotence: bool = typer.Option(
        False,
        "--enable-idempotence",
        help="Turn on idempotent producers across every producer (needs a broker with producer-id support). Off by default.",
    ),
    reload: bool = typer.Option(
        True,
        "--reload/--no-reload",
        help="Watch source files and restart the worker on change (preset ON for the dev loop).",
    ),
    reload_dir: list[str] = typer.Option(
        None,
        "--reload-dir",
        help="Directory to watch with --reload (repeatable). Defaults to the current directory.",
    ),
    app_dir: str = typer.Option(
        ".",
        "--app-dir",
        help="Directory inserted on sys.path for resolving 'module:attr' targets. Defaults to the current directory.",
    ),
    group_id: str | None = typer.Option(
        None,
        "--group-id",
        help="Kafka consumer-group override applied to every node. Defaults to each node's id.",
    ),
    env_file: str | None = typer.Option(
        None,
        "--env-file",
        help="Path to a dotenv file to load. Defaults to ./.env if present.",
    ),
) -> None:
    """Run node(s) against the local dev mesh, spawning its broker if needed."""
    _load_env(env_file)  # FIRST: a .env-set CALFKIT_MESH_URL must be visible to host resolution
    if detach:
        _detach_run(
            targets,
            host=host,
            provision=provision,
            enable_idempotence=enable_idempotence,
            reload=reload,
            reload_dir=reload_dir,
            app_dir=app_dir,
            group_id=group_id,
            env_file=env_file,
        )
        return
    servers = resolve_mesh_url(_parse_host(host))
    target = _normalize_or_exit(servers)
    _ensure_or_exit(target)  # in the PARENT: reload children only ever connect (spec §4.3)
    _run_command(
        targets=targets,
        host=_forward_host(target),
        provision=provision,
        enable_idempotence=enable_idempotence,
        reload=reload,
        reload_dir=reload_dir,
        app_dir=app_dir,
        group_id=group_id,
        env_file=env_file,
        # The hidden internals, forwarded explicitly with their dev presets so the
        # forwards-every-parameter guard keeps its equality contract: the 5s heartbeat applies
        # to foreground dev runs too (agent-lifecycle spec §5.6); a foreground run is never a
        # daemon, so no ownership marker.
        dev_daemon=None,
        heartbeat_interval=_dev_agents.DEV_HEARTBEAT_INTERVAL,
    )


def _detach_run(
    targets: list[str],
    *,
    host: str | None,
    provision: bool,
    enable_idempotence: bool,
    reload: bool,
    reload_dir: list[str] | None,
    app_dir: str,
    group_id: str | None,
    env_file: str | None,
) -> None:
    """The ``run -d`` path (agent-lifecycle spec §3.1/§5.1): preflight fails fast at the prompt,
    then broker ensure, then connect-or-spawn the agent daemon and return only once its names are
    online — so ``... && ck dev chat`` lands on a mesh where the agents already exist."""
    try:
        plan = _dev_agents.preflight(targets, app_dir=app_dir)
    except _dev_agents.DevAgentError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(2) from exc
    target = _normalize_or_exit(resolve_mesh_url(_parse_host(host)))
    _ensure_or_exit(target)
    run_args = _dev_agents.RunOptions(
        provision=provision,
        reload=reload,
        reload_dir=tuple(reload_dir) if reload_dir else None,
        app_dir=app_dir,
        group_id=group_id,
        env_file=env_file,
        enable_idempotence=enable_idempotence,
    )
    try:
        report = asyncio.run(_ensure_agents_with_client(plan, target, run_args))
    except KeyboardInterrupt:
        # §3.4: the daemon (if one was spawned) is deliberately left running — recoverable state,
        # never half-killed on an interrupted wait.
        typer.echo("interrupted — a launched daemon may still be starting in the background; check 'ck dev status'.", err=True)
        raise typer.Exit(130) from None
    except (_dev_agents.DevAgentError, DevBrokerError) as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(2) from exc
    _echo_detach_report(report)


async def _ensure_agents_with_client(
    plan: list[_dev_agents.TargetNodes],
    target: Target,
    run_args: _dev_agents.RunOptions,
) -> _dev_agents.EnsureReport:
    """Run the ensure under a short-lived readiness client: its ``mesh`` view is the §5.2 poll's
    snapshot source; closed on every path so the CLI never leaks a consumer."""
    from calfkit.client import Client

    client = Client.connect(_session_servers(target))
    try:
        return await _dev_agents.ensure_agents(plan, target, client.mesh, run_args=run_args)
    finally:
        await client.aclose()


def _echo_detach_report(report: _dev_agents.EnsureReport) -> None:
    """§3.1's per-name lines: launched names state their lifetime + supervisor pid + log; reused
    names state the heartbeat age (the staleness-window honesty device, spec §5.5)."""
    for outcome in report.outcomes:
        for kind_word, names in (("agent", outcome.target.agent_names), ("tool", outcome.target.tool_names)):
            for name in names:
                if outcome.reused:
                    age = _format_age(outcome.ages.get(name, 0.0))
                    typer.echo(f"ck dev: reusing {kind_word} '{name}' (online, last seen {age} ago)")
                else:
                    lifetime = f"runs until 'ck dev stop {name}'"
                    typer.echo(f"ck dev: launched {kind_word} '{name}' (pid {report.pid}) — {lifetime} — logs: {report.log_path}")


def _format_age(seconds: float) -> str:
    """A compact heartbeat age: ``3s``, ``2m05s``, ``1h04m``."""
    whole = max(0, int(seconds))
    if whole < 60:
        return f"{whole}s"
    if whole < 3600:
        return f"{whole // 60}m{whole % 60:02d}s"
    return f"{whole // 3600}h{(whole % 3600) // 60:02d}m"


@dev_app.command(name="chat")
def dev_chat(
    args: list[str] | None = typer.Argument(
        None,
        help="An agent NAME to attach to, or 'module:attr' TARGET(s) to run as a session worker "
        "inside this chat process (they stop when the session ends). Omit to pick from the "
        "online agents.",
    ),
    host: str | None = typer.Option(None, "--host", "-H", help=_HOST_HELP),
    provision: bool = typer.Option(
        True,
        "--provision/--no-provision",
        help="Create this client's reply inbox topic (preset ON — the bundled broker has no topic auto-create).",
    ),
    env_file: str | None = typer.Option(
        None,
        "--env-file",
        help="Path to a dotenv file to load. Defaults to ./.env if present.",
    ),
    timeout: float | None = typer.Option(
        None,
        "--timeout",
        help="Per-turn patience in seconds. On timeout the turn is abandoned and the session continues. Default: wait indefinitely.",
    ),
) -> None:
    """Chat with an agent on the local dev mesh, spawning its broker if needed."""
    _load_env(env_file)
    # The §3.2 grammar: an argument containing ':' is a module:attr target; a bare word is an
    # agent name. Mixing them, or more than one bare name, is a usage error.
    values = list(args or [])
    target_specs = [value for value in values if ":" in value]
    names = [value for value in values if ":" not in value]
    if target_specs and names:
        typer.echo(
            "Error: cannot mix agent names and 'module:attr' targets — attach to ONE online agent by name, or launch target(s) for this session.",
            err=True,
        )
        raise typer.Exit(2)
    if len(names) > 1:
        typer.echo(
            f"Error: chat attaches to one agent, got {len(names)} names ({', '.join(names)}); pass one name or omit it to pick.",
            err=True,
        )
        raise typer.Exit(2)
    if target_specs:
        _chat_targets(target_specs, host=host, provision=provision, env_file=env_file, timeout=timeout)
        return
    servers = resolve_mesh_url(_parse_host(host))
    target = _normalize_or_exit(servers)
    _ensure_or_exit(target)
    _chat_attach(names[0] if names else None, target, provision=provision, timeout=timeout)


def _chat_attach(name: str | None, target: Target, *, provision: bool, timeout: float | None) -> None:
    """The attach path (no args / bare NAME): the plain chat session, plus the dev layer's §7
    offline-daemon hint — a named agent that turns out offline gets the marker-scan diagnosis
    (its daemon exists but stopped advertising: broken edit / mid-restart) appended to the
    shipped not-online error."""
    from calfkit.cli import chat as chat_module
    from calfkit.cli._chat import run_chat_session

    session = run_chat_session(
        name,
        _session_servers(target),
        timeout,
        provision,
        offline_daemon_hint=_offline_daemon_hint(target.key),
    )
    chat_module.run_session_command(session)


def _offline_daemon_hint(host_key: str) -> Any:
    """Build the §7 hint provider: scan the marker daemons at *host_key* for one owning the
    offline name. Degrades to no hint on a core install (the scan needs the ``[mesh]`` extra) —
    the shipped not-online error then stands alone."""

    def hint(name: str) -> str | None:
        try:
            daemons = _dev_agents.find_daemons(host_key)
        except DevBrokerError:  # incl. MeshExtraMissingError — never let the hint break the error
            return None
        owner = next((daemon for daemon in daemons if name in daemon.names), None)
        if owner is None:
            return None
        return f"a managed daemon for '{name}' exists but its agents are offline — logs: {owner.log_path} (ck dev status)"

    return hint


def _chat_targets(targets: list[str], *, host: str | None, provision: bool, env_file: str | None, timeout: float | None) -> None:
    """The ``chat TARGET...`` session mode (agent-lifecycle spec §3.2): ensure the broker,
    preflight the targets, then run the in-process session — the worker lives inside the chat
    process and dies with it, atomically, by construction. No reload (an in-process worker
    cannot re-import user code): the edit loop is save → /exit → rerun."""
    target = _normalize_or_exit(resolve_mesh_url(_parse_host(host)))
    _ensure_or_exit(target)
    try:
        plan = _dev_agents.preflight(targets, app_dir=".")
    except _dev_agents.DevAgentError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(2) from exc
    from calfkit.cli import chat as chat_module
    from calfkit.cli._chat import run_chat_session

    session = run_chat_session(None, _session_servers(target), timeout, provision, session_plan=plan, session_host_key=target.key)
    try:
        chat_module.run_session_command(session)
    except (_dev_agents.DevAgentError, DevBrokerError) as exc:
        # DevBrokerError covers MeshExtraMissingError from the lazy daemon scan inside the
        # session's connect-or-spawn evaluation (a core install): exit 2 with the install hint,
        # never a raw traceback.
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(2) from exc


# --- status / stop / down: the agent-daemon management surface (agent-lifecycle spec §3.3/§3.4) --------

_STATUS_HEADER = ("KIND", "NAME", "STATE", "PID", "SINCE", "TARGET", "LOGS")
_EMPTY = "—"


@dev_app.command(name="status")
def dev_status(
    host: str | None = typer.Option(None, "--host", "-H", help=_HOST_HELP),
    env_file: str | None = typer.Option(None, "--env-file", help="Path to a dotenv file to load. Defaults to ./.env if present."),
) -> None:
    """Show the broker, every online node, and the managed agent daemons — unfiltered.

    Never errors on a down mesh: the broker line degrades, presence columns read
    'unknown (mesh unreachable)', and daemon rows still render from the process scan.
    """
    target = _target_or_exit(host, env_file)
    try:
        broker_report = _dev_broker.status(target)
        daemons = _dev_agents.find_daemons(target.key)
    except DevBrokerError as exc:  # the scan needs the [mesh] extra — without it there is no answer
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(2) from exc
    agents: dict[str, Any] = {}
    tools: dict[str, Any] = {}
    if broker_report.reachable:
        # An unusable presence plane (open_failed on a fresh broker) reads as ZERO online rows —
        # only an unreachable broker degrades the presence columns (spec §3.3).
        agents, tools = asyncio.run(_read_presence_maps(target))
    for line in _render_table(_status_rows(broker_report, daemons, agents, tools)):
        typer.echo(line)


async def _read_presence_maps(target: Target) -> tuple[dict[str, AgentInfo], dict[str, ToolInfo]]:
    """One point-in-time presence snapshot per kind via a short-lived client; an unusable view
    degrades to empty, never errors.

    The degrade is silent only for ``open_failed`` (the spec'd first-run state: no presence
    plane yet ⇒ zero online rows). Any other reason (``establishing``, ``reader_dead``) is a
    read failure on a reachable mesh — rendered the same way, but announced on stderr so the
    degraded table never masquerades as a healthy empty mesh (review round 1, Ryan-approved).
    """
    from calfkit.client import Client
    from calfkit.exceptions import MeshUnavailableError

    def _warn_unless_first_run(kind: str, exc: MeshUnavailableError) -> None:
        if exc.reason != "open_failed":
            typer.echo(f"warning: presence unreadable ({kind} view, {exc.reason}): {exc}", err=True)

    client = Client.connect(_session_servers(target))
    try:
        try:
            agents = dict(await client.mesh.get_agents())
        except MeshUnavailableError as exc:
            _warn_unless_first_run("agents", exc)
            agents = {}
        try:
            tools = dict(await client.mesh.get_tools())
        except MeshUnavailableError as exc:
            _warn_unless_first_run("tools", exc)
            tools = {}
    finally:
        await client.aclose()
    return agents, tools


def _status_rows(
    report: _dev_broker.MeshStatus,
    daemons: list[_dev_agents.DaemonHit],
    agents: dict[str, AgentInfo],
    tools: dict[str, ToolInfo],
) -> list[tuple[str, ...]]:
    """The §3.3 join: broker row(s), then managed-daemon rows (scan ⋈ presence by name), then
    every remaining online node — nothing online is ever filtered out (Ryan's transparency rule)."""
    rows: list[tuple[str, ...]] = [_STATUS_HEADER]
    target_elements = set(report.target_key.split(","))
    managed_brokers = [broker for broker in report.brokers if broker.listener in target_elements]
    for broker in managed_brokers:
        rows.append(("broker", broker.listener, "running", str(broker.pid), broker.started_at or _EMPTY, _EMPTY, _EMPTY))
    if not managed_brokers:
        state = "reachable, not managed by calfkit" if report.reachable else "no broker reachable"
        rows.append(("broker", report.target_key, state, _EMPTY, _EMPTY, _EMPTY, _EMPTY))
    for hit in daemons:
        for name in hit.names:
            if name in agents:
                kind, state = "agent", f"online (last seen {_format_age(_age_of(agents[name]))} ago)"
            elif name in tools:
                kind, state = "tool", f"online (last seen {_format_age(_age_of(tools[name]))} ago)"
            elif not report.reachable:
                kind, state = "unknown", "unknown (mesh unreachable)"
            else:
                # Daemon process alive, no presence record: crashed on a broken edit, mid-restart,
                # or still booting — statelessly indistinguishable, so honestly unknown (Ryan,
                # 2026-07-02), pointing at the LOGS column.
                kind, state = "unknown", "unknown (see logs)"
            rows.append((kind, name, state, str(hit.pid), hit.started_at, ",".join(hit.targets) or _EMPTY, hit.log_path))
    managed_names = {name for hit in daemons for name in hit.names}
    annotation = "not a ck dev daemon (stop it where it runs)"
    for name, agent_info in sorted(agents.items()):
        if name not in managed_names:
            rows.append(("agent", name, f"online (last seen {_format_age(_age_of(agent_info))} ago) — {annotation}", _EMPTY, _EMPTY, _EMPTY, _EMPTY))
    for name, tool_info in sorted(tools.items()):
        if name not in managed_names:
            rows.append(("tool", name, f"online (last seen {_format_age(_age_of(tool_info))} ago) — {annotation}", _EMPTY, _EMPTY, _EMPTY, _EMPTY))
    return rows


def _age_of(info: AgentInfo | ToolInfo) -> float:
    from datetime import datetime, timezone

    age: float = (datetime.now(tz=timezone.utc) - info.last_seen).total_seconds()
    return age


def _render_table(rows: list[tuple[str, ...]]) -> list[str]:
    widths = [max(len(row[column]) for row in rows) for column in range(len(rows[0]))]
    return ["  ".join(cell.ljust(widths[column]) for column, cell in enumerate(row)).rstrip() for row in rows]


@dev_app.command(name="stop")
def dev_stop(
    names: list[str] | None = typer.Argument(None, help="Agent/tool name(s) whose daemon to stop (whole-daemon: co-hosted names stop together)."),
    stop_all: bool = typer.Option(False, "--all", help="Stop every ck dev agent daemon on every address (ignores --host)."),
    host: str | None = typer.Option(None, "--host", "-H", help=_HOST_HELP),
    env_file: str | None = typer.Option(None, "--env-file", help="Path to a dotenv file to load. Defaults to ./.env if present."),
) -> None:
    """Stop agent daemon(s) by name; the broker stays up."""
    try:
        if stop_all:
            _stop_daemon_sweep(_dev_agents.find_daemons(None))
            return
        if not names:
            typer.echo("Error: pass agent name(s) to stop, or --all ('ck dev status' lists what is running).", err=True)
            raise typer.Exit(2)
        target = _target_or_exit(host, env_file)
        daemons = _dev_agents.find_daemons(target.key)
        # The online set feeds only the not-a-daemon explanation; read it just when a name
        # is not covered by any daemon (and degrade to empty if the mesh is unusable).
        online: set[str] = set()
        if any(name not in {n for hit in daemons for n in hit.names} for name in names):
            online = asyncio.run(_online_name_set(target))
        for hit in _dev_agents.resolve_stop(names, daemons, host_key=target.key, online=online):
            if not _dev_agents.stop_daemon(hit):
                raise _dev_agents.DevAgentError(f"cannot stop the agent daemon (pid {hit.pid}) — it is owned by another user.")
            _echo_stopped(hit)
    except (_dev_agents.DevAgentError, DevBrokerError) as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(2) from exc


async def _online_name_set(target: Target) -> set[str]:
    agents, tools = await _read_presence_maps(target)
    return set(agents) | set(tools)


def _echo_stopped(hit: _dev_agents.DaemonHit) -> None:
    typer.echo(f"stopped daemon pid {hit.pid} (agents: {', '.join(hit.names)})")


def _stop_daemon_sweep(daemons: list[_dev_agents.DaemonHit]) -> None:
    """Stop every given daemon, narrated; a denied or stuck one warns and never aborts the sweep
    (the broker stop_all discipline) — one bad daemon must not leave the rest running or skip
    ``down``'s broker stop."""
    if not daemons:
        typer.echo("no agent daemons to stop")
        return
    for hit in daemons:
        try:
            stopped = _dev_agents.stop_daemon(hit)
        except DevBrokerError as exc:  # e.g. survived SIGKILL — warn and keep sweeping
            typer.echo(f"warning: {exc}", err=True)
            continue
        if stopped:
            _echo_stopped(hit)
        else:
            typer.echo(f"warning: cannot stop the agent daemon (pid {hit.pid}) — it is owned by another user.", err=True)


@dev_app.command(name="down")
def dev_down(
    host: str | None = typer.Option(None, "--host", "-H", help=_HOST_HELP),
    env_file: str | None = typer.Option(None, "--env-file", help="Path to a dotenv file to load. Defaults to ./.env if present."),
) -> None:
    """Stop every agent daemon, then the broker at the resolved address.

    Foreground and chat-session workers survive by design (they carry no marker) and will error
    against the stopped broker — stop them where they run.
    """
    target = _target_or_exit(host, env_file)
    try:
        _stop_daemon_sweep(_dev_agents.find_daemons(None))
        if _dev_broker.stop(target):
            typer.echo(f"stopped {target.key}")
        else:
            typer.echo(f"no managed broker at {target.key} (no memory-engine tansu is bound there)")
    except (_dev_agents.DevAgentError, DevBrokerError) as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(2) from exc


_ENV_FILE_HELP = "Path to a dotenv file to load. Defaults to ./.env if present."


def _target_or_exit(host: str | None, env_file: str | None = None) -> Target:
    # Load the env first, exactly like `dev run`/`dev chat`: an env-set CALFKIT_MESH_URL must
    # target the SAME address across every `ck dev` command, or `broker stop` would miss the
    # broker `dev run` spawned.
    _load_env(env_file)
    return _normalize_or_exit(resolve_mesh_url(_parse_host(host)))


def _run_broker_foreground(target: Target) -> None:
    """Spawn an attached broker and block on it until it exits or Ctrl-C stops it (spec §5.2)."""
    try:
        proc = _dev_broker.spawn_foreground(target, resolve_bin=_resolve_bin, timeout=_dev_broker.DEFAULT_TIMEOUT)
    except DevBrokerError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(2) from exc
    typer.echo(f"ck dev: broker listening at {target.listener} (pid {proc.pid}) — Ctrl-C to stop")
    try:
        returncode = proc.wait()
    except KeyboardInterrupt:
        # Ctrl-C reached Tansu too (shared session); reap its clean shutdown and exit success.
        with suppress(KeyboardInterrupt):
            proc.wait()
        typer.echo("ck dev: broker stopped")
        return
    if returncode:
        typer.echo(f"ck dev: broker exited with code {returncode}", err=True)
        raise typer.Exit(returncode if returncode > 0 else 1)


@mesh_app.command(name="start")
def mesh_start(
    host: str | None = typer.Option(None, "--host", "-H", help=_HOST_HELP),
    detach: bool = typer.Option(
        False,
        "--detach",
        "-d",
        help="Run the broker as a detached background daemon and return, instead of in the foreground.",
    ),
    env_file: str | None = typer.Option(None, "--env-file", help=_ENV_FILE_HELP),
) -> None:
    """Run the dev broker in the foreground (Ctrl-C stops it); -d runs it as a detached daemon.

    Foreground is the default: the broker's output streams to your terminal and it stops when you
    press Ctrl-C. With -d it connect-or-spawns a detached daemon and returns (idempotent) — the
    shape 'ck dev run' and 'ck dev chat' share when they ensure a broker.
    """
    target = _target_or_exit(host, env_file)
    if detach:
        _ensure_or_exit(target)
        return
    _run_broker_foreground(target)


@mesh_app.command(name="stop")
def mesh_stop(
    host: str | None = typer.Option(None, "--host", "-H", help=_HOST_HELP),
    stop_all: bool = typer.Option(
        False,
        "--all",
        help="Stop every running dev broker (ignores --host).",
    ),
    env_file: str | None = typer.Option(None, "--env-file", help=_ENV_FILE_HELP),
) -> None:
    """Stop the dev broker at the target address — the memory-engine tansu the §5.4 scan finds.

    A no-op for anything else reachable there (a durable tansu, your own Kafka — never signalled).
    """
    try:
        if stop_all:
            stopped = _dev_broker.stop_all()
            if stopped:
                for key in stopped:
                    typer.echo(f"stopped {key}")
            else:
                typer.echo("no managed brokers to stop")
            return
        target = _target_or_exit(host, env_file)
        if _dev_broker.stop(target):
            typer.echo(f"stopped {target.key}")
        else:
            typer.echo(f"no managed broker at {target.key} (no memory-engine tansu is bound there)")
    except DevBrokerError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(2) from exc


@mesh_app.command(name="status")
def mesh_status(
    host: str | None = typer.Option(None, "--host", "-H", help=_HOST_HELP),
    env_file: str | None = typer.Option(None, "--env-file", help=_ENV_FILE_HELP),
) -> None:
    """Report the running dev broker(s) and probe the target address."""
    target = _target_or_exit(host, env_file)
    try:
        report = _dev_broker.status(target)
    except DevBrokerError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(2) from exc
    for broker in report.brokers:
        typer.echo(f"{broker.listener}: pid {broker.pid}, running, started {broker.started_at}")
    managed_listeners = {broker.listener for broker in report.brokers}
    # The canonical key is the comma-join of the normalized per-element addresses, so a
    # multi-address target counts as managed only when EVERY element has a dev broker.
    if not set(report.target_key.split(",")) <= managed_listeners:
        if report.reachable:
            typer.echo(f"{report.target_key}: reachable, not managed by calfkit")
        else:
            typer.echo(f"{report.target_key}: no broker reachable")


@mesh_app.command(name="restart")
def mesh_restart(
    host: str | None = typer.Option(None, "--host", "-H", help=_HOST_HELP),
    env_file: str | None = typer.Option(None, "--env-file", help=_ENV_FILE_HELP),
) -> None:
    """Stop then start the target's managed broker — the clean slate (the memory engine is ephemeral)."""
    target = _target_or_exit(host, env_file)
    try:
        broker = _dev_broker.restart(target, resolve_bin=_resolve_bin, timeout=_dev_broker.DEFAULT_TIMEOUT)
    except DevBrokerError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(2) from exc
    _echo_broker_line(broker)
