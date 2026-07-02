"""The ``ck dev`` command group — a calfkit project against a zero-setup local mesh (spec §4).

``ck dev run`` / ``ck dev chat`` are thin wrappers: load ``.env`` **first** (so a ``.env``-set
``CALFKIT_MESH_URL`` is visible when the address is normalized), resolve + normalize the host,
**ensure** a broker there via the connect-or-spawn supervisor, print the managed-vs-reused line,
then delegate to the existing ``run()``/``chat()`` command functions forwarding every argument
explicitly — with the **normalized** ``listen_ip:port`` as ``host`` (a multi-address borrow
forwards the user's list unchanged). The preset: provisioning ON (Tansu has no topic auto-create),
reload ON (``dev run`` only), idempotence OFF. ``ck dev broker`` controls the managed daemon
directly.

Import hygiene (load-bearing): ``_build_app()`` imports this module on **every** ``ck``
invocation, so nothing here may import ``psutil`` or ``calfkit_mesh`` at module top — both belong
to the ``[mesh]`` extra. The locator thunk imports ``calfkit_mesh`` lazily and only ever runs in
the supervisor's spawn branch.
"""

from __future__ import annotations

import asyncio

import typer

from calfkit.cli import _dev_agents, _dev_broker
from calfkit.cli._common import _load_env, _parse_host
from calfkit.cli._dev_broker import BrokerInfo, DevBrokerError, Target, normalize
from calfkit.cli.chat import chat as _chat_command
from calfkit.cli.run import run as _run_command
from calfkit.client._mesh_url import resolve_mesh_url

dev_app = typer.Typer(
    name="dev",
    help="Run against a zero-setup local mesh: connect to (or spawn) a managed dev broker.",
    no_args_is_help=True,
)
broker_app = typer.Typer(
    name="broker",
    help="Control the managed dev broker daemon directly.",
    no_args_is_help=True,
)
dev_app.add_typer(broker_app, name="broker")

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
    from calfkit_mesh import resolve_broker_bin  # type: ignore[import-not-found]

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
        help="Launch as a detached agent daemon: return once the agents are online; they run until 'ck dev stop'.",
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

    client = Client.connect(_forward_host(target))
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
    _chat_command(
        name=names[0] if names else None,
        host=_forward_host(target),
        provision=provision,
        env_file=env_file,
        timeout=timeout,
    )


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

    session = run_chat_session(None, _forward_host(target), timeout, provision, session_plan=plan, session_host_key=target.key)
    try:
        chat_module.run_session_command(session)
    except _dev_agents.DevAgentError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(2) from exc


_ENV_FILE_HELP = "Path to a dotenv file to load. Defaults to ./.env if present."


def _target_or_exit(host: str | None, env_file: str | None = None) -> Target:
    # Load the env first, exactly like `dev run`/`dev chat`: an env-set CALFKIT_MESH_URL must
    # target the SAME address across every `ck dev` command, or `broker stop` would miss the
    # broker `dev run` spawned.
    _load_env(env_file)
    return _normalize_or_exit(resolve_mesh_url(_parse_host(host)))


@broker_app.command(name="start")
def broker_start(
    host: str | None = typer.Option(None, "--host", "-H", help=_HOST_HELP),
    env_file: str | None = typer.Option(None, "--env-file", help=_ENV_FILE_HELP),
) -> None:
    """Connect-or-spawn the dev broker and return (the daemon keeps running). Idempotent."""
    _ensure_or_exit(_target_or_exit(host, env_file))


@broker_app.command(name="stop")
def broker_stop(
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


@broker_app.command(name="status")
def broker_status(
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


@broker_app.command(name="restart")
def broker_restart(
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
