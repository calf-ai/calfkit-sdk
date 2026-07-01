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

import typer

from calfkit.cli import _dev_broker
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
    )


@dev_app.command(name="chat")
def dev_chat(
    name: str | None = typer.Argument(
        None,
        help="Agent to chat with. Omit to pick from the list of online agents.",
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
    servers = resolve_mesh_url(_parse_host(host))
    target = _normalize_or_exit(servers)
    _ensure_or_exit(target)
    _chat_command(
        name=name,
        host=_forward_host(target),
        provision=provision,
        env_file=env_file,
        timeout=timeout,
    )


def _target_or_exit(host: str | None) -> Target:
    # Load ./.env first, exactly like `dev run`/`dev chat`: a .env-set CALFKIT_MESH_URL must
    # target the SAME address across every `ck dev` command, or `broker stop` would miss the
    # broker `dev run` spawned.
    _load_env(None)
    return _normalize_or_exit(resolve_mesh_url(_parse_host(host)))


@broker_app.command(name="start")
def broker_start(host: str | None = typer.Option(None, "--host", "-H", help=_HOST_HELP)) -> None:
    """Connect-or-spawn the dev broker and return (the daemon keeps running). Idempotent."""
    _ensure_or_exit(_target_or_exit(host))


@broker_app.command(name="stop")
def broker_stop(
    host: str | None = typer.Option(None, "--host", "-H", help=_HOST_HELP),
    stop_all: bool = typer.Option(
        False,
        "--all",
        help="Stop every running dev broker (ignores --host).",
    ),
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
        target = _target_or_exit(host)
        if _dev_broker.stop(target):
            typer.echo(f"stopped {target.key}")
        else:
            typer.echo(f"no managed broker at {target.key} (no memory-engine tansu is bound there)")
    except DevBrokerError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(2) from exc


@broker_app.command(name="status")
def broker_status(host: str | None = typer.Option(None, "--host", "-H", help=_HOST_HELP)) -> None:
    """Report the running dev broker(s) and probe the target address."""
    target = _target_or_exit(host)
    try:
        report = _dev_broker.status(target)
    except DevBrokerError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(2) from exc
    for broker in report.brokers:
        typer.echo(f"{broker.listener}: pid {broker.pid}, running, started {broker.started_at}")
    managed_listeners = {broker.listener for broker in report.brokers}
    if report.target_key not in managed_listeners:
        if report.reachable:
            typer.echo(f"{report.target_key}: reachable, not managed by calfkit")
        else:
            typer.echo(f"{report.target_key}: no broker reachable")


@broker_app.command(name="restart")
def broker_restart(host: str | None = typer.Option(None, "--host", "-H", help=_HOST_HELP)) -> None:
    """Stop then start the target's managed broker — the clean slate (the memory engine is ephemeral)."""
    target = _target_or_exit(host)
    try:
        broker = _dev_broker.restart(target, resolve_bin=_resolve_bin, timeout=_dev_broker.DEFAULT_TIMEOUT)
    except DevBrokerError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(2) from exc
    _echo_broker_line(broker)
