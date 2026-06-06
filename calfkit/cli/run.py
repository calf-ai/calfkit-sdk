"""``calfkit run`` typer command.

A development convenience that runs one or more node(s) as a worker without the
``Client``/``Worker``/``run()`` boilerplate — point it at ``module:attr``
targets and it starts serving, FastAPI-CLI style::

    calfkit run weather_tool:get_weather
    calfkit run myapp.agents:weather_agent myapp.tools:get_weather --reload

Targets are dotted Python import paths (``module:attr``); each ``attr`` may be
a single node or an iterable of nodes. All resolved nodes run in one worker.

**Development only.** Requires the ``cli`` optional extra (typer + watchfiles).
If typer is not installed, the import raises with a clear remediation message.

Exit codes:
    0 — clean shutdown (Ctrl-C, or the worker stopped on its own)
    2 — configuration error (bad ``module:attr`` spec, import failure, non-node
        object, or zero nodes resolved). Surfaced before the worker starts —
        including under ``--reload``, where the targets are pre-flighted in the
        parent so a broken config fails fast instead of leaving an idle watcher.

Note: under ``--reload``, a *runtime* failure that only appears once the worker
is live (e.g. the broker is unreachable) is reported in the child process and
the supervisor keeps watching for a fix — the standard dev-server reload
contract. Config errors are caught up front (see above).
"""

from __future__ import annotations

import os

try:
    import typer
except ImportError as e:  # pragma: no cover -- exercised manually
    raise ImportError("the calfkit CLI requires the 'cli' optional extra. Install with: pip install calfkit[cli]") from e

from calfkit.cli._loader import load_nodes
from calfkit.cli._run import serve


def run(
    targets: list[str] = typer.Argument(
        ...,
        help="One or more 'module:attr' targets. Each attr is a node or an iterable of nodes.",
    ),
    host: str | None = typer.Option(
        None,
        "--host",
        "-H",
        help="Kafka bootstrap server(s), comma-separated. Precedence: this flag > $CALF_HOST_URL > localhost.",
    ),
    provision: bool = typer.Option(
        False,
        "--provision",
        help="Opt-in dev topic auto-creation (EXPERIMENTAL; rf=1, no ACLs). Off by default.",
    ),
    reload: bool = typer.Option(
        False,
        "--reload",
        help="Watch source files and restart the worker on change (dev only).",
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
    """Run node(s) as a worker until stopped (Ctrl-C)."""
    abs_app_dir = os.path.abspath(app_dir)

    if reload:
        # Pre-flight the targets in the parent before starting the supervisor so
        # a misconfigured run fails fast (exit 2) instead of leaving an idle
        # watcher around a child that never started. This imports the target
        # module(s) once in the parent; the spawned child re-imports them
        # cleanly on each restart. (Runtime/broker failures still surface in the
        # child and restart on the next edit — the standard reload contract.)
        load_nodes(list(targets), app_dir=abs_app_dir)

        # The reload supervisor (parent) only watches files; watchfiles spawns
        # serve() in a fresh process on every change. serve must stay a
        # module-level function with picklable args for the spawn to reconstruct it.
        from watchfiles import PythonFilter, run_process

        dirs = reload_dir or [os.getcwd()]
        run_process(
            *dirs,
            target=serve,
            args=(list(targets), host, provision, group_id, env_file, abs_app_dir),
            watch_filter=PythonFilter(),
        )
    else:
        try:
            serve(list(targets), host, provision, group_id, env_file, abs_app_dir)
        except KeyboardInterrupt:
            # Ctrl-C in a window where FastStream's own SIGINT handler isn't
            # active yet (startup/teardown), or on a platform whose event loop
            # lacks add_signal_handler: treat as a clean stop instead of dumping
            # a traceback. On the normal path FastStream handles SIGINT itself
            # and serve() returns without raising — this is the fallback.
            raise typer.Exit(0) from None
