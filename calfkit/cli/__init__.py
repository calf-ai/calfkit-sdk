"""calfkit top-level CLI entry point.

Mounts subcommands as typer sub-apps: ``mcp`` (with ``mcp codegen`` /
``mcp schema``) and ``topics`` (with ``topics provision``), plus the
top-level ``run`` command. Future subcommands land alongside via the same
mounting pattern.

Invoked via the ``calfkit`` console script registered in pyproject.toml's
``[project.scripts]``.
"""

from __future__ import annotations

from typing import Any


def _build_app() -> Any:
    """Construct the top-level ``calfkit`` typer app with all sub-apps mounted.

    typer is imported lazily so calfkit's regular runtime imports
    (e.g. ``from calfkit import Agent``) don't pay the typer cost. If typer
    isn't installed (the ``cli`` optional extra wasn't selected), the import
    error is surfaced with a clear remediation message.
    """
    try:
        import typer
    except ImportError as e:
        raise SystemExit("The calfkit CLI requires the 'cli' optional extra. Install with: pip install \"calfkit[cli]\"") from e

    from calfkit.cli.mcp import app as mcp_app
    from calfkit.cli.run import run as run_command
    from calfkit.cli.topics import app as topics_app

    app = typer.Typer(name="calfkit", help="Calfkit SDK command-line tools.", no_args_is_help=True)
    app.add_typer(mcp_app, name="mcp")
    app.add_typer(topics_app, name="topics")
    app.command(name="run")(run_command)
    return app


def main() -> None:
    """Entry point for the ``calfkit`` console script."""
    _build_app()()
