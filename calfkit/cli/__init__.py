"""``ck`` top-level CLI entry point.

Mounts subcommands as typer sub-apps: ``topics`` (with ``topics
provision``), plus the top-level ``run`` command. Future subcommands land
alongside via the same mounting pattern.

Invoked via the ``ck`` console script registered in pyproject.toml's
``[project.scripts]``.
"""

from __future__ import annotations

from typing import Any


def _build_app() -> Any:
    """Construct the top-level ``ck`` typer app with all sub-apps mounted.

    typer is imported lazily so calfkit's regular runtime imports
    (e.g. ``from calfkit import Agent``) don't pay the typer import cost.
    """
    import typer

    from calfkit.cli.run import run as run_command
    from calfkit.cli.topics import app as topics_app

    app = typer.Typer(name="ck", help="Calfkit SDK command-line tools.", no_args_is_help=True)
    app.add_typer(topics_app, name="topics")
    app.command(name="run")(run_command)
    return app


def main() -> None:
    """Entry point for the ``ck`` console script."""
    _build_app()()
