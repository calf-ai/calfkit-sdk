"""calfkit top-level CLI entry point.

Mounts subcommands as typer sub-apps. Currently only ``mcp`` is mounted
(with ``mcp codegen``). Future subcommands land alongside via the same
mounting pattern.

Invoked via the ``calfkit`` console script registered in pyproject.toml's
``[project.scripts]``.
"""

from __future__ import annotations


def main() -> None:
    """Entry point for the ``calfkit`` console script.

    typer is imported lazily so calfkit's regular runtime imports
    (e.g. ``from calfkit import Agent``) don't pay the typer cost. If
    typer isn't installed (the ``mcp-codegen`` optional extra wasn't
    selected), the import error is surfaced with a clear remediation
    message.
    """
    try:
        import typer
    except ImportError as e:
        raise SystemExit("The calfkit CLI requires the 'mcp-codegen' optional extra. Install with: pip install calfkit[mcp-codegen]") from e

    from calfkit.cli.mcp import app as mcp_app

    app = typer.Typer(name="calfkit", help="Calfkit SDK command-line tools.", no_args_is_help=True)
    app.add_typer(mcp_app, name="mcp")
    app()
