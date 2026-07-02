"""``ck chat`` typer command — chat with an agent on the mesh (plan §5).

Discovers the agents currently online (``client.mesh.get_agents()``), lets you pick
one (or names it directly), then runs a multi-turn REPL that streams each turn's
intermediate work (messages, tool calls/results, handoffs) live, then the answer.

Exit codes:
    0 — clean exit (``/exit``, ``/quit``, Ctrl-D, or Ctrl-C), or when no agents are online.
    2 — an invalid config value, a named agent that isn't online, or an unusable mesh
        directory (``MeshUnavailableError``). A bad ``--env-file`` warns and continues;
        an unreachable broker surfaces as the mesh-unavailable path.
"""

from __future__ import annotations

import asyncio
import traceback
from typing import TYPE_CHECKING, Any

import typer

from calfkit.cli._common import _load_env, _parse_host
from calfkit.exceptions import MeshUnavailableError

if TYPE_CHECKING:
    from collections.abc import Coroutine

# Reason -> remedy hint for an unusable mesh directory (the closed
# MeshUnavailableError.reason set; ``.get`` degrades gracefully if it ever grows).
_MESH_HINTS = {
    "establishing": "still catching up — try again in a moment",
    "open_failed": "no agents/tools are online yet, or the broker is unreachable",
    "reader_dead": "the mesh reader failed — restart ck chat",
}


def chat(
    name: str | None = typer.Argument(
        None,
        help="Agent to chat with. Omit to pick from the list of online agents.",
    ),
    host: str | None = typer.Option(
        None,
        "--host",
        "-H",
        help="Kafka bootstrap server(s), comma-separated. Precedence: this flag > $CALFKIT_MESH_URL > localhost.",
    ),
    provision: bool = typer.Option(
        False,
        "--provision",
        help="Opt-in creation of this client's reply inbox topic (experimental); for brokers without topic auto-create (e.g. Tansu).",
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
    """Chat with an agent on the mesh."""
    from calfkit.cli._chat import run_chat_session

    try:
        _load_env(env_file)
        server_urls = _parse_host(host)
    except ValueError as exc:  # an invalid config value (defensive — the parsers don't raise today)
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(2) from exc

    run_session_command(run_chat_session(name, server_urls, timeout, provision))


def run_session_command(session: Coroutine[Any, Any, None]) -> None:
    """The shared command boundary around a chat session (``ck chat`` and ``ck dev chat``).

    Runs the session in a try SEPARATE from any config parsing: a ValueError raised in here
    (e.g. a pydantic ValidationError, which subclasses ValueError) is a real bug and must
    propagate with its traceback — not be masked as a clean config-error Exit(2).
    """
    try:
        asyncio.run(session)
    except KeyboardInterrupt:
        # Ctrl-C at a prompt or mid-turn: a clean stop, not a traceback (the
        # add_reader-based reader cancels cleanly — see _chat_io).
        raise typer.Exit(0) from None
    except MeshUnavailableError as exc:
        # Fail fast on an unusable mesh, but preserve the traceback so the cause is debuggable.
        hint = _MESH_HINTS.get(exc.reason, "unavailable")
        typer.echo(f"Error: mesh unavailable ({exc.reason}): {hint}", err=True)
        typer.echo(traceback.format_exc(), err=True)
        raise typer.Exit(2) from exc
