"""``ck chat`` orchestration (plan §6/§7/§9): discover -> pick -> REPL.

This module owns every ``print`` and the per-turn streaming; the renderer
(:mod:`calfkit.cli._chat_render`) is pure and the stdin reader
(:mod:`calfkit.cli._chat_io`) is the one isolated I/O primitive. A turn is
``start().stream()`` (the live work-log) followed by ``result()`` (the projected
answer + the message history threaded into the next turn); ``--timeout`` bounds the
response wait (``stream()`` + ``result()``, which have no internal timeout) — the
dispatch (``start()``) is not under it.
"""

from __future__ import annotations

import asyncio
import shutil
from typing import TYPE_CHECKING, Any, NamedTuple

import typer

from calfkit.cli._chat_io import make_reader
from calfkit.cli._chat_render import _error_line, _render_answer, _render_fault, _render_step, format_picker
from calfkit.client import Client, RunCompleted, RunFailed
from calfkit.exceptions import NodeFaultError
from calfkit.provisioning import ProvisioningConfig

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping

    from calfkit._vendor.pydantic_ai.messages import ModelMessage
    from calfkit.client import AgentGateway
    from calfkit.client.hub import InvocationHandle
    from calfkit.client.mesh import AgentInfo

    ReadLine = Callable[[str], Awaitable[str]]


def _width() -> int:
    return shutil.get_terminal_size().columns


def _emit(lines: list[str]) -> None:
    for line in lines:
        print(line)


class _TurnResult(NamedTuple):
    """One turn's outcome threaded back to the REPL: the ``history`` for the next turn and the
    ``responder`` — the agent that produced the answer. On a handoff the responder is the *target*
    (control transferred), on a consult or a direct answer it stays the addressed agent. The loop
    re-binds to ``responder`` so a handoff **sticks** across turns."""

    history: list[ModelMessage]
    responder: str


async def run_chat_session(name: str | None, server_urls: str | list[str] | None, timeout: float | None, provision: bool = False) -> None:
    """Connect, discover the online agents, resolve the target, and run the REPL.

    A not-ready mesh raises ``MeshUnavailableError`` from ``get_agents()`` — left to
    bubble to the ``chat.py`` boundary (F2). The ``async with`` closes the client
    (mesh views then broker) on every exit path.

    ``provision`` (``--provision``) opt-in creates this client's reply inbox topic at broker
    start — needed on brokers that don't auto-create topics (e.g. Tansu). The agent's own topics
    are provisioned by its worker (``ck run --provision``), not here.
    """
    read_line = make_reader(asyncio.get_running_loop())
    provisioning = ProvisioningConfig(enabled=True) if provision else None
    async with Client.connect(server_urls, provisioning=provisioning) as client:
        print("Discovering agents...")
        agents = await client.mesh.get_agents()
        if not agents:  # ready, but zero live agents
            print("No agents are online on the mesh.")
            return
        picked = await _resolve_target(name, agents, read_line)
        if picked is None:  # user quit at the picker
            return
        await _chat_loop(client, picked, timeout, read_line)


async def _resolve_target(name: str | None, agents: Mapping[str, AgentInfo], read_line: ReadLine) -> str | None:
    """The named agent (erroring if it isn't online), or the user's picker choice
    (``None`` if they quit)."""
    if name is not None:
        if name in agents:
            return name
        typer.echo(f"Agent {name!r} is not online. Online agents: {', '.join(sorted(agents))}.", err=True)
        raise typer.Exit(2)

    names = sorted(agents)  # sort ONCE — the displayed numbering and the index->name selection share it
    _emit(format_picker(names, agents))
    while True:
        try:
            choice = (await read_line(f"\nSelect an agent [1-{len(names)}, q to quit]: ")).strip()
        except EOFError:
            print()
            return None
        if choice.lower() == "q":
            return None
        if choice.isdigit() and 1 <= int(choice) <= len(names):
            return names[int(choice) - 1]
        print(f"Enter a number from 1 to {len(names)}, or q to quit.")


async def _chat_loop(client: Client, agent_name: str, timeout: float | None, read_line: ReadLine) -> None:
    """The multi-turn REPL. Starts with the picked agent; a handoff **sticks** — when a turn's
    answer comes from a different agent (the handoff target, ``RunCompleted.agent``), the REPL
    re-binds to it so the *next* turn goes there, honoring the transfer (the handing agent
    relinquishes and does not regain control). A consult (``message_agent``) keeps control, so its
    responder is the current agent and nothing re-binds. ``message_history`` is threaded turn to
    turn — the continuity mechanism across the (possibly changing) responder."""
    active = agent_name
    gw = client.agent(active)
    history: list[ModelMessage] = []
    print(f"\nChatting with {active}. Type /exit or press Ctrl-D to leave.")
    print("-" * _width())
    while True:
        try:
            line = (await read_line("\nyou > ")).strip()
        except EOFError:  # Ctrl-D
            print()
            break
        if line in {"/exit", "/quit"}:
            break
        if not line:
            continue
        outcome = await _run_turn(gw, active, line, history, timeout)
        if outcome is None:  # the turn failed / timed out: keep the old history AND the current agent
            continue
        history = outcome.history
        if outcome.responder != active:  # a handoff moved control — follow it so the transfer sticks
            active = outcome.responder
            gw = client.agent(active)
            print(f"\n(now chatting with {active})")


async def _run_turn(
    gw: AgentGateway[Any],
    agent_name: str,
    prompt: str,
    history: list[ModelMessage],
    timeout: float | None,
) -> _TurnResult | None:
    """Dispatch one turn, render it, and return its ``_TurnResult`` (next history + responder) —
    or ``None`` if the dispatch failed, the turn faulted, or it exceeded ``--timeout`` (the REPL
    keeps going, and keeps the current agent, either way)."""
    try:
        handle = await gw.start(prompt, message_history=history)
    except Exception as exc:  # the turn could not be dispatched (broker I/O, or building/serializing the request): surface it, keep the REPL alive
        _emit(_error_line(agent_name, f"turn could not be started ({type(exc).__name__}: {exc})"))
        return None
    collect = _render_and_collect(handle, agent_name)
    try:
        if timeout is None:
            return await collect  # no timeout: a real TimeoutError here propagates, never mislabelled
        try:
            return await asyncio.wait_for(collect, timeout)
        except asyncio.TimeoutError:  # scoped to the wait_for path only
            print("\n(no response within the timeout)")
            return None
    except NodeFaultError as exc:
        _emit(_render_fault(agent_name, exc))
        return None


async def _render_and_collect(handle: InvocationHandle, agent_name: str) -> _TurnResult:
    """Render the turn's live step events (the work-log) as they stream, then the projected final
    answer; return the next-turn history plus the ``responder`` — who actually answered
    (``RunCompleted.agent``: the handoff target when control moved, else ``agent_name``). The
    responder heads the answer line AND is what the REPL re-binds to for the next turn."""
    responder = agent_name
    current_emitter: str | None = None
    async for event in handle.stream():  # intermediates, then the terminal (last)
        if isinstance(event, RunCompleted):
            responder = event.agent or agent_name
        elif isinstance(event, RunFailed):
            pass  # surfaced by result() below as NodeFaultError
        else:
            lines, current_emitter = _render_step(event, current_emitter)
            _emit(lines)
    result = await handle.result()  # cached terminal: projects str + history (or raises NodeFaultError)
    _emit(_render_answer(responder, result.output))
    return _TurnResult(result.message_history, responder)
