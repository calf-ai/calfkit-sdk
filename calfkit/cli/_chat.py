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
from typing import TYPE_CHECKING, Any

import typer

from calfkit.cli._chat_io import make_reader
from calfkit.cli._chat_render import _error_line, _render_answer, _render_fault, _render_step, format_picker
from calfkit.client import Client, RunCompleted, RunFailed
from calfkit.exceptions import NodeFaultError

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


async def run_chat_session(name: str | None, server_urls: str | list[str] | None, timeout: float | None) -> None:
    """Connect, discover the online agents, resolve the target, and run the REPL.

    A not-ready mesh raises ``MeshUnavailableError`` from ``get_agents()`` — left to
    bubble to the ``chat.py`` boundary (F2). The ``async with`` closes the client
    (mesh views then broker) on every exit path.
    """
    read_line = make_reader(asyncio.get_running_loop())
    async with Client.connect(server_urls) as client:
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
    """The multi-turn REPL with one agent. ``message_history`` is threaded turn to
    turn — the only continuity mechanism."""
    gw = client.agent(agent_name)
    history: list[ModelMessage] = []
    print(f"\nChatting with {agent_name}. Type /exit or press Ctrl-D to leave.")
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
        new_history = await _run_turn(gw, agent_name, line, history, timeout)
        if new_history is not None:  # None == the turn failed / timed out: keep the old history
            history = new_history


async def _run_turn(
    gw: AgentGateway[Any],
    agent_name: str,
    prompt: str,
    history: list[ModelMessage],
    timeout: float | None,
) -> list[ModelMessage] | None:
    """Dispatch one turn, render it, and return the next history — or ``None`` if the
    dispatch failed, the turn faulted, or it exceeded ``--timeout`` (the REPL keeps
    going either way)."""
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


async def _render_and_collect(handle: InvocationHandle, agent_name: str) -> list[ModelMessage]:
    """Render the turn's live step events (the work-log) as they stream, then the
    projected final answer; return the message history for the next turn."""
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
    return result.message_history
