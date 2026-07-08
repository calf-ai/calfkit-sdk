"""The ``ck dev chat`` live agent picker (spec ``docs/designs/cli-live-feedback-spec.md`` §5).

A cbreak-mode ``rich.Live`` selector whose online-agent menu refreshes as agents come and go.
The selection logic is a pure state-machine (:class:`PickerModel`) kept apart from the terminal
I/O so it is testable without a tty.
"""

from __future__ import annotations

import asyncio
import contextlib
import sys
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING

from rich.console import Console, Group, RenderableType
from rich.live import Live
from rich.text import Text

from calfkit.cli._chat_io import Key, make_key_reader

if TYPE_CHECKING:
    from calfkit.client import Client
    from calfkit.client.mesh import AgentInfo


def _roster_descriptions(agents: Mapping[str, AgentInfo]) -> dict[str, str | None]:
    """Project the mesh's agent directory down to the picker's ``name → description`` roster — the
    only fields the live menu needs, keeping :class:`PickerModel` decoupled from the mesh DTO."""
    return {name: info.description for name, info in agents.items()}


class PickerModel:
    """The picker's pure selection state: the display *order* (arrival order — existing rows stay
    put, new agents append), the *highlighted* agent tracked by name (not index), and each agent's
    advertised *description*. Reconciled against the live online roster by :meth:`sync`."""

    def __init__(self) -> None:
        self._order: list[str] = []
        self._highlight: str | None = None
        self._descriptions: dict[str, str | None] = {}

    def sync(self, agents: Mapping[str, str | None]) -> None:
        """Reconcile with the current online roster (name → description): drop agents that went
        offline (keeping the rest in place), append new arrivals (sorted within the batch), refresh
        descriptions, and keep the highlight on its agent — moving it to the nearest surviving row
        if that agent went offline."""
        online = set(agents)
        old_index = self._order.index(self._highlight) if self._highlight in self._order else 0
        self._order = [name for name in self._order if name in online]
        for name in sorted(online):
            if name not in self._order:
                self._order.append(name)
        if self._highlight not in self._order:
            self._highlight = self._order[min(old_index, len(self._order) - 1)] if self._order else None
        self._descriptions = dict(agents)

    def move(self, delta: int) -> None:
        """Shift the highlight by *delta* rows, clamped to the list ends."""
        if not self._order or self._highlight is None:
            return
        index = self._order.index(self._highlight)
        clamped = max(0, min(len(self._order) - 1, index + delta))
        self._highlight = self._order[clamped]

    @property
    def names(self) -> list[str]:
        return list(self._order)

    @property
    def highlighted(self) -> str | None:
        return self._highlight

    def description(self, name: str) -> str | None:
        return self._descriptions.get(name)


def render_menu(model: PickerModel) -> RenderableType:
    """Render the picker: a header, then one row per online agent — ``❯`` marks the highlighted row,
    and each agent's description follows its (padded) name. An empty roster shows a waiting hint
    (agents may still be coming online)."""
    header = Text("Select an agent  (↑/↓ move · Enter pick · q quit)")
    if not model.names:
        return Group(header, Text("  (no agents online yet — waiting… press q to quit)"))
    name_width = max(len(name) for name in model.names)
    rows: list[RenderableType] = []
    for name in model.names:
        marker = "❯ " if name == model.highlighted else "  "
        description = model.description(name)
        row = f"{marker}{name.ljust(name_width)}" + (f"  {description}" if description else "")
        rows.append(Text(row))
    return Group(header, *rows)


async def _run_picker(
    read_key: Callable[[], Awaitable[Key]],
    poll_agents: Callable[[], Awaitable[Mapping[str, str | None]]],
    render: Callable[[PickerModel], None],
    *,
    cadence: float,
) -> str | None:
    """The picker's control loop, decoupled from the terminal (spec §5.1): race a keypress against a
    re-poll tick. On a tick, re-sync the model from *poll_agents* and re-render; on a key, move /
    select (Enter) / cancel (quit). The keypress task persists across ticks so a press is never lost
    to a poll boundary. Returns the selected agent name, or ``None`` if cancelled."""
    model = PickerModel()
    model.sync(await poll_agents())
    render(model)
    key_task = asyncio.ensure_future(read_key())
    try:
        while True:
            tick_task = asyncio.ensure_future(asyncio.sleep(cadence))
            done, _ = await asyncio.wait({key_task, tick_task}, return_when=asyncio.FIRST_COMPLETED)
            if key_task in done:
                tick_task.cancel()
                key = key_task.result()
                if key == "quit":
                    return None
                if key == "enter" and model.highlighted is not None:
                    return model.highlighted
                # Enter on an empty roster is ignored (falls through) — not a selection, and never a
                # silent cancel — so the picker stays open until an agent arrives or the user quits.
                if key == "up":
                    model.move(-1)
                elif key == "down":
                    model.move(1)
                render(model)
                key_task = asyncio.ensure_future(read_key())
            else:  # poll tick — the keypress task stays armed
                model.sync(await poll_agents())
                render(model)
    finally:
        key_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await key_task


def is_interactive() -> bool:
    """True when both stdin and stdout are real terminals — the precondition for the live picker
    (POSIX cbreak input). Otherwise the caller falls back to the static numbered picker."""
    return sys.stdin.isatty() and sys.stdout.isatty()


async def live_pick(client: Client, *, cadence: float = 1.0) -> str | None:
    """The interactive live agent picker (spec §5.1): render the online-agent menu with ``rich.Live``
    and read single keys in **cbreak** mode (line-editing + echo off, output translation kept so Live
    renders cleanly — not raw mode, which would fight Live). Re-polls ``get_agents()`` every *cadence*
    seconds so agents appear/disappear live. The terminal mode is restored on every exit path; Ctrl-C
    (a SIGINT under cbreak) is treated as cancel, alongside ``q``/``Esc``. POSIX-only, like the ``ck
    chat`` line reader."""
    import termios
    import tty

    loop = asyncio.get_running_loop()
    fd = sys.stdin.fileno()
    console = Console()
    read_key = make_key_reader(loop, fd)

    async def poll_agents() -> dict[str, str | None]:
        return _roster_descriptions(await client.mesh.get_agents())

    old_attrs = termios.tcgetattr(fd)
    tty.setcbreak(fd)
    try:
        with Live("", console=console, auto_refresh=False, transient=True) as live:

            def render(model: PickerModel) -> None:
                live.update(render_menu(model), refresh=True)

            try:
                return await _run_picker(read_key, poll_agents, render, cadence=cadence)
            except KeyboardInterrupt:
                return None
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_attrs)
