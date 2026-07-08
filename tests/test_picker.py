"""Unit tests for the live agent picker (``calfkit.cli._picker``).

The picker splits into a pure selection state-machine (``PickerModel`` — tested here with
no terminal), a pure renderer, and the raw-mode interactive loop (spec
``docs/designs/cli-live-feedback-spec.md`` §5).
"""

from __future__ import annotations

import asyncio
import re
from io import StringIO

from rich.console import Console

from calfkit.cli._picker import PickerModel, _run_picker, render_menu

_ANSI = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")


def _menu_text(renderable: object) -> str:
    buf = StringIO()
    Console(file=buf, width=80, color_system=None).print(renderable)
    return _ANSI.sub("", buf.getvalue())


def test_sync_populates_order_and_highlights_the_first() -> None:
    model = PickerModel()
    model.sync(["triage", "backend"])
    assert model.names == ["backend", "triage"]  # deterministic (sorted) initial order
    assert model.highlighted == "backend"


def test_move_shifts_highlight_and_clamps_at_the_ends() -> None:
    model = PickerModel()
    model.sync(["a", "b", "c"])  # highlight 'a'
    model.move(1)
    assert model.highlighted == "b"
    model.move(1)
    assert model.highlighted == "c"
    model.move(1)  # clamp at the bottom
    assert model.highlighted == "c"
    model.move(-5)  # clamp at the top
    assert model.highlighted == "a"


def test_new_arrivals_append_at_the_bottom_preserving_existing_order() -> None:
    model = PickerModel()
    model.sync(["m"])
    model.sync(["a", "m"])  # 'a' arrives; it appends, it does NOT re-sort above 'm'
    assert model.names == ["m", "a"]


def test_offline_highlighted_agent_moves_highlight_to_the_nearest_survivor() -> None:
    model = PickerModel()
    model.sync(["a", "b", "c"])
    model.move(1)  # highlight 'b'
    model.sync(["a", "c"])  # 'b' goes offline
    assert model.names == ["a", "c"]
    assert model.highlighted == "c"


def test_all_offline_empties_the_list_and_clears_the_highlight() -> None:
    model = PickerModel()
    model.sync(["a"])
    model.sync([])
    assert model.names == []
    assert model.highlighted is None


def test_render_menu_marks_the_highlighted_row() -> None:
    model = PickerModel()
    model.sync(["backend", "triage"])
    model.move(1)  # highlight 'triage'
    text = _menu_text(render_menu(model))
    assert "❯ triage" in text  # highlighted row carries the caret marker
    assert "  backend" in text  # non-highlighted row is indented, no marker


def test_render_menu_shows_an_empty_hint_when_no_agents() -> None:
    model = PickerModel()
    model.sync([])
    text = _menu_text(render_menu(model))
    assert "no agents online" in text.lower()


async def test_run_picker_moves_and_selects() -> None:
    keys = iter(["down", "enter"])

    async def read_key() -> str:
        return next(keys)

    async def poll() -> list[str]:
        return ["a", "b", "c"]

    result = await _run_picker(read_key, poll, lambda _m: None, cadence=100.0)
    assert result == "b"  # 'down' moves a->b, 'enter' selects it


async def test_run_picker_resyncs_the_roster_on_a_poll_tick() -> None:
    rosters = iter([["a"], ["a", "z"]])

    async def poll() -> list[str]:
        return next(rosters, ["a", "z"])

    keys: asyncio.Queue[str] = asyncio.Queue()

    async def read_key() -> str:
        return await keys.get()

    seen: list[list[str]] = []
    task = asyncio.create_task(_run_picker(read_key, poll, lambda m: seen.append(m.names), cadence=0.01))
    await asyncio.sleep(0.08)  # let a poll tick fire → 'z' appears
    await keys.put("quit")
    assert await asyncio.wait_for(task, 1.0) is None
    assert any("z" in names for names in seen)  # the tick picked up the newly-online agent


async def test_live_pick_end_to_end_over_a_pty(monkeypatch: object) -> None:
    """Smoke-test the real glue (cbreak + key reader + loop) over a pseudo-terminal: navigate down
    one and select. Exercises termios set/restore and make_key_reader against a real tty."""
    import os
    import pty
    from types import SimpleNamespace

    master, slave = pty.openpty()
    monkeypatch.setattr("sys.stdin", SimpleNamespace(fileno=lambda: slave, isatty=lambda: True))  # type: ignore[attr-defined]

    class _Mesh:
        async def get_agents(self) -> dict[str, None]:
            return {"a": None, "b": None}

    client = SimpleNamespace(mesh=_Mesh())
    from calfkit.cli._picker import live_pick

    task = asyncio.create_task(live_pick(client, cadence=100.0))  # type: ignore[arg-type]
    try:
        await asyncio.sleep(0.05)
        os.write(master, b"\x1b[B")  # down -> highlight 'b'
        await asyncio.sleep(0.05)
        os.write(master, b"\r")  # enter -> select
        assert await asyncio.wait_for(task, 2.0) == "b"
    finally:
        os.close(master)
        os.close(slave)
