"""Unit tests for the live agent picker (``calfkit.cli._picker``).

The picker splits into a pure selection state-machine (``PickerModel`` — tested here with
no terminal), a pure renderer, and the cbreak interactive loop (spec
``docs/designs/cli-live-feedback-spec.md`` §5).
"""

from __future__ import annotations

import asyncio
import re
from io import StringIO
from typing import Any

from rich.console import Console

from calfkit.cli._picker import PickerModel, _run_picker, render_menu

_ANSI = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")


def _menu_text(renderable: object) -> str:
    buf = StringIO()
    Console(file=buf, width=80, color_system=None).print(renderable)
    return _ANSI.sub("", buf.getvalue())


def _roster(*names: str) -> dict[str, str | None]:
    """A roster with no descriptions — for the ordering/selection tests."""
    return {name: None for name in names}


def test_sync_populates_order_and_highlights_the_first() -> None:
    model = PickerModel()
    model.sync(_roster("triage", "backend"))
    assert model.names == ["backend", "triage"]  # deterministic (sorted) initial order
    assert model.highlighted == "backend"


def test_move_shifts_highlight_and_clamps_at_the_ends() -> None:
    model = PickerModel()
    model.sync(_roster("a", "b", "c"))  # highlight 'a'
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
    model.sync(_roster("m"))
    model.sync(_roster("a", "m"))  # 'a' arrives; it appends, it does NOT re-sort above 'm'
    assert model.names == ["m", "a"]


def test_offline_highlighted_agent_moves_highlight_to_the_nearest_survivor() -> None:
    model = PickerModel()
    model.sync(_roster("a", "b", "c"))
    model.move(1)  # highlight 'b'
    model.sync(_roster("a", "c"))  # 'b' goes offline
    assert model.names == ["a", "c"]
    assert model.highlighted == "c"


def test_all_offline_empties_the_list_and_clears_the_highlight() -> None:
    model = PickerModel()
    model.sync(_roster("a"))
    model.sync(_roster())
    assert model.names == []
    assert model.highlighted is None


def test_render_menu_marks_the_highlighted_row() -> None:
    model = PickerModel()
    model.sync(_roster("backend", "triage"))
    model.move(1)  # highlight 'triage'
    text = _menu_text(render_menu(model))
    assert "❯ triage" in text  # highlighted row carries the caret marker
    assert "  backend" in text  # non-highlighted row is indented, no marker


def test_render_menu_shows_each_agent_description() -> None:
    model = PickerModel()
    model.sync({"researcher": "Deep web research", "support-bot": "Handles support tickets"})
    text = _menu_text(render_menu(model))
    assert "Deep web research" in text  # descriptions render alongside names (D9 parity with the static picker)
    assert "Handles support tickets" in text


def test_render_menu_tolerates_a_missing_description() -> None:
    model = PickerModel()
    model.sync({"researcher": None})  # no advertised description
    text = _menu_text(render_menu(model))
    assert "researcher" in text  # renders the name without a description, no crash


def test_render_menu_shows_an_empty_hint_when_no_agents() -> None:
    model = PickerModel()
    model.sync(_roster())
    text = _menu_text(render_menu(model))
    assert "no agents online" in text.lower()


async def test_run_picker_moves_and_selects() -> None:
    keys = iter(["down", "enter"])

    async def read_key() -> str:
        return next(keys)

    async def poll() -> dict[str, str | None]:
        return _roster("a", "b", "c")

    result = await _run_picker(read_key, poll, lambda _m: None, cadence=100.0)
    assert result == "b"  # 'down' moves a->b, 'enter' selects it


async def test_run_picker_up_moves_the_highlight() -> None:
    keys = iter(["down", "down", "up", "enter"])

    async def read_key() -> str:
        return next(keys)

    async def poll() -> dict[str, str | None]:
        return _roster("a", "b", "c")

    result = await _run_picker(read_key, poll, lambda _m: None, cadence=100.0)
    assert result == "b"  # down->b, down->c, up->b, enter selects b


async def test_run_picker_quit_returns_none() -> None:
    async def read_key() -> str:
        return "quit"

    async def poll() -> dict[str, str | None]:
        return _roster("a", "b")

    assert await _run_picker(read_key, poll, lambda _m: None, cadence=100.0) is None


async def test_run_picker_enter_on_empty_roster_does_not_select() -> None:
    # An empty roster + Enter must NOT return (returning None would read as a cancel and drop the
    # user out of the picker); it stays open until an agent arrives or the user quits.
    consumed: list[str] = []
    keys = iter(["enter", "quit"])

    async def read_key() -> str:
        key = next(keys)
        consumed.append(key)
        return key

    async def poll() -> dict[str, str | None]:
        return {}  # no agents online yet

    result = await _run_picker(read_key, poll, lambda _m: None, cadence=100.0)
    assert result is None
    assert consumed == ["enter", "quit"]  # the Enter was ignored; only the later quit ended the loop


async def test_run_picker_resyncs_the_roster_on_a_poll_tick() -> None:
    rosters = iter([_roster("a"), _roster("a", "z")])

    async def poll() -> dict[str, str | None]:
        return next(rosters, _roster("a", "z"))

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
        async def get_agents(self) -> dict[str, object]:
            return {"a": SimpleNamespace(description="alpha"), "b": SimpleNamespace(description="beta")}

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


async def test_live_pick_restores_the_terminal_on_keyboard_interrupt(monkeypatch: object) -> None:
    """A SIGINT surfaced as KeyboardInterrupt cancels cleanly (returns None) and always restores the
    terminal attributes captured on entry — the picker must never leave the tty in cbreak mode.

    (We assert the restore *reinstates the entry attributes* rather than comparing ``tcgetattr``
    round-trips: a macOS pty slave does not report the pre-``setcbreak`` lflag back verbatim.)"""
    import os
    import pty
    import termios
    from types import SimpleNamespace

    master, slave = pty.openpty()
    monkeypatch.setattr("sys.stdin", SimpleNamespace(fileno=lambda: slave, isatty=lambda: True))  # type: ignore[attr-defined]

    entry_attrs: list[Any] = []
    restored_attrs: list[Any] = []
    real_get, real_set = termios.tcgetattr, termios.tcsetattr

    def spy_get(fd: int) -> Any:
        attrs = real_get(fd)
        if fd == slave:
            entry_attrs.append(attrs)
        return attrs

    def spy_set(fd: int, when: int, attrs: Any) -> None:
        if fd == slave:
            restored_attrs.append(attrs)
        real_set(fd, when, attrs)

    monkeypatch.setattr(termios, "tcgetattr", spy_get)  # type: ignore[attr-defined]
    monkeypatch.setattr(termios, "tcsetattr", spy_set)  # type: ignore[attr-defined]

    class _Mesh:
        async def get_agents(self) -> dict[str, object]:
            raise KeyboardInterrupt  # the first poll is interrupted

    client = SimpleNamespace(mesh=_Mesh())
    from calfkit.cli._picker import live_pick

    try:
        assert await live_pick(client, cadence=100.0) is None  # type: ignore[arg-type]
        assert restored_attrs and restored_attrs[-1] == entry_attrs[0]  # restored to the entry snapshot
    finally:
        os.close(master)
        os.close(slave)
