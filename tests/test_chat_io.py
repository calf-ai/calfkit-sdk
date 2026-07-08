"""Unit tests for the ``ck chat`` stdin line reader (``calfkit.cli._chat_io``).

The reader is exercised over an ``os.pipe()`` (the plan's §11 transport) so the
buffering contract — queue-first serving, partial accumulation, multibyte decode,
and EOF — is asserted against a real fd without a tty.
"""

from __future__ import annotations

import asyncio
import io
import os
from collections.abc import Awaitable
from types import SimpleNamespace

import pytest

from calfkit.cli._chat_io import _resolve_stdin_fd, make_key_reader, make_reader


async def _drain(coro: Awaitable[str], timeout: float = 1.0) -> str:
    return await asyncio.wait_for(coro, timeout)


async def test_reads_one_line() -> None:
    r, w = os.pipe()
    try:
        read_line = make_reader(asyncio.get_running_loop(), fd=r)
        os.write(w, b"hi\n")
        assert await _drain(read_line("")) == "hi"
    finally:
        os.close(r)
        os.close(w)


async def test_multiline_one_write_served_from_buffer() -> None:
    """A paste/pipe of several lines in one write: calls 2..N return from the
    buffer with NO further write (the queue-first / no-re-read property)."""
    r, w = os.pipe()
    try:
        read_line = make_reader(asyncio.get_running_loop(), fd=r)
        os.write(w, b"a\nb\nc\n")
        assert await _drain(read_line("")) == "a"
        assert await _drain(read_line("")) == "b"  # no further write
        assert await _drain(read_line("")) == "c"
    finally:
        os.close(r)
        os.close(w)


async def test_partial_then_newline_accumulates() -> None:
    r, w = os.pipe()
    try:
        read_line = make_reader(asyncio.get_running_loop(), fd=r)
        task = asyncio.create_task(read_line(""))
        os.write(w, b"hel")
        await asyncio.sleep(0.05)
        assert not task.done()  # partial line: still waiting, reader stays armed
        os.write(w, b"lo\n")
        assert await _drain(task) == "hello"
    finally:
        os.close(r)
        os.close(w)


async def test_multibyte_char_split_across_writes() -> None:
    r, w = os.pipe()
    try:
        read_line = make_reader(asyncio.get_running_loop(), fd=r)
        task = asyncio.create_task(read_line(""))
        os.write(w, b"\xe2\x9c")  # first two bytes of "✓"
        await asyncio.sleep(0.05)
        assert not task.done()  # incomplete char: decoder holds it, no line yet
        os.write(w, b"\x93\n")  # final byte + newline
        assert await _drain(task) == "✓"
    finally:
        os.close(r)
        os.close(w)


async def test_eof_with_partial_returns_partial_then_raises() -> None:
    r, w = os.pipe()
    try:
        read_line = make_reader(asyncio.get_running_loop(), fd=r)
        os.write(w, b"par")
        os.close(w)  # EOF with a partial (no trailing newline) buffered
        assert await _drain(read_line("")) == "par"
        with pytest.raises(EOFError):
            await _drain(read_line(""))
    finally:
        os.close(r)


async def test_bare_eof_raises() -> None:
    r, w = os.pipe()
    try:
        read_line = make_reader(asyncio.get_running_loop(), fd=r)
        os.close(w)  # EOF, no data
        with pytest.raises(EOFError):
            await _drain(read_line(""))
    finally:
        os.close(r)


async def test_prompt_is_written_to_stdout(capsys: pytest.CaptureFixture[str]) -> None:
    r, w = os.pipe()
    try:
        read_line = make_reader(asyncio.get_running_loop(), fd=r)
        os.write(w, b"x\n")
        await _drain(read_line("you > "))
        assert "you > " in capsys.readouterr().out
    finally:
        os.close(r)
        os.close(w)


def test_resolve_stdin_fd_returns_fileno(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sys.stdin", SimpleNamespace(fileno=lambda: 7))
    assert _resolve_stdin_fd() == 7


def test_resolve_stdin_fd_raises_when_no_real_fd(monkeypatch: pytest.MonkeyPatch) -> None:
    def _bad_fileno() -> int:
        raise io.UnsupportedOperation

    monkeypatch.setattr("sys.stdin", SimpleNamespace(fileno=_bad_fileno))
    with pytest.raises(RuntimeError, match="interactive terminal"):
        _resolve_stdin_fd()


async def test_lazy_fd_resolution_reads_from_stdin(monkeypatch: pytest.MonkeyPatch) -> None:
    r, w = os.pipe()
    try:
        monkeypatch.setattr("sys.stdin", SimpleNamespace(fileno=lambda: r))
        read_line = make_reader(asyncio.get_running_loop())  # fd=None -> resolve to r lazily
        os.write(w, b"lazy\n")
        assert await _drain(read_line("")) == "lazy"
    finally:
        os.close(r)
        os.close(w)


async def test_os_read_error_is_surfaced(monkeypatch: pytest.MonkeyPatch) -> None:
    r, w = os.pipe()
    try:
        read_line = make_reader(asyncio.get_running_loop(), fd=r)

        def _boom(_fd: int, _n: int) -> bytes:
            raise OSError("read failed")

        monkeypatch.setattr("calfkit.cli._chat_io.os.read", _boom)
        os.write(w, b"x\n")  # make the fd readable so the callback fires and hits os.read
        with pytest.raises(OSError, match="read failed"):
            await _drain(read_line(""))
    finally:
        os.close(r)
        os.close(w)


async def test_invalid_utf8_byte_is_replaced_not_hung() -> None:
    # An invalid UTF-8 byte must not raise inside the callback (which would hang the read).
    r, w = os.pipe()
    try:
        read_line = make_reader(asyncio.get_running_loop(), fd=r)
        os.write(w, b"\xff\n")  # invalid lead byte, then newline
        assert await _drain(read_line("")) == "�"
    finally:
        os.close(r)
        os.close(w)


async def test_truncated_multibyte_at_eof_is_replaced() -> None:
    # A multibyte char cut off by EOF is surfaced as U+FFFD (flushed), not silently dropped.
    r, w = os.pipe()
    try:
        read_line = make_reader(asyncio.get_running_loop(), fd=r)
        os.write(w, b"\xe2\x9c")  # first 2 of the 3 bytes of "✓"
        os.close(w)  # EOF before the char completes
        assert await _drain(read_line("")) == "�"
    finally:
        os.close(r)


async def test_cancel_in_flight_read_line_removes_reader_no_leak() -> None:
    # The whole point of the add_reader reader: Ctrl-C cancels a parked read cleanly and the
    # finally de-registers the reader (no fd/reader leak), unlike a to_thread(input) executor.
    r, w = os.pipe()
    try:
        loop = asyncio.get_running_loop()
        read_line = make_reader(loop, fd=r)
        task = asyncio.create_task(read_line(""))  # arms the reader; no data -> parks
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert loop.remove_reader(r) is False  # the read's finally already removed it — no leak
    finally:
        os.close(r)
        os.close(w)


async def test_key_reader_decodes_arrows_enter_and_quit() -> None:
    """The cbreak-mode single-key reader (picker input): arrow escape sequences, Enter, and the
    quit keys (q / Esc / a raw Ctrl-C/Ctrl-D byte) each decode to a token."""
    r, w = os.pipe()
    try:
        read_key = make_key_reader(asyncio.get_running_loop(), fd=r)
        os.write(w, b"\x1b[A")
        assert await _drain(read_key()) == "up"
        os.write(w, b"\x1b[B")
        assert await _drain(read_key()) == "down"
        os.write(w, b"\r")
        assert await _drain(read_key()) == "enter"
        os.write(w, b"q")
        assert await _drain(read_key()) == "quit"
        os.write(w, b"\x03")  # Ctrl-C
        assert await _drain(read_key()) == "quit"
        os.write(w, b"\x1b")  # lone Esc
        assert await _drain(read_key()) == "quit"
    finally:
        os.close(r)
        os.close(w)


async def test_key_reader_splits_a_merged_read_into_separate_keys() -> None:
    """A fast/pasted arrow-then-Enter arriving in one os.read is split into both keys, not dropped."""
    r, w = os.pipe()
    try:
        read_key = make_key_reader(asyncio.get_running_loop(), fd=r)
        os.write(w, b"\x1b[B\r")  # down + enter in one write
        assert await _drain(read_key()) == "down"
        assert await _drain(read_key()) == "enter"  # from the buffer, no further write
    finally:
        os.close(r)
        os.close(w)


async def test_key_reader_decodes_ctrl_d_uppercase_q_and_unmapped_keys() -> None:
    r, w = os.pipe()
    try:
        read_key = make_key_reader(asyncio.get_running_loop(), fd=r)
        os.write(w, b"\x04")
        assert await _drain(read_key()) == "quit"  # Ctrl-D byte
        os.write(w, b"Q")
        assert await _drain(read_key()) == "quit"
        os.write(w, b"x")
        assert await _drain(read_key()) == "other"  # unmapped: ignored, not an error
    finally:
        os.close(r)
        os.close(w)


def test_decode_keys_consumes_a_whole_csi_without_leaking_a_quit() -> None:
    """A CSI sequence longer than 3 bytes must decode to a SINGLE ``other`` — no interior/final byte
    may leak out as a standalone key. In particular a modified F-key like ``ESC[1;2Q`` (Shift+F2) ends
    in ``Q``; the trailing ``Q`` must NOT be re-scanned as ``quit`` and cancel the picker."""
    from calfkit.cli._chat_io import _decode_keys

    ignored_sequences = (
        b"\x1b[1;2Q",  # Shift+F2  (ends in Q — the regression trigger)
        b"\x1b[1;5Q",  # Ctrl+F2
        b"\x1b[1;3Q",  # Alt+F2
        b"\x1b[24;5~",  # Ctrl+F12
        b"\x1b[1;5A",  # Ctrl+Up (modified arrow — not the bare arrow we bind)
        b"\x1b[3~",  # Delete
        b"\x1b[H",  # Home
    )
    for seq in ignored_sequences:
        assert _decode_keys(seq) == ["other"], f"{seq!r} must be one ignored token, got {_decode_keys(seq)}"
    # the bare arrows and a lone Esc must still decode as before
    assert _decode_keys(b"\x1b[A") == ["up"]
    assert _decode_keys(b"\x1b[B") == ["down"]
    assert _decode_keys(b"\x1b") == ["quit"]  # a lone Esc IS the Escape key → quit
    assert _decode_keys(b"\x1b[B\r") == ["down", "enter"]  # merged read still splits correctly
    assert _decode_keys(b"\x1bOA\r") == ["other", "enter"]  # an unhandled ESC-lead seq + a key: both survive
    assert _decode_keys(b"\x1b[") == ["other"]  # a truncated CSI (ESC+bracket, nothing after) never quits


async def test_key_reader_ignores_unrecognized_escape_sequences_instead_of_quitting() -> None:
    """An ESC that begins an escape sequence we don't map (SS3 arrows in application-cursor mode,
    Meta/Alt combos, function keys) must decode to a harmless ``other`` — NOT ``quit``, which would
    cancel the picker out from under the user. Only a lone/terminal ESC (the Escape key) is quit."""
    r, w = os.pipe()
    try:
        read_key = make_key_reader(asyncio.get_running_loop(), fd=r)
        os.write(w, b"\x1bOA")  # SS3 up-arrow (application-cursor mode) — must be ignored, not quit
        assert await _drain(read_key()) == "other"
        os.write(w, b"\x1bOP")  # SS3 F1
        assert await _drain(read_key()) == "other"
        os.write(w, b"\x1ba")  # Meta/Alt-a
        assert await _drain(read_key()) == "other"
        os.write(w, b"\x1b")  # a lone Esc IS the Escape key → quit
        assert await _drain(read_key()) == "quit"
    finally:
        os.close(r)
        os.close(w)
