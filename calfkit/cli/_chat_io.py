"""The ``ck chat`` stdin line reader (plan §8.5).

A cancellable single-line reader built on ``loop.add_reader`` — **no executor
thread**, so Ctrl-C cancels the awaited read cleanly and there is no teardown
join to hang on. The reader keeps a persistent buffer + incremental UTF-8 decoder
in its closure, so a paste/pipe that delivers several lines (or splits a line, or
a multibyte char, across reads) is handled correctly:

- **Queue-first:** a call first serves any line already buffered; only when none
  is buffered does it arm the reader and ``os.read``.
- **Accumulate:** one ``add_reader`` per call (removed only in the ``finally``);
  the callback reads and appends until a ``\\n`` is buffered (a partial line
  leaves the reader armed, level-triggered) or EOF.
- **EOF:** an empty read with a buffered partial returns that partial once, then
  the next call (empty buffer) raises ``EOFError``.

POSIX selector loop only (Windows ``ProactorEventLoop`` has no ``add_reader`` for
stdin). ``read_line`` is the single primitive the orchestration injects, so it is
unit-tested over an ``os.pipe()`` without a tty.
"""

from __future__ import annotations

import asyncio
import codecs
import io
import os
import sys
from collections.abc import Awaitable, Callable
from typing import Literal

_READ_SIZE = 4096


def _resolve_stdin_fd() -> int:
    """The stdin file descriptor, or a clear error if stdin isn't a real fd
    (e.g. a ``CliRunner``/``BytesIO`` stream — those paths never read a line)."""
    try:
        return sys.stdin.fileno()
    except (AttributeError, ValueError, io.UnsupportedOperation) as exc:
        raise RuntimeError("ck chat needs an interactive terminal; stdin has no usable file descriptor.") from exc


def make_reader(loop: asyncio.AbstractEventLoop, fd: int | None = None) -> Callable[[str], Awaitable[str]]:
    """Build the line reader bound to ``loop``. ``fd`` defaults to stdin, resolved
    lazily on the first read (so building the reader touches no fd — the no-reader
    command paths stay usable); tests inject an ``os.pipe()`` read-end."""
    # errors="replace": an invalid/truncated byte becomes U+FFFD rather than raising — a raise
    # inside the add_reader callback would not reach the awaiter (it goes to the loop's exception
    # handler) and the read would hang. Replacement is also the right UX for an interactive REPL.
    decoder = codecs.getincrementaldecoder("utf-8")("replace")
    buffer = ""
    resolved_fd = fd

    async def read_line(prompt: str) -> str:
        nonlocal buffer, resolved_fd
        sys.stdout.write(prompt)
        sys.stdout.flush()

        # Queue-first: serve a buffered line before touching the fd.
        if "\n" in buffer:
            line, _, buffer = buffer.partition("\n")
            return line

        if resolved_fd is None:
            resolved_fd = _resolve_stdin_fd()
        read_fd = resolved_fd

        future: asyncio.Future[bool] = loop.create_future()

        def _on_readable() -> None:
            nonlocal buffer
            if future.done():  # cancel/resolve already won the race
                return  # pragma: no cover - asyncio cancel-vs-readable race; not deterministically reproducible
            try:
                chunk = os.read(read_fd, _READ_SIZE)
            except OSError as exc:  # surface a read error (bad fd, etc.) to the awaiter
                future.set_exception(exc)
                return
            if not chunk:  # EOF
                future.set_result(False)
                return
            buffer += decoder.decode(chunk)
            if "\n" in buffer:
                future.set_result(True)
            # else: partial — stay armed (level-triggered), accumulate on the next read

        loop.add_reader(read_fd, _on_readable)
        try:
            has_line = await future
        finally:
            loop.remove_reader(read_fd)

        if not has_line:  # EOF
            buffer += decoder.decode(b"", final=True)  # flush a trailing truncated char (-> U+FFFD)
            if buffer:  # a typed-ahead partial with no trailing newline: return it once
                line, buffer = buffer, ""
                return line
            raise EOFError
        line, _, buffer = buffer.partition("\n")
        return line

    return read_line


Key = Literal["up", "down", "enter", "quit", "other"]


def _decode_keys(chunk: bytes) -> list[Key]:
    """Split one raw read into picker key tokens. A single ``os.read`` is not a single key: an arrow
    is a multi-byte escape sequence, fast typing / a paste can deliver several keys at once, and an
    unhandled escape sequence must be consumed WHOLE so no interior byte leaks out as a stray key."""
    keys: list[Key] = []
    i, n = 0, len(chunk)
    while i < n:
        if chunk[i : i + 3] == b"\x1b[A":
            keys.append("up")
            i += 3
        elif chunk[i : i + 3] == b"\x1b[B":
            keys.append("down")
            i += 3
        elif chunk[i] == 0x1B and chunk[i + 1 : i + 2] == b"[":
            # An unhandled CSI (modified arrows, Home/End, F-keys, …). Consume THROUGH its final byte
            # (0x40-0x7E) — parameter/intermediate bytes are 0x20-0x3F — so a trailing byte such as the
            # 'Q' of a modified F2 (ESC[1;2Q) is never re-scanned as a standalone quit that cancels.
            j = i + 2
            while j < n and 0x20 <= chunk[j] <= 0x3F:
                j += 1
            if j < n and 0x40 <= chunk[j] <= 0x7E:
                i = j + 1  # consumed a valid final byte
            else:
                # Truncated across a read, OR an ESC/control byte aborted the CSI: stop AT that byte so
                # the next iteration re-parses it as a fresh key (e.g. an ESC[ that precedes a real arrow).
                i = max(j, i + 1)
            keys.append("other")
        elif chunk[i] == 0x1B and chunk[i + 1 : i + 2] == b"O":
            keys.append("other")  # an SS3 sequence (introducer + one final byte): app-mode arrows / F1-F4
            i = min(i + 3, n)
        elif chunk[i] == 0x1B and i + 1 < n:
            keys.append("other")  # ESC + a byte = a Meta/Alt combo (or other esc): ignored, never a cancel
            i += 2
        elif chunk[i] in (0x1B, 0x03, 0x04) or chunk[i : i + 1] in (b"q", b"Q"):
            # A lone/terminal ESC is the Escape key; q/Q is quit; a raw Ctrl-D byte arrives here (ICANON
            # off). Ctrl-C is normally a SIGINT under cbreak (ISIG on) — the 0x03 arm is only defensive.
            keys.append("quit")
            i += 1
        elif chunk[i : i + 1] in (b"\r", b"\n"):
            keys.append("enter")
            i += 1
        else:
            keys.append("other")  # any unmapped key: ignored by the picker, not an error
            i += 1
    return keys


def make_key_reader(loop: asyncio.AbstractEventLoop, fd: int | None = None) -> Callable[[], Awaitable[Key]]:
    """Build a single-key reader for the live picker. The terminal is expected to be in **cbreak**
    mode (the caller sets and restores it — not raw, which would fight ``rich.Live``). One ``os.read``
    may carry several keys, so decoded keys are buffered and returned one per call. Same ``add_reader``
    mechanics as :func:`make_reader` — cancellable, no executor thread; tests inject an ``os.pipe()``
    read-end. An empty read (EOF) yields ``quit``."""
    resolved_fd = fd
    pending: list[Key] = []

    async def read_key() -> Key:
        nonlocal resolved_fd
        if pending:
            return pending.pop(0)
        if resolved_fd is None:
            resolved_fd = _resolve_stdin_fd()
        read_fd = resolved_fd
        future: asyncio.Future[bytes] = loop.create_future()

        def _on_readable() -> None:
            if future.done():  # pragma: no cover - cancel/resolve race
                return
            try:
                chunk = os.read(read_fd, 16)
            except OSError as exc:
                future.set_exception(exc)
                return
            future.set_result(chunk)

        loop.add_reader(read_fd, _on_readable)
        try:
            chunk = await future
        finally:
            loop.remove_reader(read_fd)
        keys = _decode_keys(chunk)
        if not keys:  # empty read == EOF; treat as cancel
            return "quit"
        pending.extend(keys[1:])
        return keys[0]

    return read_key
