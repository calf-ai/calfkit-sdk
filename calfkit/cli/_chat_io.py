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
