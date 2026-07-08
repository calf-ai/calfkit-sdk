"""The CLI wait reporter seam (spec ``docs/designs/cli-live-feedback-spec.md`` §4).

A :class:`WaitReporter` is a passive, push-driven sink a foreground CLI wait uses to
visualize progress toward a *known* set of named targets and, on failure, attribute the
wait to the exact item that never resolved. The wait owns the data and completion clock;
the reporter only renders. :data:`NULL_REPORTER` is the silent default so callers that do
not opt in (unit tests, the detached daemon child) are unaffected.
"""

from __future__ import annotations

from collections.abc import Callable, Collection, Sequence
from types import TracebackType
from typing import Protocol

from rich.console import Console, Group, RenderableType
from rich.live import Live
from rich.spinner import Spinner
from rich.text import Text


class WaitReporter(Protocol):
    """The sink a wait pushes progress snapshots to. Used as a context manager so
    teardown and success/failure rendering are exception-driven; the wait's only
    obligation inside its loop is a single :meth:`update` call."""

    def __enter__(self) -> WaitReporter: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...

    def update(self, done: Collection[str]) -> None: ...


class ConsoleWaitReporter:
    """Renders a wait's progress toward a known set of named targets via Rich.

    ``waiting`` are the targets being waited on (``pending -> done`` as :meth:`update`
    is called); ``pre_done`` are already-satisfied targets, shown done from the start and
    never updated (e.g. reused agents). Self-branches on ``console.is_terminal``: an
    interactive terminal gets a live in-place view, any other stream gets append-only
    milestone lines.
    """

    def __init__(
        self,
        title: str,
        waiting: Sequence[str],
        *,
        pre_done: Sequence[str] = (),
        console: Console | None = None,
    ) -> None:
        self._title = title
        self._waiting = list(waiting)
        self._pre_done = list(pre_done)
        self._console = console if console is not None else Console()
        self._done: set[str] = set(self._pre_done)
        self._total = len(self._waiting) + len(self._pre_done)
        self._live: Live | None = None

    def __enter__(self) -> ConsoleWaitReporter:
        if self._console.is_terminal:
            self._live = Live(self._render(), console=self._console, transient=True)
            self._live.start()
        else:
            self._console.print(self._title, markup=False, highlight=False)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        pending = [name for name in self._waiting if name not in self._done]
        if self._console.is_terminal:
            if self._live is not None:
                self._live.stop()
                self._live = None
            if exc_type is not None:
                self._console.print(f"✘ still pending: {', '.join(pending)}", markup=False, highlight=False)
            else:
                self._console.print(f"✔ all {self._total} done", markup=False, highlight=False)
        elif exc_type is not None and pending:
            self._console.print(f"still pending: {', '.join(pending)}", markup=False, highlight=False)
        return None

    def update(self, done: Collection[str]) -> None:
        newly = [name for name in self._waiting if name in done and name not in self._done]
        for name in newly:
            self._done.add(name)
            if not self._console.is_terminal:
                self._console.print(f"{name} ({len(self._done)}/{self._total})", markup=False, highlight=False)
        if self._console.is_terminal and self._live is not None:
            self._live.update(self._render())

    def _display_order(self) -> list[str]:
        # pre_done (reused context) first, then the targets being waited on.
        return [*self._pre_done, *self._waiting]

    def _render(self) -> RenderableType:
        rows: list[RenderableType] = []
        for name in self._display_order():
            if name in self._done:
                rows.append(Text(f"✔ {name}"))
            else:
                rows.append(Spinner("dots", text=Text(f" {name}")))
        header = Text(f"{self._title}  {len(self._done)}/{self._total}")
        return Group(header, *rows)


class _NullWaitReporter:
    """The silent default: every method is a no-op."""

    def __enter__(self) -> _NullWaitReporter:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        return None

    def update(self, done: Collection[str]) -> None:
        return None


NULL_REPORTER: WaitReporter = _NullWaitReporter()


def make_reporter_factory(
    title: Callable[[int], str],
    *,
    console: Console | None = None,
) -> Callable[[list[str], list[str]], WaitReporter]:
    """Build a ``gate_launched_ready`` reporter factory: given the launched (``waiting``) and reused
    (``pre_done``) name lists, construct a :class:`ConsoleWaitReporter` whose title is
    ``title(len(waiting))``. The command layer supplies the title wording, so this module stays
    domain-neutral."""

    def _factory(waiting: list[str], pre_done: list[str]) -> WaitReporter:
        return ConsoleWaitReporter(title(len(waiting)), waiting, pre_done=pre_done, console=console)

    return _factory
