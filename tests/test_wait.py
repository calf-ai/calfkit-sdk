"""Unit tests for the CLI wait reporter seam (``calfkit.cli._wait``).

The reporter is a passive push-sink (spec ``docs/designs/cli-live-feedback-spec.md`` §4):
tests feed it snapshots and assert the rendered output via a ``rich.Console`` over a
``StringIO``. ``NULL_REPORTER`` is the silent default that keeps non-opted-in callers
(tests, the detached daemon child) unchanged.
"""

from __future__ import annotations

import re
from io import StringIO

import pytest
from rich.console import Console

from calfkit.cli._wait import NULL_REPORTER, ConsoleWaitReporter, make_reporter_factory

_ANSI = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")


def _non_tty_console() -> tuple[Console, StringIO]:
    buf = StringIO()
    return Console(file=buf, force_terminal=False, width=100), buf


def _tty_console() -> tuple[Console, StringIO]:
    buf = StringIO()
    return Console(file=buf, force_terminal=True, color_system=None, width=100), buf


def _render_text(reporter: ConsoleWaitReporter, width: int = 100) -> str:
    """Render the reporter's current renderable to plain text (ANSI stripped)."""
    buf = StringIO()
    Console(file=buf, width=width, color_system=None).print(reporter._render())
    return _ANSI.sub("", buf.getvalue())


def test_null_reporter_is_a_usable_silent_no_op() -> None:
    # The default parameter value: usable as a context manager, update() does nothing,
    # __exit__ suppresses no exception.
    with NULL_REPORTER as reporter:
        assert reporter is NULL_REPORTER
        reporter.update({"a", "b"})
    assert NULL_REPORTER.__exit__(None, None, None) in (None, False)


def test_non_tty_prints_milestone_line_naming_the_resolved_item() -> None:
    # A non-terminal stream gets append-only milestone lines: one per newly-resolved
    # item, naming it and the running progress (spec §4.3, Option A).
    console, buf = _non_tty_console()
    with ConsoleWaitReporter("Starting 2 nodes", ["a", "b"], console=console) as reporter:
        reporter.update({"a"})
    out = buf.getvalue()
    assert "a (1/2)" in out


def test_non_tty_failure_names_the_still_pending_items() -> None:
    # On an exception exit, the reporter attributes the failure by naming the targets that
    # never resolved (spec §4.4) — b and c here appear only in that failure summary.
    console, buf = _non_tty_console()

    class BoomError(Exception):
        pass

    with pytest.raises(BoomError):
        with ConsoleWaitReporter("Starting 3 nodes", ["a", "b", "c"], console=console) as reporter:
            reporter.update({"a"})
            raise BoomError()
    out = buf.getvalue()
    assert "b" in out
    assert "c" in out


def test_tty_render_marks_resolved_and_shows_pending_targets() -> None:
    # On a terminal the reporter renders a live roster: resolved targets carry a done
    # mark, pending targets are still shown (spec §4.3 checklist mode).
    console, _ = _tty_console()
    reporter = ConsoleWaitReporter("Starting", ["backend", "triage"], console=console)
    reporter.update({"backend"})
    text = _render_text(reporter)
    assert "backend" in text
    assert "triage" in text
    assert "✔" in text  # backend resolved


def test_tty_success_finalizes_with_a_done_summary() -> None:
    # A clean exit collapses the live roster to a single done summary (spec §4.3).
    console, buf = _tty_console()
    with ConsoleWaitReporter("Starting", ["a", "b"], console=console) as reporter:
        reporter.update({"a", "b"})
    out = _ANSI.sub("", buf.getvalue())
    assert "✔" in out
    assert "all 2 done" in out


def test_tty_failure_finalizes_by_naming_pending_targets() -> None:
    console, buf = _tty_console()

    class BoomError(Exception):
        pass

    with pytest.raises(BoomError):
        with ConsoleWaitReporter("Starting", ["a", "b", "c"], console=console) as reporter:
            reporter.update({"a"})
            raise BoomError()
    out = _ANSI.sub("", buf.getvalue())
    assert "still pending" in out
    assert "b" in out and "c" in out


def test_tty_starts_and_stops_the_live_view() -> None:
    # The live view is active for the duration of the wait and torn down on exit.
    console, _ = _tty_console()
    reporter = ConsoleWaitReporter("Starting", ["a"], console=console)
    assert reporter._live is None
    with reporter:
        assert reporter._live is not None
    assert reporter._live is None


def test_single_item_spinner_mode_resolves_and_finalizes() -> None:
    # A 1-item wait (e.g. the broker spawn) is a degenerate checklist: it resolves and
    # finalizes like any other (spec §4.1 spinner mode).
    console, buf = _non_tty_console()
    with ConsoleWaitReporter("Starting dev broker", ["dev broker"], console=console) as reporter:
        reporter.update({"dev broker"})
    out = buf.getvalue()
    assert "dev broker (1/1)" in out


def test_pre_done_targets_render_as_already_done_and_count_toward_total() -> None:
    # Reused agents (pre_done) show done from the start and are counted in the total, while only the
    # waited set drives completion (spec §4.5) — so a mixed launch reads e.g. "1/2" immediately.
    console, _ = _tty_console()
    reporter = ConsoleWaitReporter("Starting", ["triage"], pre_done=["backend"], console=console)
    text = _render_text(reporter)
    assert "✔ backend" in text  # reused agent shown done immediately
    assert "triage" in text  # still-waiting agent shown
    assert "1/2" in text  # pre_done counts toward the total from the start


def test_make_reporter_factory_builds_a_console_reporter_titled_by_launched_count() -> None:
    # The command-layer helper: a gate_launched_ready factory that titles the reporter by the
    # launched count and carries the reused set as pre_done.
    console, buf = _non_tty_console()
    factory = make_reporter_factory(lambda n: f"Starting {n} node(s)", console=console)
    with factory(waiting=["a", "b"], pre_done=["old"]):
        pass
    assert "Starting 2 node(s)" in buf.getvalue()


def test_reporter_factory_rejects_positional_args_to_prevent_transposition() -> None:
    # The factory is keyword-only (ReporterFactory Protocol) so the two same-typed lists — waiting vs
    # pre_done — can never be silently transposed: a positional call is a TypeError, not a mislabel.
    factory = make_reporter_factory(lambda n: f"Starting {n}")
    with pytest.raises(TypeError):
        factory(["a"], [])  # type: ignore[call-arg]


def test_tty_success_finalize_uses_the_success_label() -> None:
    # The finalize is titled (context), not a bare "all N done" (review round 1).
    console, buf = _tty_console()
    with ConsoleWaitReporter("Starting dev broker", ["dev broker"], success_label="dev broker ready", console=console):
        pass
    assert "✔ dev broker ready" in _ANSI.sub("", buf.getvalue())


def test_reporter_isolates_live_faults_from_the_wait() -> None:
    # A rich render/teardown glitch in the live view must never abort or mask the wait it decorates.
    console, _ = _tty_console()

    class _BoomLive:
        def update(self, *a: object, **k: object) -> None:
            raise RuntimeError("render boom")

        def stop(self) -> None:
            raise RuntimeError("stop boom")

    reporter = ConsoleWaitReporter("t", ["a"], console=console)
    with reporter:
        reporter._live = _BoomLive()  # type: ignore[assignment]
        reporter.update({"a"})  # guarded: must not raise
    # leaving the `with` (which calls _live.stop) must not raise either


def test_reporter_live_teardown_glitch_does_not_mask_the_waits_exception() -> None:
    # The load-bearing property (_wait.py __exit__): when the wait itself raises AND the live teardown
    # also glitches, the wait's OWN exception must propagate — the cosmetic stop() fault must not mask it.
    console, _ = _tty_console()

    class _BoomStopLive:
        def update(self, *a: object, **k: object) -> None: ...

        def stop(self) -> None:
            raise RuntimeError("stop boom")

    class DomainError(Exception):
        pass

    with pytest.raises(DomainError):  # the wait's DomainError, not the teardown's RuntimeError
        with ConsoleWaitReporter("t", ["a"], console=console) as reporter:
            reporter._live = _BoomStopLive()  # type: ignore[assignment]
            raise DomainError("the real failure")


def test_make_reporter_factory_threads_the_success_label_into_the_finalize() -> None:
    # The `success=` callable must reach the reporter's success finalize, titled by the launched count
    # (n = len(waiting)) — the wiring both production call sites depend on (dev.py / _chat.py).
    console, buf = _tty_console()
    factory = make_reporter_factory(lambda n: f"Starting {n}", success=lambda n: f"all {n} node(s) online", console=console)
    with factory(waiting=["a", "b"], pre_done=[]):
        pass  # clean exit -> success finalize
    assert "✔ all 2 node(s) online" in _ANSI.sub("", buf.getvalue())
