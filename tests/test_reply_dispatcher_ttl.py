"""Unit-scope contracts for the opt-in reply TTL backstop in ``_ReplyDispatcher``.

The TTL is a deliberate, default-off, caller-responsibility feature: when a
``reply_ttl`` is supplied, an unanswered future is evicted from ``_pending`` and
fails with :class:`ReplyExpiredError` so an abandoned/lost reply cannot pin
client state forever. With the default (``None``) there is no eviction at all.

These tests construct ``_ReplyDispatcher`` directly and drive the real
``asyncio`` timer path (no Kafka), which is the lightest-weight way to exercise
``expect()`` / ``_evict`` / ``_discard`` end-to-end.
"""

from __future__ import annotations

import asyncio
import logging
import pickle

import pytest

from calfkit.client import Client, InvocationHandle
from calfkit.client.reply_dispatcher import _PendingEntry, _ReplyDispatcher
from calfkit.exceptions import ReplyExpiredError
from calfkit.models.envelope import Envelope
from calfkit.models.session_context import CallFrameStack, SessionRunContext, WorkflowState
from calfkit.models.state import State

REPLY_DISPATCHER_LOGGER = "calfkit.client.reply_dispatcher"


@pytest.mark.parametrize("bad_ttl", [0, 0.0, -1.0, -0.001, float("nan"), float("inf"), float("-inf")])
def test_invalid_reply_ttl_rejected(bad_ttl):
    """``reply_ttl`` must be a positive, finite number of seconds (or ``None``).
    Zero / negative would evict every future before any reply could arrive; nan /
    inf produce undefined timer behavior. Reject at construction with a clear error."""
    with pytest.raises(ValueError, match="reply_ttl"):
        _ReplyDispatcher(reply_ttl=bad_ttl)


def test_valid_reply_ttl_accepted():
    """A positive finite TTL (and ``None``) construct without error."""
    assert _ReplyDispatcher(reply_ttl=0.5)._reply_ttl == 0.5
    assert _ReplyDispatcher(reply_ttl=None)._reply_ttl is None
    assert _ReplyDispatcher()._reply_ttl is None


def test_connect_threads_reply_ttl_to_dispatcher():
    """``Client.connect(reply_ttl=X)`` reaches the dispatcher — the public knob a
    user actually touches. A dropped/renamed kwarg would silently produce a
    TTL-less dispatcher while every direct-dispatcher test stayed green."""
    client = Client.connect("localhost", reply_ttl=12.5)
    assert client._dispatcher._reply_ttl == 12.5


def test_connect_default_reply_ttl_is_none():
    """The default leaves eviction disabled (no silent safety ceiling)."""
    client = Client.connect("localhost")
    assert client._dispatcher._reply_ttl is None


def test_reply_expired_error_is_picklable():
    """The error must survive a pickle roundtrip (it may cross process/Kafka
    boundaries) with its ``correlation_id`` and ``ttl`` intact."""
    error = ReplyExpiredError("cid-abc", 0.25)
    restored = pickle.loads(pickle.dumps(error))
    assert isinstance(restored, ReplyExpiredError)
    assert restored.correlation_id == "cid-abc"
    assert restored.ttl == 0.25
    assert str(restored) == str(error)


async def test_ttl_evicts_unanswered_future():
    """With a small ``reply_ttl``, an unanswered future raises ``ReplyExpiredError``
    after the TTL elapses and its entry is removed from ``_pending``."""
    ttl = 0.05
    dispatcher = _ReplyDispatcher(reply_ttl=ttl)
    cid = "cid-expires"

    future = dispatcher.expect(cid)
    assert cid in dispatcher._pending

    await asyncio.sleep(ttl * 3)

    assert future.done()
    with pytest.raises(ReplyExpiredError) as exc_info:
        future.result()
    assert exc_info.value.correlation_id == cid
    assert exc_info.value.ttl == ttl
    assert dispatcher._pending == {}, "expired entry must be evicted from _pending"


async def test_default_no_ttl_never_evicts():
    """With the default ``reply_ttl=None`` there is no eviction: after the same
    wait the future is still pending and the entry remains in ``_pending``."""
    dispatcher = _ReplyDispatcher()
    cid = "cid-never-evicts"

    future = dispatcher.expect(cid)

    await asyncio.sleep(0.15)

    assert not future.done(), "no TTL means the future must stay pending"
    assert cid in dispatcher._pending, "no TTL means the entry must remain registered"


async def test_normal_completion_cancels_timer():
    """A reply arriving before the TTL resolves the future, removes the entry, and
    cancels the scheduled timer so no later ``ReplyExpiredError`` is raised."""
    ttl = 0.05
    dispatcher = _ReplyDispatcher(reply_ttl=ttl)
    cid = "cid-completes"

    future = dispatcher.expect(cid)
    entry = dispatcher._pending[cid]
    assert entry.timer is not None

    sentinel = object()
    future.set_result(sentinel)  # type: ignore[arg-type]
    # Let the done-callback (_discard) run.
    await asyncio.sleep(0)

    assert dispatcher._pending == {}, "completed entry must be discarded"

    # Wait past the original TTL; the cancelled timer must not fire / re-raise.
    await asyncio.sleep(ttl * 3)
    assert future.result() is sentinel
    assert dispatcher._pending == {}


async def test_evicted_abandoned_future_does_not_log_never_retrieved():
    """An abandoned ``start`` handle — a future nobody awaits — must be
    evicted *silently*.

    The TTL exists precisely for the abandoned-handle case, so eviction must not
    spam ``asyncio``'s "Future exception was never retrieved" error when the
    (now unreferenced) future is garbage-collected. The dispatcher is responsible
    for retrieving the ``ReplyExpiredError`` it sets; an awaiting caller still
    receives it unchanged.
    """
    import gc

    cid = "cid-abandoned"
    loop = asyncio.get_running_loop()
    # Capture the FULL context dicts and chain to the previous handler so this
    # test neither suppresses nor is fooled by a sibling test's stray future
    # collected during our gc.collect(). The assertion is then scoped to *our*
    # evicted future via the ReplyExpiredError it would carry — making the test
    # order-independent.
    captured: list[dict] = []
    previous = loop.get_exception_handler()

    def _handler(active_loop, ctx):
        captured.append(ctx)
        if previous is not None:
            previous(active_loop, ctx)

    loop.set_exception_handler(_handler)
    try:
        dispatcher = _ReplyDispatcher(reply_ttl=0.02)
        # Intentionally drop the future reference to simulate an abandoned handle.
        dispatcher.expect(cid)
        await asyncio.sleep(0.06)  # let the TTL fire and _discard pop the entry
        gc.collect()  # force the unreferenced future's __del__
        await asyncio.sleep(0)  # flush any scheduled exception-handler callback
    finally:
        loop.set_exception_handler(previous)

    ours = [ctx for ctx in captured if isinstance(ctx.get("exception"), ReplyExpiredError) and ctx["exception"].correlation_id == cid]
    assert not ours, f"our evicted abandoned future logged an unretrieved-exception error: {ours}"


async def test_invocation_handle_result_raises_reply_expired_on_eviction():
    """The user-facing TTL contract: awaiting an ``InvocationHandle`` whose reply
    is evicted raises ``ReplyExpiredError`` (carrying ``correlation_id``/``ttl``),
    not a bare ``CancelledError``. ``_evict`` sets the exception on the very
    future ``InvocationHandle.result()`` awaits, so this exercises that real path."""
    ttl = 0.05
    dispatcher = _ReplyDispatcher(reply_ttl=ttl)
    cid = "cid-handle-evict"

    future = dispatcher.expect(cid)
    handle: InvocationHandle = InvocationHandle(correlation_id=cid, topic="agent.input", reply_topic="reply", _future=future)

    with pytest.raises(ReplyExpiredError) as exc_info:
        await handle.result()

    assert exc_info.value.correlation_id == cid
    assert exc_info.value.ttl == ttl
    assert dispatcher._pending == {}, "evicted entry must be removed from _pending"


async def test_late_reply_after_eviction_is_dropped_quietly(caplog):
    """A reply that arrives for a *registered but already-done* future (the narrow
    race where eviction set the exception but ``_discard`` has not yet popped the
    entry) is dropped at DEBUG via the ``future.done()`` guard — NOT surfaced as
    the "no pending future" WARNING reserved for an unknown correlation_id."""
    dispatcher = _ReplyDispatcher()
    cid = "cid-late"

    loop = asyncio.get_running_loop()
    evicted = loop.create_future()
    evicted.set_exception(ReplyExpiredError(cid, 0.01))
    evicted.exception()  # retrieve so this test's own future never warns
    # Inject the done-but-still-registered state directly (the race window).
    dispatcher._pending[cid] = _PendingEntry(future=evicted, timer=None)

    envelope = Envelope(
        context=SessionRunContext(state=State(), deps={}),
        internal_workflow_state=WorkflowState(call_stack=CallFrameStack()),
    )
    with caplog.at_level(logging.DEBUG, logger=REPLY_DISPATCHER_LOGGER):
        dispatcher._on_reply(envelope, cid, {})

    messages = [r.getMessage() for r in caplog.records]
    assert not any("no pending future" in m for m in messages), f"late reply must not warn 'no pending future': {messages}"
    assert any("already done" in m for m in messages), f"expected the DEBUG 'already done' drop: {messages}"
    assert cid in dispatcher._pending, "the done-future guard must return early without popping the entry"


async def test_close_cancels_active_timers():
    """``close()`` with TTL timers still scheduled cancels both the futures and
    their timers, so no later ``ReplyExpiredError`` / loop exception fires."""
    ttl = 0.05
    dispatcher = _ReplyDispatcher(reply_ttl=ttl)

    f1 = dispatcher.expect("c1")
    f2 = dispatcher.expect("c2")
    assert dispatcher._pending["c1"].timer is not None
    assert dispatcher._pending["c2"].timer is not None

    dispatcher.close()

    assert dispatcher._pending == {}, "close() must clear the registry"
    assert f1.cancelled() and f2.cancelled(), "close() must cancel pending futures"

    # Wait past the original TTL: the cancelled timers must not fire / re-raise.
    await asyncio.sleep(ttl * 3)
    assert f1.cancelled() and f2.cancelled()


async def test_close_retrieves_exception_for_already_evicted_future():
    """REGRESSION: ``close()`` during the evict→discard window must retrieve the
    eviction exception.

    ``_evict`` sets the exception and schedules ``_discard`` via ``call_soon`` (not
    synchronously), so there is a window where the future is ``done()`` with
    ``ReplyExpiredError`` set but still registered and ``_discard`` has not run. If
    ``close()`` runs in that window without retrieving the exception, asyncio logs
    "Future exception was never retrieved" on GC — the very warning the TTL
    machinery exists to suppress. ``test_close_cancels_active_timers`` only covers
    still-pending futures, so it misses this."""
    import gc

    cid = "cid-close-window"
    loop = asyncio.get_running_loop()
    captured: list[dict] = []
    previous = loop.get_exception_handler()

    def _handler(active_loop, ctx):
        captured.append(ctx)
        if previous is not None:
            previous(active_loop, ctx)

    loop.set_exception_handler(_handler)
    try:
        dispatcher = _ReplyDispatcher(reply_ttl=10.0)
        dispatcher.expect(cid)  # drop the future ref (abandoned handle)
        # Force eviction synchronously: sets the exception and schedules _discard
        # via call_soon, putting us in the window where the future is done() but
        # still registered and _discard has not yet run.
        dispatcher._evict(cid, 10.0)
        assert cid in dispatcher._pending and dispatcher._pending[cid].future.done()
        dispatcher.close()  # must retrieve the exception in this window
        gc.collect()
        await asyncio.sleep(0)
    finally:
        loop.set_exception_handler(previous)

    ours = [c for c in captured if isinstance(c.get("exception"), ReplyExpiredError) and c["exception"].correlation_id == cid]
    assert not ours, f"close() leaked an unretrieved eviction exception: {ours}"


async def test_duplicate_correlation_id_raises_before_scheduling_a_timer():
    """``expect()`` rejects a duplicate correlation_id BEFORE creating a
    future/timer, so the RuntimeError path leaves exactly one entry and no
    orphaned second timer under the TTL model."""
    ttl = 0.05
    dispatcher = _ReplyDispatcher(reply_ttl=ttl)
    cid = "cid-dup"

    first = dispatcher.expect(cid)
    with pytest.raises(RuntimeError, match="Duplicate correlation_id"):
        dispatcher.expect(cid)

    assert list(dispatcher._pending) == [cid], "the rejected second call must not register a second entry"
    assert dispatcher._pending[cid].future is first

    # Past the TTL: exactly one eviction fires; no orphaned timer raises.
    await asyncio.sleep(ttl * 3)
    assert first.done()
    with pytest.raises(ReplyExpiredError):
        first.result()
    assert dispatcher._pending == {}
