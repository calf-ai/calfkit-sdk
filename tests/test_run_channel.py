"""Increment E — the _RunChannel two-storage + wake model (spec §2.8).

Intermediates ride a consume-once queue with its OWN signal; the terminal is a cached, replayable
slot. terminal/close sets BOTH signals (a parked stream() wakes on completion AND on error-close); an
intermediate push sets ONLY the queue signal (never spuriously wakes a parked result()) and no-ops
once closed. stream() drains the queue (backlog first) then yields the cached terminal last.
"""

from __future__ import annotations

import asyncio

import pytest

from calfkit.client.hub import InvocationHandle, _RunChannel
from calfkit.exceptions import ClientClosedError
from calfkit.models.error_report import ErrorReport
from calfkit.models.payload import TextPart
from calfkit.models.step import AgentMessageEvent

from calfkit.client.events import RunFailed  # isort: skip — terminal that needs no envelope


def _terminal(cid: str = "c") -> RunFailed:
    return RunFailed(report=ErrorReport(error_type="x"), correlation_id=cid)


def _step(text: str = "hi") -> AgentMessageEvent:
    return AgentMessageEvent(parts=[TextPart(text=text)])


def _handle(ch: _RunChannel, *, output_type: type = str) -> InvocationHandle:
    return InvocationHandle(correlation_id="c", _channel=ch, _output_type=output_type)


class TestRunChannelTwoStorage:
    async def test_buffered_intermediates_then_terminal_in_order(self) -> None:
        ch = _RunChannel()
        ch.push_intermediate(_step("a"))
        ch.push_intermediate(_step("b"))
        term = _terminal()
        ch.push(term)
        got = [e async for e in _handle(ch).stream()]
        assert [type(e).__name__ for e in got] == ["AgentMessageEvent", "AgentMessageEvent", "RunFailed"]
        assert (got[0].parts[0].text, got[1].parts[0].text) == ("a", "b")
        assert got[-1] is term

    async def test_intermediate_push_noops_once_closed(self) -> None:
        ch = _RunChannel()
        ch.push(_terminal())
        ch.push_intermediate(_step("late"))  # a post-terminal/reordered step is dropped (§3.3)
        got = [e async for e in _handle(ch).stream()]
        assert [type(e).__name__ for e in got] == ["RunFailed"]

    async def test_late_stream_drains_backlog_once(self) -> None:
        ch = _RunChannel()
        ch.push_intermediate(_step("a"))
        ch.push(_terminal())
        got = [e async for e in _handle(ch).stream()]
        assert [type(e).__name__ for e in got] == ["AgentMessageEvent", "RunFailed"]

    async def test_parked_stream_wakes_on_terminal(self) -> None:
        ch = _RunChannel()

        async def _drive() -> None:
            await asyncio.sleep(0.01)
            ch.push_intermediate(_step("a"))
            await asyncio.sleep(0.01)
            ch.push(_terminal())

        task = asyncio.create_task(_drive())
        got = [e async for e in _handle(ch).stream()]
        await task
        assert [type(e).__name__ for e in got] == ["AgentMessageEvent", "RunFailed"]

    async def test_parked_stream_wakes_on_error_close(self) -> None:
        ch = _RunChannel()

        async def _close() -> None:
            await asyncio.sleep(0.01)
            ch.close_with(ClientClosedError(correlation_id="c"))

        task = asyncio.create_task(_close())
        with pytest.raises(ClientClosedError):
            _ = [e async for e in _handle(ch).stream()]
        await task

    async def test_intermediate_push_does_not_wake_a_parked_result(self) -> None:
        # an intermediate push sets ONLY the queue signal — a parked result() (terminal signal) stays parked.
        ch = _RunChannel()
        ch.push_intermediate(_step("a"))
        with pytest.raises((TimeoutError, asyncio.TimeoutError)):
            await asyncio.wait_for(ch.await_terminal(), timeout=0.05)

    async def test_early_break_releases_stream_active(self) -> None:
        from contextlib import aclosing

        ch = _RunChannel()
        ch.push_intermediate(_step("a"))
        ch.push(_terminal())
        h = _handle(ch)
        # The spec's "async with-scoped" iterator: wrapping stream() in aclosing() runs its finally on
        # early break, releasing the single-live-stream guard. (A bare `async for ... break` would leak
        # the guard until GC — aclosing is the documented early-exit consumption form.)
        async with aclosing(h.stream()) as s:
            async for _e in s:
                break
        assert h._stream_active is False
        # a second stream() works — the consumed intermediate is gone, the terminal still replays.
        got = [e async for e in h.stream()]
        assert [type(e).__name__ for e in got] == ["RunFailed"]

    async def test_second_concurrent_stream_raises(self) -> None:
        ch = _RunChannel()
        ch.push(_terminal())
        h = _handle(ch)
        s1 = h.stream()
        await s1.__anext__()  # activate the first stream
        with pytest.raises(RuntimeError, match="one live stream"):
            await h.stream().__anext__()
        await s1.aclose()

    async def test_steps_are_raw_not_output_type_coerced(self) -> None:
        # stream() yields the channel's raw events; the handle's output_type only projects result()'s
        # terminal, never the steps (spec §3.2/§3.4) — an AgentMessageEvent stays an AgentMessageEvent under output_type=str.
        ch = _RunChannel()
        ch.push_intermediate(AgentMessageEvent(parts=[TextPart(text="raw")]))
        ch.push(_terminal())
        got = [e async for e in _handle(ch, output_type=str).stream()]
        assert isinstance(got[0], AgentMessageEvent)
        assert got[0].parts[0].text == "raw"
