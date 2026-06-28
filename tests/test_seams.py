"""PR-6 step 1 — the seam chain runners (fault-rail spec §6.8 / §6.5).

Pure, transport-free unit tests for ``calfkit.nodes._seams``: ``run_chain``
(every seam) and ``run_chain_guarded`` (``on_node_error`` only). No broker, no
node — just callables and the chain semantics. See
notes/pr6-fault-rail-implementation-plan.md §3 step 1 / §4 layer 1.
"""

from __future__ import annotations

import asyncio

import pytest

from calfkit.exceptions import NodeFaultError
from calfkit.models.error_report import ErrorReport, FaultTypes
from calfkit.nodes._seams import _Minted, run_chain, run_chain_guarded


class TestRunChain:
    async def test_first_non_none_wins_and_short_circuits(self) -> None:
        # The defining rule (spec §6.8): in-order, first non-None return wins and
        # stops the chain — declining handlers (None) advance; the winner's
        # successors never run.
        calls: list[str] = []

        def declines(ctx: object) -> str | None:
            calls.append("a")
            return None

        def handles(ctx: object) -> str | None:
            calls.append("b")
            return "from-b"

        def never_runs(ctx: object) -> str | None:
            calls.append("c")
            return "from-c"

        result = await run_chain([declines, handles, never_runs], object())

        assert result == "from-b"
        assert calls == ["a", "b"]  # c never ran

    async def test_awaits_async_handlers(self) -> None:
        # Handlers may be sync or async (spec §6.8); an async handler's result is
        # awaited, not returned as a bare coroutine.
        async def handles(ctx: object) -> str:
            return "async-result"

        result = await run_chain([handles], object())

        assert result == "async-result"

    async def test_all_declined_returns_none(self) -> None:
        # Every handler declining (None) — and the empty chain — yield None: the
        # "no one was there" signal the pipeline reads as decline/escalate.
        assert await run_chain([lambda ctx: None, lambda ctx: None], object()) is None
        assert await run_chain([], object()) is None

    async def test_passes_seam_args_through_to_handlers(self) -> None:
        # run_chain is generic over the seam's arity: a two-arg seam
        # (ctx, fault/output) receives both, in order.
        seen: list[tuple[object, object]] = []

        def handler(ctx: object, extra: object) -> str:
            seen.append((ctx, extra))
            return "ok"

        ctx, extra = object(), object()
        assert await run_chain([handler], ctx, extra) == "ok"
        assert seen == [(ctx, extra)]


class TestRunChainGuarded:
    async def test_returns_first_recovery_value(self) -> None:
        # The happy path mirrors run_chain: the first non-None handler return is
        # the recovery value (the skeleton coerces it to the node's output).
        report = ErrorReport(error_type="calf.exception")

        async def recovers(ctx: object, fault: ErrorReport) -> str:
            return "recovered"

        result = await run_chain_guarded([recovers], object(), report)

        assert result == "recovered"

    async def test_all_handlers_decline_returns_none(self) -> None:
        # No recovery (every handler returns None) → None, which the skeleton reads
        # as "escalate the original fault" (spec §6.5/§6.8). Empty chain too.
        report = ErrorReport(error_type="calf.exception")
        assert await run_chain_guarded([lambda c, f: None], object(), report) is None
        assert await run_chain_guarded([], object(), report) is None

    async def test_accidental_raise_declines_and_chain_continues(self) -> None:
        # §6.5: an accidental (non-NodeFaultError) raise in an on_node_error
        # handler is that handler DECLINING — logged, not propagated — and the
        # chain advances (deliberate deviation from pluggy's propagate-on-raise).
        report = ErrorReport(error_type="calf.exception")

        def boom(ctx: object, fault: ErrorReport) -> str:
            raise RuntimeError("seam bug")

        def recovers(ctx: object, fault: ErrorReport) -> str:
            return "recovered-after-boom"

        result = await run_chain_guarded([boom, recovers], object(), report)

        assert result == "recovered-after-boom"

    async def test_cancelled_error_propagates_not_swallowed_as_decline(self) -> None:
        # §6.5: the decline-on-raise rule catches ``Exception``, NOT ``BaseException``. A
        # ``CancelledError`` (cooperative cancellation) must propagate OUT of the guarded
        # chain — never be mistaken for an accidental decline and swallowed — and a later
        # handler that WOULD recover must not run (the cancellation wins).
        report = ErrorReport(error_type="calf.exception")

        def cancels(ctx: object, fault: ErrorReport) -> str:
            raise asyncio.CancelledError()

        def never_runs(ctx: object, fault: ErrorReport) -> str:
            return "should-not-recover"

        with pytest.raises(asyncio.CancelledError):
            await run_chain_guarded([cancels, never_runs], object(), report)
        # the cancellation is NOT recorded as a seam-error decline breadcrumb
        assert FaultTypes.SEAM_ERRORS not in report.details

    async def test_node_fault_error_mints_and_stops_chain(self) -> None:
        # §6.5 mint rule (inside the error seam): a NodeFaultError raised in a
        # handler is a deliberate fault — the chain STOPS and the minted report is
        # returned as _Minted, with the original fault chained via causes.
        original = ErrorReport(error_type="calf.exception", message="orig")

        def mints(ctx: object, fault: ErrorReport) -> str:
            raise NodeFaultError("billing.quota_exceeded", message="minted")

        def never_runs(ctx: object, fault: ErrorReport) -> str:
            return "should-not-run"

        result = await run_chain_guarded([mints, never_runs], object(), original)

        assert isinstance(result, _Minted)
        assert result.report.error_type == "billing.quota_exceeded"
        assert result.report.message == "minted"
        # the original fault is chained as a cause (the `raise ... from` analog, §4.4)
        assert [c.report_id for c in result.report.causes] == [original.report_id]

    async def test_accidental_raise_is_noted_in_report_details(self) -> None:
        # §6.5 / scenario 9: when every handler declines (a raise counts as a
        # decline), the ORIGINAL fault escalates (None) — and the seam failure is
        # noted on that report's details so the escalation carries the breadcrumb.
        report = ErrorReport(error_type="calf.exception")

        def boom(ctx: object, fault: ErrorReport) -> str:
            raise RuntimeError("seam bug")

        result = await run_chain_guarded([boom], object(), report)

        assert result is None  # all declined → the original escalates
        notes = report.details.get(FaultTypes.SEAM_ERRORS)
        assert notes is not None and len(notes) == 1
        assert notes[0]["exc_type"] == "RuntimeError"
        assert "seam bug" in notes[0]["exc_message"]
