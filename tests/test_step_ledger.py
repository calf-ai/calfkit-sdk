"""The step-fact vocabulary + ``HopStepLedger`` in isolation (caller-side step-emission spec §3.1).

Unit tests for ``calfkit/nodes/_steps.py`` — the module is exercised UNWIRED (nothing in the kernel
threads it yet at this increment): fact shapes and their build-time coercion (mint totality), the
``Observed`` wrapper, the ledger's mint methods (``folded`` derivation, ``fold_failed``, the
``absorb`` fact table, ``note_dispatch`` per action variant), and ``flush`` (drain semantics, the
empty/rootless no-ops checked BEFORE the broker is touched, the terminal gate, identity stamped
once, and flush's own totality — NO internal best-effort wrap; the log-and-drop guard is the
kernel's exit-flush helper, not the ledger's).
"""

from __future__ import annotations

import inspect
from typing import Any

import pytest

from calfkit._protocol import HDR_EMITTER, HDR_EMITTER_KIND, HDR_WIRE
from calfkit.models.actions import Call, ReturnCall, TailCall
from calfkit.models.marker import ToolCallMarker
from calfkit.models.payload import TextPart, retry_text_part
from calfkit.models.state import State
from calfkit.models.step import AgentMessageStep, HandoffStep, StepMessage, ToolCallStep, ToolResultStep
from calfkit.nodes._steps import DeniedCall, HandedOff, HopStepLedger, Observed, Said
from tests.fakes import CaptureBroker

_MARKER = ToolCallMarker(tool_name="search", tool_call_id="c1", args={"q": "x"})
_M2 = ToolCallMarker(tool_name="lookup", tool_call_id="c2", args={"id": 7})


async def _flush(ledger: HopStepLedger, broker: Any, *, disposition: Any = None, root_callback: str | None = "client.return") -> None:
    """Flush with default identity args — tests override only what they assert on."""
    await ledger.flush(
        broker,
        disposition=disposition,
        depth=1,
        frame_id="F1",
        correlation_id="cid-1",
        emitter="agent-a",
        emitter_kind="agent",
        root_callback=root_callback,
    )


# --------------------------------------------------------------------------- #
# StepFact shapes — frozen, coerced at build (mint totality)                   #
# --------------------------------------------------------------------------- #


class TestStepFacts:
    def test_said_is_frozen(self) -> None:
        fact = Said(parts=(TextPart(text="hi"),))
        with pytest.raises(AttributeError):
            fact.parts = ()  # type: ignore[misc]

    def test_denied_call_accepts_raw_args_verbatim(self) -> None:
        # A denied call may carry the raw, off-spec model emission (spec §3.1a): str/dict/None verbatim.
        assert DeniedCall(tool_call_id="d1", tool_name="t", args='{"q"', reason_parts=()).args == '{"q"'
        assert DeniedCall(tool_call_id="d1", tool_name="t", args={"q": 1}, reason_parts=()).args == {"q": 1}
        assert DeniedCall(tool_call_id="d1", tool_name="t", args=None, reason_parts=()).args is None

    def test_denied_call_clamps_off_spec_scalar_args(self) -> None:
        # Mint totality (spec §3.1): args are coerced AT BUILD — a bare scalar from an off-spec
        # provider clamps to its string form, never raising later at the mint.
        fact = DeniedCall(tool_call_id="d1", tool_name="t", args=123, reason_parts=())  # type: ignore[arg-type]
        assert fact.args == "123"

    def test_denied_call_coerces_non_content_part_reason_elements(self) -> None:
        # Mint totality: ToolResultStep validates parts at the mint (outside any guard), so a
        # non-ContentPart reason element must be rendered total at fact build — never raise downstream.
        fact = DeniedCall(
            tool_call_id="d1",
            tool_name="t",
            args=None,
            reason_parts=({"type": "value_error", "ctx": {"error": ValueError("boom")}},),  # type: ignore[arg-type]
        )
        (part,) = fact.reason_parts
        assert isinstance(part, TextPart) and "value_error" in part.text

    def test_handed_off_holds_winner_identity(self) -> None:
        fact = HandedOff(target="billing", message="take over")
        assert (fact.target, fact.message) == ("billing", "take over")


class TestObserved:
    def test_defaults_to_empty_facts(self) -> None:
        action = ReturnCall(state=State(), value="x")
        assert Observed(action).facts == ()

    def test_pairs_action_with_facts(self) -> None:
        action = TailCall("t", State())
        fact = Said(parts=(TextPart(text="hi"),))
        observed = Observed(action, (fact,))
        assert observed.action is action and observed.facts == (fact,)

    def test_is_frozen(self) -> None:
        observed = Observed(ReturnCall(state=State(), value="x"))
        with pytest.raises(AttributeError):
            observed.facts = ()  # type: ignore[misc]


# --------------------------------------------------------------------------- #
# HopStepLedger — mint methods (the fact table lives ONLY here)                #
# --------------------------------------------------------------------------- #


class TestLedgerMints:
    async def test_folded_resolved_without_retry_marker_mints_success(self) -> None:
        ledger = HopStepLedger()
        parts = [TextPart(text="result")]
        ledger.folded(_MARKER, parts)
        broker = CaptureBroker()
        await _flush(ledger, broker)
        (event,) = broker.published[0].message.events
        assert isinstance(event, ToolResultStep)
        assert (event.tool_call_id, event.name, event.outcome) == ("c1", "search", "success")
        assert event.parts == parts  # the resolved slot parts, verbatim (I10)

    async def test_folded_retry_marked_mints_failed(self) -> None:
        ledger = HopStepLedger()
        ledger.folded(_MARKER, [retry_text_part("narrow it")])
        broker = CaptureBroker()
        await _flush(ledger, broker)
        (event,) = broker.published[0].message.events
        assert event.outcome == "failed"

    async def test_fold_failed_mints_failed_with_empty_parts(self) -> None:
        # No report parameter exists (I10): the stream carries NO raw error data on an unhandled fault.
        assert list(inspect.signature(HopStepLedger.fold_failed).parameters) == ["self", "marker"]
        ledger = HopStepLedger()
        ledger.fold_failed(_MARKER)
        broker = CaptureBroker()
        await _flush(ledger, broker)
        (event,) = broker.published[0].message.events
        assert isinstance(event, ToolResultStep)
        assert (event.tool_call_id, event.name, event.parts, event.outcome) == ("c1", "search", [], "failed")

    async def test_absorb_said_mints_agent_message(self) -> None:
        ledger = HopStepLedger()
        ledger.absorb((Said(parts=(TextPart(text="thinking"),)),))
        broker = CaptureBroker()
        await _flush(ledger, broker)
        (event,) = broker.published[0].message.events
        assert isinstance(event, AgentMessageStep) and event.parts[0].text == "thinking"

    async def test_absorb_denied_call_expands_the_born_closed_pair_atomically(self) -> None:
        # One fact → BOTH halves (a half-pair is unrepresentable, spec §3.1a).
        ledger = HopStepLedger()
        ledger.absorb((DeniedCall(tool_call_id="d1", tool_name="ghost", args='{"x": 1}', reason_parts=(TextPart(text="unknown tool"),)),))
        broker = CaptureBroker()
        await _flush(ledger, broker)
        call, result = broker.published[0].message.events
        assert isinstance(call, ToolCallStep) and (call.tool_call_id, call.name, call.args) == ("d1", "ghost", '{"x": 1}')
        assert isinstance(result, ToolResultStep) and (result.tool_call_id, result.name, result.outcome) == ("d1", "ghost", "denied")
        assert result.parts[0].text == "unknown tool"

    async def test_absorb_handed_off_maps_message_to_wire_reason(self) -> None:
        ledger = HopStepLedger()
        ledger.absorb((HandedOff(target="billing", message="take over"),))
        broker = CaptureBroker()
        await _flush(ledger, broker)
        (event,) = broker.published[0].message.events
        assert isinstance(event, HandoffStep) and (event.target, event.reason) == ("billing", "take over")

    def test_no_public_append_exists(self) -> None:
        # Capability-sealed (I7): only fact-/marker-shaped mint methods — possession of a hand-built
        # *Step is useless against the ledger.
        assert not hasattr(HopStepLedger, "append")


class TestNoteDispatch:
    async def _dispatched(self, action: Any) -> list[Any]:
        ledger = HopStepLedger()
        ledger.note_dispatch(action)
        broker = CaptureBroker()
        await _flush(ledger, broker)
        return list(broker.published[0].message.events) if broker.published else []

    async def test_bare_marked_call_mints_one(self) -> None:
        events = await self._dispatched(Call("t.in", State(), tag="c1", marker=_MARKER))
        (event,) = events
        assert isinstance(event, ToolCallStep)
        assert (event.tool_call_id, event.name, event.args) == ("c1", "search", {"q": "x"})

    async def test_fanout_list_mints_one_per_marked_element(self) -> None:
        calls = [
            Call("t.in", State(), tag="c1", marker=_MARKER),
            Call("t.in", State(), tag="x9"),  # unmarked sibling → nothing
            Call("u.in", State(), tag="c2", marker=_M2),
        ]
        events = await self._dispatched(calls)
        assert [type(e) for e in events] == [ToolCallStep, ToolCallStep]
        assert [e.tool_call_id for e in events] == ["c1", "c2"]  # production order

    async def test_unmarked_bare_call_mints_nothing(self) -> None:
        assert await self._dispatched(Call("t.in", State(), tag="c1")) == []

    async def test_tailcall_and_returncall_mint_nothing(self) -> None:
        assert await self._dispatched(TailCall("t.in", State())) == []
        assert await self._dispatched(ReturnCall(state=State(), value="done")) == []


# --------------------------------------------------------------------------- #
# flush — drain, no-op guards, terminal gate, identity, totality               #
# --------------------------------------------------------------------------- #


class TestFlush:
    async def test_empty_ledger_is_a_no_op_before_the_broker_is_touched(self) -> None:
        # The empty guard runs FIRST: broker=None (test_routed_dispatch/test_frameless_inbound pass
        # no broker) must not be touched, let alone published to.
        await _flush(HopStepLedger(), None)

    async def test_rootless_flush_is_a_no_op_before_the_broker_is_touched(self) -> None:
        ledger = HopStepLedger()
        ledger.folded(_MARKER, [TextPart(text="r")])
        await _flush(ledger, None, root_callback=None)  # fire-and-forget run: no publish, no broker touch

    async def test_flush_drains_second_flush_is_an_empty_no_op(self) -> None:
        # L18j: at-most-one StepMessage per hop holds BY DRAIN — e.g. the fault exit after a failed
        # action publish re-invokes flush and finds the ledger empty.
        ledger = HopStepLedger()
        ledger.folded(_MARKER, [TextPart(text="r")])
        broker = CaptureBroker()
        await _flush(ledger, broker)
        await _flush(ledger, broker)
        assert len(broker.published) == 1

    async def test_rootless_flush_drains_too(self) -> None:
        # Drain is unconditional: events that had no publishable destination do not linger to
        # over-report on a later exit of the same hop.
        ledger = HopStepLedger()
        ledger.folded(_MARKER, [TextPart(text="r")])
        await _flush(ledger, None, root_callback=None)
        broker = CaptureBroker()
        await _flush(ledger, broker)
        assert broker.published == []

    async def test_one_message_identity_stamped_once_production_order(self) -> None:
        # Production order: fold mints, then body facts, then dispatch mints (informative, §3.4).
        ledger = HopStepLedger()
        ledger.folded(_MARKER, [TextPart(text="r")])
        ledger.absorb((Said(parts=(TextPart(text="preamble"),)),))
        ledger.note_dispatch(Call("t.in", State(), tag="c2", marker=_M2))
        broker = CaptureBroker()
        await _flush(ledger, broker, disposition=Call("t.in", State(), tag="c2", marker=_M2))
        (pub,) = broker.published
        message = pub.message
        assert isinstance(message, StepMessage)
        assert (message.correlation_id, message.emitter, message.depth, message.frame_id) == ("cid-1", "agent-a", 1, "F1")
        assert [type(e) for e in message.events] == [ToolResultStep, AgentMessageStep, ToolCallStep]
        assert pub.topic == "client.return"
        assert pub.key == b"cid-1"
        assert pub.headers == {HDR_WIRE: StepMessage.WIRE, HDR_EMITTER: "agent-a", HDR_EMITTER_KIND: "agent"}

    async def test_terminal_gate_drops_agent_message_on_return_call_disposition(self) -> None:
        # §3.4: a returning hop's answer rides its ReturnMessage — an agent_message would double-report.
        # Denied pairs and fold results on the same hop SURVIVE the gate.
        ledger = HopStepLedger()
        ledger.folded(_MARKER, [TextPart(text="r")])
        ledger.absorb((Said(parts=(TextPart(text="answer preamble"),)),))
        broker = CaptureBroker()
        await _flush(ledger, broker, disposition=ReturnCall(state=State(), value="answer"))
        (pub,) = broker.published
        assert [type(e) for e in pub.message.events] == [ToolResultStep]

    async def test_terminal_gate_does_not_fire_on_fault_exit_disposition_none(self) -> None:
        # A fault exit flushes with disposition=None: the hop's return never published, so a held
        # agent_message is the only record, not a double report (§3.4).
        ledger = HopStepLedger()
        ledger.absorb((Said(parts=(TextPart(text="preamble"),)),))
        broker = CaptureBroker()
        await _flush(ledger, broker, disposition=None)
        (pub,) = broker.published
        assert [type(e) for e in pub.message.events] == [AgentMessageStep]

    async def test_terminal_gate_leaving_nothing_publishes_nothing(self) -> None:
        ledger = HopStepLedger()
        ledger.absorb((Said(parts=(TextPart(text="only a preamble"),)),))
        broker = CaptureBroker()
        await _flush(ledger, broker, disposition=ReturnCall(state=State(), value="answer"))
        assert broker.published == []

    async def test_broker_none_with_publishable_events_clamps_and_drains(self) -> None:
        # The impossible-by-contract input (a rooted, non-empty flush with NO broker) clamps —
        # total, never raises — and still drains (at-most-once holds even for this shape).
        ledger = HopStepLedger()
        ledger.folded(_MARKER, [TextPart(text="r")])
        await _flush(ledger, None, root_callback="client.return")
        broker = CaptureBroker()
        await _flush(ledger, broker)
        assert broker.published == []  # drained by the clamped flush

    async def test_flush_is_total_no_internal_wrap(self) -> None:
        # The best-effort log-and-drop guard lives at the KERNEL's exit-flush helper (F1), not here —
        # a broker raise propagates (this is what makes the I1 monkeypatch guard test meaningful).
        ledger = HopStepLedger()
        ledger.folded(_MARKER, [TextPart(text="r")])
        with pytest.raises(RuntimeError, match="broker down"):
            await _flush(ledger, CaptureBroker(raises=RuntimeError("broker down")))
