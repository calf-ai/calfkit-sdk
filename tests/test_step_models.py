"""Increment A — the step wire vocabulary (intermediate-step-streaming spec §2.4 / §3.1 / §3.2).

Inert, offline, no broker: the ``StepMessage`` wire body + the ``StepEvent`` discriminated
union (non-frozen, identity excluded-from-wire and back-filled by ``StepMessage``'s validator),
the ``x-calf-wire`` header + ``WireKind`` + ``wire_filter``, and ``Envelope.WIRE``. Nothing here
emits, routes, or receives a step.
"""

from __future__ import annotations

import json
import typing
from types import SimpleNamespace

from calfkit._protocol import HDR_WIRE, WireKind, wire_filter
from calfkit.models.envelope import Envelope
from calfkit.models.payload import TextPart, ToolCallPart
from calfkit.models.step import (
    AgentMessageEvent,
    AgentThinkingEvent,
    HandoffEvent,
    StepEvent,
    StepMessage,
    ToolCallEvent,
    ToolResultEvent,
)


def _msg(**headers: object) -> SimpleNamespace:
    """A stand-in for a FastStream ``StreamMessage`` carrying only ``.headers`` (what ``wire_filter`` reads)."""
    return SimpleNamespace(headers=headers)


class TestWireConstants:
    def test_hdr_wire_name(self) -> None:
        assert HDR_WIRE == "x-calf-wire"

    def test_wire_kind_value_space(self) -> None:
        assert typing.get_args(WireKind) == ("envelope", "step")

    def test_envelope_wire_classvar(self) -> None:
        assert Envelope.WIRE == "envelope"

    def test_step_message_wire_classvar(self) -> None:
        assert StepMessage.WIRE == "step"


class TestWireFilter:
    """``wire_filter`` returns a SYNC predicate; FastStream wraps it via ``to_async`` at registration
    (``usecase.py:274``), so a sync predicate is the correct shape."""

    def test_matches_own_wire_str(self) -> None:
        assert wire_filter(Envelope)(_msg(**{HDR_WIRE: "envelope"})) is True
        assert wire_filter(StepMessage)(_msg(**{HDR_WIRE: "step"})) is True

    def test_matches_own_wire_bytes(self) -> None:
        # inbound Kafka headers arrive as bytes; decode_header_str coerces.
        assert wire_filter(Envelope)(_msg(**{HDR_WIRE: b"envelope"})) is True
        assert wire_filter(StepMessage)(_msg(**{HDR_WIRE: b"step"})) is True

    def test_rejects_other_wire(self) -> None:
        # strict positive (no absent-fallback): an envelope filter rejects a step body and vice versa.
        assert wire_filter(Envelope)(_msg(**{HDR_WIRE: "step"})) is False
        assert wire_filter(StepMessage)(_msg(**{HDR_WIRE: "envelope"})) is False

    def test_rejects_absent_header(self) -> None:
        # unstamped → dropped (intended for foreign producers; every calfkit publish stamps).
        assert wire_filter(Envelope)(_msg()) is False
        assert wire_filter(StepMessage)(_msg()) is False


def _sample_events() -> list[StepEvent]:
    return [
        AgentMessageEvent(parts=[TextPart(text="let me look that up")]),
        ToolCallEvent(tool_call_id="c1", name="search", args={"q": "x"}),
        ToolResultEvent(tool_call_id="c1", name="search_tool", parts=[TextPart(text="result")], is_error=False),
        HandoffEvent(target="billing", reason="needs billing"),
    ]


def _sample_message() -> StepMessage:
    return StepMessage(correlation_id="run-1", emitter="agent-a", depth=2, frame_id="f1", events=_sample_events())


class TestStepEventKinds:
    def test_kind_literals(self) -> None:
        assert AgentMessageEvent(parts=[]).kind == "agent_message"
        assert ToolCallEvent(tool_call_id="c", name="n").kind == "tool_call"
        assert ToolResultEvent(tool_call_id="c", name="n", parts=[]).kind == "tool_result"
        assert HandoffEvent(target="t", reason="r").kind == "handoff"
        assert AgentThinkingEvent(parts=[]).kind == "agent_thinking"

    def test_kinds_distinct_from_contentpart(self) -> None:
        # The step ``kind`` space must not collide with ContentPart's — esp. ToolCallEvent ("tool_call")
        # vs ToolCallPart ("tool").
        step_kinds = {"agent_message", "tool_call", "tool_result", "handoff", "agent_thinking"}
        content_kinds = {"text", "file", "data", "tool"}
        assert step_kinds.isdisjoint(content_kinds)
        assert ToolCallEvent(tool_call_id="c", name="n").kind != ToolCallPart(tool_call_id="c", kwargs={}, tool_name="n").kind


class TestToolCallArgs:
    def test_args_accepts_str(self) -> None:
        assert ToolCallEvent(tool_call_id="c", name="n", args='{"q": "x"}').args == '{"q": "x"}'

    def test_args_accepts_dict(self) -> None:
        assert ToolCallEvent(tool_call_id="c", name="n", args={"q": "x"}).args == {"q": "x"}

    def test_args_defaults_none(self) -> None:
        assert ToolCallEvent(tool_call_id="c", name="n").args is None


class TestToolResult:
    def test_is_error_defaults_false(self) -> None:
        assert ToolResultEvent(tool_call_id="c", name="n", parts=[]).is_error is False

    def test_is_error_settable(self) -> None:
        assert ToolResultEvent(tool_call_id="c", name="n", parts=[], is_error=True).is_error is True


class TestStepEventMutability:
    """Load-bearing: ``StepMessage``'s validator back-fills identity by in-place assignment, so a
    StepEvent MUST be non-frozen. A frozen model would raise during decode → the lenient decoder
    swallows → every step silently dropped forever (spec §3.1)."""

    def test_identity_is_assignable(self) -> None:
        e = AgentMessageEvent(parts=[TextPart(text="hi")])
        e.correlation_id = "run-9"
        e.depth = 3
        e.frame_id = "f9"
        e.emitter = "n9"
        assert (e.correlation_id, e.depth, e.frame_id, e.emitter) == ("run-9", 3, "f9", "n9")

    def test_identity_defaults_none(self) -> None:
        e = ToolCallEvent(tool_call_id="c", name="n")
        assert (e.correlation_id, e.depth, e.frame_id, e.emitter) == (None, None, None, None)


class TestStepMessageIdentityBackfill:
    def test_validator_stamps_each_event_identity_on_construction(self) -> None:
        sm = _sample_message()
        for e in sm.events:
            assert e.correlation_id == "run-1"
            assert e.depth == 2
            assert e.frame_id == "f1"
            assert e.emitter == "agent-a"

    def test_dump_excludes_nested_identity_keeps_message_level(self) -> None:
        raw = json.loads(_sample_message().model_dump_json())
        assert raw["correlation_id"] == "run-1"
        assert raw["emitter"] == "agent-a"
        assert raw["depth"] == 2
        assert raw["frame_id"] == "f1"
        for ev in raw["events"]:
            assert "correlation_id" not in ev
            assert "depth" not in ev
            assert "frame_id" not in ev
            assert "emitter" not in ev

    def test_decode_backfills_identity_and_resolves_union(self) -> None:
        back = StepMessage.model_validate_json(_sample_message().model_dump_json())
        assert [type(e) for e in back.events] == [AgentMessageEvent, ToolCallEvent, ToolResultEvent, HandoffEvent]
        for e in back.events:
            assert e.correlation_id == "run-1"
            assert e.depth == 2
            assert e.frame_id == "f1"
            assert e.emitter == "agent-a"

    def test_round_trips_equal(self) -> None:
        sm = _sample_message()
        assert StepMessage.model_validate_json(sm.model_dump_json()) == sm

    def test_event_specific_fields_survive_round_trip(self) -> None:
        back = StepMessage.model_validate_json(_sample_message().model_dump_json())
        tc = next(e for e in back.events if isinstance(e, ToolCallEvent))
        assert (tc.tool_call_id, tc.name, tc.args) == ("c1", "search", {"q": "x"})
        tr = next(e for e in back.events if isinstance(e, ToolResultEvent))
        assert (tr.name, tr.is_error) == ("search_tool", False)
        assert isinstance(tr.parts[0], TextPart) and tr.parts[0].text == "result"
        ho = next(e for e in back.events if isinstance(e, HandoffEvent))
        assert (ho.target, ho.reason) == ("billing", "needs billing")


class TestPublicReExport:
    def test_step_event_types_are_re_exported_from_calfkit(self) -> None:
        import calfkit

        # the four EMITTED step events re-export at the public top level (collision-free as *Event types)
        assert calfkit.AgentMessageEvent is AgentMessageEvent
        assert calfkit.ToolCallEvent is ToolCallEvent
        assert calfkit.ToolResultEvent is ToolResultEvent
        assert calfkit.HandoffEvent is HandoffEvent
        for name in ("AgentMessageEvent", "ToolCallEvent", "ToolResultEvent", "HandoffEvent"):
            assert name in calfkit.__all__
        # calfkit.Handoff stays the peer capability handle — NOT the step event (the rename's whole point)
        assert calfkit.Handoff is not HandoffEvent


class TestAgentThinkingDefinedNotEmitted:
    """Increment F — AgentThinkingEvent is DEFINED + wire-valid, but NOT emitted in v1 (spec §5)."""

    def test_agent_thinking_event_round_trips(self) -> None:
        sm = StepMessage(correlation_id="c", emitter="a", depth=1, frame_id="f", events=[AgentThinkingEvent(parts=[TextPart(text="hmm")])])
        back = StepMessage.model_validate_json(sm.model_dump_json())
        assert isinstance(back.events[0], AgentThinkingEvent)
        assert back.events[0].parts[0].text == "hmm"

    def test_agent_thinking_is_not_a_run_event_member_in_v1(self) -> None:
        # Honest-naming (§5): not surfaced on the stream, so it is NOT a RunEvent member yet.
        from calfkit.client.events import RunEvent

        assert AgentThinkingEvent not in typing.get_args(RunEvent)
