"""The step wire vocabulary — two parallel frozen families (intermediate-step-streaming spec §3.1/§3.2,
impl-plan §6).

WIRE/DRAFT ``*Step`` (frozen, NO identity) carried by ``StepMessage`` (frozen, no validator); identity
rides once on the message. SURFACE ``*Event`` (frozen, identity REQUIRED) are the public ``RunEvent``
members, built once caller-side by ``_to_surface``. The two families are separate (not subclasses) so a
surface event cannot ride the wire. Nothing here emits, routes, or receives a step.
"""

from __future__ import annotations

import json
import typing
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from calfkit._protocol import HDR_WIRE, WireKind, wire_filter
from calfkit.models.envelope import Envelope
from calfkit.models.payload import TextPart, ToolCallPart
from calfkit.models.step import (
    AgentMessageEvent,
    AgentMessageStep,
    AgentThinkingEvent,
    AgentThinkingStep,
    HandoffEvent,
    HandoffStep,
    RunStepEvent,
    StepEvent,
    StepMessage,
    ToolCallEvent,
    ToolCallStep,
    ToolResultEvent,
    ToolResultStep,
)

# (wire `*Step`, surface `*Event`, kind) for every step-event kind.
_PAIRS = [
    (AgentMessageStep, AgentMessageEvent, "agent_message"),
    (ToolCallStep, ToolCallEvent, "tool_call"),
    (ToolResultStep, ToolResultEvent, "tool_result"),
    (HandoffStep, HandoffEvent, "handoff"),
    (AgentThinkingStep, AgentThinkingEvent, "agent_thinking"),
]
_IDENTITY = {"correlation_id", "depth", "frame_id", "emitter"}
# AgentThinking is in the WIRE union (the decoder must resolve every kind) but defined-not-emitted (§5):
# never surfaced, so absent from RunStepEvent and _SURFACE_BY_KIND.
_DEFINED_NOT_EMITTED = {"agent_thinking"}


def _kind_of(model_cls: type) -> str:
    """The ``kind`` literal of a ``*Step`` / ``*Event`` model class."""
    return model_cls.model_fields["kind"].default


def _union_members(union: object) -> tuple[type, ...]:
    """Concrete model classes of a (possibly Annotated) union — handles the wire ``StepEvent``
    (``Annotated[Union, Discriminator]``) and the plain surface ``RunStepEvent`` union."""
    args = typing.get_args(union)
    return typing.get_args(args[0]) if (args and typing.get_args(args[0])) else args


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


# --------------------------------------------------------------------------- #
# WIRE / DRAFT family — `*Step` (frozen, no identity)                          #
# --------------------------------------------------------------------------- #


def _wire_events() -> list[StepEvent]:
    return [
        AgentMessageStep(parts=[TextPart(text="let me look that up")]),
        ToolCallStep(tool_call_id="c1", name="search", args={"q": "x"}),
        ToolResultStep(tool_call_id="c1", name="search_tool", parts=[TextPart(text="result")], is_error=False),
        HandoffStep(target="billing", reason="needs billing"),
    ]


def _message() -> StepMessage:
    return StepMessage(correlation_id="run-1", emitter="agent-a", depth=2, frame_id="f1", events=_wire_events())


class TestWireStep:
    def test_kind_literals(self) -> None:
        assert AgentMessageStep(parts=[]).kind == "agent_message"
        assert ToolCallStep(tool_call_id="c", name="n").kind == "tool_call"
        assert ToolResultStep(tool_call_id="c", name="n", parts=[]).kind == "tool_result"
        assert HandoffStep(target="t", reason="r").kind == "handoff"
        assert AgentThinkingStep(parts=[]).kind == "agent_thinking"

    def test_kinds_distinct_from_contentpart(self) -> None:
        # ToolCallStep.kind ("tool_call") must not collide with ContentPart's ToolCallPart.kind ("tool").
        step_kinds = {k for _, _, k in _PAIRS}
        assert step_kinds.isdisjoint({"text", "file", "data", "tool"})
        assert ToolCallStep(tool_call_id="c", name="n").kind != ToolCallPart(tool_call_id="c", kwargs={}, tool_name="n").kind

    def test_wire_step_carries_no_identity_fields(self) -> None:
        # Identity rides ONCE on StepMessage; the wire events structurally cannot carry it.
        for wire, _, _ in _PAIRS:
            assert _IDENTITY.isdisjoint(set(wire.model_fields)), wire.__name__

    def test_wire_step_is_frozen(self) -> None:
        s = ToolCallStep(tool_call_id="c", name="n")
        with pytest.raises(ValidationError):
            s.name = "other"  # type: ignore[misc]

    def test_args_accepts_str_dict_none(self) -> None:
        assert ToolCallStep(tool_call_id="c", name="n", args='{"q": "x"}').args == '{"q": "x"}'
        assert ToolCallStep(tool_call_id="c", name="n", args={"q": "x"}).args == {"q": "x"}
        assert ToolCallStep(tool_call_id="c", name="n").args is None

    def test_is_error_default_and_settable(self) -> None:
        assert ToolResultStep(tool_call_id="c", name="n", parts=[]).is_error is False
        assert ToolResultStep(tool_call_id="c", name="n", parts=[], is_error=True).is_error is True


class TestStepMessageWire:
    def test_step_message_is_frozen(self) -> None:
        sm = _message()
        with pytest.raises(ValidationError):
            sm.correlation_id = "other"  # type: ignore[misc]

    def test_wire_dump_has_message_identity_only(self) -> None:
        # The wire byte shape: identity ONCE at the message level, NONE per event (wire-compatible with the
        # superseded unified design, which already excluded per-event identity).
        raw = json.loads(_message().model_dump_json())
        assert (raw["correlation_id"], raw["emitter"], raw["depth"], raw["frame_id"]) == ("run-1", "agent-a", 2, "f1")
        for ev in raw["events"]:
            assert _IDENTITY.isdisjoint(ev.keys())

    def test_decode_resolves_the_wire_union(self) -> None:
        back = StepMessage.model_validate_json(_message().model_dump_json())
        assert [type(e) for e in back.events] == [AgentMessageStep, ToolCallStep, ToolResultStep, HandoffStep]

    def test_round_trips_equal(self) -> None:
        sm = _message()
        assert StepMessage.model_validate_json(sm.model_dump_json()) == sm

    def test_event_specific_fields_survive_round_trip(self) -> None:
        back = StepMessage.model_validate_json(_message().model_dump_json())
        tc = next(e for e in back.events if isinstance(e, ToolCallStep))
        assert (tc.tool_call_id, tc.name, tc.args) == ("c1", "search", {"q": "x"})
        tr = next(e for e in back.events if isinstance(e, ToolResultStep))
        assert (tr.name, tr.is_error) == ("search_tool", False)
        assert isinstance(tr.parts[0], TextPart) and tr.parts[0].text == "result"
        ho = next(e for e in back.events if isinstance(e, HandoffStep))
        assert (ho.target, ho.reason) == ("billing", "needs billing")


# --------------------------------------------------------------------------- #
# SURFACE family — `*Event` (frozen, identity required)                       #
# --------------------------------------------------------------------------- #


def _surface_kwargs() -> dict[str, object]:
    return {"correlation_id": "run-1", "depth": 2, "frame_id": "f1", "emitter": "agent-a"}


class TestSurfaceEvent:
    def test_kind_literals(self) -> None:
        kw = _surface_kwargs()
        assert AgentMessageEvent(parts=[], **kw).kind == "agent_message"
        assert ToolCallEvent(tool_call_id="c", name="n", **kw).kind == "tool_call"
        assert ToolResultEvent(tool_call_id="c", name="n", parts=[], **kw).kind == "tool_result"
        assert HandoffEvent(target="t", reason="r", **kw).kind == "handoff"
        assert AgentThinkingEvent(parts=[], **kw).kind == "agent_thinking"

    def test_surface_requires_identity(self) -> None:
        # Honest public type: identity is non-null on the surface, so construction without it fails.
        with pytest.raises(ValidationError):
            AgentMessageEvent(parts=[])
        with pytest.raises(ValidationError):
            ToolCallEvent(tool_call_id="c", name="n")

    def test_surface_event_is_frozen(self) -> None:
        e = ToolCallEvent(tool_call_id="c", name="n", **_surface_kwargs())
        with pytest.raises(ValidationError):
            e.name = "other"  # type: ignore[misc]

    def test_surface_event_rejected_on_the_wire(self) -> None:
        # Parallel hierarchies: a surface `*Event` is NOT a member of the wire `StepEvent` union, so a live
        # surface instance is rejected by StepMessage.events (identity can never leak per-event onto the wire).
        surf = ToolCallEvent(tool_call_id="c", name="n", **_surface_kwargs())
        with pytest.raises(ValidationError):
            StepMessage(correlation_id="run-1", emitter="agent-a", depth=2, frame_id="f1", events=[surf])  # type: ignore[list-item]


# --------------------------------------------------------------------------- #
# Mapping seam — `_to_surface` (wire `*Step` + message identity → surface `*Event`)
# --------------------------------------------------------------------------- #


class TestToSurface:
    def test_maps_each_kind_stamping_identity(self) -> None:
        from calfkit.client.hub import _SURFACE_BY_KIND, _to_surface

        msg = _message()
        for step in msg.events:
            surf = _to_surface(step, msg)
            assert isinstance(surf, _SURFACE_BY_KIND[step.kind])
            assert surf.kind == step.kind
            assert (surf.correlation_id, surf.depth, surf.frame_id, surf.emitter) == ("run-1", 2, "f1", "agent-a")

    def test_payload_fields_carry_over(self) -> None:
        from calfkit.client.hub import _to_surface

        msg = _message()
        tc_step = next(e for e in msg.events if isinstance(e, ToolCallStep))
        surf = _to_surface(tc_step, msg)
        assert isinstance(surf, ToolCallEvent)
        assert (surf.tool_call_id, surf.name, surf.args) == ("c1", "search", {"q": "x"})

    def test_nested_parts_pass_by_reference_no_reserialize(self) -> None:
        from calfkit.client.hub import _to_surface

        msg = _message()
        am_step = next(e for e in msg.events if isinstance(e, AgentMessageStep))
        surf = _to_surface(am_step, msg)
        assert surf.parts[0] is am_step.parts[0]


# --------------------------------------------------------------------------- #
# Mechanical parity — the parallel hierarchies' only unenforced invariant     #
# --------------------------------------------------------------------------- #


class TestFamilyParity:
    """The parallel hierarchies' field lockstep + the _SURFACE_BY_KIND allowlist are not type-enforced;
    these are the guard. Anchored to the REAL unions (not just the hand-written _PAIRS), so a new wire kind
    that someone forgets to pair / surface / denylist fails the suite — instead of being silently dropped by
    _on_step's `kind not in _SURFACE_BY_KIND: continue` filter (the feature's one silent-drop seam)."""

    def test_pairs_enumerate_the_whole_wire_union(self) -> None:
        # _PAIRS must cover every StepEvent member, or the lockstep + no-identity guards below skip a kind.
        assert {_kind_of(w) for w, _, _ in _PAIRS} == {_kind_of(m) for m in _union_members(StepEvent)}

    def test_pairs_enumerate_the_whole_surface_family(self) -> None:
        # the surface side of _PAIRS == RunStepEvent's members + the defined-not-emitted surface types.
        assert {_kind_of(s) for _, s, _ in _PAIRS} == {_kind_of(m) for m in _union_members(RunStepEvent)} | _DEFINED_NOT_EMITTED

    def test_field_lockstep_wire_plus_identity_equals_surface(self) -> None:
        # A wire-only or surface-only payload field would silently drift (dropped by _to_surface, or a
        # required surface field would fault every step). Pin lockstep so drift is a test failure.
        for wire, surface, _ in _PAIRS:
            assert set(wire.model_fields) | _IDENTITY == set(surface.model_fields), wire.__name__

    def test_surface_by_kind_covers_every_wire_kind(self) -> None:
        # _SURFACE_BY_KIND is an allowlist: a wire kind missing an entry is SILENTLY dropped by _on_step.
        # Anchored to the real wire union: every kind is either surfaced or explicitly defined-not-emitted.
        from calfkit.client.hub import _SURFACE_BY_KIND

        assert {_kind_of(m) for m in _union_members(StepEvent)} == set(_SURFACE_BY_KIND) | _DEFINED_NOT_EMITTED


# --------------------------------------------------------------------------- #
# Public surface + AgentThinking (defined-not-emitted)                        #
# --------------------------------------------------------------------------- #


class TestPublicReExport:
    def test_surface_step_events_re_export_from_calfkit(self) -> None:
        import calfkit

        # the four EMITTED surface events re-export at the public top level (collision-free as *Event types)
        assert calfkit.AgentMessageEvent is AgentMessageEvent
        assert calfkit.ToolCallEvent is ToolCallEvent
        assert calfkit.ToolResultEvent is ToolResultEvent
        assert calfkit.HandoffEvent is HandoffEvent
        for name in ("AgentMessageEvent", "ToolCallEvent", "ToolResultEvent", "HandoffEvent"):
            assert name in calfkit.__all__
        # calfkit.Handoff stays the peer capability handle — NOT the step event.
        assert calfkit.Handoff is not HandoffEvent


class TestAgentThinkingDefinedNotEmitted:
    """``AgentThinkingStep`` is wire-valid (the decoder must resolve every kind) and ``AgentThinkingEvent``
    is a defined surface type, but neither is emitted/surfaced in v1 (spec §5)."""

    def test_agent_thinking_step_round_trips_on_the_wire(self) -> None:
        sm = StepMessage(correlation_id="c", emitter="a", depth=1, frame_id="f", events=[AgentThinkingStep(parts=[TextPart(text="hmm")])])
        back = StepMessage.model_validate_json(sm.model_dump_json())
        assert isinstance(back.events[0], AgentThinkingStep)
        assert back.events[0].parts[0].text == "hmm"

    def test_agent_thinking_event_is_not_a_run_event_member(self) -> None:
        from calfkit.client.events import RunEvent

        assert AgentThinkingEvent not in typing.get_args(RunEvent)
