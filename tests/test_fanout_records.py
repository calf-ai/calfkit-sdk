"""PR-4 step 1 + PR-6 4.4: the durable fan-out record models.

ktables stores these as JSON (`KafkaTable.json(model=...)` decodes via
`model_validate_json`; `KafkaTableWriter.json(...)` encodes via `model_dump_json`),
so every record must round-trip through JSON. The fault rail (4.4) hard-swaps
`FanoutOutcome` off the `CalfToolResult` blob carriage onto the reply-slot carriage:
a resolved slot carries `parts` (the typed `ContentPart` vocabulary), a failed slot
carries `fault` (a typed `ErrorReport`) — `parts` XOR `fault`. `SlotRef`/`FanoutOutcome`
also carry `target_topic` (the per-slot fault-group topology, decision 5).
`FanoutOpen.expected` registers a batch iff the caller's state must survive the call independently of
the round-trip — a true fan-out (N>=2) OR a lone `isolate_state` call (a degenerate singleton, L13);
PR-B relaxed `min_length` 2->1, so an empty batch stays unrepresentable but a singleton is admitted.
"""

from typing import TypeVar

import pytest
from pydantic import BaseModel, ValidationError

from calfkit.models.error_report import ErrorReport
from calfkit.models.fanout import (
    EnvelopeSnapshot,
    FanoutBaseState,
    FanoutOpen,
    FanoutOutcome,
    FanoutState,
    SlotRef,
)
from calfkit.models.payload import DataPart, TextPart
from calfkit.models.session_context import CallFrame, Stack, WorkflowState
from calfkit.models.state import State

_M = TypeVar("_M", bound=BaseModel)


def _roundtrip(model: _M) -> _M:
    return type(model).model_validate_json(model.model_dump_json())


def test_fanout_open_roundtrips() -> None:
    reg = FanoutOpen(
        fanout_id="x",
        node_id="agent",
        expected=[SlotRef(frame_id="f1", tag="tc1", target_topic="tool.a"), SlotRef(frame_id="f2", tag="tc2", target_topic="tool.b")],
    )
    rt = _roundtrip(reg)
    assert rt == reg
    assert rt.expected[0].frame_id == "f1"
    assert rt.expected[0].target_topic == "tool.a"


def test_slot_ref_carries_target_topic() -> None:
    # decision 5: SlotRef gains target_topic, captured at OPEN from Call.target_topic, so the fold
    # can source FanoutOutcome.target_topic (the per-slot fault-group topology) from the matched ref.
    ref = SlotRef(frame_id="f1", tag="tc1", target_topic="tool.a")
    assert _roundtrip(ref).target_topic == "tool.a"


def test_fanout_open_accepts_singleton_expected() -> None:
    # PR-B / L13: the N>=2 invariant is generalized to "register a batch iff the caller's state must
    # survive the call independently of the round-trip". A singleton batch IS registrable now — a lone
    # `isolate_state` call (e.g. a lone `message_agent`) opens a degenerate one-element durable batch.
    reg = FanoutOpen(fanout_id="x", node_id="agent", expected=[SlotRef(frame_id="f1", tag="tc1", target_topic="agent.peer.private.input")])
    assert len(reg.expected) == 1
    assert _roundtrip(reg).expected[0].frame_id == "f1"


def test_fanout_open_rejects_empty_expected() -> None:
    with pytest.raises(ValidationError):
        FanoutOpen(fanout_id="x", node_id="agent", expected=[])


def test_fanout_outcome_resolved_carries_typed_parts() -> None:
    # A resolved slot (a return, or a handled on_callee_error substitute) carries `parts` in the
    # ContentPart vocabulary; `fault` is None. The parts come back TYPED via the discriminator.
    outcome = FanoutOutcome(slot="f1", tag="tc1", target_topic="tool.a", handled=False, parts=[DataPart(data={"r": 1}), TextPart(text="ok")])
    rt = _roundtrip(outcome)
    assert rt.fault is None
    assert rt.parts is not None
    assert isinstance(rt.parts[0], DataPart) and rt.parts[0].data == {"r": 1}
    assert isinstance(rt.parts[1], TextPart) and rt.parts[1].text == "ok"


def test_fanout_outcome_failed_carries_typed_fault() -> None:
    # A failed slot (an unhandled callee fault) carries a typed `ErrorReport`; `parts` is None.
    outcome = FanoutOutcome(slot="f1", tag="tc1", target_topic="tool.a", handled=False, fault=ErrorReport(error_type="callee.boom", message="bang"))
    rt = _roundtrip(outcome)
    assert rt.parts is None
    assert rt.fault is not None and rt.fault.error_type == "callee.boom" and rt.fault.message == "bang"


def test_fanout_outcome_records_handled_and_target_topic() -> None:
    # `handled` distinguishes an on_callee_error substitute (True ⇒ counts as that tool's failure for
    # the deferred budget) from a plain fault-free return (False); `target_topic` is the per-slot
    # fault-group topology. Both round-trip.
    outcome = FanoutOutcome(slot="f1", tag="tc1", target_topic="tool.a", handled=True, parts=[TextPart(text="substitute")])
    rt = _roundtrip(outcome)
    assert rt.handled is True
    assert rt.target_topic == "tool.a"


def test_fanout_state_accumulates_outcomes_and_roundtrips() -> None:
    state = FanoutState(
        open=FanoutOpen(
            fanout_id="x",
            node_id="agent",
            expected=[SlotRef(frame_id="f1", tag="tc1", target_topic="tool.a"), SlotRef(frame_id="f2", tag="tc2", target_topic="tool.b")],
        ),
        outcomes={"f1": FanoutOutcome(slot="f1", tag="tc1", target_topic="tool.a", handled=False, parts=[TextPart(text="ok")])},
    )
    rt = _roundtrip(state)
    assert rt.outcomes["f1"].slot == "f1"
    assert rt.outcomes["f1"].parts is not None and isinstance(rt.outcomes["f1"].parts[0], TextPart)


def test_fanout_state_outcomes_default_empty() -> None:
    expected = [SlotRef(frame_id="f1", tag="tc1", target_topic="tool.a"), SlotRef(frame_id="f2", tag="tc2", target_topic="tool.b")]
    state = FanoutState(open=FanoutOpen(fanout_id="x", node_id="agent", expected=expected))
    assert state.outcomes == {}


def test_envelope_snapshot_and_basestate_roundtrip() -> None:
    snap = EnvelopeSnapshot(
        state=State(),
        stack=WorkflowState(call_stack=Stack([CallFrame(target_topic="t", callback_topic="cb")])),
        deps={"k": "v"},
    )
    base = FanoutBaseState(fanout_id="x", snapshot=snap)
    rt = _roundtrip(base)
    assert rt.fanout_id == "x"
    assert rt.snapshot.deps == {"k": "v"}
    assert rt.snapshot.stack.current_frame.target_topic == "t"
