"""PR-4 step 1: the durable fan-out record models.

ktables stores these as JSON (`KafkaTable.json(model=...)` decodes via
`model_validate_json`; `KafkaTableWriter.json(...)` encodes via `model_dump_json`),
so every record must round-trip through JSON. `FanoutOutcome.result` mirrors
`State.tool_results`' value type, so a `CalfToolResult` (incl. a `FailedToolCall`)
must come back TYPED via its discriminator, not as a bare dict. Every value carries
a `version` defaulting to 1.
"""

from typing import TypeVar

from pydantic import BaseModel

from calfkit._vendor.pydantic_ai.messages import ToolReturn
from calfkit.models.fanout import (
    EnvelopeSnapshot,
    FanoutBaseState,
    FanoutOpen,
    FanoutOutcome,
    FanoutState,
    SlotRef,
)
from calfkit.models.session_context import CallFrame, Stack, WorkflowState
from calfkit.models.state import FailedToolCall, State

_M = TypeVar("_M", bound=BaseModel)


def _roundtrip(model: _M) -> _M:
    return type(model).model_validate_json(model.model_dump_json())


def test_fanout_open_roundtrips_and_defaults_version() -> None:
    reg = FanoutOpen(fanout_id="x", node_id="agent", expected=[SlotRef(frame_id="f1", tag="tc1")])
    assert reg.version == 1
    rt = _roundtrip(reg)
    assert rt == reg
    assert rt.expected[0].frame_id == "f1"


def test_fanout_outcome_preserves_typed_tool_return() -> None:
    outcome = FanoutOutcome(slot="f1", tag="tc1", result=ToolReturn(return_value="ok"))
    rt = _roundtrip(outcome)
    assert isinstance(rt.result, ToolReturn)
    assert rt.result.return_value == "ok"


def test_fanout_outcome_preserves_failed_tool_call_marker() -> None:
    marker = FailedToolCall.build_safe(tool_name="t", tool_call_id="tc1", exc_type="ValueError", exc_message="boom")
    outcome = FanoutOutcome(slot="f1", tag="tc1", result=marker)
    rt = _roundtrip(outcome)
    assert isinstance(rt.result, FailedToolCall)
    assert rt.result.exc_type == "ValueError"


def test_fanout_state_accumulates_outcomes_and_roundtrips() -> None:
    state = FanoutState(
        open=FanoutOpen(fanout_id="x", node_id="agent", expected=[SlotRef(frame_id="f1", tag="tc1")]),
        outcomes={"f1": FanoutOutcome(slot="f1", tag="tc1", result=ToolReturn(return_value="ok"))},
    )
    rt = _roundtrip(state)
    assert rt.outcomes["f1"].slot == "f1"
    assert isinstance(rt.outcomes["f1"].result, ToolReturn)


def test_fanout_state_outcomes_default_empty() -> None:
    state = FanoutState(open=FanoutOpen(fanout_id="x", node_id="agent", expected=[]))
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
