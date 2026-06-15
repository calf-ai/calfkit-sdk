"""PR-4 step 2: the fan-out marker carrier.

``CallFrame`` carries ``fanout_id`` — the fan-out node's own inbound ``frame_id``,
stamped on each sibling ``Call``'s frame copy at fan-out dispatch (the stamping
itself lands in step 5/6). It defaults to ``None`` (unmarked) on every non-sibling
frame, and ``WorkflowState.invoke_frame`` accepts it as a keyword-only arg so the
existing positional call sites are untouched. The marker must survive the JSON hop.
"""

from calfkit.models.actions import Call
from calfkit.models.session_context import CallFrame, Stack, WorkflowState
from calfkit.models.state import State


def test_callframe_fanout_id_defaults_none() -> None:
    assert CallFrame(target_topic="t", callback_topic="cb").fanout_id is None


def test_callframe_carries_fanout_id() -> None:
    assert CallFrame(target_topic="t", callback_topic="cb", fanout_id="X").fanout_id == "X"


def test_invoke_frame_unmarked_by_default() -> None:
    ws = WorkflowState(call_stack=Stack())
    ws.invoke_frame(Call(target_topic="t", state=State()), callback_topic="cb")
    assert ws.current_frame.fanout_id is None


def test_invoke_frame_stamps_fanout_id() -> None:
    ws = WorkflowState(call_stack=Stack())
    ws.invoke_frame(Call(target_topic="t", state=State()), callback_topic="cb", fanout_id="X")
    assert ws.current_frame.fanout_id == "X"


def test_workflowstate_roundtrips_fanout_id() -> None:
    ws = WorkflowState(call_stack=Stack([CallFrame(target_topic="t", callback_topic="cb", fanout_id="X")]))
    rt = WorkflowState.model_validate_json(ws.model_dump_json())
    assert rt.current_frame.fanout_id == "X"
