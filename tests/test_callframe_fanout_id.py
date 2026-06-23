"""PR-4: the fan-out marker carrier + caller-chosen slot frame ids.

``CallFrame`` carries ``fanout_id`` — the fan-out node's own inbound ``frame_id``.
At fan-out dispatch it is stamped on the node's OWN frame within each sibling's stack
copy (so it survives the callee's return-pop and is the top frame when the sibling
reply re-enters this node), *not* on the pushed callee frame — ``WorkflowState.mark_fanout``
stamps it by replacing the frozen top frame. It defaults to ``None`` (unmarked) on every
non-sibling frame and must survive the JSON hop.

``WorkflowState.invoke_frame`` takes a keyword-only ``frame_id`` so the fan-out OPEN
can pre-mint each callee slot's frame id: the published callee frame *is* that id, so
a reply's ``in_reply_to`` matches the registered slot directly. Existing positional
call sites are untouched, and ``invoke_frame`` never stamps the marker.
"""

from calfkit.models.actions import Call
from calfkit.models.session_context import CallFrame, Stack, WorkflowState
from calfkit.models.state import State


def test_callframe_fanout_id_defaults_none() -> None:
    assert CallFrame(target_topic="t", callback_topic="cb").fanout_id is None


def test_callframe_carries_fanout_id() -> None:
    assert CallFrame(target_topic="t", callback_topic="cb", fanout_id="X").fanout_id == "X"


def test_invoke_frame_does_not_mark_pushed_frame() -> None:
    # invoke_frame pushes the CALLEE frame; the fan-out marker belongs on the node's
    # OWN frame, so the pushed callee frame is always unmarked.
    ws = WorkflowState(call_stack=Stack())
    ws.invoke_frame(Call(target_topic="t", state=State()), callback_topic="cb")
    assert ws.current_frame.fanout_id is None


def test_invoke_frame_uses_caller_chosen_frame_id() -> None:
    # The fan-out OPEN pre-mints each callee slot id and publishes the sibling on it,
    # so a reply's in_reply_to matches the registered SlotRef.frame_id directly.
    ws = WorkflowState(call_stack=Stack())
    ws.invoke_frame(Call(target_topic="t", state=State()), callback_topic="cb", frame_id="slot-1")
    assert ws.current_frame.frame_id == "slot-1"


def test_invoke_frame_mints_frame_id_when_unspecified() -> None:
    # Without a caller-chosen id, invoke_frame keeps minting a fresh uuid7 hex.
    ws = WorkflowState(call_stack=Stack())
    ws.invoke_frame(Call(target_topic="t", state=State()), callback_topic="cb")
    assert len(ws.current_frame.frame_id) == 32  # uuid7().hex


def test_workflowstate_roundtrips_fanout_id() -> None:
    ws = WorkflowState(call_stack=Stack([CallFrame(target_topic="t", callback_topic="cb", fanout_id="X")]))
    rt = WorkflowState.model_validate_json(ws.model_dump_json())
    assert rt.current_frame.fanout_id == "X"


def test_mark_fanout_stamps_own_frame_id_on_top_frame() -> None:
    # The marker value IS the frame's own id (the batch key); mark_fanout sets it on the
    # node's own (top) frame without changing the frame_id.
    ws = WorkflowState(call_stack=Stack())
    ws.invoke_frame(Call(target_topic="t", state=State()), callback_topic="cb", frame_id="agent-frame")
    ws.mark_fanout()
    assert ws.current_frame.fanout_id == "agent-frame"
    assert ws.current_frame.frame_id == "agent-frame"


def test_mark_fanout_only_marks_the_top_frame() -> None:
    ws = WorkflowState(call_stack=Stack())
    ws.invoke_frame(Call(target_topic="caller", state=State()), callback_topic=None, frame_id="bottom")
    ws.invoke_frame(Call(target_topic="agent", state=State()), callback_topic="cb", frame_id="agent-frame")
    ws.mark_fanout()
    top = ws.unwind_frame()
    assert top.fanout_id == "agent-frame"
    assert ws.current_frame.fanout_id is None  # the lower frame is untouched


def test_call_carries_tag() -> None:
    assert Call(target_topic="t", state=State(), tag="tc1").tag == "tc1"


def test_invoke_frame_sets_caller_tag_on_frame() -> None:
    # The fan-out OPEN passes each sibling's tool_call_id as the callee frame's tag, so
    # the tool's reply echoes it (reply.tag) — a self-describing sibling reply.
    ws = WorkflowState(call_stack=Stack())
    ws.invoke_frame(Call(target_topic="t", state=State()), callback_topic="cb", tag="tc1")
    assert ws.current_frame.tag == "tc1"


# ── ADR-0016: per-frame caller identity for the messaging cycle guard ─────────


def test_callframe_caller_node_defaults_none() -> None:
    f = CallFrame(target_topic="t", callback_topic="cb")
    assert f.caller_node_id is None and f.caller_node_kind is None


def test_callframe_carries_caller_node() -> None:
    f = CallFrame(target_topic="t", callback_topic="cb", caller_node_id="planner", caller_node_kind="agent")
    assert f.caller_node_id == "planner" and f.caller_node_kind == "agent"


def test_invoke_frame_stamps_caller_node() -> None:
    # ADR-0016: the cycle guard records the DISPATCHING node's identity on the pushed callee frame, so
    # the accumulated inbound stack carries the ancestor chain. invoke_frame takes caller_node_id/kind.
    ws = WorkflowState(call_stack=Stack())
    ws.invoke_frame(Call(target_topic="t", state=State()), callback_topic="cb", caller_node_id="planner", caller_node_kind="agent")
    assert ws.current_frame.caller_node_id == "planner"
    assert ws.current_frame.caller_node_kind == "agent"


def test_invoke_frame_caller_node_defaults_none() -> None:
    ws = WorkflowState(call_stack=Stack())
    ws.invoke_frame(Call(target_topic="t", state=State()), callback_topic="cb")
    assert ws.current_frame.caller_node_id is None and ws.current_frame.caller_node_kind is None


def test_mark_fanout_preserves_caller_node() -> None:
    # replace() preserves unlisted fields, so the cycle-guard identity survives fan-out marking
    # (the caller is NEVER re-stamped to self — that would manufacture a false self-cycle).
    ws = WorkflowState(call_stack=Stack())
    ws.invoke_frame(Call(target_topic="t", state=State()), callback_topic="cb", frame_id="A", caller_node_id="planner", caller_node_kind="agent")
    ws.mark_fanout()
    assert ws.current_frame.caller_node_id == "planner" and ws.current_frame.fanout_id == "A"


def test_workflowstate_roundtrips_caller_node() -> None:
    ws = WorkflowState(call_stack=Stack([CallFrame(target_topic="t", callback_topic="cb", caller_node_id="planner", caller_node_kind="agent")]))
    rt = WorkflowState.model_validate_json(ws.model_dump_json())
    assert rt.current_frame.caller_node_id == "planner" and rt.current_frame.caller_node_kind == "agent"
