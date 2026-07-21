"""``WorkflowState.to_topology()`` — the lean fault carriage's stack projection
(state-elision spec D2).

The projection keeps the call graph's routing skeleton — per frame: ``frame_id``, both
topics, ``tag``, ``marker``, ``fanout_id``, and the caller identity — and drops call
*content*: ``payload``, ``overrides``, and ``WorkflowState.metadata``. The kept fields are
what escalation addressing (``callback_topic``), slot matching (``in_reply_to``/``tag``/
``marker``), fold routing (``fanout_id``), and ``ancestor_callers`` derivation need; the
dropped ones are needed to *run* a call, not to deliver an error about one.
"""

from calfkit.models.marker import ToolCallMarker
from calfkit.models.session_context import CallFrame, Stack, WorkflowState


def _frame(i: int) -> CallFrame:
    return CallFrame(
        target_topic=f"target-{i}",
        callback_topic=f"callback-{i}",
        frame_id=f"frame-{i}",
        payload={"big": "x" * 64, "i": i},
        overrides=OverridesState(model_settings={"temperature": 0.1}),
        tag=f"tag-{i}",
        marker=ToolCallMarker(tool_name=f"tool-{i}", tool_call_id=f"tc-{i}", args={"q": i}),
        fanout_id=f"frame-{i}" if i == 1 else None,
        caller_node_id=f"caller-{i}",
        caller_node_kind="agent",
    )


def test_to_topology_nulls_payload_and_overrides_and_drops_metadata() -> None:
    ws = WorkflowState(call_stack=Stack([_frame(0), _frame(1)]), metadata={"app": "data"})
    topo = ws.to_topology()
    assert topo.metadata is None
    assert all(f.payload is None for f in topo.call_stack._internal_list)
    assert all(f.overrides is None for f in topo.call_stack._internal_list)


def test_to_topology_preserves_order_and_identity_fields() -> None:
    ws = WorkflowState(call_stack=Stack([_frame(0), _frame(1)]))
    topo = ws.to_topology()
    assert len(topo.call_stack) == 2
    for i, frame in enumerate(topo.call_stack._internal_list):
        assert frame.frame_id == f"frame-{i}"
        assert frame.target_topic == f"target-{i}"
        assert frame.callback_topic == f"callback-{i}"
        assert frame.tag == f"tag-{i}"
        assert frame.marker == ToolCallMarker(tool_name=f"tool-{i}", tool_call_id=f"tc-{i}", args={"q": i})
        assert frame.caller_node_id == f"caller-{i}"
        assert frame.caller_node_kind == "agent"
    # the fan-out marker survives — it is what routes a sibling reply into the durable fold
    assert topo.call_stack._internal_list[1].fanout_id == "frame-1"
    assert topo.call_stack._internal_list[0].fanout_id is None


def test_to_topology_returns_fresh_objects_never_aliasing_the_source() -> None:
    ws = WorkflowState(call_stack=Stack([_frame(0)]))
    topo = ws.to_topology()
    assert topo is not ws
    assert topo.call_stack is not ws.call_stack
    assert topo.call_stack._internal_list is not ws.call_stack._internal_list
    assert topo.call_stack._internal_list[0] is not ws.call_stack._internal_list[0]  # frames are copies, not shared
    # mutating the source stack afterward must not leak into the projection
    ws.call_stack.push(_frame(9))
    assert len(topo.call_stack) == 1
    # the source keeps its heavy fields — the projection never wrote back
    assert ws.call_stack._internal_list[0].payload is not None
    assert ws.call_stack._internal_list[0].overrides is not None


def test_to_topology_on_empty_stack() -> None:
    topo = WorkflowState(call_stack=Stack()).to_topology()
    assert topo.call_stack.is_empty()
    assert topo.metadata is None
