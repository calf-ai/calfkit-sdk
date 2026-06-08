"""§8.2 — header route dispatch: the Next sentinel, wire-model carriers, and the
Chain-of-Responsibility dispatch in BaseNodeDef.handler()."""

import pytest

from calfkit._protocol import HDR_ROUTE
from calfkit.models import Call, CallFrame, CallFrameStack, Next, Silent, TailCall, WorkflowState


def test_next_is_a_distinct_sentinel_from_silent() -> None:
    assert isinstance(Next(), Next)
    assert not isinstance(Next(), Silent)
    assert not isinstance(Silent(), Next)


def test_hdr_route_header_name() -> None:
    assert HDR_ROUTE == "x-calf-route"


def test_callframe_carries_optional_payload_defaulting_none() -> None:
    assert CallFrame(target_topic="t", callback_topic=None).payload is None
    framed = CallFrame(target_topic="t", callback_topic=None, payload={"x": 1})
    assert framed.payload == {"x": 1}


def test_invoke_frame_threads_explicit_payload_onto_the_frame() -> None:
    ws = WorkflowState(call_stack=CallFrameStack())
    call = Call("target", object(), route="order.created", body={"k": 1})
    ws.invoke_frame(call, "callback.topic", payload=call.body)
    assert ws.current_frame.payload == {"k": 1}
    assert ws.current_frame.target_topic == "target"


def test_call_carries_optional_route_and_body() -> None:
    call = Call("topic", object(), route="order.created", body={"x": 1})
    assert call.route == "order.created"
    assert call.body == {"x": 1}
    plain = Call("topic", object())
    assert plain.route is None and plain.body is None


def test_tailcall_does_not_accept_route_or_body() -> None:
    with pytest.raises(TypeError):
        TailCall("topic", object(), route="x")  # type: ignore[call-arg]
