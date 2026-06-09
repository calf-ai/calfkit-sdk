"""The base node handler tolerates an inbound envelope with no call frame
(empty ``call_stack``) — the shape a terminal-sink consumer sees when it taps a
producer's ``publish_topic`` after that producer's ``ReturnCall`` unwound the
stack. Pins that the two frame reads (``prepare_context`` and ``handler``)
degrade to "no frame" instead of raising, while the framed path is unchanged.
"""

from typing import Any, cast

from calfkit.models import (
    CallFrame,
    CallFrameStack,
    Envelope,
    SessionRunContext,
    Silent,
    State,
    WorkflowState,
)
from calfkit.nodes.node import NodeDef

_CORR = "corr-frameless"


def _frameless_envelope() -> Envelope:
    return Envelope(
        internal_workflow_state=WorkflowState(call_stack=CallFrameStack()),
        context=SessionRunContext(state=State(), deps={}),
    )


def _framed_envelope(*, callback_topic: str | None = "reply.topic") -> Envelope:
    stack = CallFrameStack()
    stack.push(CallFrame(target_topic="t", callback_topic=callback_topic))
    return Envelope(
        internal_workflow_state=WorkflowState(call_stack=stack),
        context=SessionRunContext(state=State(), deps={}),
    )


# --- Stack / WorkflowState primitives -------------------------------------- #


def test_stack_is_empty():
    stack = CallFrameStack()
    assert stack.is_empty() is True
    stack.push(CallFrame(target_topic="t", callback_topic=None))
    assert stack.is_empty() is False


def test_current_frame_or_none_is_none_on_empty_stack():
    ws = WorkflowState(call_stack=CallFrameStack())
    assert ws.current_frame_or_none is None


def test_current_frame_or_none_returns_top_frame():
    ws = _framed_envelope(callback_topic="cb").internal_workflow_state
    frame = ws.current_frame_or_none
    assert frame is not None
    assert frame.callback_topic == "cb"


# --- prepare_context tolerates a missing frame ----------------------------- #


async def test_prepare_context_frameless_sets_frame_id_none():
    node = NodeDef(node_id="n", subscribe_topics=["t"])
    ctx = await node.prepare_context(_frameless_envelope(), correlation_id=_CORR)
    assert ctx.frame_id is None
    assert ctx.state.overrides is None  # nothing to lift from a missing frame


async def test_prepare_context_framed_still_threads_frame_id():
    """Regression: the framed path is unchanged — frame_id is threaded onto ctx."""
    env = _framed_envelope()
    node = NodeDef(node_id="n", subscribe_topics=["t"])
    ctx = await node.prepare_context(env, correlation_id=_CORR)
    assert ctx.frame_id == env.internal_workflow_state.current_frame.frame_id


# --- handler tolerates a missing frame end-to-end -------------------------- #


async def test_handler_frameless_base_run_declines_without_raising():
    # Base run() is the @handler('*') catch-all returning Next → output None →
    # handler returns the envelope unchanged. The point: no raise on empty stack.
    node = NodeDef(node_id="n", subscribe_topics=["t"])
    resp = await node.handler(_frameless_envelope(), correlation_id=_CORR, headers={}, broker=cast(Any, None))
    assert resp is not None


async def test_handler_frameless_dispatches_to_overridden_run():
    ran: list[str] = []

    class N(NodeDef[Any]):
        async def run(self, ctx: SessionRunContext) -> Any:
            ran.append("run")
            return Silent()

    node = N(node_id="n", subscribe_topics=["t"])
    resp = await node.handler(_frameless_envelope(), correlation_id=_CORR, headers={}, broker=cast(Any, None))
    assert ran == ["run"]
    assert resp is not None
