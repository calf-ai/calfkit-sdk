"""PR-4 step 5: _publish_reentry — the bespoke frame-preserving self-return.

The fan-out closure is a self-published re-entry to the node's own return inbox, NOT a
ReturnCall (which would pop the frame). This pins the deltas (plan §5): no pop, self-
addressed to _return_topic, in_reply_to = the fan-out frame's own id, parts = [], context
state+deps cleared (rebuilt from basestate at close), kind=return, and NOT broadcast-
mirrored. Follows the project's TestKafkaBroker capture pattern (see test_headers.py).
"""

from typing import Annotated, Any

from faststream import Context
from faststream.kafka import KafkaBroker, TestKafkaBroker

from calfkit._protocol import HDR_KIND, decode_header_str
from calfkit.models.envelope import Envelope
from calfkit.models.session_context import CallFrame, SessionRunContext, Stack, WorkflowState
from calfkit.models.state import State
from calfkit.nodes.base import BaseNodeDef


def _fanout_envelope() -> Envelope:
    # The state at a completing fold: the node's OWN marked fan-out frame on top
    # (fanout_id == frame_id), with stale context that the re-entry must clear.
    frame = CallFrame(target_topic="agent", callback_topic="caller.return", frame_id="A", fanout_id="A", tag="agentTag")
    return Envelope(
        context=SessionRunContext(state=State(metadata="stale"), deps={"k": "v"}),
        internal_workflow_state=WorkflowState(call_stack=Stack([frame])),
    )


async def test_publish_reentry_deltas() -> None:
    broker = KafkaBroker("localhost")
    captured: dict[str, Any] = {}
    mirror_hits: list[Any] = []

    @broker.subscriber("agent.private.return", group_id="reentry_cap")
    async def _cap(
        body: Envelope,
        headers: Annotated[dict[str, Any], Context("message.headers")],
        msg: Annotated[Any, Context("message")],
    ) -> None:
        captured["envelope"] = body
        captured["headers"] = dict(headers)
        captured["correlation_id"] = msg.correlation_id

    @broker.subscriber("agent.broadcast", group_id="reentry_mirror")
    async def _mirror(body: Any) -> None:
        mirror_hits.append(body)

    node = BaseNodeDef(node_id="agent", subscribe_topics=["agent.input"], publish_topic="agent.broadcast")

    async with TestKafkaBroker(broker):
        await node._publish_reentry(_fanout_envelope(), "corr-123", broker)

    env: Envelope = captured["envelope"]
    assert env.reply is not None
    assert env.reply.in_reply_to == "A"  # the fan-out frame's own id (== fanout_id)
    assert env.reply.tag == "agentTag"
    assert env.reply.parts == []  # no payload — the closure reads from the durable tables
    assert env.context.state.metadata is None  # state cleared (was "stale")
    assert env.context.deps == {}  # deps cleared
    assert env.internal_workflow_state.current_frame.frame_id == "A"  # NOT popped — frame stays
    assert decode_header_str(captured["headers"].get(HDR_KIND)) == "return"
    assert captured["correlation_id"] == "corr-123"
    assert mirror_hits == []  # NOT broadcast-mirrored
