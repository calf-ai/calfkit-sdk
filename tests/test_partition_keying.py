"""The client-side partition-keying seam and the entry publish going through it.

The keying contract (spec §11 / ADR): the Kafka partition key must co-locate every pair
of messages whose handlers mutate the same await-spanning workflow state. Today's policy
keys by ``correlation_id``; the policy is expected to change in a later PR, so call sites
and tests reference the SEAM, never an inline ``correlation_id.encode()`` literal.
"""

from calfkit.client import Client
from calfkit.keying import partition_key
from calfkit.models import State


def test_partition_key_matches_todays_node_side_convention() -> None:
    # Node-side publishes key with correlation_id bytes; the seam must agree so entry
    # messages and continuations share a serialization domain.
    assert partition_key("abc-123") == b"abc-123"
    assert isinstance(partition_key("x"), bytes)


async def test_entry_publish_is_keyed_via_the_seam(monkeypatch) -> None:  # noqa: ANN001
    """The gateway's entry publish carries the seam's partition key — previously it sent
    only ``correlation_id=`` (a FastStream trace/header value, NOT the partition key), so
    entry Calls were keyless and round-robined across lanes."""
    client = Client.connect()
    published: list[dict] = []

    async def spy_publish(message, **kwargs):  # noqa: ANN001, ANN003
        published.append(kwargs)

    monkeypatch.setattr(client._broker, "publish", spy_publish)

    cid, state, overrides = client._build_state_and_overrides(
        "hello",
        correlation_id="cid-entry-1",
        temp_instructions=None,
        message_history=None,
        tool_overrides=None,
        model_settings=None,
        author=None,
    )
    await client._publish_call(topic="some.topic", correlation_id=cid, state=state, overrides=overrides, deps=None)

    assert len(published) == 1
    assert published[0]["key"] == partition_key("cid-entry-1")
    assert published[0]["correlation_id"] == "cid-entry-1"
    assert isinstance(state, State)


# ---------------------------------------------------------------------------
# The keying invariant on node-side publishes (spec §11: "every message a
# caller-capable node consumes carries the policy's partition key" — the node
# OUTPUT paths produce those inbound messages: Call dispatches, TailCall
# self-retries, ReturnCall replies, and auto-faults).
# ---------------------------------------------------------------------------

from typing import Any  # noqa: E402

from calfkit._registry import handler  # noqa: E402
from calfkit.models import (  # noqa: E402
    Call,
    CallFrame,
    CallFrameStack,
    Envelope,
    ReturnCall,
    SessionRunContext,
    TailCall,
    WorkflowState,
)
from calfkit.nodes.node import NodeDef  # noqa: E402
from tests.fakes import CaptureBroker  # noqa: E402

_CORR = "corr-keying-1"


def _envelope(callback_topic: str | None = "reply.topic") -> Envelope:
    stack = CallFrameStack()
    stack.push(CallFrame(target_topic="t", callback_topic=callback_topic))
    return Envelope(
        internal_workflow_state=WorkflowState(call_stack=stack),
        context=SessionRunContext(state=State(), deps={}),
    )


async def _drive(node: NodeDef, *, callback_topic: str | None = "reply.topic") -> CaptureBroker:
    spy = CaptureBroker()
    await node.handler(_envelope(callback_topic), correlation_id=_CORR, headers={}, broker=spy)
    assert spy.published, "the driven path emitted no publish — the test drove nothing"
    return spy


def _assert_all_keyed(spy: CaptureBroker) -> None:
    for call in spy.published:
        assert call.key == partition_key(_CORR), (
            f"node-side publish lost the partition key: topic={call.topic!r} key={call.key!r} — per-correlation serialization would silently break"
        )


async def test_call_dispatch_publish_carries_the_partition_key() -> None:
    class N(NodeDef):
        async def run(self, ctx: SessionRunContext) -> Any:
            return Call("downstream.topic", ctx.state)

    _assert_all_keyed(await _drive(N(node_id="n-call", subscribe_topics=["t"])))


async def test_tailcall_self_retry_publish_carries_the_partition_key() -> None:
    class N(NodeDef):
        async def run(self, ctx: SessionRunContext) -> Any:
            return TailCall("t", ctx.state)

    _assert_all_keyed(await _drive(N(node_id="n-tail", subscribe_topics=["t"])))


async def test_returncall_reply_publish_carries_the_partition_key() -> None:
    class N(NodeDef):
        async def run(self, ctx: SessionRunContext) -> Any:
            return ReturnCall(state=ctx.state, value="done")

    _assert_all_keyed(await _drive(N(node_id="n-ret", subscribe_topics=["t"])))


async def test_auto_fault_publish_carries_the_partition_key() -> None:
    # A reply-owing delivery whose route matches nothing (and no run() fallback)
    # auto-faults to the callback — that fault publish must be keyed too.
    class N(NodeDef):
        @handler("known.route")
        async def on_known(self, ctx: SessionRunContext) -> Any:
            return ReturnCall(state=ctx.state, value="handled")

    spy = CaptureBroker()
    node = N(node_id="n-fault", subscribe_topics=["t"])
    await node.handler(
        _envelope("reply.topic"),
        correlation_id=_CORR,
        headers={"x-calf-route": "unmatched.route"},
        broker=spy,
    )
    assert spy.published
    _assert_all_keyed(spy)
