"""The partition-keying seam and every publish path going through it.

The keying contract (task-keying prep spec §3 / the task-keying ADR): the Kafka
partition key must co-locate every pair of messages whose handlers mutate the same
await-spanning workflow state. The policy keys by ``task_id`` — the mesh-wide unit of
partition affinity (task-affinity ⊇ run-affinity: task and correlation are both
minted-once + forwarded, so per-run co-partition sets are identical). Call sites and
tests reference the SEAM, never an inline ``.encode()`` literal.

**§5-G — the flip is observable:** every drive passes a task value DISTINCT from the
correlation id, so an assert of ``key == correlation`` (the old policy) must fail here.
These per-site asserts are the mis-partition tripwire: a swept site that silently kept
``correlation_id.encode()`` — or re-minted mid-path — fails its site's test, not just a
grep. Covered sites (spec §3-D + the client re-key): entry publish, Call dispatch,
parallel-Call fan-out, TailCall self-retry, ReturnCall reply, auto-fault, the fan-out
sibling build, the re-entry self-publish, and the step flush.
"""

from typing import Any

from calfkit._registry import handler
from calfkit._vendor.pydantic_ai.messages import ModelResponse, TextPart
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit.client import Client
from calfkit.keying import partition_key
from calfkit.models import (
    Call,
    CallFrame,
    CallFrameStack,
    Envelope,
    ReturnCall,
    SessionRunContext,
    TailCall,
    WorkflowState,
)
from calfkit.models.marker import ToolCallMarker
from calfkit.models.session_context import Stack
from calfkit.models.state import State
from calfkit.nodes import StatelessAgent
from calfkit.nodes._fanout_store import FANOUT_STORE_KEY
from calfkit.nodes._steps import HopStepLedger
from calfkit.nodes.node import NodeDef
from tests._broker_fakes import CaptureBroker
from tests._fanout_fakes import FakeFanoutBatchStore

_CORR = "corr-keying-1"
_TASK = "task-keying-distinct-1"  # DISTINCT from _CORR — the §5-G observability rider


def test_partition_key_encoding_convention() -> None:
    # The seam's byte encoding: the key is the task id's utf-8 bytes — entry messages
    # and continuations passing the same task_id land in one serialization domain.
    assert partition_key("abc-123") == b"abc-123"
    assert isinstance(partition_key("x"), bytes)


async def test_entry_publish_is_keyed_by_the_minted_task(monkeypatch) -> None:  # noqa: ANN001
    """The client re-key (spec §3-E): ``_publish_call`` keys via the seam whose source is
    the minted ``task_id`` — NOT the (caller-supplied) correlation id."""
    client = Client.connect()
    published: list[dict] = []

    async def spy_publish(message, **kwargs):  # noqa: ANN001, ANN003
        published.append(kwargs)

    monkeypatch.setattr(client._broker, "publish", spy_publish)

    cid, task_id, state = client._build_state(
        "hello",
        correlation_id="cid-entry-1",
        temp_instructions=None,
        message_history=None,
        author=None,
    )
    await client._publish_call(topic="some.topic", correlation_id=cid, task_id=task_id, state=state, deps=None)

    assert len(published) == 1
    assert published[0]["key"] == partition_key(task_id)
    assert published[0]["key"] != b"cid-entry-1"  # the old policy's key must NOT survive the flip
    assert published[0]["correlation_id"] == "cid-entry-1"  # run identity unchanged
    assert isinstance(state, State)


# ---------------------------------------------------------------------------
# The keying invariant on node-side publishes (spec §3-D: the swept sites) —
# each drive threads a DISTINCT task value; every publish must key by it.
# ---------------------------------------------------------------------------


def _envelope(callback_topic: str | None = "reply.topic") -> Envelope:
    stack = CallFrameStack()
    stack.push(CallFrame(target_topic="t", callback_topic=callback_topic))
    return Envelope(
        internal_workflow_state=WorkflowState(call_stack=stack),
        context=SessionRunContext(state=State(), deps={}),
    )


async def _drive(node: NodeDef, *, callback_topic: str | None = "reply.topic") -> CaptureBroker:
    spy = CaptureBroker()
    await node.handler(_envelope(callback_topic), correlation_id=_CORR, task_id=_TASK, headers={}, broker=spy)
    assert spy.published, "the driven path emitted no publish — the test drove nothing"
    return spy


def _assert_all_keyed(spy: CaptureBroker) -> None:
    for call in spy.published:
        assert call.key == partition_key(_TASK), (
            f"node-side publish is not task-keyed: topic={call.topic!r} key={call.key!r} — per-task serialization would silently break"
        )
        assert call.key != _CORR.encode()  # the old correlation policy must fail here (§5-G)


async def test_call_dispatch_publish_carries_the_partition_key() -> None:
    class N(NodeDef):
        async def run(self, ctx: SessionRunContext) -> Any:
            return Call("downstream.topic", ctx.state)

    _assert_all_keyed(await _drive(N(node_id="n-call", subscribe_topics=["t"])))


async def test_parallel_call_dispatch_keys_every_sibling() -> None:
    # The list branch of _publish_action (previously uncovered — cross-PR MAJOR-3):
    # every parallel sibling must carry the task key, or one branch mis-partitions.
    class N(NodeDef):
        async def run(self, ctx: SessionRunContext) -> Any:
            return [Call("down.a", ctx.state), Call("down.b", ctx.state)]

    spy = await _drive(N(node_id="n-par", subscribe_topics=["t"]))
    assert len(spy.published) == 2
    _assert_all_keyed(spy)


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
        task_id=_TASK,
        headers={"x-calf-route": "unmatched.route"},
        broker=spy,
    )
    assert spy.published
    _assert_all_keyed(spy)


async def test_fanout_sibling_publishes_carry_the_partition_key() -> None:
    # The fan-out sibling build (previously uncovered): keyless/mis-keyed siblings would
    # scatter one batch across lanes and violate the FanoutBatchStore single-writer.
    def _model(_messages: object, _info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[TextPart("ok")])

    agent = StatelessAgent(name="a", subscribe_topics=["a.in"], model_client=FunctionModel(_model))
    ctx = SessionRunContext(state=State(), deps={})
    ctx._resources = {FANOUT_STORE_KEY: FakeFanoutBatchStore()}
    ctx._correlation_id = _CORR
    own = CallFrame(target_topic="a", callback_topic="caller", frame_id="A")
    env = Envelope(
        context=SessionRunContext(state=State(), deps={}),
        internal_workflow_state=WorkflowState(call_stack=Stack([own])),
    )
    spy = CaptureBroker()

    await agent._handle_fanout_open(ctx, [Call("tool.a", State(), tag="tc1"), Call("tool.b", State(), tag="tc2")], env, _CORR, _TASK, spy)

    assert len(spy.published) == 2
    _assert_all_keyed(spy)


async def test_reentry_self_publish_carries_the_partition_key() -> None:
    # The re-entry self-publish (previously uncovered): it must land on the SAME
    # partition as the batch's folds (single-writer), so it keys by the same task.
    node = NodeDef(node_id="n-re", subscribe_topics=["t"])
    frame = CallFrame(target_topic="n-re", callback_topic="caller.return", frame_id="A", fanout_id="A")
    env = Envelope(
        context=SessionRunContext(state=State(), deps={}),
        internal_workflow_state=WorkflowState(call_stack=Stack([frame])),
    )
    spy = CaptureBroker()

    await node._publish_reentry(env, _CORR, _TASK, spy)

    _assert_all_keyed(spy)


async def test_step_flush_publish_carries_the_partition_key() -> None:
    # The step flush (previously uncovered; the step-flush task-half — spec §5): the
    # StepMessage co-partitions with the terminal, so its key rides task_id too. The
    # x-calf-task HEADER stamp on step messages stays PR-2's (keyed-but-headerless).
    ledger = HopStepLedger()
    ledger.note_dispatch(Call("t.in", State(), tag="c1", marker=ToolCallMarker(tool_name="t", tool_call_id="c1", args={})))
    spy = CaptureBroker()

    await ledger.flush(
        spy,
        disposition=None,
        depth=1,
        frame_id="F1",
        correlation_id=_CORR,
        task_id=_TASK,
        emitter="agent-a",
        emitter_kind="agent",
        root_callback="client.return",
    )

    _assert_all_keyed(spy)
