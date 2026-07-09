"""PR-4 step 6b: the in-node fan-out machinery on BaseNodeDef.

Tested in isolation against the injected fake store + constructed envelopes (the
@resource never runs offline, so a fan-out agent just gets `agent.resources[KEY] =
fake`). These pin the pieces the staged handler wires together in 6b-B:

- _is_fanout_capable: only non-sequential agents fan out durably
- _resolve_fanout_store: the store comes from ctx.resources, required for fan-out
- _classify_fanout: marker + reply slot => SIBLING fold / RE-ENTRY close / NORMAL
"""

import logging
from typing import Annotated, Any, cast

import pytest
from aiokafka.errors import KafkaError  # type: ignore[import-untyped]
from faststream import Context
from faststream.kafka import KafkaBroker, TestKafkaBroker
from pydantic import ValidationError

from calfkit._protocol import HDR_KIND
from calfkit._vendor.pydantic_ai.messages import ModelResponse, TextPart, ToolCallPart, ToolReturn
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit._vendor.pydantic_ai.models.test import TestModel
from calfkit.models import Call
from calfkit.models.envelope import Envelope
from calfkit.models.error_report import FaultTypes
from calfkit.models.fanout import FanoutOpen, SlotRef
from calfkit.models.reply import FaultMessage, ReturnMessage
from calfkit.models.session_context import CallFrame, SessionRunContext, Stack, WorkflowState
from calfkit.models.state import State
from calfkit.nodes import Agent
from calfkit.nodes._fanout_store import FANOUT_STORE_KEY
from calfkit.nodes.base import BaseNodeDef
from calfkit.nodes.node import NodeDef
from calfkit.worker.lifecycle import ResourceSetupContext
from tests._fanout_fakes import FakeFanoutBatchStore


def _ctx_with_store(store: FakeFanoutBatchStore, *, deps: dict[str, Any] | None = None) -> SessionRunContext:
    ctx = SessionRunContext(state=State(), deps=deps or {})
    ctx._resources = {FANOUT_STORE_KEY: store}
    ctx._correlation_id = "corr-1"
    return ctx


def _model(_messages: object, _info: AgentInfo) -> ModelResponse:
    return ModelResponse(parts=[TextPart("ok")])


def _agent(*, sequential: bool = False) -> Agent[str]:
    return Agent(
        name="a",
        subscribe_topics=["a.in"],
        model_client=FunctionModel(_model),
        sequential_only_mode=sequential,
    )


def _envelope(*, frame_id: str, fanout_id: str | None, reply_in_reply_to: str | None) -> Envelope:
    frame = CallFrame(target_topic="a", callback_topic="caller", frame_id=frame_id, fanout_id=fanout_id)
    reply = ReturnMessage(in_reply_to=reply_in_reply_to, tag="tc1", parts=[]) if reply_in_reply_to is not None else None
    return Envelope(
        context=SessionRunContext(state=State(), deps={}),
        internal_workflow_state=WorkflowState(call_stack=Stack([frame])),
        reply=reply,
    )


# ── capability gate ──────────────────────────────────────────────────────────


def test_base_node_is_not_fanout_capable() -> None:
    node = BaseNodeDef(node_id="n", subscribe_topics=["n.in"])
    assert node._is_fanout_capable is False


def test_agent_is_fanout_capable() -> None:
    assert _agent()._is_fanout_capable is True


def test_sequential_agent_is_not_fanout_capable() -> None:
    assert _agent(sequential=True)._is_fanout_capable is False


# ── store resolution ─────────────────────────────────────────────────────────


def test_resolve_fanout_store_returns_injected_store() -> None:
    agent = _agent()
    fake = FakeFanoutBatchStore()
    ctx = SessionRunContext(state=State(), deps={})
    ctx._resources = {FANOUT_STORE_KEY: fake}
    assert agent._resolve_fanout_store(ctx) is fake


def test_resolve_fanout_store_raises_when_absent() -> None:
    agent = _agent()
    ctx = SessionRunContext(state=State(), deps={})
    ctx._resources = {}
    with pytest.raises(RuntimeError):
        agent._resolve_fanout_store(ctx)


# ── @resource registration ───────────────────────────────────────────────────


def test_fanout_agent_registers_durable_store_resource() -> None:
    # A non-sequential agent registers the node-owned fan-out store @resource (opened by the
    # worker lifecycle in production; injected by prepare_worker offline). The @resource itself
    # is not run here — only its registration is asserted.
    names = {name for name, _ in _agent()._resource_registry()}
    assert FANOUT_STORE_KEY in names


def test_sequential_agent_registers_no_store_resource() -> None:
    names = {name for name, _ in _agent(sequential=True)._resource_registry()}
    assert FANOUT_STORE_KEY not in names


# ── classification ───────────────────────────────────────────────────────────


def test_classify_unmarked_frame_is_normal() -> None:
    env = _envelope(frame_id="A", fanout_id=None, reply_in_reply_to="A")
    assert _agent()._classify_fanout(env) is None


def test_classify_marked_frame_sibling_reply() -> None:
    # marked frame (fanout_id == frame_id == A); reply addresses a sibling callee (B != A)
    env = _envelope(frame_id="A", fanout_id="A", reply_in_reply_to="B")
    assert _agent()._classify_fanout(env) == "sibling"


def test_classify_marked_frame_reentry() -> None:
    # marked frame; reply addresses the fan-out frame itself (in_reply_to == frame_id == A)
    env = _envelope(frame_id="A", fanout_id="A", reply_in_reply_to="A")
    assert _agent()._classify_fanout(env) == "reentry"


def test_classify_non_capable_node_is_normal() -> None:
    # a marked frame on a non-fan-out node never classifies as fan-out
    node = BaseNodeDef(node_id="n", subscribe_topics=["n.in"])
    env = _envelope(frame_id="A", fanout_id="A", reply_in_reply_to="B")
    assert node._classify_fanout(env) is None


def test_classify_marked_frame_without_reply_is_normal() -> None:
    # (coverage d) a marked frame with NO reply slot is not a fold/close continuation → None
    env = _envelope(frame_id="A", fanout_id="A", reply_in_reply_to=None)  # reply_in_reply_to=None ⇒ reply=None
    assert env.reply is None
    assert _agent()._classify_fanout(env) is None


# ── OPEN dispatch path ───────────────────────────────────────────────────────


async def test_handle_fanout_open_writes_open_and_publishes_marked_siblings() -> None:
    broker = KafkaBroker("localhost")
    captured: dict[str, Envelope] = {}

    @broker.subscriber("tool.a", group_id="ta")
    async def _ta(body: Envelope, _h: Annotated[dict[str, Any], Context("message.headers")]) -> None:
        captured["tool.a"] = body

    @broker.subscriber("tool.b", group_id="tb")
    async def _tb(body: Envelope, _h: Annotated[dict[str, Any], Context("message.headers")]) -> None:
        captured["tool.b"] = body

    agent = _agent()
    fake = FakeFanoutBatchStore()
    ctx = _ctx_with_store(fake, deps={"k": "v"})
    own = CallFrame(target_topic="a", callback_topic="caller", frame_id="A")
    env = Envelope(
        context=SessionRunContext(state=State(), deps={"k": "v"}),
        internal_workflow_state=WorkflowState(call_stack=Stack([own])),
    )
    calls = [Call(target_topic="tool.a", state=State(), tag="tc1"), Call(target_topic="tool.b", state=State(), tag="tc2")]

    async with TestKafkaBroker(broker):
        await agent._handle_fanout_open(ctx, calls, env, "corr-1", broker)

    state = await fake.read_state("A")
    assert state is not None
    assert {s.tag for s in state.open.expected} == {"tc1", "tc2"}
    assert state.outcomes == {}
    base = await fake.read_basestate("A")
    assert base is not None
    assert base.snapshot.deps == {"k": "v"}

    assert set(captured) == {"tool.a", "tool.b"}
    open_slot_ids = {s.frame_id for s in state.open.expected}
    published_callee_ids = set()
    for topic, tag in (("tool.a", "tc1"), ("tool.b", "tc2")):
        stack = captured[topic].internal_workflow_state.call_stack._internal_list
        callee, own_copy = stack[-1], stack[-2]
        assert callee.target_topic == topic
        assert callee.tag == tag  # the callee frame carries the tag (echoed on its reply)
        assert callee.callback_topic == "a.private.return"  # returns to the agent's inbox
        assert callee.fanout_id is None  # the callee frame is NOT marked
        assert own_copy.frame_id == "A" and own_copy.fanout_id == "A"  # the node's OWN frame IS
        published_callee_ids.add(callee.frame_id)
    assert published_callee_ids == open_slot_ids  # OPEN slots == published callee frame ids


# NOTE: the isolated `_handle_sibling_fold` tests were removed in the dead-code sweep — that graft
# helper was subsumed by BaseNodeDef._aggregate. The sibling-fold + re-entry-close behavior is
# covered by tests/test_staged_pipeline.py::TestAggregate (fold→park, complete→publish re-entry,
# re-entry→close+restore) and end-to-end by tests/test_durable_fanout_e2e.py.


# ── the OPEN gate: N>=2 OR a single `isolate_state` call (PR-B / L13) ─────────


def test_call_isolate_state_field() -> None:
    # PR-B / C1: `Call.isolate_state` is the in-process signal that the callee runs on a state isolated
    # from the caller's (a fresh seed, e.g. a `message_agent` peer) — so the caller must snapshot/restore
    # rather than adopt the callee's returned state. Default False (an ordinary tool call); opt-in True.
    assert Call("t", State()).isolate_state is False
    assert Call("t", State(), isolate_state=True).isolate_state is True


def test_needs_durable_batch_decouples_from_fanout_capability() -> None:
    # decision 1(b), NARROWED in PR-C: the durable-batch machinery is needed iff the node can parallel-
    # fan-out (`_is_fanout_capable`) OR can dispatch an `isolate_state` call (it carries a `Messaging`
    # handle, signalled by `_messaging_handles`). Decoupled so a `sequential_only_mode` messaging agent
    # still gets the machinery for a lone `message_agent`; for non-messaging nodes — incl. a Handoff-only
    # agent (a winning handoff is a TailCall disposition, never a dispatched Call) — it equals `_is_fanout_capable`.
    plain = NodeDef(node_id="n", subscribe_topics=["n.in"])  # not fan-out-capable, no messaging
    assert plain._needs_durable_batch is False
    fanout = _SingleCallFanoutNode(node_id="f", subscribe_topics=["f.in"])  # fan-out-capable
    assert fanout._needs_durable_batch is True
    plain._messaging_handles = ("messaging-handle",)  # type: ignore[attr-defined]  # a sequential messaging agent
    assert plain._needs_durable_batch is True
    plain._messaging_handles = ()  # type: ignore[attr-defined]  # no messaging handle => no machinery
    assert plain._needs_durable_batch is False


def test_fanout_open_accepts_singleton_expected() -> None:
    # PR-B / L13: the N>=2 record invariant is generalized — a single-slot batch IS representable now,
    # because a lone `isolate_state` call (e.g. a lone `message_agent`) opens a degenerate one-element
    # durable batch (snapshot/restore the caller's state). `min_length` relaxed 2->1; empty still rejects.
    reg = FanoutOpen(fanout_id="x", node_id="a", expected=[SlotRef(frame_id="f1", tag="tc1", target_topic="agent.peer.private.input")])
    assert len(reg.expected) == 1
    with pytest.raises(ValidationError):
        FanoutOpen(fanout_id="x", node_id="a", expected=[])


class _CaptureBroker:
    """Node-side broker stub: records (topic, headers, envelope) per publish."""

    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, str], Envelope]] = []

    async def publish(self, envelope: Envelope, *, topic: str, correlation_id: str, key: bytes, headers: dict[str, str]) -> None:
        self.published.append((topic, headers, envelope))


class _RaisingBroker:
    """Node-side broker stub whose ``publish`` always raises ``KafkaError`` — exercises the OPEN
    dispatch-abort (§4.4): a sibling publish failing after the batch is durably registered."""

    async def publish(self, envelope: Envelope, *, topic: str, correlation_id: str, key: bytes, headers: dict[str, str]) -> None:
        raise KafkaError("simulated sibling publish failure")


class _SingleCallFanoutNode(NodeDef[Any]):
    """A fan-out-capable node whose body returns a ONE-element list[Call]."""

    @property
    def _is_fanout_capable(self) -> bool:
        return True

    async def run(self, ctx: SessionRunContext) -> Any:
        return [Call("only.tool", ctx.state, tag="tc1")]


async def test_single_call_list_does_not_open_durable_batch() -> None:
    # (#2) A fan-out-capable node whose body returns a 1-element list[Call] is a single stateless
    # continuation, NOT a durable batch: the OPEN dispatch's len(output) >= 2 gate reroutes it
    # through _publish_action (the plain parallel publish), so the store is never opened and the
    # node's own frame is never marked.
    node = _SingleCallFanoutNode(node_id="fan", subscribe_topics=["fan.in"])
    fake = FakeFanoutBatchStore()
    node.resources[FANOUT_STORE_KEY] = fake
    own = CallFrame(target_topic="fan.in", callback_topic="caller", frame_id="A")
    env = Envelope(
        context=SessionRunContext(state=State(), deps={}),
        internal_workflow_state=WorkflowState(call_stack=Stack([own])),
    )
    broker = _CaptureBroker()

    await node.handler(env, correlation_id="corr-1", headers={HDR_KIND: "call"}, broker=cast(Any, broker))

    # The durable store was never opened — no batch registered for the single call.
    assert await fake.read_state("A") is None
    assert await fake.read_basestate("A") is None
    # The single call was published via the normal parallel path (callee frame NOT fan-out-marked).
    assert [t for t, _, _ in broker.published] == ["only.tool"]
    callee = broker.published[0][2].internal_workflow_state.current_frame
    assert callee.target_topic == "only.tool"
    assert callee.fanout_id is None  # not marked: this is not a durable fan-out


class _IsolateStateBareCallNode(NodeDef[Any]):
    """A fan-out-capable node whose body returns a BARE ``Call(isolate_state=True)`` — a lone peer message."""

    @property
    def _is_fanout_capable(self) -> bool:
        return True

    async def run(self, ctx: SessionRunContext) -> Any:
        return Call("agent.peer.private.input", State(), tag="tc1", isolate_state=True)


class _SequentialMessagingNode(NodeDef[Any]):
    """A NON-fan-out-capable node (a ``sequential_only_mode`` analog) carrying a ``Messaging`` handle, whose
    body returns a BARE ``Call(isolate_state=True)``: ``_needs_durable_batch`` is True via
    ``_messaging_handles`` (1(b), narrowed to messaging in PR-C — a Handoff-only agent needs no batch)."""

    _messaging_handles = ("messaging-handle",)

    async def run(self, ctx: SessionRunContext) -> Any:
        return Call("agent.peer.private.input", State(), tag="tc1", isolate_state=True)


async def _drive_open(node: NodeDef[Any]) -> tuple[FakeFanoutBatchStore, _CaptureBroker]:
    fake = FakeFanoutBatchStore()
    node.resources[FANOUT_STORE_KEY] = fake
    own = CallFrame(target_topic=node.subscribe_topics[0], callback_topic="caller", frame_id="A")
    env = Envelope(
        context=SessionRunContext(state=State(), deps={}),
        internal_workflow_state=WorkflowState(call_stack=Stack([own])),
    )
    broker = _CaptureBroker()
    await node.handler(env, correlation_id="corr-1", headers={HDR_KIND: "call"}, broker=cast(Any, broker))
    return fake, broker


async def test_lone_isolate_state_call_opens_degenerate_batch() -> None:
    # PR-B / C1: a fan-out-capable node whose body returns a BARE Call(isolate_state=True) (a lone
    # message_agent) opens a degenerate one-element durable batch — caller state snapshotted — NOT the
    # single-Call fast path, so the caller resumes on its OWN state, not the callee's return.
    node = _IsolateStateBareCallNode(node_id="fan", subscribe_topics=["fan.in"])
    fake, _ = await _drive_open(node)
    assert await fake.read_state("A") is not None  # batch registered
    assert await fake.read_basestate("A") is not None  # caller state snapshotted at OPEN


async def test_sequential_messaging_node_opens_degenerate_batch() -> None:
    # decision 1(b): a NON-fan-out-capable node (sequential_only_mode analog) carrying a Messaging handle
    # is `_needs_durable_batch`, so it still opens the degenerate batch for a lone isolate_state call.
    node = _SequentialMessagingNode(node_id="seq", subscribe_topics=["seq.in"])
    assert node._is_fanout_capable is False and node._needs_durable_batch is True
    fake, _ = await _drive_open(node)
    assert await fake.read_state("A") is not None
    assert await fake.read_basestate("A") is not None


async def test_degenerate_batch_snapshots_the_caller_state_not_the_seed() -> None:
    # C1 precision, composed on a NON-empty state (the tests above use an empty State()): the degenerate
    # batch must snapshot the CALLER's state — the one carrying the message_agent registration — NOT the
    # isolate_state Call's fresh seed. The basestate snapshot carries the caller; the published sibling
    # carries the seed. Restore at close therefore resumes on the caller's own history, not the peer's.
    class _SeededIsolateNode(NodeDef[Any]):
        @property
        def _is_fanout_capable(self) -> bool:
            return True

        async def run(self, ctx: SessionRunContext) -> Any:
            seed = State()
            seed.add_tool_call(ToolCallPart(tool_name="t", args={}, tool_call_id="SEED_ONLY"))
            return Call("agent.peer.private.input", seed, tag="tc1", isolate_state=True)

    node = _SeededIsolateNode(node_id="fan", subscribe_topics=["fan.in"])
    fake = FakeFanoutBatchStore()
    node.resources[FANOUT_STORE_KEY] = fake
    caller_state = State()
    caller_state.add_tool_call(ToolCallPart(tool_name="message_agent", args={}, tool_call_id="CALLER_ONLY"))
    own = CallFrame(target_topic="fan.in", callback_topic="caller", frame_id="A")
    env = Envelope(context=SessionRunContext(state=caller_state, deps={}), internal_workflow_state=WorkflowState(call_stack=Stack([own])))
    broker = _CaptureBroker()
    await node.handler(env, correlation_id="c", headers={HDR_KIND: "call"}, broker=cast(Any, broker))

    base = await fake.read_basestate("A")
    assert base is not None
    assert "CALLER_ONLY" in base.snapshot.state.tool_calls  # the snapshot is the caller's state
    assert "SEED_ONLY" not in base.snapshot.state.tool_calls
    sibling_state = broker.published[0][2].context.state  # the published sibling carries the seed
    assert "SEED_ONLY" in sibling_state.tool_calls
    assert "CALLER_ONLY" not in sibling_state.tool_calls


async def test_unflagged_bare_call_stays_fast_path() -> None:
    # An unflagged BARE Call (a lone tool call) is a stateless continuation: the OPEN trigger's bare->list
    # normalization must NOT batch it — no store opened, callee frame not fan-out-marked. (The unflagged
    # 1-element list[Call] case is covered by test_single_call_list_does_not_open_durable_batch above.)
    class _BareUnflaggedNode(NodeDef[Any]):
        @property
        def _is_fanout_capable(self) -> bool:
            return True

        async def run(self, ctx: SessionRunContext) -> Any:
            return Call("only.tool", ctx.state, tag="tc1")

    bare = _BareUnflaggedNode(node_id="fan2", subscribe_topics=["fan.in"])
    fake, broker = await _drive_open(bare)
    assert await fake.read_state("A") is None
    assert broker.published[0][2].internal_workflow_state.current_frame.fanout_id is None


def test_classify_fanout_uses_needs_durable_batch() -> None:
    # decision 1(b): the fold/close continuation gate keys on `_needs_durable_batch`, not
    # `_is_fanout_capable` — so a sequential messaging agent (Messaging handle, not fan-out-capable)
    # classifies + folds its marked sibling/re-entry continuations.
    node = _SequentialMessagingNode(node_id="seq", subscribe_topics=["seq.in"])
    marked = CallFrame(target_topic="seq.in", callback_topic="caller", frame_id="A", fanout_id="A")
    env = Envelope(
        context=SessionRunContext(state=State(), deps={}),
        internal_workflow_state=WorkflowState(call_stack=Stack([marked])),
        reply=ReturnMessage(in_reply_to="slot-1", tag="tc1", parts=[]),  # in_reply_to != frame_id => sibling
    )
    assert node._classify_fanout(env) == "sibling"


# ── ADR-0016: the 3 dispatch pushes stamp the caller's identity ──────────────


class _BareCallNode(NodeDef[Any]):
    """A node whose body returns a single BARE Call — the single-Call dispatch (base.py:529)."""

    async def run(self, ctx: SessionRunContext) -> Any:
        return Call("only.tool", ctx.state, tag="tc1")


class _ParallelListNode(NodeDef[Any]):
    """A NON-fan-out-capable node returning a 2-element list[Call] — the plain parallel publish
    (_publish_action's list branch, base.py:509)."""

    async def run(self, ctx: SessionRunContext) -> Any:
        return [Call("a.tool", ctx.state, tag="tc1"), Call("b.tool", ctx.state, tag="tc2")]


async def test_single_call_stamps_caller_node_on_pushed_frame() -> None:
    # ADR-0016: a single Call's pushed callee frame carries the DISPATCHING node's identity, so the
    # accumulated inbound stack gives the agent resolver the ancestor chain (the cycle guard).
    node = _BareCallNode(node_id="planner", subscribe_topics=["planner.in"])
    _, broker = await _drive_open(node)
    frame = broker.published[0][2].internal_workflow_state.current_frame
    assert frame.caller_node_id == "planner" and frame.caller_node_kind == node._node_kind


async def test_fanout_open_stamps_caller_node_on_sibling_frames() -> None:
    # The parallel/degenerate-batch amplifier (ADR-0016 C1): EVERY sibling frame pushed at fan-out OPEN
    # must carry the dispatching node's identity, or cycle detection is silently disabled for a parallel
    # (or lone-isolate_state) message_agent batch — exactly the amplifier ADR-0016 exists to bound.
    node = _IsolateStateBareCallNode(node_id="planner", subscribe_topics=["planner.in"])
    _, broker = await _drive_open(node)
    frame = broker.published[0][2].internal_workflow_state.current_frame
    assert frame.caller_node_id == "planner" and frame.caller_node_kind == node._node_kind


async def test_parallel_list_stamps_caller_node_on_each_frame() -> None:
    # Plain parallel publish (base.py:509) also stamps the caller on every pushed frame.
    node = _ParallelListNode(node_id="planner", subscribe_topics=["planner.in"])
    _, broker = await _drive_open(node)
    assert len(broker.published) == 2
    for _topic, _headers, env in broker.published:
        assert env.internal_workflow_state.current_frame.caller_node_id == "planner"


async def test_prepare_context_derives_ancestor_callers() -> None:
    # ADR-0016: prepare_context exposes ctx.ancestor_callers — the (caller_node_id, caller_node_kind)
    # set from the inbound stack — so the agent resolver can reject a message_agent whose target is
    # already a suspended ancestor (a ring). Frames with no caller (e.g. the public entry) are filtered.
    node = _BareCallNode(node_id="n", subscribe_topics=["n.in"])
    stack = Stack(
        [
            CallFrame(target_topic="client", callback_topic=None),  # public entry: no caller
            CallFrame(target_topic="b.in", callback_topic="cb", caller_node_id="alpha", caller_node_kind="agent"),
            CallFrame(target_topic="n.in", callback_topic="cb", caller_node_id="beta", caller_node_kind="agent"),
        ]
    )
    env = Envelope(context=SessionRunContext(state=State(), deps={}), internal_workflow_state=WorkflowState(call_stack=stack))
    ctx = await node.prepare_context(env)
    assert ctx.ancestor_callers == frozenset({("alpha", "agent"), ("beta", "agent")})


# ── OPEN dispatch-abort path (§4.4) ──────────────────────────────────────────


async def test_handle_fanout_open_sibling_publish_failure_aborts_and_escalates() -> None:
    # (§4.4 dispatch-abort, 4.6) After the batch is registered, a sibling publish that raises must NOT
    # propagate: _handle_fanout_open tombstones both records AND escalates a fault to the caller. The
    # point-to-point fault publish also fails on the raising broker, but the broadcast mirror carries it.
    agent = _agent()
    fake = FakeFanoutBatchStore()
    ctx = _ctx_with_store(fake)
    own = CallFrame(target_topic="a", callback_topic="caller", frame_id="A")
    env = Envelope(
        context=SessionRunContext(state=State(), deps={}),
        internal_workflow_state=WorkflowState(call_stack=Stack([own])),
    )
    calls = [Call(target_topic="tool.a", state=State(), tag="tc1"), Call(target_topic="tool.b", state=State(), tag="tc2")]

    resp = await agent._handle_fanout_open(ctx, calls, env, "corr-1", cast(Any, _RaisingBroker()))

    assert isinstance(resp.body.reply, FaultMessage)  # escalated — did NOT propagate the KafkaError
    assert resp.body.reply.error.error_type == FaultTypes.FANOUT_ABORTED
    assert resp.body.reply.error.details[FaultTypes.REASON] == FaultTypes.REASON_DISPATCH_FAILED
    assert await fake.read_state("A") is None  # aborted: both records tombstoned
    assert await fake.read_basestate("A") is None


async def test_handle_fanout_open_store_failure_aborts_and_escalates(caplog: pytest.LogCaptureFixture) -> None:
    # (§4.4 dispatch-abort, 4.6) If the durable store fails at OPEN (terminal unavailability, or a
    # writer error in the real store), _handle_fanout_open best-effort tombstones AND escalates a fault
    # to the caller rather than letting the exception escape (which under ACK_FIRST would drop + strand).
    agent = _agent()
    fake = FakeFanoutBatchStore()
    fake.make_unavailable()  # store.open raises FanoutStoreUnavailableError
    ctx = _ctx_with_store(fake)
    own = CallFrame(target_topic="a", callback_topic="caller", frame_id="A")
    env = Envelope(
        context=SessionRunContext(state=State(), deps={}),
        internal_workflow_state=WorkflowState(call_stack=Stack([own])),
    )
    calls = [Call(target_topic="tool.a", state=State(), tag="tc1"), Call(target_topic="tool.b", state=State(), tag="tc2")]
    broker = _CaptureBroker()

    with caplog.at_level(logging.ERROR, logger="calfkit.nodes.base"):
        resp = await agent._handle_fanout_open(ctx, calls, env, "corr-1", cast(Any, broker))

    assert isinstance(resp.body.reply, FaultMessage)  # escalated, did not propagate
    assert resp.body.reply.error.error_type == FaultTypes.FANOUT_ABORTED
    # The fault was published point-to-point to the caller (kind=fault), addressed by the inbound stack.
    assert any(headers.get(HDR_KIND) == "fault" for _, headers, _ in broker.published)
    assert any("fan-out OPEN failed" in r.getMessage() for r in caplog.records)


class _NonKafkaRaisingBroker:
    """``publish`` raises a NON-``KafkaError`` — the leg the narrow ``(KafkaError,
    FanoutStoreUnavailableError)`` catch missed (the C1 escape). Pre-fix this escaped
    ``_handle_fanout_open`` and ``_handle_delivery`` to FastStream = silent drop under ACK_FIRST."""

    async def publish(self, envelope: Envelope, *, topic: str, correlation_id: str, key: bytes, headers: dict[str, str]) -> None:
        raise ValueError("simulated non-Kafka sibling publish failure")


class _FanoutNode(NodeDef[Any]):
    """A fan-out-capable node whose body returns a 2-element ``list[Call]`` (opens a durable batch)."""

    @property
    def _is_fanout_capable(self) -> bool:
        return True

    async def run(self, ctx: SessionRunContext) -> Any:
        return [Call("tool.a", ctx.state, tag="tc1"), Call("tool.b", ctx.state, tag="tc2")]


async def test_handle_fanout_open_non_kafka_publish_failure_aborts_and_escalates() -> None:
    # C1: a NON-KafkaError raised during a sibling publish must abort + escalate exactly like the
    # KafkaError leg — never escape. Pre-fix the narrow `except (KafkaError, FanoutStoreUnavailableError)`
    # let this ValueError escape _handle_fanout_open (silent drop + hung caller under ACK_FIRST).
    agent = _agent()
    fake = FakeFanoutBatchStore()
    ctx = _ctx_with_store(fake)
    own = CallFrame(target_topic="a", callback_topic="caller", frame_id="A")
    env = Envelope(
        context=SessionRunContext(state=State(), deps={}),
        internal_workflow_state=WorkflowState(call_stack=Stack([own])),
    )
    calls = [Call(target_topic="tool.a", state=State(), tag="tc1"), Call(target_topic="tool.b", state=State(), tag="tc2")]

    resp = await agent._handle_fanout_open(ctx, calls, env, "corr-1", cast(Any, _NonKafkaRaisingBroker()))

    assert isinstance(resp.body.reply, FaultMessage)  # escalated — did NOT propagate the ValueError
    assert resp.body.reply.error.error_type == FaultTypes.FANOUT_ABORTED
    assert await fake.read_state("A") is None  # aborted: both records tombstoned
    assert await fake.read_basestate("A") is None


async def test_fanout_open_missing_store_faults_caller_not_escape() -> None:
    # C1: a fan-out through the FULL handler with NO durable store registered must fault the caller,
    # NOT escape. _resolve_fanout_store raises RuntimeError; pre-fix it escaped the unguarded OPEN
    # dispatch in _handle_delivery and reached FastStream (silent drop + hung caller under ACK_FIRST).
    node = _FanoutNode(node_id="fan", subscribe_topics=["fan.in"])  # deliberately NO FANOUT_STORE_KEY resource
    own = CallFrame(target_topic="fan.in", callback_topic="caller", frame_id="A")
    env = Envelope(
        context=SessionRunContext(state=State(), deps={}),
        internal_workflow_state=WorkflowState(call_stack=Stack([own])),
    )
    broker = _CaptureBroker()

    resp = await node.handler(env, correlation_id="corr-1", headers={HDR_KIND: "call"}, broker=cast(Any, broker))

    assert isinstance(resp.body.reply, FaultMessage)  # faulted the caller, did NOT escape to FastStream
    assert resp.body.reply.error.error_type == FaultTypes.FANOUT_ABORTED


# ── @resource preconditions (coverage d) ─────────────────────────────────────


class _NoBootstrapWorker:
    """Minimal worker stub whose bootstrap address is underivable (client built without connect())."""

    def _derive_bootstrap_servers(self) -> str | None:
        return None


async def test_fanout_store_resource_raises_without_worker() -> None:
    # (coverage d) The fan-out store @resource cannot open without a hosting worker.
    agent = _agent()
    agent._worker = None
    ctx = ResourceSetupContext(owner=agent, resources={})
    gen = agent._fanout_store_resource(ctx)
    with pytest.raises(RuntimeError, match="no hosting worker"):
        await gen.__anext__()


async def test_fanout_store_resource_raises_without_bootstrap() -> None:
    # (coverage d) A worker present but with no derivable bootstrap address also raises (rather than
    # opening a store against nothing).
    agent = _agent()
    agent._worker = cast(Any, _NoBootstrapWorker())
    ctx = ResourceSetupContext(owner=agent, resources={})
    gen = agent._fanout_store_resource(ctx)
    with pytest.raises(RuntimeError, match="bootstrap servers"):
        await gen.__anext__()


# ── parallel-mode incomplete-batch guard in run() (coverage d) ────────────────


async def test_parallel_run_on_incomplete_batch_raises_runtime_error() -> None:
    # (coverage d) run() must only be re-entered on a COMPLETE batch (the durable close materializes
    # every outcome first). A parallel-mode ctx whose latest tool-call set is incomplete (one call
    # has no result) is the lost-batch/rebalance signal — run() raises a diagnostic RuntimeError
    # rather than silently proceeding.
    agent = Agent(
        "agent_incomplete_batch",
        system_prompt="x",
        subscribe_topics="agent_incomplete_batch.input",
        publish_topic="agent_incomplete_batch.output",
        model_client=TestModel(),
    )

    state = State()
    done_id, pending_id = "tc-done", "tc-pending"
    for tool_name, tcid in (("tool_done", done_id), ("tool_pending", pending_id)):
        part = ToolCallPart(tool_name=tool_name, args={}, tool_call_id=tcid)
        state.add_tool_call(part)
        state.message_history.append(ModelResponse(parts=[part]))
    state.add_tool_result(done_id, ToolReturn(return_value="ok"))
    # pending_id deliberately has NO result → the batch is incomplete.

    ctx = SessionRunContext(state=state, deps={})
    ctx._correlation_id = "cid-incomplete-batch"
    ctx._frame_id = "frame-incomplete-batch"

    with pytest.raises(RuntimeError, match="incomplete tool calls in run"):
        await agent.run(ctx)
