"""PR-4 step 6b: the in-node fan-out machinery on BaseNodeDef.

Tested in isolation against the injected fake store + constructed envelopes (the
@resource never runs offline, so a fan-out agent just gets `agent.resources[KEY] =
fake`). These pin the pieces the staged handler wires together in 6b-B:

- _is_fanout_capable: only non-sequential agents fan out durably
- _resolve_fanout_store: the store comes from ctx.resources, required for fan-out
- _classify_fanout: marker + reply slot => SIBLING fold / RE-ENTRY close / NORMAL
"""

from typing import Annotated, Any

import pytest
from faststream import Context
from faststream.kafka import KafkaBroker, TestKafkaBroker

from calfkit._vendor.pydantic_ai.messages import ModelResponse, TextPart
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit.models import Call
from calfkit.models.envelope import Envelope
from calfkit.models.reply import ReturnMessage
from calfkit.models.session_context import CallFrame, SessionRunContext, Stack, WorkflowState
from calfkit.models.state import State
from calfkit.nodes import Agent
from calfkit.nodes._fanout_store import FANOUT_STORE_KEY
from calfkit.nodes.base import BaseNodeDef
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
        node_id="a",
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
