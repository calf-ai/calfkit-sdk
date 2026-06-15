"""PR-4 step 6b: the in-node fan-out machinery on BaseNodeDef.

Tested in isolation against the injected fake store + constructed envelopes (the
@resource never runs offline, so a fan-out agent just gets `agent.resources[KEY] =
fake`). These pin the pieces the staged handler wires together in 6b-B:

- _is_fanout_capable: only non-sequential agents fan out durably
- _resolve_fanout_store: the store comes from ctx.resources, required for fan-out
- _classify_fanout: marker + reply slot => SIBLING fold / RE-ENTRY close / NORMAL
"""

import pytest

from calfkit._vendor.pydantic_ai.messages import ModelResponse, TextPart
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit.models.envelope import Envelope
from calfkit.models.reply import ReturnMessage
from calfkit.models.session_context import CallFrame, SessionRunContext, Stack, WorkflowState
from calfkit.models.state import State
from calfkit.nodes import Agent
from calfkit.nodes._fanout_store import FANOUT_STORE_KEY
from calfkit.nodes.base import BaseNodeDef
from tests._fanout_fakes import FakeFanoutBatchStore


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
