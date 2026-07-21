"""Characterization pins for the two TailCall mint sites + the retarget chokepoint.

Pin 1 (overrides-removal spec §3.2/H2): the handoff-built TailCall (``_dispatch_handoff``,
a method builder) and the self-retry TailCall (the inline mint in ``run()``'s
all-resolved-pre-dispatch arm) are TWO DISTINCT code paths — they must stay separate
(spec D3) because PR-1's Engagement stamp attaches at the handoff mint site. Asserted
structurally: the handoff arm routes THROUGH the builder (spied), the self-retry arm
never touches it, and the two products retarget differently (peer topic vs own return
inbox). PR-1's engagement seam test supersedes this pin with a stronger data-level assert.

Pin 2 (overrides-removal impl-plan Step 1.2): the publish chokepoint's TailCall retarget
(§4.2/§15) — byte-for-byte on the FULL published envelope, proving the retarget
``replace`` keeps every non-cleared field: frame identity (``frame_id``/``tag``/
``callback_topic``/``marker``/``caller_node_id``/``caller_node_kind``) preserved,
``payload``/``fanout_id`` cleared, workflow metadata + carried state/deps intact, no
reply. Exercised for both retarget targets the two mint sites produce — a peer topic
and the node's own return inbox.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any
from unittest import mock

from calfkit._protocol import HDR_KIND
from calfkit._vendor.pydantic_ai.messages import ModelResponse
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit.models import CallFrame, CallFrameStack, Envelope, SessionRunContext, State, TailCall, WorkflowState
from calfkit.models.agents import derive_input_topic
from calfkit.models.marker import ToolCallMarker
from calfkit.nodes import StatelessAgent
from calfkit.nodes.base import BaseNodeDef
from calfkit.peers import Handoff
from tests._broker_fakes import CaptureBroker
from tests._peer_fakes import agents_view, ctx_with_view, handoff_part, triage_agent
from tests.test_tool_errors import _unwrap


def _handoff_model() -> FunctionModel:
    def _fn(messages: list[Any], info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[handoff_part("billing", "take over")])

    return FunctionModel(_fn)


# --------------------------------------------------------------------------- #
# Pin 1 — the two TailCall mint sites are distinct code paths                  #
# --------------------------------------------------------------------------- #


async def test_handoff_and_self_retry_tailcalls_mint_from_distinct_paths() -> None:
    # Handoff arm: a winning handoff relinquishes to the PEER's input topic,
    # minted THROUGH the _dispatch_handoff builder (spied, pass-through).
    handoff_agent = triage_agent(_handoff_model(), peers=[Handoff("billing")])
    handoff_ctx = ctx_with_view(agents_view({"billing": "Billing."}))
    with mock.patch.object(StatelessAgent, "_dispatch_handoff", autospec=True, side_effect=StatelessAgent._dispatch_handoff) as handoff_spy:
        handoff_tc = _unwrap(await handoff_agent.run(handoff_ctx))
    assert isinstance(handoff_tc, TailCall)
    assert handoff_tc.target_topic == derive_input_topic("billing")
    assert handoff_spy.call_count == 1  # minted BY the handoff builder

    # Self-retry arm: the same call REJECTED (nobody online) resolves every call
    # pre-dispatch, so run() tail-calls SELF (its own return inbox) for the retry —
    # minted INLINE in run(), never through the handoff builder (the arms are not merged).
    retry_agent = triage_agent(_handoff_model(), peers=[Handoff("billing")])
    retry_ctx = ctx_with_view(agents_view({}))
    with mock.patch.object(StatelessAgent, "_dispatch_handoff", autospec=True, side_effect=StatelessAgent._dispatch_handoff) as retry_spy:
        retry_tc = _unwrap(await retry_agent.run(retry_ctx))
    assert isinstance(retry_tc, TailCall)
    assert retry_tc.target_topic == retry_agent._return_topic
    assert retry_spy.call_count == 0  # the self-retry never touches the handoff path


# --------------------------------------------------------------------------- #
# Pin 2 — retarget chokepoint, byte-for-byte on the full published envelope    #
# --------------------------------------------------------------------------- #


def _inbound_frame() -> CallFrame:
    return CallFrame(
        target_topic="orchestrator.in",
        callback_topic="caller.return",
        frame_id="F1",
        payload="old-body",
        tag="t1",
        fanout_id="X",
        caller_node_id="caller-a",
        caller_node_kind="agent",
        marker=ToolCallMarker(tool_name="search", tool_call_id="t1", args={"q": "x"}),
    )


def _inbound_envelope(frame: CallFrame) -> Envelope:
    stack = CallFrameStack()
    stack.push(frame)
    return Envelope(
        internal_workflow_state=WorkflowState(call_stack=stack, metadata={"wf": 1}),
        context=SessionRunContext(state=State(temp_instructions="inbound"), deps={"k": "v"}),
    )


def _expected_envelope(frame: CallFrame, target_topic: str, carried_state: State) -> Envelope:
    retargeted = replace(frame, target_topic=target_topic, payload=None, fanout_id=None)
    stack = CallFrameStack()
    stack.push(retargeted)
    return Envelope(
        internal_workflow_state=WorkflowState(call_stack=stack, metadata={"wf": 1}),
        context=SessionRunContext(state=carried_state, deps={"k": "v"}),
    )


def _orchestrator() -> BaseNodeDef:
    return BaseNodeDef(node_id="orchestrator", subscribe_topics=["in"])


async def _publish_and_compare(node: BaseNodeDef, tail_call: TailCall[State]) -> None:
    frame = _inbound_frame()
    envelope = _inbound_envelope(frame)
    broker = CaptureBroker()

    await node._publish_action(tail_call, envelope, "cid", "task-under-test", broker)

    published = broker.published[0]
    assert published.topic == tail_call.target_topic
    assert published.key == b"task-under-test"  # keyed by the threaded task_id (task-keying cutover)
    assert published.correlation_id == "cid"
    assert published.headers[HDR_KIND] == "call"
    expected = _expected_envelope(frame, tail_call.target_topic, tail_call.state)
    assert published.message.model_dump(mode="json") == expected.model_dump(mode="json")


async def test_tailcall_retargets_to_a_peer_topic_byte_for_byte() -> None:
    await _publish_and_compare(_orchestrator(), TailCall[State](target_topic="billing.in", state=State(temp_instructions="carried")))


async def test_tailcall_retargets_to_own_return_topic_byte_for_byte() -> None:
    node = _orchestrator()
    await _publish_and_compare(node, TailCall[State](target_topic=node._return_topic, state=State(temp_instructions="carried")))
