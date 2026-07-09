"""The echo marker rail (echo-rail spec §4) — the marker rides the frame, echoes verbatim on the
reply wherever ``in_reply_to``/``tag`` are echoed today, and is read at the fold. Cohesive test home
for the rail's plumbing (``CallFrame.marker`` / ``invoke_frame`` / ``_ReplyBase.marker`` / ``Call.marker``
/ the base echo + threading / ``CalleeResult.marker``).
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest
from faststream.kafka import KafkaBroker, TestKafkaBroker

from calfkit._vendor.pydantic_ai.messages import ToolCallPart
from calfkit._vendor.pydantic_ai.models.test import TestModel
from calfkit.models.actions import Call, ReturnCall, TailCall
from calfkit.models.envelope import Envelope
from calfkit.models.error_report import ErrorReport
from calfkit.models.fanout import SlotRef
from calfkit.models.marker import ToolCallMarker
from calfkit.models.reply import FaultMessage, ReturnMessage
from calfkit.models.seam_context import CalleeResult, SeamContext
from calfkit.models.session_context import CallFrame, SessionRunContext, Stack, WorkflowState
from calfkit.models.state import State
from calfkit.nodes import Agent
from calfkit.nodes._fanout_store import FANOUT_STORE_KEY
from calfkit.nodes.base import BaseNodeDef
from calfkit.peers import Messaging
from tests._fanout_fakes import FakeFanoutBatchStore

_MARKER = ToolCallMarker(tool_name="message_agent", tool_call_id="m1", args={"name": "billing", "message": "hi"})
_M2 = ToolCallMarker(tool_name="web_search", tool_call_id="c1", args={"q": "kafka"})


def _framed_env(*, marker: ToolCallMarker | None = None, callback_topic: str | None = "caller.return") -> Envelope:
    """An inbound envelope with a single answerable frame (optionally marker-stamped)."""
    frame = CallFrame(target_topic="n.in", callback_topic=callback_topic, frame_id="F", tag="t1", marker=marker)
    return Envelope(context=SessionRunContext(state=State(), deps={}), internal_workflow_state=WorkflowState(call_stack=Stack([frame])))


# ── E2: CallFrame.marker + invoke_frame(marker=) ──────────────────────────────


class TestCallFrameMarker:
    def test_defaults_none(self) -> None:
        assert CallFrame(target_topic="t", callback_topic="cb").marker is None

    def test_carries_the_marker(self) -> None:
        assert CallFrame(target_topic="t", callback_topic="cb", marker=_MARKER).marker == _MARKER

    def test_replace_preserves_the_marker(self) -> None:
        # The TailCall retarget shape (base.py:617) — the marker rides via ``replace()`` with NO code (D4).
        frame = CallFrame(target_topic="t", callback_topic="cb", marker=_MARKER)
        assert replace(frame, target_topic="other", payload=None, fanout_id=None).marker == _MARKER


class TestInvokeFrameThreadsMarker:
    @staticmethod
    def _wf() -> WorkflowState:
        root = CallFrame(target_topic="root", callback_topic=None, frame_id="ROOT")
        return WorkflowState(call_stack=Stack([root]))

    def test_threads_marker_on_the_fresh_uuid_branch(self) -> None:
        wf = self._wf()
        wf.invoke_frame(Call("callee", State()), "cb", marker=_MARKER)
        assert wf.current_frame.marker == _MARKER

    def test_threads_marker_on_the_preminted_frame_id_branch(self) -> None:
        wf = self._wf()
        wf.invoke_frame(Call("callee", State()), "cb", frame_id="SLOT", marker=_MARKER)
        assert wf.current_frame.frame_id == "SLOT"
        assert wf.current_frame.marker == _MARKER

    def test_defaults_none_when_unthreaded(self) -> None:
        wf = self._wf()
        wf.invoke_frame(Call("callee", State()), "cb")
        assert wf.current_frame.marker is None


class TestWorkflowStateWireRoundTrip:
    def test_marked_frame_survives_the_wire_typed(self) -> None:
        # [R1-m2] ``CallFrame`` is a stdlib dataclass (no ctor validation) — the marker is validated ONLY
        # here, on the ``WorkflowState`` pydantic decode. Confirm it round-trips as a typed ``ToolCallMarker``.
        frame = CallFrame(target_topic="t", callback_topic="cb", frame_id="F", marker=_MARKER)
        wf = WorkflowState(call_stack=Stack([frame]))
        restored = WorkflowState.model_validate_json(wf.model_dump_json())
        rmarker = restored.current_frame.marker
        assert isinstance(rmarker, ToolCallMarker)
        assert rmarker == _MARKER


# ── E3: _ReplyBase.marker (the echo slot) ─────────────────────────────────────


class TestReplyMarker:
    def test_return_message_carries_and_defaults(self) -> None:
        assert ReturnMessage(in_reply_to="f", tag="t", parts=[]).marker is None
        assert ReturnMessage(in_reply_to="f", tag="t", parts=[], marker=_MARKER).marker == _MARKER

    def test_fault_message_carries_and_defaults(self) -> None:
        err = ErrorReport(error_type="calf.exception", message="boom")
        assert FaultMessage(in_reply_to="f", tag="t", error=err).marker is None
        assert FaultMessage(in_reply_to="f", tag="t", error=err, marker=_MARKER).marker == _MARKER

    def test_reply_marker_survives_the_wire_typed(self) -> None:
        msg = ReturnMessage(in_reply_to="f", tag="t", parts=[], marker=_MARKER)
        restored = ReturnMessage.model_validate_json(msg.model_dump_json())
        assert isinstance(restored.marker, ToolCallMarker)
        assert restored.marker == _MARKER


# ── E4: Call.marker (added alongside the interim tool_name — Parallel Change expand) ──


class TestCallMarkerField:
    def test_carries_and_defaults(self) -> None:
        s = State()
        assert Call("t", s, marker=_MARKER).marker == _MARKER
        assert Call("t", s).marker is None

    def test_marker_is_keyword_only(self) -> None:
        # framework-reserved: everything after ``state`` is keyword-only, so a 3rd positional fails.
        s = State()
        with pytest.raises(TypeError):
            Call("t", s, _MARKER)  # type: ignore[misc]

    def test_marker_participates_in_equality(self) -> None:
        s = State()
        other = ToolCallMarker(tool_name="x", tool_call_id="y")
        assert Call("t", s, marker=_MARKER) != Call("t", s, marker=other)


# ── E5: base echo (the two answering mints) ───────────────────────────────────


class TestEchoMints:
    async def test_return_call_echoes_the_answered_frames_marker(self) -> None:
        broker = KafkaBroker("localhost")

        @broker.subscriber("caller.return", group_id="e5rc")
        async def _cap(body: Envelope) -> None:  # capture so the callback publish has a subscriber
            return None

        node = BaseNodeDef(node_id="n", subscribe_topics=["n.in"])
        async with TestKafkaBroker(broker):
            pub, kind = await node._publish_action(ReturnCall(state=State(), value="ok"), _framed_env(marker=_MARKER), "cid", broker)
        assert kind == "return"
        assert isinstance(pub.reply, ReturnMessage)
        assert pub.reply.marker == _MARKER

    async def test_fault_echoes_the_answered_frames_marker(self) -> None:
        broker = KafkaBroker("localhost")

        @broker.subscriber("caller.return", group_id="e5f")
        async def _cap(body: Envelope) -> None:
            return None

        node = BaseNodeDef(node_id="n", subscribe_topics=["n.in"])
        inbound = _framed_env(marker=_MARKER)
        snapshot = node._stack_snapshot(inbound)  # captured BEFORE the pop — the marker rides with it
        async with TestKafkaBroker(broker):
            mirror, kind = await node._publish_fault(ErrorReport(error_type="calf.exception", message="boom"), snapshot, inbound, "cid", broker)
        assert kind == "fault"
        assert isinstance(mirror.reply, FaultMessage)
        assert mirror.reply.marker == _MARKER

    async def test_frameless_fault_mints_marker_none(self) -> None:
        # [R1-M4] the ``frame is None`` arm of the ``marker = frame.marker if frame is not None else None`` ternary.
        node = BaseNodeDef(node_id="n", subscribe_topics=["n.in"])
        frameless = Envelope(context=SessionRunContext(state=State(), deps={}), internal_workflow_state=WorkflowState(call_stack=Stack([])))
        snapshot = node._stack_snapshot(frameless)
        broker = KafkaBroker("localhost")
        async with TestKafkaBroker(broker):
            mirror, _ = await node._publish_fault(ErrorReport(error_type="calf.exception", message="boom"), snapshot, frameless, "cid", broker)
        assert isinstance(mirror.reply, FaultMessage)
        assert mirror.reply.marker is None

    async def test_fault_from_an_unmarked_frame_echoes_marker_none(self) -> None:
        # The marker-absence path (spec §7.4): an escalation hop / a fault answering an unstamped call —
        # the frame EXISTS but carries no marker (distinct from the frameless branch above) → echoes None.
        broker = KafkaBroker("localhost")

        @broker.subscriber("caller.return", group_id="e5fu")
        async def _cap(body: Envelope) -> None:
            return None

        node = BaseNodeDef(node_id="n", subscribe_topics=["n.in"])
        inbound = _framed_env(marker=None)  # a present-but-unstamped frame (the escalation-hop shape)
        snapshot = node._stack_snapshot(inbound)
        async with TestKafkaBroker(broker):
            mirror, _ = await node._publish_fault(ErrorReport(error_type="calf.exception", message="boom"), snapshot, inbound, "cid", broker)
        assert isinstance(mirror.reply, FaultMessage)
        assert mirror.reply.marker is None

    async def test_reentry_mints_no_marker_even_from_a_marked_frame(self) -> None:
        # D3: the fan-out re-entry answers no Call, so it mints NO marker — even though the frame carries one.
        broker = KafkaBroker("localhost")
        captured: dict[str, Envelope] = {}

        @broker.subscriber("n.private.return", group_id="e5re")
        async def _cap(body: Envelope) -> None:
            captured["e"] = body

        node = BaseNodeDef(node_id="n", subscribe_topics=["n.in"])
        frame = CallFrame(target_topic="n", callback_topic="caller", frame_id="A", fanout_id="A", tag="t", marker=_MARKER)
        env = Envelope(context=SessionRunContext(state=State(), deps={}), internal_workflow_state=WorkflowState(call_stack=Stack([frame])))
        async with TestKafkaBroker(broker):
            await node._publish_reentry(env, "cid", broker)
        assert captured["e"].reply is not None
        assert captured["e"].reply.marker is None


# ── E5: threading the marker through the three invoke_frame pushes ─────────────


class TestMarkerThreading:
    async def test_single_call_arm_threads_the_marker(self) -> None:
        broker = KafkaBroker("localhost")

        @broker.subscriber("callee", group_id="e5sc")
        async def _cap(body: Envelope) -> None:
            return None

        node = BaseNodeDef(node_id="n", subscribe_topics=["n.in"])
        async with TestKafkaBroker(broker):
            pub, kind = await node._publish_action(Call("callee", State(), marker=_M2), _framed_env(), "cid", broker)
        assert kind == "call"
        assert pub.internal_workflow_state.current_frame.marker == _M2

    async def test_tailcall_retarget_preserves_the_marker(self) -> None:
        # D4 regression guard: ``replace()`` preserves the marker on the retargeted frame with NO code, so
        # a handoff target answers the caller's slot echoing the ORIGINAL caller's marker.
        broker = KafkaBroker("localhost")

        @broker.subscriber("newtarget", group_id="e5tc")
        async def _cap(body: Envelope) -> None:
            return None

        node = BaseNodeDef(node_id="n", subscribe_topics=["n.in"])
        async with TestKafkaBroker(broker):
            pub, _ = await node._publish_action(TailCall("newtarget", State()), _framed_env(marker=_MARKER), "cid", broker)
        assert pub.internal_workflow_state.current_frame.target_topic == "newtarget"
        assert pub.internal_workflow_state.current_frame.marker == _MARKER

    async def test_non_durable_parallel_arm_threads_each_marker(self) -> None:
        # The dead-but-uniform arm (Note C): parity with tag/caller_node_id through the same invoke_frame push.
        broker = KafkaBroker("localhost")
        captured: dict[str, Envelope] = {}

        @broker.subscriber("s.a", group_id="e5pa")
        async def _a(body: Envelope) -> None:
            captured["s.a"] = body

        @broker.subscriber("s.b", group_id="e5pb")
        async def _b(body: Envelope) -> None:
            captured["s.b"] = body

        node = BaseNodeDef(node_id="n", subscribe_topics=["n.in"])
        calls = [Call("s.a", State(), marker=_MARKER), Call("s.b", State(), marker=_M2)]
        async with TestKafkaBroker(broker):
            await node._publish_action(calls, _framed_env(), "cid", broker)
        assert captured["s.a"].internal_workflow_state.current_frame.marker == _MARKER
        assert captured["s.b"].internal_workflow_state.current_frame.marker == _M2

    async def test_fanout_open_threads_marker_onto_each_sibling_frame(self) -> None:
        broker = KafkaBroker("localhost")
        captured: dict[str, Envelope] = {}

        @broker.subscriber("s.a", group_id="e5foa")
        async def _a(body: Envelope) -> None:
            captured["s.a"] = body

        @broker.subscriber("s.b", group_id="e5fob")
        async def _b(body: Envelope) -> None:
            captured["s.b"] = body

        agent = Agent(name="fa", subscribe_topics=["fa.in"], model_client=TestModel())
        ctx = SessionRunContext(state=State(), deps={})
        ctx._resources = {FANOUT_STORE_KEY: FakeFanoutBatchStore()}
        ctx._correlation_id = "cid"
        own = CallFrame(target_topic="fa", callback_topic="caller", frame_id="A")
        env = Envelope(context=SessionRunContext(state=State(), deps={}), internal_workflow_state=WorkflowState(call_stack=Stack([own])))
        calls = [Call("s.a", State(), tag="t1", marker=_MARKER), Call("s.b", State(), tag="t2", marker=_M2)]
        async with TestKafkaBroker(broker):
            await agent._handle_fanout_open(ctx, calls, env, "cid", broker)
        assert captured["s.a"].internal_workflow_state.current_frame.marker == _MARKER
        assert captured["s.b"].internal_workflow_state.current_frame.marker == _M2


# ── E6: CalleeResult.marker + _resolve_callee populates it (fault arm) ─────────


def _seam_ctx() -> SeamContext[State]:
    return SeamContext(
        state=State(),
        deps={},
        resources={},
        payload=None,
        node_id="n",
        correlation_id="c",
        emitter_node_id=None,
        route=None,
        delivery_kind="fault",
        awaiting_reply=True,
    )


class TestCalleeResultMarker:
    def test_carries_and_defaults(self) -> None:
        assert CalleeResult(frame_id="f", marker=_MARKER).marker == _MARKER
        assert CalleeResult(frame_id="f").marker is None


# ── The sole producer + the net-simplified SlotRef (interim carriage retired) ──


class TestMessageAgentProducer:
    def test_message_agent_call_stamps_the_marker(self) -> None:
        # ``_message_agent_call`` is the sole rail producer in this PR — it stamps the full call identity
        # (name, id, args) onto ``Call.marker`` (was the interim ``Call.tool_name``).
        agent = Agent("caller", subscribe_topics="caller.in", model_client=TestModel(), peers=[Messaging("billing")])
        call = agent._message_agent_call(ToolCallPart("message_agent", {"name": "billing", "message": "hi"}, tool_call_id="m1"))
        assert call.marker == ToolCallMarker(tool_name="message_agent", tool_call_id="m1", args={"name": "billing", "message": "hi"})
        assert call.isolate_state is True
        assert call.tag == "m1"


class TestSlotRefNetSimplified:
    def test_slot_ref_carries_no_tool_identity(self) -> None:
        # D5: the marker rides the reply, NOT the durable ``SlotRef`` — the fan-out tables are
        # net-simplified (``SlotRef`` LOSES its interim ``tool_name``; nothing is added).
        assert set(SlotRef.model_fields) == {"frame_id", "tag", "target_topic"}


class TestResolveCalleeEchoesMarker:
    def _capturing_node(self, captured: dict[str, Any]) -> BaseNodeDef:
        node = BaseNodeDef(node_id="n", subscribe_topics=["in"])

        @node.on_callee_error
        def capture(ctx: SeamContext[State], fault: ErrorReport) -> None:
            captured["marker"] = ctx.failing_call.marker if ctx.failing_call is not None else "NO-FAILING-CALL"
            return None

        return node

    async def test_fault_arm_sets_failing_call_marker_from_the_reply(self) -> None:
        captured: dict[str, Any] = {}
        node = self._capturing_node(captured)
        reply = FaultMessage(in_reply_to="f1", tag="m1", marker=_MARKER, error=ErrorReport(error_type="calf.exception", message="boom"))
        await node._resolve_callee(_seam_ctx(), "fault", reply, target_topic="agent.billing.private.input")
        assert captured["marker"] == _MARKER  # the echoed marker reaches the failing_call the seam reads

    async def test_normal_tool_fault_has_no_marker(self) -> None:
        captured: dict[str, Any] = {}
        node = self._capturing_node(captured)
        reply = FaultMessage(in_reply_to="f1", tag="c1", error=ErrorReport(error_type="calf.exception", message="boom"))  # unstamped
        await node._resolve_callee(_seam_ctx(), "fault", reply, target_topic=None)
        assert captured["marker"] is None
