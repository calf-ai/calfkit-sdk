"""Interim ``SlotRef`` tool-identity carriage (# TODO(echo-rail)) — unit coverage.

Carries the failing tool's *name* from dispatch to the fold for the ONE arm whose reply state is
foreign (``message_agent``/``isolate_state``): ``Call.tool_name`` → ``SlotRef.tool_name`` (OPEN) →
threaded through ``_resolve_callee`` onto ``CalleeResult.tool_name`` (the ``failing_call`` the
``on_callee_error`` seam reads). Set only by ``_message_agent_call``; ``None`` throughout for a normal
``Call``. This whole mechanism is reworked wholesale by the echo marker rail
(``docs/issues/echo-marker-rail.md``) — locate it by ``grep -rn "TODO(echo-rail)"``.
"""

from __future__ import annotations

from typing import Any

import pytest
from faststream.kafka import KafkaBroker, TestKafkaBroker

from calfkit._vendor.pydantic_ai.messages import ToolCallPart
from calfkit._vendor.pydantic_ai.models.test import TestModel
from calfkit.models.actions import Call
from calfkit.models.envelope import Envelope
from calfkit.models.error_report import ErrorReport
from calfkit.models.fanout import SlotRef
from calfkit.models.reply import FaultMessage
from calfkit.models.seam_context import CalleeResult, SeamContext
from calfkit.models.session_context import CallFrame, SessionRunContext, Stack, WorkflowState
from calfkit.models.state import State
from calfkit.nodes import Agent
from calfkit.nodes._fanout_store import FANOUT_STORE_KEY
from calfkit.nodes.base import BaseNodeDef
from calfkit.peers import Messaging
from tests._fanout_fakes import FakeFanoutBatchStore

# ── the three carried fields ──────────────────────────────────────────────────


class TestCarriedFields:
    def test_call_tool_name_is_keyword_only_and_defaults_none(self) -> None:
        s = State()
        assert Call("t", s, tool_name="message_agent").tool_name == "message_agent"
        assert Call("t", s).tool_name is None  # default — a normal Call carries no tool_name
        # keyword-only (framework-reserved): the extra fields all sit after ``*``, so a 3rd positional fails.
        with pytest.raises(TypeError):
            Call("t", s, "message_agent")  # type: ignore[misc]
        # participates in equality (init=False dataclass field)
        assert Call("t", s, tool_name="a") != Call("t", s, tool_name="b")

    def test_slotref_tool_name_defaults_none(self) -> None:
        assert SlotRef(frame_id="f", tag="t", target_topic="tt", tool_name="message_agent").tool_name == "message_agent"
        assert SlotRef(frame_id="f", tag="t", target_topic="tt").tool_name is None

    def test_callee_result_tool_name_defaults_none(self) -> None:
        assert CalleeResult(frame_id="f", tool_name="message_agent").tool_name == "message_agent"
        assert CalleeResult(frame_id="f").tool_name is None


# ── the only setter: _message_agent_call ──────────────────────────────────────


class TestMessageAgentCallSetsToolName:
    def test_message_agent_call_stamps_the_well_known_name(self) -> None:
        agent = Agent("caller", subscribe_topics="caller.in", model_client=TestModel(), peers=[Messaging("billing")])
        call = agent._message_agent_call(ToolCallPart("message_agent", {"name": "billing", "message": "hi"}, tool_call_id="m1"))
        assert call.tool_name == "message_agent"  # the ONLY setter (the empty-state arm)
        assert call.isolate_state is True and call.tag == "m1"


# ── the OPEN copy: Call.tool_name → SlotRef.tool_name ─────────────────────────


def _fanout_agent() -> Agent[str]:
    return Agent(name="a", subscribe_topics=["a.in"], model_client=TestModel())  # non-sequential ⇒ fan-out capable


def _ctx_with_store(store: FakeFanoutBatchStore) -> SessionRunContext:
    ctx = SessionRunContext(state=State(), deps={})
    ctx._resources = {FANOUT_STORE_KEY: store}
    ctx._correlation_id = "corr-1"
    return ctx


class TestOpenCopiesToolName:
    async def test_open_copies_tool_name_from_each_call_to_its_slotref(self) -> None:
        broker = KafkaBroker("localhost")

        @broker.subscriber("agent.billing.private.input", group_id="peer")
        async def _peer(body: Envelope) -> None:
            return None

        @broker.subscriber("tool.a", group_id="ta")
        async def _ta(body: Envelope) -> None:
            return None

        agent = _fanout_agent()
        fake = FakeFanoutBatchStore()
        ctx = _ctx_with_store(fake)
        own = CallFrame(target_topic="a", callback_topic="caller", frame_id="A")
        env = Envelope(context=SessionRunContext(state=State(), deps={}), internal_workflow_state=WorkflowState(call_stack=Stack([own])))
        calls = [
            Call("agent.billing.private.input", State(), tag="m1", isolate_state=True, tool_name="message_agent"),
            Call("tool.a", State(), tag="c1"),  # a normal tool sibling — no tool_name
        ]

        async with TestKafkaBroker(broker):
            await agent._handle_fanout_open(ctx, calls, env, "corr-1", broker)

        state = await fake.read_state("A")
        assert state is not None
        by_tag = {s.tag: s.tool_name for s in state.open.expected}
        assert by_tag == {"m1": "message_agent", "c1": None}  # carried per-slot; None for the normal tool


# ── the threading: _resolve_callee → failing_call.tool_name ───────────────────


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


class TestResolveCalleeThreadsToolName:
    async def test_failing_call_carries_tool_name_to_the_seam(self) -> None:
        node = BaseNodeDef(node_id="n", subscribe_topics=["in"])
        captured: dict[str, Any] = {}

        @node.on_callee_error
        def capture(ctx: SeamContext[State], fault: ErrorReport) -> None:
            captured["tool_name"] = ctx.failing_call.tool_name if ctx.failing_call is not None else "NO-FAILING-CALL"
            return None  # decline

        reply = FaultMessage(in_reply_to="f1", tag="m1", error=ErrorReport(error_type="calf.exception", message="boom"))
        await node._resolve_callee(_seam_ctx(), "fault", reply, target_topic="agent.billing.private.input", tool_name="message_agent")
        assert captured["tool_name"] == "message_agent"  # the carriage reaches the failing_call the seam reads

    async def test_normal_tool_fold_has_no_tool_name(self) -> None:
        # None-safety: a normal tool fault threads tool_name=None → failing_call.tool_name is None.
        node = BaseNodeDef(node_id="n", subscribe_topics=["in"])
        captured: dict[str, Any] = {}

        @node.on_callee_error
        def capture(ctx: SeamContext[State], fault: ErrorReport) -> None:
            captured["tool_name"] = ctx.failing_call.tool_name if ctx.failing_call is not None else "NO-FAILING-CALL"
            return None

        reply = FaultMessage(in_reply_to="f1", tag="c1", error=ErrorReport(error_type="calf.exception", message="boom"))
        await node._resolve_callee(_seam_ctx(), "fault", reply, target_topic=None, tool_name=None)
        assert captured["tool_name"] is None
