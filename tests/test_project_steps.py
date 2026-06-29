"""Increment D — emission: Stack accessors, the _step_draft slot, project_steps, and the
disposition-chokepoint behavior (intermediate-step-streaming spec §2.5 / §2.7 / §3.2 / §3.3).

Offline, no broker for the unit pieces; the chokepoint pieces drive node.handler with a capture
broker (mirroring test_fault_pipeline.py).
"""

from __future__ import annotations

from typing import Annotated
from unittest.mock import AsyncMock

import pytest
from pydantic import BeforeValidator

from calfkit._protocol import HDR_EMITTER, HDR_EMITTER_KIND, HDR_KIND, HDR_WIRE
from calfkit._vendor.pydantic_ai.messages import ModelRequest, ModelResponse, RetryPromptPart, ThinkingPart, ToolCallPart, ToolReturn, UserPromptPart
from calfkit._vendor.pydantic_ai.messages import TextPart as VTextPart
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit.models import CallFrame, CallFrameStack, Envelope, ReturnCall, SessionRunContext, State, TailCall, WorkflowState
from calfkit.models.payload import TextPart, retry_text_part
from calfkit.models.step import AgentMessageStep, StepMessage, ToolCallStep, ToolResultStep
from calfkit.models.tool_context import ToolContext
from calfkit.nodes import Agent, ToolNodeDef, agent_tool
from calfkit.nodes._projection import step_preamble
from calfkit.nodes.base import BaseNodeDef
from calfkit.peers import Messaging
from tests.test_message_agent import _ctx_with_view, _msg_call, _view
from tests.test_tool_errors import _final_text_model, _make_ctx, _model_emits_tool_calls, _register_tool_call

_HEADERS = {HDR_EMITTER: b"upstream", HDR_EMITTER_KIND: b"agent"}


def _delivery_envelope(state: State, *, callback_topic: str = "caller.inbox", tag: str | None = None) -> Envelope:
    """A depth-1 (single root frame) inbound envelope for driving ``_handle_delivery`` at the chokepoint."""
    frame = CallFrame(target_topic="node.in", callback_topic=callback_topic, tag=tag)
    return Envelope(context=SessionRunContext(state=state, deps={}), internal_workflow_state=WorkflowState(call_stack=CallFrameStack([frame])))


def _published_steps(broker: AsyncMock) -> list:
    """The StepMessage publishes captured on a mock broker (the action/terminal/fault publishes are Envelopes)."""
    return [c for c in broker.publish.call_args_list if c.args and isinstance(c.args[0], StepMessage)]


@agent_tool
def _echo_tool(ctx) -> str:  # noqa: ANN001
    return "tool-output"


def _typed_tool_node() -> ToolNodeDef:
    def typed_tool(ctx: ToolContext, x: int) -> str:
        return f"got {x}"

    return ToolNodeDef.create_tool_node(func=typed_tool, subscribe_topics="tool.typed.input", publish_topic="tool.typed.output")


def _angry_validator(v: int) -> int:
    raise RuntimeError("angry validator")


def _bad_validator_tool_node() -> ToolNodeDef:
    def bad_validator_tool(ctx: ToolContext, x: Annotated[int, BeforeValidator(_angry_validator)]) -> str:
        return f"x={x}"

    return ToolNodeDef.create_tool_node(func=bad_validator_tool, subscribe_topics="tool.angry.input", publish_topic="tool.angry.output")


def _drafted(ctx: SessionRunContext) -> tuple[list[ToolCallStep], list[ToolResultStep]]:
    """The (ToolCallStep, is_error ToolResultStep) pair the hop authored into ``_step_draft``."""
    draft = ctx._step_draft or []
    return ([e for e in draft if isinstance(e, ToolCallStep)], [e for e in draft if isinstance(e, ToolResultStep)])


def _agent_emitting(calls: list[ToolCallPart], *, name: str = "a", topics: str = "a.in", **kw: object) -> Agent:
    """An agent whose model deterministically emits ``calls`` — for driving the dispatch loop's arms."""
    return Agent(name, system_prompt="x", subscribe_topics=topics, model_client=_model_emits_tool_calls(calls), **kw)


class TestRejectionArmsAuthorPairedSteps:
    """Every calfkit-caught invalid call surfaces as a ToolCallStep + a paired is_error
    ToolResultStep in the hop's draft (spec §3.2). One test per REACHABLE rejection arm —
    including the message_agent peer-consult arm, whose missing pairing was the round-1 review's
    MAJOR-1. (The ``binding is None`` arm is not exercised: a name absent from the toolset is
    auto-retried INSIDE pydantic-ai and never reaches calfkit dispatch — agent.py:599-602.)"""

    async def test_malformed_args_arm(self) -> None:
        bad = ToolCallPart(tool_name="typed_tool", args="not-valid-json", tool_call_id="m1")
        agent = _agent_emitting([bad], tools=[_typed_tool_node()])
        await agent.run(_ctx := _make_ctx(State()))
        tcs, trs = _drafted(_ctx)
        assert len(tcs) == 1 and tcs[0].tool_call_id == "m1"
        assert len(trs) == 1 and trs[0].is_error is True and "Malformed tool arguments" in trs[0].parts[0].text

    async def test_schema_validation_arm_renders_list_dict_content(self) -> None:
        # the ValidationError arm's RetryPromptPart content is a list[dict]; the step must render it to text.
        bad = ToolCallPart(tool_name="typed_tool", args={"x": "not-a-number"}, tool_call_id="s1")
        agent = _agent_emitting([bad], tools=[_typed_tool_node()])
        await agent.run(_ctx := _make_ctx(State()))
        tcs, trs = _drafted(_ctx)
        assert len(tcs) == 1 and tcs[0].tool_call_id == "s1"
        assert len(trs) == 1 and trs[0].is_error is True and isinstance(trs[0].parts[0], TextPart) and trs[0].parts[0].text

    async def test_validator_raise_arm(self) -> None:
        bad = ToolCallPart(tool_name="bad_validator_tool", args={"x": 5}, tool_call_id="v1")
        agent = _agent_emitting([bad], tools=[_bad_validator_tool_node()])
        await agent.run(_ctx := _make_ctx(State()))
        tcs, trs = _drafted(_ctx)
        assert len(tcs) == 1 and tcs[0].tool_call_id == "v1"
        assert len(trs) == 1 and trs[0].is_error is True and "RuntimeError" in trs[0].parts[0].text

    async def test_off_spec_scalar_args_coerced_to_str(self) -> None:
        # a ToolCallPart whose args is a bare int (off-spec provider) — ToolCallStep.args stays str|dict|None.
        bad = ToolCallPart(tool_name="typed_tool", args=123, tool_call_id="i1")
        agent = _agent_emitting([bad], tools=[_typed_tool_node()])
        await agent.run(_ctx := _make_ctx(State()))
        tcs, _ = _drafted(_ctx)
        assert len(tcs) == 1 and tcs[0].args == "123"

    async def test_message_agent_rejection_arm_pairs_a_toolresult(self) -> None:
        # MAJOR-1 regression: a rejected message_agent consult (self-target) authored a ToolCallStep but
        # NO paired is_error ToolResultStep — a dangling call on the caller's stream. Both must be present,
        # and the model-facing RetryPromptPart must still land (unchanged).
        agent = _agent_emitting([_msg_call("triage")], name="triage", topics="triage.in", peers=[Messaging(discover=True)])
        ctx = _ctx_with_view(_view({"triage": None, "billing": None}))
        await agent.run(ctx)
        assert isinstance(ctx.state.tool_results.get("tc1"), RetryPromptPart)  # model-facing rejection unchanged
        tcs, trs = _drafted(ctx)
        assert len(tcs) == 1 and tcs[0].name == "message_agent" and tcs[0].tool_call_id == "tc1"
        assert len(trs) == 1 and trs[0].name == "message_agent" and trs[0].tool_call_id == "tc1" and trs[0].is_error is True


class TestChokepointEmission:
    """The disposition chokepoint in _handle_delivery (spec §2.5/§3.3): the depth-1 terminal gate, the
    no-step-on-fault rule, and the step's own header dict. Driven through _handle_delivery with a mock
    broker (mirroring test_consumer.py), so the real fault rail / publish branch runs."""

    async def test_depth1_returncall_terminal_emits_no_step_even_with_a_tag(self) -> None:
        # The run's final answer (a depth-1 ReturnCall) emits no step — and the gate keys on depth==1, NOT
        # on the frame tag. Demonstrate the gate is load-bearing: WITHOUT it, agent.project_steps mis-emits
        # the final answer as a ToolResultStep precisely because the root frame carries a tag.
        agent = Agent("term", system_prompt="x", subscribe_topics="node.in", model_client=_final_text_model())
        tagged_root = CallFrame(target_topic="node.in", callback_topic="caller.inbox", tag="root-tag")
        would_emit = agent.project_steps(ReturnCall(state=State(), value="final answer"), _make_ctx(State()), tagged_root)
        assert would_emit and isinstance(would_emit[0], ToolResultStep)  # without the gate, a spurious step
        # WITH the gate (depth-1 ReturnCall), the full _handle_delivery emits no step:
        state = State()
        _register_tool_call(state, tool_name="happy", tool_call_id="t1")
        state.add_tool_result("t1", ToolReturn(return_value="ok", metadata={"tool_call_id": "t1"}))
        broker = AsyncMock()
        await agent._handle_delivery(_delivery_envelope(state, tag="root-tag"), correlation_id="cid-term", headers=_HEADERS, broker=broker)
        assert broker.publish.await_count >= 1  # the hop ran to completion (published its terminal)
        assert _published_steps(broker) == []  # ...and the depth-1 gate suppressed any step despite the tag

    async def test_faulting_hop_emits_no_step(self) -> None:
        # A fault reaches the caller as the terminal RunFailed via the rail (the early-return precedes the
        # chokepoint), never as a step. A model raise faults the hop.
        def _boom_model(messages: list, info: AgentInfo) -> ModelResponse:
            raise RuntimeError("model boom")

        agent = Agent("f", system_prompt="x", subscribe_topics="node.in", model_client=FunctionModel(_boom_model))
        broker = AsyncMock()
        await agent._handle_delivery(_delivery_envelope(State()), correlation_id="cid-fault", headers=_HEADERS, broker=broker)
        assert broker.publish.await_count >= 1  # the fault WAS published (the hop produced a terminal)
        assert _published_steps(broker) == []  # ...but never a step

    async def test_step_publish_uses_its_own_header_dict_and_correlation_key(self) -> None:
        # A step-emitting hop (a depth-1 tool-dispatch) publishes its StepMessage to the root callback with
        # x-calf-wire=step + emitter headers, NO business x-calf-kind, keyed by correlation_id (co-partition).
        agent = _agent_emitting([ToolCallPart(tool_name="_echo_tool", args={}, tool_call_id="c1")], name="disp", topics="node.in", tools=[_echo_tool])
        broker = AsyncMock()
        await agent._handle_delivery(_delivery_envelope(State()), correlation_id="cid-hdr", headers=_HEADERS, broker=broker)
        steps = _published_steps(broker)
        assert len(steps) == 1
        call = steps[0]
        assert call.kwargs["topic"] == "caller.inbox"  # the root frame's callback_topic
        assert call.kwargs["key"] == b"cid-hdr"  # co-partitions with the terminal
        headers = call.kwargs["headers"]
        assert headers[HDR_WIRE] == StepMessage.WIRE and HDR_KIND not in headers  # a step has no business kind
        assert headers[HDR_EMITTER] == "disp" and headers[HDR_EMITTER_KIND] == agent._node_kind


def _stub_model(messages: list, info: AgentInfo) -> ModelResponse:  # never run in project_steps unit tests
    return ModelResponse(parts=[VTextPart(content="hi")])


def _agent_node() -> Agent:
    return Agent("peer_agent", system_prompt="x", subscribe_topics="peer.in", model_client=FunctionModel(_stub_model))


class TestStackAccessors:
    def test_len_counts_frames(self) -> None:
        s = CallFrameStack()
        assert len(s) == 0
        s.push(CallFrame(target_topic="a", callback_topic=None))
        s.push(CallFrame(target_topic="b", callback_topic="cb"))
        assert len(s) == 2

    def test_root_is_the_bottom_first_pushed_frame(self) -> None:
        # root = the originating caller frame (the client's root, caller.py pushes it first).
        s = CallFrameStack()
        s.push(CallFrame(target_topic="root", callback_topic="inbox"))
        s.push(CallFrame(target_topic="inner", callback_topic="cb"))
        assert s.root.target_topic == "root"
        assert s.root.callback_topic == "inbox"

    def test_root_on_empty_raises(self) -> None:
        with pytest.raises(Exception):
            _ = CallFrameStack().root


class TestStepDraftSlot:
    def test_defaults_none(self) -> None:
        ctx = SessionRunContext(state=State(), deps={})
        assert ctx._step_draft is None


class TestStepPreamble:
    """The new AgentMessageStep preamble extractor (spec §3.2): the FINAL ModelResponse's TextPart
    text only, excluding thinking/tool-call parts; empty ⇒ no AgentMessageStep."""

    def test_extracts_text_excludes_tool_calls(self) -> None:
        msgs = [ModelResponse(parts=[VTextPart(content="let me look that up"), ToolCallPart("search")])]
        assert step_preamble(msgs) == "let me look that up"

    def test_final_model_response_only_not_concatenated(self) -> None:
        # internal-retry shape: two ModelResponses; only the FINAL one's text is surfaced (concatenating
        # all would surface §2.2-out-of-scope internal-retry preamble).
        msgs = [
            ModelResponse(parts=[VTextPart(content="first internal-retry attempt")]),
            ModelResponse(parts=[VTextPart(content="final preamble"), ToolCallPart("search")]),
        ]
        assert step_preamble(msgs) == "final preamble"

    def test_empty_when_no_textpart(self) -> None:
        assert step_preamble([ModelResponse(parts=[ToolCallPart("search")])]) == ""

    def test_excludes_thinking(self) -> None:
        msgs = [ModelResponse(parts=[ThinkingPart(content="hmm"), VTextPart(content="visible")])]
        assert step_preamble(msgs) == "visible"

    def test_empty_when_no_model_response(self) -> None:
        assert step_preamble([ModelRequest(parts=[UserPromptPart(content="hi")])]) == ""


class TestProjectStepsBaseNode:
    def test_plain_custom_node_emits_nothing(self) -> None:
        node = BaseNodeDef(node_id="custom", subscribe_topics=["in"])
        ctx = SessionRunContext(state=State(), deps={})
        frame = CallFrame(target_topic="custom", callback_topic="cb", tag="t1")
        assert node.project_steps(ReturnCall(state=State(), value="x"), ctx, frame) == []


class TestProjectStepsToolNode:
    def test_returncall_success_becomes_toolresult(self) -> None:
        ctx = SessionRunContext(state=State(), deps={})
        frame = CallFrame(target_topic="echo", callback_topic="cb", tag="call-1")
        events = _echo_tool.project_steps(ReturnCall(state=State(), value="tool-output"), ctx, frame)
        assert len(events) == 1
        tr = events[0]
        assert isinstance(tr, ToolResultStep)
        assert tr.tool_call_id == "call-1"
        assert tr.name == _echo_tool.node_id
        assert tr.is_error is False
        assert tr.parts and isinstance(tr.parts[0], TextPart) and tr.parts[0].text == "tool-output"

    def test_returncall_modelretry_is_error_true(self) -> None:
        # a tool ModelRetry rides as a calf.retry-marked TextPart on ReturnCall.value -> is_error.
        ctx = SessionRunContext(state=State(), deps={})
        frame = CallFrame(target_topic="echo", callback_topic="cb", tag="call-2")
        events = _echo_tool.project_steps(ReturnCall(state=State(), value=[retry_text_part("narrow it")]), ctx, frame)
        assert events[0].is_error is True

    def test_is_error_coerce_first_no_attributeerror_on_scalar(self) -> None:
        # coerce FIRST: a scalar success value must not AttributeError in is_retry(raw_scalar).
        ctx = SessionRunContext(state=State(), deps={})
        frame = CallFrame(target_topic="echo", callback_topic="cb", tag="call-3")
        events = _echo_tool.project_steps(ReturnCall(state=State(), value="plain"), ctx, frame)  # must not raise
        assert events[0].is_error is False


class TestProjectStepsAgentNode:
    def test_non_returncall_output_returns_the_draft(self) -> None:
        agent = _agent_node()
        ctx = SessionRunContext(state=State(), deps={})
        ctx._step_draft = [AgentMessageStep(parts=[TextPart(text="thinking")])]
        out = TailCall(target_topic="t", state=State())
        assert agent.project_steps(out, ctx, CallFrame(target_topic="t", callback_topic="cb")) == ctx._step_draft

    def test_no_draft_emits_nothing(self) -> None:
        # the pre-model re-dispatch double-emit guard: _step_draft None -> [].
        agent = _agent_node()
        ctx = SessionRunContext(state=State(), deps={})
        assert ctx._step_draft is None
        out = TailCall(target_topic="t", state=State())
        assert agent.project_steps(out, ctx, CallFrame(target_topic="t", callback_topic="cb")) == []

    def test_inner_returncall_peer_consult_becomes_toolresult(self) -> None:
        # an agent answering as a consulted peer (inner-frame ReturnCall, depth>1) -> ToolResultStep keyed by
        # the frame tag; name = the peer's node_id (pairs by tool_call_id, not name).
        agent = _agent_node()
        ctx = SessionRunContext(state=State(), deps={})
        frame = CallFrame(target_topic="peer.in", callback_topic="cb", tag="consult-1")
        events = agent.project_steps(ReturnCall(state=State(), value="peer answer"), ctx, frame)
        assert len(events) == 1 and isinstance(events[0], ToolResultStep)
        assert events[0].tool_call_id == "consult-1"
        assert events[0].name == agent.node_id
        assert events[0].is_error is False
