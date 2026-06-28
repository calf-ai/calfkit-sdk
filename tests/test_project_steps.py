"""Increment D — emission: Stack accessors, the _step_draft slot, project_steps, and the
disposition-chokepoint behavior (intermediate-step-streaming spec §2.5 / §2.7 / §3.2 / §3.3).

Offline, no broker for the unit pieces; the chokepoint pieces drive node.handler with a capture
broker (mirroring test_fault_pipeline.py).
"""

from __future__ import annotations

import pytest

from calfkit._vendor.pydantic_ai.messages import ModelRequest, ModelResponse, ThinkingPart, ToolCallPart, UserPromptPart
from calfkit._vendor.pydantic_ai.messages import TextPart as VTextPart
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit.models import CallFrame, CallFrameStack, ReturnCall, SessionRunContext, State, TailCall
from calfkit.models.payload import TextPart, retry_text_part
from calfkit.models.step import AgentMessage, ToolResult
from calfkit.nodes import Agent, agent_tool
from calfkit.nodes._projection import step_preamble
from calfkit.nodes.base import BaseNodeDef


@agent_tool
def _echo_tool(ctx) -> str:  # noqa: ANN001
    return "tool-output"


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
    """The new AgentMessage preamble extractor (spec §3.2): the FINAL ModelResponse's TextPart
    text only, excluding thinking/tool-call parts; empty ⇒ no AgentMessage."""

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
        assert isinstance(tr, ToolResult)
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
        ctx._step_draft = [AgentMessage(parts=[TextPart(text="thinking")])]
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
        # an agent answering as a consulted peer (inner-frame ReturnCall, depth>1) -> ToolResult keyed by
        # the frame tag; name = the peer's node_id (pairs by tool_call_id, not name).
        agent = _agent_node()
        ctx = SessionRunContext(state=State(), deps={})
        frame = CallFrame(target_topic="peer.in", callback_topic="cb", tag="consult-1")
        events = agent.project_steps(ReturnCall(state=State(), value="peer answer"), ctx, frame)
        assert len(events) == 1 and isinstance(events[0], ToolResult)
        assert events[0].tool_call_id == "consult-1"
        assert events[0].name == agent.node_id
        assert events[0].is_error is False
