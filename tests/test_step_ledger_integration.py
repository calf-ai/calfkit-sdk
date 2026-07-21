"""Body-fact declaration through the kernel + the step publish's wire shape (caller-side
step-emission spec §3.1/§3.4) — successor to the v1 chokepoint-projection suite (that machinery is
deleted; the ledger's unit contracts live in ``test_step_ledger.py``, the kernel pair law in
``test_step_pair_law.py``, outcome mapping E2E in ``test_step_outcome_e2e.py``).

What lives here: every REACHABLE pre-dispatch rejection arm declares its born-closed ``DeniedCall``
fact on the ``Observed`` return (the pairing pin, round-1 review MAJOR-1); the fact's build-time
coercions at the arm level; the ``StepMessage`` publish's own header dict / key / topic through the
real ``_handle_delivery``; the ``Stack`` accessors the exit-flush helper's guarded root read leans
on; and the ``step_preamble`` extractor (carried forward unchanged, spec §5.4).
"""

from __future__ import annotations

from typing import Annotated
from unittest.mock import AsyncMock

import pytest
from pydantic import BeforeValidator

from calfkit._protocol import HDR_EMITTER, HDR_EMITTER_KIND, HDR_KIND, HDR_WIRE
from calfkit._vendor.pydantic_ai.messages import ModelRequest, ModelResponse, RetryPromptPart, ThinkingPart, ToolCallPart, UserPromptPart
from calfkit._vendor.pydantic_ai.messages import TextPart as VTextPart
from calfkit.models import CallFrame, CallFrameStack, Envelope, SessionRunContext, State, WorkflowState
from calfkit.models.step import StepMessage
from calfkit.models.tool_context import ToolContext
from calfkit.nodes import Agent, ToolNodeDef, agent_tool
from calfkit.nodes._projection import step_preamble
from calfkit.nodes._steps import DeniedCall, Observed
from calfkit.peers import Messaging
from tests.test_message_agent import _ctx_with_view, _msg_call, _view
from tests.test_tool_errors import _make_ctx, _model_emits_tool_calls

_HEADERS = {HDR_EMITTER: b"upstream", HDR_EMITTER_KIND: b"agent"}


def _delivery_envelope(state: State, *, callback_topic: str = "caller.inbox", tag: str | None = None) -> Envelope:
    """A depth-1 (single root frame) inbound envelope for driving ``_handle_delivery``."""
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


def _denied(observed: object) -> list[DeniedCall]:
    """The DeniedCall facts a fact-capable dispatch exit declared (the born-closed pairs)."""
    assert isinstance(observed, Observed), f"expected Observed, got {type(observed).__name__}"
    return [f for f in observed.facts if isinstance(f, DeniedCall)]


def _agent_emitting(calls: list[ToolCallPart], *, name: str = "a", topics: str = "a.in", **kw: object) -> Agent:
    """An agent whose model deterministically emits ``calls`` — for driving the dispatch loop's arms."""
    return Agent(name, system_prompt="x", subscribe_topics=topics, model_client=_model_emits_tool_calls(calls), **kw)


class TestRejectionArmsDeclareDeniedFacts:
    """Every calfkit-caught invalid call declares a born-closed ``DeniedCall`` fact (spec §3.1a) —
    one test per REACHABLE rejection arm, including the message_agent peer-consult arm, whose
    missing pairing was the round-1 review's MAJOR-1. (The ``binding is None`` arm is not exercised:
    a name absent from the toolset is auto-retried INSIDE pydantic-ai and never reaches calfkit
    dispatch.) The ledger expands each fact into its paired call + ``denied`` result — pinned at the
    unit level in ``test_step_ledger.py`` and E2E in ``test_step_outcome_e2e.py``."""

    async def test_malformed_args_arm(self) -> None:
        bad = ToolCallPart(tool_name="typed_tool", args="not-valid-json", tool_call_id="m1")
        agent = _agent_emitting([bad], tools=[_typed_tool_node()])
        observed = await agent.run(_make_ctx(State()))
        (fact,) = _denied(observed)
        assert fact.tool_call_id == "m1"
        assert "Malformed tool arguments" in fact.reason_parts[0].text

    async def test_schema_validation_arm_renders_list_dict_content(self) -> None:
        # the ValidationError arm's RetryPromptPart content is a list[dict]; the fact must render it to text.
        bad = ToolCallPart(tool_name="typed_tool", args={"x": "not-a-number"}, tool_call_id="s1")
        agent = _agent_emitting([bad], tools=[_typed_tool_node()])
        observed = await agent.run(_make_ctx(State()))
        (fact,) = _denied(observed)
        assert fact.tool_call_id == "s1"
        assert isinstance(fact.reason_parts[0].text, str) and fact.reason_parts[0].text

    async def test_validator_raise_arm(self) -> None:
        bad = ToolCallPart(tool_name="bad_validator_tool", args={"x": 5}, tool_call_id="v1")
        agent = _agent_emitting([bad], tools=[_bad_validator_tool_node()])
        observed = await agent.run(_make_ctx(State()))
        (fact,) = _denied(observed)
        assert fact.tool_call_id == "v1"
        assert "RuntimeError" in fact.reason_parts[0].text

    async def test_off_spec_scalar_args_clamped_at_the_arm(self) -> None:
        # a ToolCallPart whose args is a bare int (off-spec provider) — the fact clamps to str at build.
        bad = ToolCallPart(tool_name="typed_tool", args=123, tool_call_id="i1")
        agent = _agent_emitting([bad], tools=[_typed_tool_node()])
        observed = await agent.run(_make_ctx(State()))
        (fact,) = _denied(observed)
        assert fact.args == "123"

    async def test_message_agent_rejection_arm_declares_the_fact(self) -> None:
        # MAJOR-1 regression: a rejected message_agent consult (self-target) landed a model retry with
        # NO paired observation — a dangling call on the caller's stream. The fact carries both halves,
        # and the model-facing RetryPromptPart still lands (unchanged).
        agent = _agent_emitting([_msg_call("triage")], name="triage", topics="triage.in", peers=[Messaging(discover=True)])
        ctx = _ctx_with_view(_view({"triage": None, "billing": None}))
        observed = await agent.run(ctx)
        assert isinstance(ctx.state.tool_results.get("tc1"), RetryPromptPart)  # model-facing rejection unchanged
        (fact,) = _denied(observed)
        assert fact.tool_name == "message_agent" and fact.tool_call_id == "tc1"

    def test_reject_invalid_call_render_is_total_on_non_serializable_content(self) -> None:
        # _reject_invalid_call's list-content render (pydantic_core.to_json) runs OUTSIDE the exit-flush
        # helper's guard, so it must be LOCALLY total: a future caller passing context-bearing ErrorDetails
        # (e.g. a validator's raised exception in `ctx`, present when errors() keeps context) must NOT
        # raise — which would fault the run.
        agent = _agent_emitting([])
        ctx = _make_ctx(State())
        tool_call = ToolCallPart(tool_name="t", args={}, tool_call_id="x1")
        facts: list = []
        content = [{"type": "value_error", "loc": ("x",), "msg": "bad", "input": "5", "ctx": {"error": ValueError("boom")}}]
        agent._reject_invalid_call(tool_call, ctx, facts, content)  # must not raise on the non-serializable ctx
        (fact,) = facts
        assert isinstance(fact, DeniedCall) and fact.reason_parts[0].text


class TestStepPublishWireShape:
    async def test_step_publish_uses_its_own_header_dict_and_correlation_key(self) -> None:
        # A step-emitting hop (a depth-1 tool-dispatch) publishes its StepMessage to the root callback
        # with x-calf-wire=step + emitter headers, NO business x-calf-kind, keyed by correlation_id
        # (co-partitions with the terminal) — through the REAL _handle_delivery.
        agent = _agent_emitting([ToolCallPart(tool_name="_echo_tool", args={}, tool_call_id="c1")], name="disp", topics="node.in", tools=[_echo_tool])
        broker = AsyncMock()
        await agent._handle_delivery(
            _delivery_envelope(State()), correlation_id="cid-hdr", task_id="task-under-test", headers=_HEADERS, broker=broker
        )
        steps = _published_steps(broker)
        assert len(steps) == 1
        call = steps[0]
        assert call.kwargs["topic"] == "caller.inbox"  # the root frame's callback_topic
        assert call.kwargs["key"] == b"task-under-test"  # keyed by the threaded task_id — co-partitions with the terminal
        headers = call.kwargs["headers"]
        assert headers[HDR_WIRE] == StepMessage.WIRE and HDR_KIND not in headers  # a step has no business kind
        assert headers[HDR_EMITTER] == "disp" and headers[HDR_EMITTER_KIND] == agent._node_kind


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
        # The exit-flush helper's guarded root read leans on this raise (an empty stack must be
        # guarded, never silently defaulted).
        with pytest.raises(Exception):
            _ = CallFrameStack().root


class TestStepPreamble:
    """The ``Said`` preamble extractor (spec §5.4, carried forward): the FINAL ModelResponse's
    TextPart text only, excluding thinking/tool-call parts; empty ⇒ no fact declared."""

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
