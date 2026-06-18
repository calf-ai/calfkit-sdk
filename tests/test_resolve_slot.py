"""Step 4.3 — ``_resolve_slot`` + the slot-outcome vocabulary (fault-rail §6.9).

A resolved callee slot (a return, an ``on_callee_error`` substitute, or an unhandled fault) is
recorded on the base as a ``CalleeResult``; the AGENT additionally materializes it into the model
conversation (``state.tool_results``, the unchanged ``DeferredToolResults`` consumer) — its only
per-type codec. Additive + unwired: the staged pipeline does not call ``_resolve_slot`` until the
carriage switch (step 4.4).
"""

from __future__ import annotations

from typing import Any

from calfkit._vendor.pydantic_ai.messages import ModelResponse, RetryPromptPart, ToolCallPart, ToolReturn
from calfkit._vendor.pydantic_ai.messages import TextPart as ModelTextPart
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit.models.error_report import ErrorReport
from calfkit.models.payload import RETRY_MARKER, DataPart, TextPart, retry_text_part
from calfkit.models.seam_context import SeamContext
from calfkit.models.state import State
from calfkit.nodes import Agent
from calfkit.nodes.base import BaseNodeDef, _SlotFailed, _SlotResolved


def _seam_ctx(state: State | None = None) -> SeamContext[State]:
    return SeamContext(
        state=state if state is not None else State(),
        deps={},
        resources={},
        payload=None,
        node_id="n",
        correlation_id="cid",
        emitter_node_id=None,
        route=None,
        delivery_kind="return",
        awaiting_reply=False,
    )


def _node() -> BaseNodeDef:
    return BaseNodeDef(node_id="n", subscribe_topics=["in"])


def _agent() -> Agent[Any]:
    def _fn(messages: list[Any], info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[ModelTextPart("done")])

    return Agent(node_id="a", subscribe_topics="in", model_client=FunctionModel(_fn))


class TestBaseResolveSlot:
    def test_resolved_slot_appends_a_callee_result(self) -> None:
        ctx = _seam_ctx()
        _node()._resolve_slot(ctx, _SlotResolved(frame_id="f1", tag="t1", target_topic="tool.in", parts=[TextPart(text="ok")], handled=False))
        assert len(ctx.callee_results) == 1
        cr = ctx.callee_results[0]
        assert (cr.frame_id, cr.tag, cr.target_topic) == ("f1", "t1", "tool.in")
        assert cr.parts == [TextPart(text="ok")] and cr.fault is None and cr.handled is False

    def test_failed_slot_appends_a_callee_result_with_the_fault(self) -> None:
        ctx = _seam_ctx()
        _node()._resolve_slot(ctx, _SlotFailed(frame_id="f1", tag="t1", target_topic="tool.in", report=ErrorReport(error_type="callee.boom")))
        cr = ctx.callee_results[0]
        assert cr.fault is not None and cr.fault.error_type == "callee.boom"
        assert cr.parts is None and cr.handled is False

    def test_base_does_not_materialize_into_tool_results(self) -> None:
        # The base only RECORDS the slot; materialization is the agent's per-type override.
        ctx = _seam_ctx()
        _node()._resolve_slot(ctx, _SlotResolved(frame_id="f1", tag="t1", target_topic="tool.in", parts=[TextPart(text="ok")], handled=False))
        assert ctx.state.get_tool_result("t1") is None

    def test_single_call_slot_has_no_target_topic(self) -> None:
        # A single (non-fan-out) call resolves with target_topic=None — there is no SlotRef to source
        # it from (the fault-group topology only matters for a fan-out batch). CalleeResult.target_topic
        # is str|None to carry this (decision 5).
        ctx = _seam_ctx()
        _node()._resolve_slot(ctx, _SlotResolved(frame_id="f1", tag="t1", target_topic=None, parts=[TextPart(text="ok")], handled=False))
        assert ctx.callee_results[0].target_topic is None


class TestAgentResolveSlot:
    def test_materializes_a_plain_return_as_a_tool_return(self) -> None:
        ctx = _seam_ctx()
        _agent()._resolve_slot(ctx, _SlotResolved(frame_id="f1", tag="t1", target_topic="tool.in", parts=[DataPart(data={"r": 1})], handled=False))
        result = ctx.state.get_tool_result("t1")
        assert isinstance(result, ToolReturn)
        assert result.return_value == {"r": 1}
        assert result.metadata == {"tool_call_id": "t1"}
        assert len(ctx.callee_results) == 1  # also recorded via super()

    def test_materializes_a_calf_retry_as_a_retry_prompt_part(self) -> None:
        ctx = _seam_ctx()
        ctx.state.add_tool_call(ToolCallPart(tool_name="my_tool", args={}, tool_call_id="t1"))  # for the tool_name hydration
        _agent()._resolve_slot(
            ctx, _SlotResolved(frame_id="f1", tag="t1", target_topic="tool.in", parts=[retry_text_part("retry me")], handled=False)
        )
        result = ctx.state.get_tool_result("t1")
        assert isinstance(result, RetryPromptPart)
        assert result.content == "retry me"  # raw message (option 1; the provider renders the suffix once)
        assert result.tool_name == "my_tool" and result.tool_call_id == "t1"

    def test_failed_slot_does_not_materialize(self) -> None:
        ctx = _seam_ctx()
        _agent()._resolve_slot(ctx, _SlotFailed(frame_id="f1", tag="t1", target_topic="tool.in", report=ErrorReport(error_type="callee.boom")))
        assert ctx.state.get_tool_result("t1") is None  # the fault escalates; it never reaches the model
        assert len(ctx.callee_results) == 1 and ctx.callee_results[0].fault.error_type == "callee.boom"

    def test_retry_content_is_str_when_a_data_part_precedes_the_marked_text(self) -> None:
        # The retry branch must extract STR content only (spec §6.9 ``_text(p)``), NOT ``extract_lenient``
        # (which returns ``DataPart.data`` FIRST — an arbitrary non-str). A wire that carries both a
        # DataPart and the calf.retry-marked TextPart must still materialize the TEXT into
        # ``RetryPromptPart.content`` (typed ``list[ErrorDetails] | str``), never the dict.
        ctx = _seam_ctx()
        parts = [DataPart(data={"unrelated": "blob"}), retry_text_part("retry me")]
        _agent()._resolve_slot(ctx, _SlotResolved(frame_id="f1", tag="t1", target_topic="tool.in", parts=parts, handled=False))
        result = ctx.state.get_tool_result("t1")
        assert isinstance(result, RetryPromptPart)
        assert isinstance(result.content, str)  # never the DataPart's dict
        assert result.content == "retry me"

    def test_retry_content_is_str_even_without_a_text_part(self) -> None:
        # A future producer could mark a non-TextPart (``is_retry`` reads only the open ``metadata`` slot,
        # so it is total over the part vocabulary). The retry branch must still yield a ``str`` for
        # ``RetryPromptPart.content`` — a scalar/dict would misbehave in ``model_response()``.
        ctx = _seam_ctx()
        marked_data = DataPart(data={"k": "v"}, metadata={RETRY_MARKER: True})
        _agent()._resolve_slot(ctx, _SlotResolved(frame_id="f1", tag="t1", target_topic="tool.in", parts=[marked_data], handled=False))
        result = ctx.state.get_tool_result("t1")
        assert isinstance(result, RetryPromptPart)
        assert isinstance(result.content, str)  # str-only contract holds in the no-TextPart edge

    def test_retry_content_picks_the_marked_text_not_an_unmarked_preamble(self) -> None:
        # ``is_retry`` keys on the calf.retry MARKER (any part), so ``_retry_content`` must extract the
        # MARKED part's text — not merely the first TextPart. When an unmarked TextPart precedes the
        # marked one (``[TextPart("preamble"), retry_text_part("the real error")]``), the model must
        # receive the real retry message in ``RetryPromptPart.content``, not the unmarked preamble.
        ctx = _seam_ctx()
        parts = [TextPart(text="preamble"), retry_text_part("the real error")]
        _agent()._resolve_slot(ctx, _SlotResolved(frame_id="f1", tag="t1", target_topic="tool.in", parts=parts, handled=False))
        result = ctx.state.get_tool_result("t1")
        assert isinstance(result, RetryPromptPart)
        assert result.content == "the real error"  # the MARKED text, never the unmarked "preamble"
