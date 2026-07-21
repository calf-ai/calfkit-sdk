"""PR-A — the per-delivery reply slot (return half of the §4 wire model).

Layer-1: the new wire models round-trip and the new fields default correctly.
See notes/reply-slot-for-returns-pr-implementation-plan.md.
"""

from __future__ import annotations

import typing

import pydantic_core
import pytest
from pydantic import BaseModel

import calfkit._protocol as protocol
from calfkit._protocol import HDR_KIND, MessageKind
from calfkit.exceptions import DeserializationError
from calfkit.models import (
    CallFrame,
    CallFrameStack,
    ConsumerContext,
    DataPart,
    Envelope,
    ReturnCall,
    SessionRunContext,
    TextPart,
    WorkflowState,
)
from calfkit.models._coerce import _coerce_to_parts
from calfkit.models.error_report import ErrorReport
from calfkit.models.node_result import InvocationResult
from calfkit.models.payload import FilePart, ToolCallPart, is_retry, retry_text_part
from calfkit.models.reply import FaultMessage, ReturnMessage, _ReplyBase
from calfkit.models.state import State


def _envelope(*, reply: ReturnMessage | FaultMessage | None = None) -> Envelope:
    stack = CallFrameStack()
    stack.push(CallFrame(target_topic="t", callback_topic="reply.topic"))
    return Envelope(
        internal_workflow_state=WorkflowState(call_stack=stack),
        context=SessionRunContext(state=State(), deps={}),
        reply=reply,
    )


class TestReturnMessage:
    def test_extends_reply_base_and_is_kind_return(self) -> None:
        rm = ReturnMessage(in_reply_to="f1", tag="t1", parts=[TextPart(text="hi")])
        assert isinstance(rm, _ReplyBase)
        assert rm.kind == "return"

    def test_in_reply_to_and_tag_accept_none(self) -> None:
        rm = ReturnMessage(in_reply_to=None, tag=None, parts=[])
        assert rm.in_reply_to is None
        assert rm.tag is None

    def test_round_trips_through_json(self) -> None:
        rm = ReturnMessage(in_reply_to="f1", tag="call-7", parts=[TextPart(text="hi")])
        back = ReturnMessage.model_validate_json(rm.model_dump_json())
        assert back == rm
        assert back.parts[0].text == "hi"


class TestEnvelopeReply:
    def test_reply_defaults_none(self) -> None:
        assert _envelope().reply is None

    def test_carries_reply_and_round_trips(self) -> None:
        env = _envelope(reply=ReturnMessage(in_reply_to="f1", tag="t1", parts=[TextPart(text="out")]))
        back = Envelope.model_validate_json(env.model_dump_json())
        assert back.reply is not None
        assert back.reply.kind == "return"
        assert back.reply.in_reply_to == "f1"
        assert back.reply.parts[0].text == "out"

    def test_none_reply_round_trips(self) -> None:
        back = Envelope.model_validate_json(_envelope().model_dump_json())
        assert back.reply is None

    def test_carries_fault_reply_and_round_trips(self) -> None:
        # The slot widens to ReturnMessage | FaultMessage (discriminated on kind) so a
        # fault rides the same per-delivery reply carriage as a success (spec §4.2).
        env = _envelope(reply=FaultMessage(in_reply_to="f1", tag="t1", error=ErrorReport(error_type="calf.exception", message="boom")))
        back = Envelope.model_validate_json(env.model_dump_json())
        assert isinstance(back.reply, FaultMessage)
        assert back.reply.kind == "fault"
        assert back.reply.error.error_type == "calf.exception"
        assert back.reply.error.message == "boom"


class TestCallFrameTag:
    def test_tag_defaults_none(self) -> None:
        assert CallFrame(target_topic="t", callback_topic=None).tag is None

    def test_tag_is_set_and_survives_envelope_round_trip(self) -> None:
        stack = CallFrameStack()
        stack.push(CallFrame(target_topic="t", callback_topic="cb", tag="call-9"))
        env = Envelope(
            internal_workflow_state=WorkflowState(call_stack=stack),
            context=SessionRunContext(state=State(), deps={}),
        )
        back = Envelope.model_validate_json(env.model_dump_json())
        assert back.internal_workflow_state.current_frame.tag == "call-9"


class TestReturnCallValue:
    def test_value_defaults_none(self) -> None:
        assert ReturnCall(state=State()).value is None

    def test_value_is_carried_and_in_eq(self) -> None:
        parts = [TextPart(text="hi")]
        rc = ReturnCall(state=State(), value=parts)
        assert rc.value == parts
        assert rc == ReturnCall(state=rc.state, value=parts)


class _Struct(BaseModel):
    n: int


class TestCoerceToParts:
    def test_str_becomes_one_textpart(self) -> None:
        assert _coerce_to_parts("hi") == [TextPart(text="hi")]

    def test_none_becomes_empty_list(self) -> None:
        assert _coerce_to_parts(None) == []

    def test_list_of_content_parts_passes_through(self) -> None:
        parts = [TextPart(text="a"), DataPart(data={"k": "v"})]
        assert _coerce_to_parts(parts) == parts

    def test_dict_becomes_one_datapart(self) -> None:
        assert _coerce_to_parts({"k": "v"}) == [DataPart(data={"k": "v"})]

    def test_basemodel_becomes_datapart_and_is_wire_safe(self) -> None:
        out = _coerce_to_parts(_Struct(n=7))
        assert out == [DataPart(data=_Struct(n=7))]
        # the coerced part must serialize (eager wire-safety, tool.py precedent)
        pydantic_core.to_json(out[0])

    def test_non_serializable_value_raises(self) -> None:
        with pytest.raises(pydantic_core.PydanticSerializationError):
            _coerce_to_parts(object())

    def test_bare_content_part_wraps_to_singleton_list(self) -> None:
        # A bare part is a fixed point of the coercion into its own vocabulary — never
        # buried as opaque data inside a DataPart (docs/issues/coerce-to-parts-drops-
        # bare-content-part-metadata.md).
        for part in (
            TextPart(text="hi"),
            FilePart(media_type="image/png"),
            DataPart(data={"k": "v"}),
            ToolCallPart(tool_call_id="c1", kwargs={}, tool_name="t"),
        ):
            assert _coerce_to_parts(part) == [part]

    def test_bare_retry_part_keeps_the_marker(self) -> None:
        # The calf.retry marker must survive coercion of a BARE marked part, or a tool
        # error renders to the model as a success (is_error fidelity lost downstream).
        part = retry_text_part("boom: rate limited")
        result = _coerce_to_parts(part)
        assert result == [part]
        assert is_retry(result)

    def test_bare_part_coerces_same_as_wrapped_part(self) -> None:
        # The faithful-embedding property: coerce(part) == coerce([part]).
        for part in (TextPart(text="hi"), DataPart(data={"k": "v"})):
            assert _coerce_to_parts(part) == _coerce_to_parts([part])

    def test_mixed_list_of_part_and_str_still_buries_the_marker(self) -> None:
        # Gate 0/C (agent-tool-error-reception): the precursor (#333) fixed the BARE-part
        # burial but deliberately did NOT fix a MIXED list — the all-parts check requires
        # EVERY element to be a part, so ``[marked_part, "str"]`` falls to the DataPart arm and
        # the ``calf.retry`` marker is buried out of ``is_retry``'s per-part scanning range. Pinned
        # so a handler returning a mixed list is a KNOWN, documented trap (issue §6.3-C4), not a
        # silent regression: an ``on_tool_error`` handler must return a bare part or a list of ONLY
        # parts. The precursor does not fix mixed lists — the reception feature documents it, not
        # relies on it.
        marked = retry_text_part("boom: rate limited")
        result = _coerce_to_parts([marked, "postscript"])
        assert result == [DataPart(data=[marked, "postscript"])]  # one DataPart; both elements buried
        assert not is_retry(result)  # marker out of scanning range → would render is_error=False


class TestProtocolKind:
    def test_hdr_kind_header_name(self) -> None:
        assert HDR_KIND == "x-calf-kind"

    def test_message_kind_value_space(self) -> None:
        # Widened with "fault" by the fault-wire-model PR (only the rail stamps it).
        assert typing.get_args(MessageKind) == ("call", "return", "fault")

    def test_dead_event_type_constants_removed(self) -> None:
        # Drift cleanup: HDR_EVENT_TYPE / EventType had zero call sites.
        assert not hasattr(protocol, "HDR_EVENT_TYPE")
        assert not hasattr(protocol, "EventType")


def _reply_env(parts: list) -> Envelope:
    """A frameless envelope carrying a reply, with _reply stamped as a handler/
    dispatcher would (prepare_context / _on_reply)."""
    env = Envelope(
        context=SessionRunContext(state=State(), deps={}),
        internal_workflow_state=WorkflowState(call_stack=CallFrameStack()),
        reply=ReturnMessage(in_reply_to=None, tag=None, parts=parts),
    )
    env.context._stamp_transport(correlation_id="cid", task_id="t-under-test", emitter_node_id=None, emitter_node_kind=None)
    env.context._reply = env.reply
    return env


class TestProjectionFromReply:
    """Output projects from the reply slot, not State.final_output_parts (spec §4.5)."""

    def test_node_result_projects_output_from_reply_parts(self) -> None:
        result = InvocationResult.from_envelope(_reply_env([TextPart(text="from-reply")]), correlation_id="cid")
        assert result.output == "from-reply"
        assert result.output_parts == [TextPart(text="from-reply")]

    def test_consumer_context_projects_from_ctx_reply(self) -> None:
        cc = ConsumerContext.from_run_context(_reply_env([DataPart(data={"k": 1})]).context)
        assert cc.output == {"k": 1}
        assert cc.output_parts == [DataPart(data={"k": 1})]

    def test_present_but_invalid_data_part_raises_deserialization_error(self) -> None:
        # A structured output_type whose DataPart is present but fails validation surfaces as the
        # closed-set DeserializationError (spec §2.5), never a raw pydantic.ValidationError (M2).
        with pytest.raises(DeserializationError):
            InvocationResult.from_envelope(_reply_env([DataPart(data={"wrong": "x"})]), _Struct, correlation_id="cid")

    def test_strict_empty_reply_raises(self) -> None:
        with pytest.raises(DeserializationError):
            InvocationResult.from_envelope(_reply_env([]), correlation_id="cid")

    def test_lenient_empty_reply_returns_none(self) -> None:
        cc = ConsumerContext.from_run_context(_reply_env([]).context)  # strict=False
        assert cc.output is None
        assert cc.output_parts == []

    def test_from_envelope_does_not_expose_a_strict_escape(self) -> None:
        # InvocationResult is client-only and always strict (spec §2.4): the lenient knob
        # lives on project_output / ConsumerContext, so output stays non-optional here.
        with pytest.raises(TypeError):
            InvocationResult.from_envelope(_reply_env([TextPart(text="x")]), correlation_id="cid", strict=False)  # type: ignore[call-arg]

    def test_from_context_does_not_expose_a_strict_escape(self) -> None:
        with pytest.raises(TypeError):
            InvocationResult.from_context(_reply_env([TextPart(text="x")]).context, strict=False)  # type: ignore[call-arg]


class TestStrCoercion:
    """``output_type=str`` coerces ALL reply parts to one newline-joined string (spec §2.2):
    ``TextPart`` verbatim, ``DataPart`` as JSON, File/ToolCall skipped — never a
    ``DeserializationError`` (that is reserved for a *structured* ``output_type`` mismatch)."""

    def test_str_stringifies_a_data_part_as_json(self) -> None:
        result = InvocationResult.from_envelope(_reply_env([DataPart(data={"k": 1})]), str, correlation_id="cid")
        assert result.output == pydantic_core.to_json({"k": 1}).decode()

    def test_str_joins_text_and_data_with_a_newline(self) -> None:
        result = InvocationResult.from_envelope(_reply_env([TextPart(text="lead"), DataPart(data={"k": 1})]), str, correlation_id="cid")
        assert result.output == "lead\n" + pydantic_core.to_json({"k": 1}).decode()

    def test_str_single_text_part_is_unchanged(self) -> None:
        result = InvocationResult.from_envelope(_reply_env([TextPart(text="hi")]), str, correlation_id="cid")
        assert result.output == "hi"

    def test_str_skips_file_and_tool_parts(self) -> None:
        result = InvocationResult.from_envelope(_reply_env([TextPart(text="hi"), FilePart(media_type="image/png")]), str, correlation_id="cid")
        assert result.output == "hi"

    def test_str_empty_reply_is_the_empty_string(self) -> None:
        # Distinct from the _UNSET/auto path, which raises on empty (test_strict_empty_reply_raises):
        # an explicit str ask coerces, so an empty reply is the empty string, never a raise.
        result = InvocationResult.from_envelope(_reply_env([]), str, correlation_id="cid")
        assert result.output == ""

    def test_consumer_str_also_stringifies_a_data_part(self) -> None:
        # The coercion lives on the SHARED projection, so a @consumer with output_type=str gets the
        # same string (Ryan-ratified 2026-06-27: shared, not client-only).
        cc = ConsumerContext.from_run_context(_reply_env([DataPart(data={"k": 1})]).context, str)
        assert cc.output == pydantic_core.to_json({"k": 1}).decode()


def _fault_env(report: ErrorReport) -> Envelope:
    """A frameless envelope whose reply slot carries a FaultMessage, stamped as a
    handler/dispatcher would. The success-only readers must tolerate it."""
    env = Envelope(
        context=SessionRunContext(state=State(), deps={}),
        internal_workflow_state=WorkflowState(call_stack=CallFrameStack()),
        reply=FaultMessage(in_reply_to=None, tag=None, error=report),
    )
    env.context._stamp_transport(correlation_id="cid", task_id="t-under-test", emitter_node_id=None, emitter_node_kind=None)
    env.context._reply = env.reply
    return env


class TestFaultReaderTolerance:
    """Success-only readers never crash on a FaultMessage reply (spec §2 / §4.2): they
    treat it as no-parts (→ []/None), never AttributeError on the missing .parts. The
    fault is floored at the producing hop; the typed reception (NodeFaultError) is the
    deferred reception PR's job."""

    def test_output_parts_is_empty_on_a_fault(self) -> None:
        env = _fault_env(ErrorReport(error_type="calf.exception"))
        assert env.context.output_parts == []  # no AttributeError on FaultMessage.parts

    def test_project_output_is_none_on_a_fault_when_lenient(self) -> None:
        from calfkit.models.node_result import project_output

        assert project_output(_fault_env(ErrorReport(error_type="x")).reply, strict=False) is None

    def test_consumer_context_tolerates_a_fault(self) -> None:
        cc = ConsumerContext.from_run_context(_fault_env(ErrorReport(error_type="x")).context)
        assert cc.output is None
        assert cc.output_parts == []
