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

from calfkit.models import (
    CallFrame,
    CallFrameStack,
    DataPart,
    Envelope,
    ReturnCall,
    SessionRunContext,
    TextPart,
    WorkflowState,
)
from calfkit.models._coerce import _coerce_to_parts
from calfkit.models.reply import ReturnMessage, _ReplyBase
from calfkit.models.state import State


def _envelope(*, reply: ReturnMessage | None = None) -> Envelope:
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


class TestProtocolKind:
    def test_hdr_kind_header_name(self) -> None:
        assert HDR_KIND == "x-calf-kind"

    def test_message_kind_value_space(self) -> None:
        assert typing.get_args(MessageKind) == ("call", "return")

    def test_dead_event_type_constants_removed(self) -> None:
        # Drift cleanup: HDR_EVENT_TYPE / EventType had zero call sites.
        assert not hasattr(protocol, "HDR_EVENT_TYPE")
        assert not hasattr(protocol, "EventType")
