"""4.4 — ``_resolve_callee``: the uniform stage-1 slot resolution (fault-rail §6.8/§6.9).

ONE path resolves a callee slot for both a return and a fault, single-call and fan-out:

- a **return** → ``_SlotResolved(parts, handled=False)`` (no ``on_callee_error`` — only faults run it);
- a **fault** → ``on_callee_error``: a substitute value → ``_SlotResolved(parts, handled=True)``;
  ``None`` (declined) → ``_SlotFailed(report)`` (escalate); a slot-scoped raise → ``_SlotFailed``
  carrying the chained error, NEVER a node-own failure (§6.5): a ``NodeFaultError`` honored verbatim,
  anything else wrapped ``calf.exception``, both with the inbound fault chained via ``causes``.

A wire-unsafe substitute coerces to ``_SlotFailed(calf.slot.materialization_failed)`` at stage-1.
"""

from __future__ import annotations

from typing import Any

from calfkit.exceptions import NodeFaultError
from calfkit.models.error_report import ErrorReport, FaultTypes
from calfkit.models.payload import TextPart
from calfkit.models.reply import FaultMessage, ReturnMessage
from calfkit.models.seam_context import SeamContext
from calfkit.models.state import State
from calfkit.nodes.base import BaseNodeDef, _SlotFailed, _SlotResolved


def _seam_ctx() -> SeamContext[State]:
    return SeamContext(
        state=State(),
        deps={},
        resources={},
        payload=None,
        node_id="n",
        correlation_id="cid",
        emitter_node_id=None,
        route=None,
        delivery_kind="fault",
        awaiting_reply=True,
    )


def _node(**seams: Any) -> BaseNodeDef:
    return BaseNodeDef(node_id="n", subscribe_topics=["in"], **seams)


def _return(parts: list[Any], *, in_reply_to: str = "f1", tag: str | None = "t1") -> ReturnMessage:
    return ReturnMessage(in_reply_to=in_reply_to, tag=tag, parts=parts)


def _fault(error_type: str = "callee.boom", *, in_reply_to: str = "f1", tag: str | None = "t1") -> FaultMessage:
    return FaultMessage(in_reply_to=in_reply_to, tag=tag, error=ErrorReport(error_type=error_type))


class TestReturnResolution:
    async def test_return_resolves_unhandled_carrying_parts_and_identity(self) -> None:
        out = await _node()._resolve_callee(_seam_ctx(), "return", _return([TextPart(text="ok")]), target_topic="tool.a")
        assert isinstance(out, _SlotResolved)
        assert out.handled is False and out.parts == [TextPart(text="ok")]
        assert (out.frame_id, out.tag, out.target_topic) == ("f1", "t1", "tool.a")

    async def test_single_call_return_has_no_target_topic(self) -> None:
        out = await _node()._resolve_callee(_seam_ctx(), "return", _return([TextPart(text="ok")]), target_topic=None)
        assert isinstance(out, _SlotResolved) and out.target_topic is None


class TestFaultResolution:
    async def test_unhandled_fault_escalates_as_slot_failed(self) -> None:
        # No on_callee_error → the default (§8): escalate the callee's fault, unwrapped.
        out = await _node()._resolve_callee(_seam_ctx(), "fault", _fault(), target_topic="tool.a")
        assert isinstance(out, _SlotFailed)
        assert out.report.error_type == "callee.boom"
        assert (out.frame_id, out.target_topic) == ("f1", "tool.a")

    async def test_handled_fault_resolves_as_slot_resolved_handled(self) -> None:
        node = _node(on_callee_error=lambda ctx, fault: "recovered")
        out = await node._resolve_callee(_seam_ctx(), "fault", _fault(), target_topic="tool.a")
        assert isinstance(out, _SlotResolved)
        assert out.handled is True and out.parts == [TextPart(text="recovered")]

    async def test_declined_fault_escalates(self) -> None:
        node = _node(on_callee_error=lambda ctx, fault: None)
        out = await node._resolve_callee(_seam_ctx(), "fault", _fault(), target_topic=None)
        assert isinstance(out, _SlotFailed) and out.report.error_type == "callee.boom"

    async def test_nodefaulterror_in_seam_is_slot_scoped_minted_verbatim(self) -> None:
        def _convert(ctx: Any, fault: Any) -> Any:
            raise NodeFaultError("billing.quota", message="no")

        out = await _node(on_callee_error=_convert)._resolve_callee(_seam_ctx(), "fault", _fault(), target_topic=None)
        assert isinstance(out, _SlotFailed)
        assert out.report.error_type == "billing.quota"  # the minted type, honored verbatim (the per-slot transformation)
        assert out.report.causes and out.report.causes[0].error_type == "callee.boom"  # original chained

    async def test_arbitrary_raise_in_seam_wraps_unhandled_not_node_own(self) -> None:
        def _boom(ctx: Any, fault: Any) -> Any:
            raise RuntimeError("seam bug")

        out = await _node(on_callee_error=_boom)._resolve_callee(_seam_ctx(), "fault", _fault(), target_topic=None)
        assert isinstance(out, _SlotFailed)
        assert out.report.error_type == FaultTypes.EXCEPTION  # slot-scoped wrap, never a whole-invocation fault
        assert out.report.causes and out.report.causes[0].error_type == "callee.boom"

    async def test_failing_call_is_set_during_seam_then_cleared(self) -> None:
        seen: dict[str, Any] = {}

        def _capture(ctx: Any, fault: Any) -> Any:
            seen["failing_call"] = ctx.failing_call
            return "ok"

        ctx = _seam_ctx()
        await _node(on_callee_error=_capture)._resolve_callee(ctx, "fault", _fault(tag="t9"), target_topic="tool.a")
        fc = seen["failing_call"]
        assert fc is not None and fc.tag == "t9" and fc.target_topic == "tool.a"
        assert fc.fault is not None and fc.fault.error_type == "callee.boom"
        assert ctx.failing_call is None  # cleared after (set during on_callee_error ONLY)


class TestMaterializationFailed:
    async def test_wire_unsafe_substitute_is_materialization_failed(self) -> None:
        class _Bad:
            pass

        node = _node(on_callee_error=lambda ctx, fault: {"x": _Bad()})  # non-JSON-serializable substitute
        out = await node._resolve_callee(_seam_ctx(), "fault", _fault(), target_topic=None)
        assert isinstance(out, _SlotFailed)
        assert out.report.error_type == FaultTypes.SLOT_MATERIALIZATION_FAILED


class TestSlotPositionIsExemptFromOutputValidation:
    async def test_on_callee_error_substitute_is_not_output_type_validated(self) -> None:
        # scenario 44 (exempt half): on_callee_error substitutes are SLOT-position (a callee-output
        # substitute materialized at the slot), NOT the node's own output — so they are NOT validated
        # against the node's declared output type (e.g. surface_to_model's failure strings on a
        # structured-output agent). _resolve_callee coerces via _coerce_to_parts, never _coerce_output.
        class _StructuredNode(BaseNodeDef):
            @property
            def _seam_output_type(self) -> Any:
                return int  # a type a plain string would FAIL if it were validated as output

        node = _StructuredNode(node_id="n", subscribe_topics=["in"], on_callee_error=lambda ctx, fault: "a failure string")
        out = await node._resolve_callee(_seam_ctx(), "fault", _fault(), target_topic=None)
        assert isinstance(out, _SlotResolved)  # resolved, NOT rejected against the int output type
        assert out.handled is True and out.parts == [TextPart(text="a failure string")]
