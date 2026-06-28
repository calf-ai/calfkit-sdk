"""PR-6 step 1 — the capability-scoped seam context (fault-rail spec §6.3).

Pure unit tests for ``CalleeResult`` (the transport view of one resolved slot,
with its ``value`` auto-extraction) and ``SeamContext`` (the façade handed to
seam handlers). No broker, no node — built directly. See
notes/pr6-fault-rail-implementation-plan.md §3 step 1 / §4 layer 3.
"""

from __future__ import annotations

from typing import Any

from calfkit.models.error_report import ErrorReport
from calfkit.models.payload import DataPart, TextPart
from calfkit.models.seam_context import CalleeResult, SeamContext
from calfkit.models.state import State


def _ctx(**overrides: Any) -> SeamContext[State]:
    """Build a SeamContext with placeholder defaults; tests override what they assert."""
    defaults: dict[str, Any] = dict(
        state=State(),
        deps={},
        resources={},
        payload=None,
        node_id="n",
        correlation_id="cid",
        emitter_node_id=None,
        route=None,
        delivery_kind="return",
        awaiting_reply=False,
        callee_results=[],
    )
    defaults.update(overrides)
    return SeamContext(**defaults)


class TestCalleeResultValue:
    def test_value_prefers_datapart_data(self) -> None:
        # value is the convenience view of parts: DataPart.data first (spec §6.3).
        r = CalleeResult(
            frame_id="f1",
            tag="t1",
            target_topic="tool.x",
            parts=[TextPart(text="hi"), DataPart(data={"k": 1})],
        )
        assert r.value == {"k": 1}

    def test_value_falls_back_to_textpart_text(self) -> None:
        # No DataPart → first TextPart.text (spec §6.3).
        r = CalleeResult(frame_id="f1", tag="t1", target_topic="tool.x", parts=[TextPart(text="hello")])
        assert r.value == "hello"

    def test_value_is_none_for_empty_or_absent_parts(self) -> None:
        # Empty parts, no parts, or parts with neither Data/Text → None (lenient,
        # never raises — unlike the strict client projection).
        assert CalleeResult(frame_id="f", tag=None, target_topic="t", parts=[]).value is None
        assert CalleeResult(frame_id="f", tag=None, target_topic="t", parts=None).value is None

    def test_carries_fault_handled_and_raw_parts(self) -> None:
        # The transport view: raw parts kept alongside value; fault set on failure;
        # handled True when on_callee_error resolved it.
        parts = [TextPart(text="x")]
        err = ErrorReport(error_type="calf.exception")
        r = CalleeResult(frame_id="f", tag="t", target_topic="tool", parts=parts, fault=err, handled=True)
        assert r.parts == parts
        assert r.fault is not None and r.fault.error_type == "calf.exception"
        assert r.handled is True


class TestSeamContext:
    def test_callee_result_returns_the_single_slot(self) -> None:
        # The single-call convenience (spec §6.3): exactly one resolved slot → it.
        r = CalleeResult(frame_id="f1", tag="t1", target_topic="tool.x", parts=[TextPart(text="hi")])
        assert _ctx(callee_results=[r]).callee_result is r

    def test_callee_result_none_when_empty_or_multiple(self) -> None:
        # Not the single-call case → None (ingress, or a fan-out batch).
        r1 = CalleeResult(frame_id="f1", tag="t1", target_topic="tool.x")
        r2 = CalleeResult(frame_id="f2", tag="t2", target_topic="tool.y")
        assert _ctx(callee_results=[]).callee_result is None
        assert _ctx(callee_results=[r1, r2]).callee_result is None

    def test_state_and_per_stage_fields_are_mutable(self) -> None:
        # state is the input-transform channel; failing_call/exception are set by the
        # framework per stage on the live object — the context is deliberately not frozen.
        ctx = _ctx()
        ctx.state.metadata = {"changed": True}
        ctx.failing_call = CalleeResult(frame_id="f", tag="t", target_topic="x")
        ctx.exception = RuntimeError("boom")
        assert ctx.state.metadata == {"changed": True}
        assert ctx.failing_call is not None and ctx.failing_call.frame_id == "f"
        assert isinstance(ctx.exception, RuntimeError)
