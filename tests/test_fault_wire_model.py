"""PR-5 — the fault wire model (fault half of the §4 wire vocabulary).

Additive vocabulary the rail (PR-6) speaks; nothing here produces, routes, or
catches a fault. All unit-level, no broker. See
notes/pr5-fault-wire-model-implementation-plan.md.
"""

from __future__ import annotations

import typing

import pydantic_core
import pytest
from pydantic import ValidationError

from calfkit._protocol import HDR_ERROR_TYPE, MessageKind
from calfkit.exceptions import NodeFaultError
from calfkit.models import CallFrame, CallFrameStack, Envelope, SessionRunContext, WorkflowState
from calfkit.models.error_report import (
    _MAX_CAUSES_DEPTH,
    ErrorReport,
    ExceptionInfo,
    FaultTypes,
    FrameRef,
    _harvest_exception,
)
from calfkit.models.reply import FaultMessage, _ReplyBase
from calfkit.models.state import State


class TestFrameRef:
    def test_carries_only_frame_id_and_target_topic(self) -> None:
        ref = FrameRef(frame_id="f1", target_topic="orders")
        assert ref.frame_id == "f1"
        assert ref.target_topic == "orders"
        # topology-only: no payload/overrides leak surface (spec §4.3)
        assert set(FrameRef.model_fields) == {"frame_id", "target_topic"}

    def test_round_trips(self) -> None:
        ref = FrameRef(frame_id="f1", target_topic="orders")
        assert FrameRef.model_validate_json(ref.model_dump_json()) == ref


def _envelope_with_reply(reply: FaultMessage) -> Envelope:
    """A minimal Envelope carrying *reply*, for a full Envelope→FaultMessage→ErrorReport
    wire round-trip (mirrors the canonical construction in test_reply_slot.py)."""
    stack = CallFrameStack()
    stack.push(CallFrame(target_topic="t", callback_topic="reply.topic"))
    return Envelope(
        internal_workflow_state=WorkflowState(call_stack=stack),
        context=SessionRunContext(state=State(), deps={}),
        reply=reply,
    )


class TestExceptionInfo:
    """The typed projection of a raised exception carried on ErrorReport.exception (spec §5)."""

    def test_carries_type_module_and_attrs(self) -> None:
        info = ExceptionInfo(type="ModelHTTPError", module="httpx", attrs={"status_code": 400})
        assert info.type == "ModelHTTPError"
        assert info.module == "httpx"
        assert info.attrs == {"status_code": 400}

    def test_module_and_attrs_default(self) -> None:
        info = ExceptionInfo(type="ValueError")
        assert info.module is None
        assert info.attrs == {}

    def test_is_frozen(self) -> None:
        # Frozen like the other wire values (spec §5): the projection travels and is read at
        # many surfaces, so field reassignment is blocked.
        info = ExceptionInfo(type="ValueError")
        with pytest.raises(ValidationError):
            info.type = "KeyError"  # type: ignore[misc]

    def test_round_trips_through_json(self) -> None:
        info = ExceptionInfo(type="ModelHTTPError", module="m", attrs={"status_code": 400, "body": {"code": "x"}})
        assert ExceptionInfo.model_validate_json(info.model_dump_json()) == info


class TestErrorReportExceptionField:
    """ErrorReport.exception — the additive slot populated only on the from_exception path (spec §5)."""

    def test_defaults_none(self) -> None:
        assert ErrorReport(error_type="x").exception is None

    def test_additive_decode_missing_key_is_none(self) -> None:
        # Backward-compatible (spec §5): an old wire message without the field validates to
        # None, so a pre-feature producer's fault still decodes.
        report = ErrorReport.model_validate_json('{"error_type":"x"}')
        assert report.exception is None

    def test_unknown_sibling_key_ignored(self) -> None:
        # extra="ignore" (spec §5): an unknown sibling key does not break decode.
        report = ErrorReport.model_validate_json('{"error_type":"x","some_future_field":123}')
        assert report.exception is None
        assert not hasattr(report, "some_future_field")

    def test_carries_exception_through_full_envelope_round_trip(self) -> None:
        # A hand-built exception slot survives Envelope→FaultMessage→ErrorReport
        # serialization and decodes as plain pydantic validation (spec §11).
        report = ErrorReport(
            error_type="x",
            exception=ExceptionInfo(type="ModelHTTPError", module="m", attrs={"status_code": 400, "body": {"code": "ctx"}}),
        )
        env = _envelope_with_reply(FaultMessage(in_reply_to="f1", tag="t1", error=report))
        back = Envelope.model_validate_json(env.model_dump_json())
        assert isinstance(back.reply, FaultMessage)
        assert back.reply.error.exception is not None
        assert back.reply.error.exception.type == "ModelHTTPError"
        assert back.reply.error.exception.attrs["status_code"] == 400
        assert back.reply.error.exception.attrs["body"]["code"] == "ctx"

    def test_to_minimal_omits_exception(self) -> None:
        # The 256 KB strip floor is identity-only — the harvest is dropped wholesale (spec §7).
        report = ErrorReport(error_type="x", exception=ExceptionInfo(type="ModelHTTPError", attrs={"status_code": 400}))
        assert report.to_minimal().exception is None

    def test_minted_fault_has_no_exception_slot(self) -> None:
        # Non-harvest path (spec §3): a deliberately-minted NodeFaultError converts verbatim and
        # carries NO exception slot — there is no foreign exception to introspect.
        assert NodeFaultError("billing.quota_exceeded", message="over quota").report.exception is None

    def test_framework_build_safe_fault_has_no_exception_slot(self) -> None:
        # A framework fault built directly via build_safe (e.g. calf.delivery.rejected) carries no
        # exception slot — only the from_exception path harvests one (spec §5/§7).
        report = ErrorReport.build_safe(error_type=FaultTypes.DELIVERY_REJECTED, message="declined")
        assert report.exception is None


class TestErrorReport:
    def test_minimal_construction_and_defaults(self) -> None:
        r = ErrorReport(error_type="billing.quota_exceeded")
        assert r.error_type == "billing.quota_exceeded"
        assert r.message == ""
        assert r.retryable is False
        assert r.origin_node_id is None
        assert r.origin_frame_id is None
        assert r.frame_chain == []
        assert r.details == {}
        assert r.causes == []

    def test_report_id_auto_mints_and_is_distinct(self) -> None:
        a = ErrorReport(error_type="x")
        b = ErrorReport(error_type="x")
        assert a.report_id and b.report_id
        assert a.report_id != b.report_id

    def test_message_clamped_to_2000_on_construction(self) -> None:
        r = ErrorReport(error_type="x", message="a" * 5000)
        assert len(r.message) == 2000

    def test_message_clamped_to_2000_on_inbound_decode(self) -> None:
        # A rejecting constraint would poison inbound decode (spec §4.3); the
        # clamp must apply on deserialization too, not only at construction.
        raw = '{"error_type":"x","message":"%s"}' % ("b" * 5000)
        assert len(ErrorReport.model_validate_json(raw).message) == 2000

    def test_round_trips_with_all_fields(self) -> None:
        r = ErrorReport(
            error_type="x",
            message="boom",
            retryable=True,
            origin_node_id="n1",
            origin_frame_id="f1",
            frame_chain=[FrameRef(frame_id="f1", target_topic="t")],
            details={"k": "v"},
        )
        back = ErrorReport.model_validate_json(r.model_dump_json())
        assert back == r

    def test_nested_causes_round_trip(self) -> None:
        inner = ErrorReport(error_type="calf.exception", message="inner")
        group = ErrorReport(error_type="calf.fault_group", causes=[inner, inner])
        back = ErrorReport.model_validate_json(group.model_dump_json())
        assert len(back.causes) == 2
        assert back.causes[0].error_type == "calf.exception"
        assert back.causes[0].message == "inner"


def _linear_cause_chain(depth: int) -> ErrorReport:
    """A report whose causes nest linearly to ``depth`` (depth=1 ⇒ no causes)."""
    node = ErrorReport(error_type=f"calf.level.{depth}")
    for level in range(depth - 1, 0, -1):
        node = ErrorReport(error_type=f"calf.level.{level}", causes=[node])
    return node


def _max_cause_depth(report: ErrorReport) -> int:
    if not report.causes:
        return 1
    return 1 + max(_max_cause_depth(c) for c in report.causes)


class TestBuildSafe:
    def test_normal_path_carries_fields(self) -> None:
        r = ErrorReport.build_safe(error_type="x", message="m", retryable=True, origin_node_id="n1")
        assert (r.error_type, r.message, r.retryable, r.origin_node_id) == ("x", "m", True, "n1")

    def test_non_str_message_coerced_keeps_causes_and_frame_chain(self) -> None:
        # round 1: a non-str ``message`` must NOT collapse the whole build into the
        # last-resort fallback (which drops causes/frame_chain). The offending scalar
        # is coerced to str on the primary path; everything else survives.
        child = ErrorReport(error_type="calf.child")
        frame = FrameRef(frame_id="f1", target_topic="t")
        r = ErrorReport.build_safe(
            error_type="calf.parent",
            message=99999,  # type: ignore[arg-type]
            causes=[child],
            frame_chain=[frame],
        )
        assert r.error_type == "calf.parent"  # primary path survived (no fallback rewrite)
        assert isinstance(r.message, str) and r.message == "99999"
        assert [c.error_type for c in r.causes] == ["calf.child"]  # cause kept
        assert r.frame_chain == [frame]  # topology kept
        assert "fallback" not in r.details.get(FaultTypes.ELIDED, {})  # not the last-resort arm

    def test_non_str_error_type_coerced_keeps_causes_and_frame_chain(self) -> None:
        # round 1: a non-str ``error_type`` must likewise survive the primary path —
        # coerced to str, NOT rewritten to calf.exception by the fallback arm, and not
        # dropping causes/frame_chain.
        child = ErrorReport(error_type="calf.child")
        frame = FrameRef(frame_id="f1", target_topic="t")
        r = ErrorReport.build_safe(
            error_type=12345,  # type: ignore[arg-type]
            message="keep",
            causes=[child],
            frame_chain=[frame],
        )
        assert isinstance(r.error_type, str) and r.error_type == "12345"
        assert r.error_type != FaultTypes.EXCEPTION  # not collapsed into the fallback
        assert r.message == "keep"
        assert [c.error_type for c in r.causes] == ["calf.child"]  # cause kept
        assert r.frame_chain == [frame]  # topology kept
        assert "fallback" not in r.details.get(FaultTypes.ELIDED, {})

    def test_never_raises_on_malformed_input(self) -> None:
        # A cause that is neither an ErrorReport nor a valid dict would make the
        # plain constructor raise; build_safe must still return a report so the
        # error path never itself raises (spec §4.3).
        r = ErrorReport.build_safe(error_type="x", message="keep", causes=[123])  # type: ignore[list-item]
        assert isinstance(r, ErrorReport)
        assert r.error_type == "x"
        assert r.message == "keep"
        assert r.causes == []

    def test_clamps_message(self) -> None:
        assert len(ErrorReport.build_safe(error_type="x", message="a" * 5000).message) == 2000

    def test_elides_causes_past_total_budget(self) -> None:
        causes = [ErrorReport(error_type=f"calf.c.{i}") for i in range(70)]
        r = ErrorReport.build_safe(error_type="calf.fault_group", causes=causes)
        assert len(r.causes) == 64
        assert r.details[FaultTypes.ELIDED]["causes"] == 6

    def test_elides_causes_past_depth_budget(self) -> None:
        r = ErrorReport.build_safe(error_type="calf.deep", causes=[_linear_cause_chain(12)])
        assert _max_cause_depth(r) <= 8

    def test_elides_frame_chain_head_and_tail(self) -> None:
        chain = [FrameRef(frame_id=f"f{i}", target_topic=f"t{i}") for i in range(70)]
        r = ErrorReport.build_safe(error_type="x", frame_chain=chain)
        assert len(r.frame_chain) == 64
        assert r.frame_chain[0] == chain[0]  # head preserved
        assert r.frame_chain[-1] == chain[-1]  # tail preserved

    def test_clamps_oversized_details(self) -> None:
        r = ErrorReport.build_safe(error_type="x", details={"big": "z" * 20000})
        assert "big" not in r.details
        assert r.details[FaultTypes.ELIDED]["details_bytes"] > 0

    def test_fallback_arm_records_breadcrumb_never_silent(self) -> None:
        # A malformed input lands in the except-fallback; the wholesale drop of
        # causes/frame_chain/details must NOT be silent — the whole feature exists
        # to kill silent drops (review round 1, convergent MAJOR).
        r = ErrorReport.build_safe(error_type="x", message="keep", causes=[123])  # type: ignore[list-item]
        assert r.error_type == "x"
        assert r.message == "keep"
        assert r.causes == []
        assert FaultTypes.ELIDED in r.details  # the drop is recorded

    def test_unserializable_details_records_distinct_breadcrumb(self) -> None:
        # Reachable when something other than NodeFaultError (which pre-checks at
        # mint) calls build_safe directly — e.g. the rail. No magic -1 byte count.
        r = ErrorReport.build_safe(error_type="x", details={"bad": object()})
        assert isinstance(r, ErrorReport)
        elided = r.details[FaultTypes.ELIDED]
        assert elided.get("details_unserializable") is True
        assert "details_bytes" not in elided

    def test_report_id_preserved_through_cause_bounding(self) -> None:
        child = ErrorReport(error_type="c")
        r = ErrorReport.build_safe(error_type="root", causes=[child])
        assert r.causes[0].report_id == child.report_id  # dedup key survives model_copy

    def test_causes_boundary_64_kept_65_elides_one(self) -> None:
        kept = [ErrorReport(error_type=f"c{i}") for i in range(64)]
        r64 = ErrorReport.build_safe(error_type="g", causes=kept)
        assert len(r64.causes) == 64 and FaultTypes.ELIDED not in r64.details
        r65 = ErrorReport.build_safe(error_type="g", causes=[*kept, ErrorReport(error_type="extra")])
        assert len(r65.causes) == 64 and r65.details[FaultTypes.ELIDED]["causes"] == 1

    def test_frame_chain_boundary_64_kept_65_elides_one(self) -> None:
        c = [FrameRef(frame_id=f"f{i}", target_topic="t") for i in range(64)]
        r64 = ErrorReport.build_safe(error_type="x", frame_chain=c)
        assert len(r64.frame_chain) == 64 and FaultTypes.ELIDED not in r64.details
        r65 = ErrorReport.build_safe(error_type="x", frame_chain=[*c, FrameRef(frame_id="f64", target_topic="t")])
        assert len(r65.frame_chain) == 64 and r65.details[FaultTypes.ELIDED]["frames"] == 1

    def test_cause_depth_boundary_7_clean_8_elides(self) -> None:
        r7 = ErrorReport.build_safe(error_type="root", causes=[_linear_cause_chain(7)])
        assert FaultTypes.ELIDED not in r7.details  # chain sits at depths 2..8, all kept
        r8 = ErrorReport.build_safe(error_type="root", causes=[_linear_cause_chain(8)])
        assert r8.details[FaultTypes.ELIDED]["causes"] == 1  # the depth-9 node drops
        assert _max_cause_depth(r8) <= 8

    def test_breadcrumb_never_pushes_details_over_field_budget(self) -> None:
        # round 2: the ELIDED breadcrumb is appended after the details clamp; on the
        # buggy code a near-16-KB details + a forced elision pushed the field over its
        # budget. The details field must stay within 16 KB regardless.
        big = {"k": "v" * 16370}  # serializes just under 16 KB on its own
        chain = [FrameRef(frame_id=f"f{i}", target_topic="t") for i in range(70)]
        r = ErrorReport.build_safe(error_type="x", details=big, frame_chain=chain)
        assert FaultTypes.ELIDED in r.details  # the elision is recorded
        assert len(pydantic_core.to_json(r.details)) <= 16 * 1024

    def test_caller_calf_namespace_details_key_is_stripped(self) -> None:
        # round 2: calf.* is framework-reserved for details keys too — a caller value
        # under it must not survive (even with no framework elision) and mislead a
        # consumer; non-reserved keys are kept.
        r = ErrorReport.build_safe(error_type="x", details={"calf.elided": {"user": "data"}, "ok": 1})
        assert "calf.elided" not in r.details
        assert r.details["ok"] == 1

    def test_total_against_hostile_str_subclass(self) -> None:
        # round 2: a str subclass whose __len__ raises breaks the clamp on the primary
        # path AND, before the fix, the re-passed value re-broke it in the fallback.
        class _EvilStr(str):
            def __len__(self) -> int:
                raise RuntimeError("len boom")

        r = ErrorReport.build_safe(error_type="x", message=_EvilStr("hi"))
        assert isinstance(r, ErrorReport)
        assert r.message == ""  # the hostile subclass is coerced out
        assert r.details[FaultTypes.ELIDED]["fallback"] == "RuntimeError"

    def test_details_kept_under_reserve_with_breadcrumb_stays_in_budget(self) -> None:
        # round 3: the path the 512-byte reserve protects — user details kept just
        # under threshold AND a breadcrumb appended — must stay within 16 KB.
        chain = [FrameRef(frame_id=f"f{i}", target_topic="t") for i in range(70)]
        r = ErrorReport.build_safe(error_type="x", details={"k": "v" * 15000}, frame_chain=chain)
        assert "k" in r.details  # kept
        assert FaultTypes.ELIDED in r.details  # breadcrumb present
        assert len(pydantic_core.to_json(r.details)) <= 16 * 1024

    def test_fallback_breadcrumb_class_name_is_bounded(self) -> None:
        # round 3: a pathological (huge) exception class name must not blow the details
        # budget. (A metaclass whose __name__ *raises* is unreachable by any real
        # exception and is an accepted non-defense.)
        huge = type("E" + "x" * 20000, (Exception,), {})

        class _H(str):
            def __len__(self) -> int:
                raise huge("boom")

        r = ErrorReport.build_safe(error_type="x", message=_H("hi"))
        assert len(pydantic_core.to_json(r.details)) <= 16 * 1024

    def test_oversized_reserved_key_does_not_evict_legit_details(self) -> None:
        # round 3: a caller calf.* key is stripped; its size must not evict legit keys.
        r = ErrorReport.build_safe(error_type="x", details={"calf.elided": "z" * 20000, "ok": 1})
        assert "calf.elided" not in r.details
        assert r.details.get("ok") == 1

    def test_total_on_deep_cause_chain_keeps_precise_breadcrumb(self) -> None:
        # A causes tree deeper than Python's recursion limit must NOT RecursionError into
        # the fallback arm (which would silently downgrade the precise causes=N breadcrumb
        # to a bare "fallback"): the dropped-subtree tally is iterative. The nested-fault-
        # group path (§4.4) is exactly where deep causes trees arise.
        deep = _linear_cause_chain(2000)  # well past sys.getrecursionlimit()
        r = ErrorReport.build_safe(error_type="root", causes=[deep])
        assert r.error_type == "root"  # total — no RecursionError
        elided = r.details[FaultTypes.ELIDED]
        assert "fallback" not in elided  # the precise tally, not the last-resort arm
        assert elided["causes"] == 1993  # 2000 chain nodes − 7 kept at depths 2..8

    def test_total_on_cyclic_causes(self) -> None:
        # A self-referential causes cycle — possible only via in-process mutation of the
        # shallow-frozen list, never off the wire (JSON is acyclic) — must terminate, not
        # RecursionError. The id()-visited guard keeps each report once.
        a = ErrorReport(error_type="a")
        a.causes.append(a)  # cycle: a -> a
        r = ErrorReport.build_safe(error_type="root", causes=[a])
        assert r.error_type == "root"  # total, terminates
        assert "fallback" not in r.details.get(FaultTypes.ELIDED, {})  # handled cleanly
        assert r.causes[0].error_type == "a" and r.causes[0].causes == []  # cycle cut


class TestImmutability:
    def test_error_report_is_frozen(self) -> None:
        r = ErrorReport(error_type="x")
        with pytest.raises(ValidationError):
            r.error_type = "y"  # report_id is "the dedup key, stable across hops" — enforce it

    def test_frame_ref_is_frozen(self) -> None:
        ref = FrameRef(frame_id="f1", target_topic="t")
        with pytest.raises(ValidationError):
            ref.frame_id = "f2"


class TestWalkAndFind:
    def test_walk_yields_self_then_nested_causes_preorder(self) -> None:
        leaf_a = ErrorReport(error_type="a")
        leaf_b = ErrorReport(error_type="b")
        mid = ErrorReport(error_type="mid", causes=[leaf_a])
        root = ErrorReport(error_type="root", causes=[mid, leaf_b])
        assert [r.error_type for r in root.walk()] == ["root", "mid", "a", "b"]

    def test_find_returns_first_match_including_nested(self) -> None:
        nested = ErrorReport(error_type="calf.exception")
        group = ErrorReport(error_type="calf.fault_group", causes=[nested])
        found = group.find("calf.exception")
        assert found is nested

    def test_find_returns_none_when_absent(self) -> None:
        assert ErrorReport(error_type="x").find("calf.exception") is None

    def test_find_matches_self(self) -> None:
        r = ErrorReport(error_type="calf.fault_group")
        assert r.find("calf.fault_group") is r

    def test_walk_terminates_on_cyclic_report(self) -> None:
        # walk() is cycle-guarded: an in-process cycle (the shallow freeze leaves causes
        # mutable) yields each report once and terminates, not recursing without bound.
        a = ErrorReport(error_type="a")
        b = ErrorReport(error_type="b", causes=[a])
        a.causes.append(b)  # cycle: a -> b -> a
        assert [r.error_type for r in a.walk()] == ["a", "b"]

    def test_find_terminates_on_cyclic_report(self) -> None:
        a = ErrorReport(error_type="a")
        b = ErrorReport(error_type="b", causes=[a])
        a.causes.append(b)  # cycle
        assert a.find("b") is b
        assert a.find("absent") is None  # terminates rather than RecursionError


class TestToMinimal:
    def test_keeps_identity_drops_heavy_fields(self) -> None:
        r = ErrorReport(
            error_type="x",
            message="boom",
            retryable=True,
            origin_node_id="n1",
            origin_frame_id="f1",
            frame_chain=[FrameRef(frame_id="f1", target_topic="t")],
            details={"k": "v"},
            causes=[ErrorReport(error_type="child")],
        )
        m = r.to_minimal()
        assert m.report_id == r.report_id
        assert m.error_type == "x"
        assert m.message == "boom"
        assert m.origin_node_id == "n1"
        assert m.origin_frame_id == "f1"
        # heavy fields stripped (the strip-and-retry floor target, spec §4.3)
        assert m.causes == []
        assert m.details == {}
        assert m.frame_chain == []

    def test_minimal_round_trips(self) -> None:
        r = ErrorReport(error_type="x", causes=[ErrorReport(error_type="c")])
        m = r.to_minimal()
        assert ErrorReport.model_validate_json(m.model_dump_json()) == m


class TestFaultMessage:
    def test_extends_reply_base_and_is_kind_fault(self) -> None:
        fm = FaultMessage(in_reply_to="f1", tag="t1", error=ErrorReport(error_type="x"))
        assert isinstance(fm, _ReplyBase)
        assert fm.kind == "fault"

    def test_in_reply_to_and_tag_accept_none(self) -> None:
        fm = FaultMessage(in_reply_to=None, tag=None, error=ErrorReport(error_type="x"))
        assert fm.in_reply_to is None
        assert fm.tag is None

    def test_round_trips_through_json_with_nested_report(self) -> None:
        fm = FaultMessage(
            in_reply_to="f1",
            tag="call-7",
            error=ErrorReport(error_type="calf.fault_group", causes=[ErrorReport(error_type="calf.exception")]),
        )
        back = FaultMessage.model_validate_json(fm.model_dump_json())
        assert back == fm
        assert back.error.causes[0].error_type == "calf.exception"


class TestNodeFaultError:
    def test_mint_builds_report_from_fields(self) -> None:
        nfe = NodeFaultError("billing.quota_exceeded", message="over limit", retryable=True, details={"plan": "free"})
        assert isinstance(nfe.report, ErrorReport)
        assert nfe.report.error_type == "billing.quota_exceeded"
        assert nfe.report.message == "over limit"
        assert nfe.report.retryable is True
        assert nfe.report.details == {"plan": "free"}

    def test_mint_rejects_reserved_calf_namespace(self) -> None:
        # Consumers branching on calf.* must trust the namespace, so a user mint
        # under it fails at the keyboard (spec §4.3, scenario 26).
        with pytest.raises(ValueError, match="calf"):
            NodeFaultError("calf.something", message="nope")

    def test_mint_rejects_unserializable_details_at_construction(self) -> None:
        with pytest.raises(ValueError):
            NodeFaultError("billing.x", details={"bad": object()})

    def test_receive_wraps_report_verbatim(self) -> None:
        report = ErrorReport(error_type="x", message="m")
        nfe = NodeFaultError(report)
        assert nfe.report is report

    def test_receive_allows_calf_namespace_report(self) -> None:
        # Wrapping a framework-minted calf.* report (the client re-raise path) must
        # NOT trip the mint guard — the guard is only for user string mints.
        report = ErrorReport(error_type=FaultTypes.EXCEPTION)
        assert NodeFaultError(report).report.error_type == FaultTypes.EXCEPTION

    def test_receive_rejects_mint_only_kwargs(self) -> None:
        # message/retryable/details are mint-only; passing them with a report is a
        # misuse that must fail loudly, not silently ignore them.
        report = ErrorReport(error_type="x")
        with pytest.raises(ValueError, match="mint-only"):
            NodeFaultError(report, message="ignored")

    def test_str_uses_message_then_error_type(self) -> None:
        assert str(NodeFaultError("billing.x")) == "billing.x"
        assert str(NodeFaultError("billing.x", message="over limit")) == "over limit"

    def test_mint_rejects_calf_namespace_details_keys(self) -> None:
        # round 2: the calf.* reservation extends to details keys so consumers can
        # trust the namespace there too.
        with pytest.raises(ValueError, match="calf"):
            NodeFaultError("billing.x", details={"calf.foo": 1})

    def test_mint_rejects_empty_or_blank_error_type(self) -> None:
        # round 2: error_type is the contract; an empty/blank code is meaningless and
        # would make str(e) empty and find("") match.
        with pytest.raises(ValueError):
            NodeFaultError("")
        with pytest.raises(ValueError):
            NodeFaultError("   ")

    def test_str_is_never_empty_even_for_blank_received_report(self) -> None:
        nfe = NodeFaultError(ErrorReport(error_type=""))
        assert str(nfe) != ""

    def test_reduce_reconstructs_from_report(self) -> None:
        # __reduce__ must rebuild from the report, not replay self.args (a message
        # string) through __init__, which would mis-route into the mint arm.
        nfe = NodeFaultError("billing.quota_exceeded", message="m", details={"k": "v"})
        factory, args = nfe.__reduce__()
        rebuilt = factory(*args)
        assert isinstance(rebuilt, NodeFaultError)
        assert rebuilt.report == nfe.report


class TestFaultTypes:
    def test_known_error_type_codes(self) -> None:
        assert FaultTypes.MODEL_CONTEXT_WINDOW_EXCEEDED == "calf.model.context_window_exceeded"
        assert FaultTypes.FAULT_GROUP == "calf.fault_group"
        assert FaultTypes.EXCEPTION == "calf.exception"
        assert FaultTypes.DELIVERY_REJECTED == "calf.delivery.rejected"
        assert FaultTypes.DELIVERY_UNDECODABLE == "calf.delivery.undecodable"
        assert FaultTypes.SLOT_MATERIALIZATION_FAILED == "calf.slot.materialization_failed"
        assert FaultTypes.AGENT_SELF_RETRY_EXHAUSTED == "calf.agent.self_retry_exhausted"

    def test_elided_details_key(self) -> None:
        assert FaultTypes.ELIDED == "calf.elided"


class TestProtocol:
    def test_error_type_header_name(self) -> None:
        assert HDR_ERROR_TYPE == "x-calf-error-type"

    def test_message_kind_value_space_includes_fault(self) -> None:
        assert typing.get_args(MessageKind) == ("call", "return", "fault")


class TestPublicExports:
    def test_top_level_exports(self) -> None:
        import calfkit

        assert calfkit.NodeFaultError is NodeFaultError
        assert calfkit.ErrorReport is ErrorReport
        assert calfkit.FaultTypes is FaultTypes

    def test_models_package_exports(self) -> None:
        from calfkit import models

        assert models.FaultMessage is FaultMessage
        assert models.FrameRef is FrameRef
        assert models.ErrorReport is ErrorReport
        assert models.FaultTypes is FaultTypes


class TestHarvestException:
    """_harvest_exception — the TOTAL, leaf-safe projection of vars(exc) into ExceptionInfo
    (spec §6/§8). It runs OUTSIDE build_safe's try, on the unguarded chokepoint path, so it must
    never raise; a residually-unserializable attr is dropped-and-tallied, never silent."""

    # ── fidelity (spec §5.B / §15) ───────────────────────────────────────────────────────

    def test_harvests_real_shaped_model_http_error(self) -> None:
        # The motivating case: a provider error's structured state survives as JSON data.
        from calfkit._vendor.pydantic_ai.exceptions import ModelHTTPError

        body = {"type": "invalid_request_error", "code": "context_length_exceeded"}
        info, dropped = _harvest_exception(ModelHTTPError(status_code=400, model_name="m", body=body))
        assert info.type == "ModelHTTPError"
        assert info.module == "calfkit._vendor.pydantic_ai.exceptions"
        assert info.attrs["status_code"] == 400
        assert info.attrs["body"]["code"] == "context_length_exceeded"
        assert "message" not in info.attrs  # redundant with ErrorReport.message
        assert dropped == 0

    def test_embedded_dict_round_trips_to_python_dict(self) -> None:
        exc = ValueError()
        exc.payload = {"nested": {"k": [1, 2, 3]}}  # type: ignore[attr-defined]
        info, dropped = _harvest_exception(exc)
        assert info.attrs["payload"] == {"nested": {"k": [1, 2, 3]}}
        assert isinstance(info.attrs["payload"], dict)
        assert dropped == 0

    def test_non_utf8_and_valid_utf8_bytes_become_base64(self) -> None:
        # Raw HTTP/socket error bodies are often non-utf8; bytes_mode='base64' is lossless and is
        # used even for valid-utf8 bytes (spec §6). pydantic_core emits URL-safe base64.
        import base64

        exc = ValueError()
        exc.raw = b"\xff\xfe"  # type: ignore[attr-defined]  # non-utf8
        exc.text = b"hello"  # type: ignore[attr-defined]  # valid utf8
        info, dropped = _harvest_exception(exc)
        assert info.attrs["raw"] == base64.urlsafe_b64encode(b"\xff\xfe").decode()
        assert info.attrs["text"] == base64.urlsafe_b64encode(b"hello").decode()  # base64 even for valid utf8
        # lossless: the URL-safe base64 decodes back to the original bytes
        assert base64.urlsafe_b64decode(info.attrs["raw"]) == b"\xff\xfe"
        assert dropped == 0

    def test_nan_and_inf_become_strings_wire_consistent(self) -> None:
        # inf_nan_mode='strings' so nan/inf survive losslessly and identically in-process and
        # after the publish path's JSON (which emits null otherwise) (spec §6/§11).
        exc = ValueError()
        exc.vals = {"n": float("nan"), "p": float("inf"), "m": float("-inf")}  # type: ignore[attr-defined]
        info, dropped = _harvest_exception(exc)
        assert info.attrs["vals"] == {"n": "NaN", "p": "Infinity", "m": "-Infinity"}
        back = ExceptionInfo.model_validate_json(ExceptionInfo(type="X", attrs=info.attrs).model_dump_json())
        assert back.attrs["vals"] == {"n": "NaN", "p": "Infinity", "m": "-Infinity"}  # wire-consistent
        assert dropped == 0

    def test_cyclic_container_attr_is_marked_not_dropped(self) -> None:
        # A cyclic container serializes to a "..." marker (handled, NOT dropped) (spec §8).
        exc = ValueError()
        cyc: list = [1]
        cyc.append(cyc)
        exc.cyc = cyc  # type: ignore[attr-defined]
        info, dropped = _harvest_exception(exc)
        assert "cyc" in info.attrs  # kept, not dropped
        assert "..." in str(info.attrs["cyc"])
        assert dropped == 0

    def test_deeply_nested_attr_is_dropped_and_tallied(self) -> None:
        # The deterministic residual drop lever: pydantic_core's serde recursion limit fires on a
        # ~2000-deep attr (stack-independent), so it is dropped-and-tallied (spec §6/§8/§5.B).
        deep: dict = {"leaf": 1}
        for _ in range(2000):
            deep = {"x": deep}
        exc = ValueError()
        exc.deep = deep  # type: ignore[attr-defined]
        exc.good = "kept"  # type: ignore[attr-defined]  # good neighbor survives
        info, dropped = _harvest_exception(exc)
        assert dropped == 1
        assert "deep" not in info.attrs
        assert info.attrs["good"] == "kept"

    def test_lone_surrogate_string_attr_is_dropped_not_coerced(self) -> None:
        # A lone surrogate RAISES PydanticSerializationError (not coercible), so it is
        # dropped-and-tallied — never silently coerced (spec §5.B).
        exc = ValueError()
        exc.bad = "\ud800"  # type: ignore[attr-defined]
        exc.good = "ok"  # type: ignore[attr-defined]
        info, dropped = _harvest_exception(exc)
        assert dropped == 1
        assert "bad" not in info.attrs
        assert info.attrs["good"] == "ok"

    def test_generator_and_basemodel_attrs_coerced(self) -> None:
        # serialize_unknown coerces unknown types (a generator, a BaseModel) rather than dropping.
        exc = ValueError()
        exc.gen = (i for i in range(3))  # type: ignore[attr-defined]
        exc.model = FrameRef(frame_id="f", target_topic="t")  # type: ignore[attr-defined]
        info, dropped = _harvest_exception(exc)
        assert "gen" in info.attrs and "model" in info.attrs  # coerced, not dropped
        assert dropped == 0

    def test_nested_unserializable_leaf_placeholdered_neighbors_kept(self) -> None:
        # A nested unserializable leaf is coerced to a placeholder while good neighbor keys in the
        # same dict survive (best-effort leaf-safety, spec §8).
        class _Weird:
            pass

        exc = ValueError()
        exc.mixed = {"good": 1, "leaf": _Weird()}  # type: ignore[attr-defined]
        info, dropped = _harvest_exception(exc)
        assert info.attrs["mixed"]["good"] == 1
        assert "leaf" in info.attrs["mixed"]  # coerced to a placeholder, not dropped
        assert dropped == 0

    def test_traceback_never_appears_in_attrs(self) -> None:
        # vars(exc) excludes __traceback__, so a raised-and-caught exception never ships a
        # TracebackType value (spec §8 "tracebacks never touched").
        try:
            raise ValueError("boom")
        except ValueError as e:
            info, dropped = _harvest_exception(e)
        assert "__traceback__" not in info.attrs
        assert dropped == 0

    def test_slots_only_exception_has_empty_attrs_but_is_typed(self) -> None:
        # No __dict__ (pure __slots__) → attrs={}, still typed (spec §6 edge case).
        class _SlotsError(Exception):
            __slots__ = ()

        info, dropped = _harvest_exception(_SlotsError())
        assert info.type == "_SlotsError"
        assert info.attrs == {}
        assert dropped == 0

    def test_message_attr_is_dropped(self) -> None:
        # The `message` attr is redundant with ErrorReport.message and dropped (spec §5).
        exc = ValueError()
        exc.message = "redundant"  # type: ignore[attr-defined]
        exc.other = "kept"  # type: ignore[attr-defined]
        info, dropped = _harvest_exception(exc)
        assert "message" not in info.attrs
        assert info.attrs["other"] == "kept"

    def test_non_str_dict_key_coerced(self) -> None:
        # attrs keys must be str (pydantic); a non-str __dict__ key is coerced via str() (spec §6).
        exc = ValueError()
        exc.__dict__[42] = "v"
        info, dropped = _harvest_exception(exc)
        assert info.attrs["42"] == "v"
        assert dropped == 0

    # ── totality (spec §5.A harvest subset; walk-totality lands with the chain) ───────────

    def test_total_when_metaclass_name_raises(self) -> None:
        class _NoNameMeta(type):
            @property
            def __name__(cls):  # type: ignore[override]
                raise RuntimeError("no name")

        class _NoNameError(Exception, metaclass=_NoNameMeta):
            pass

        with pytest.raises(RuntimeError):  # precondition: the construction is non-vacuous
            _ = type(_NoNameError()).__name__
        info, dropped = _harvest_exception(_NoNameError())  # must not raise
        assert info.type == "<unharvestable>"

    def test_total_when_metaclass_module_raises(self) -> None:
        class _NoModuleMeta(type):
            @property
            def __module__(cls):  # type: ignore[override]
                raise RuntimeError("no module")

        class _NoModuleError(Exception, metaclass=_NoModuleMeta):
            pass

        with pytest.raises(RuntimeError):
            _ = type(_NoModuleError()).__module__
        # type and module are harvested INDEPENDENTLY: a raising __module__ must NOT discard the
        # readable __name__ (the forensic class hint §9 preserves); module alone degrades to None.
        info, dropped = _harvest_exception(_NoModuleError())  # must not raise
        assert info.type == "_NoModuleError"
        assert info.module is None

    def test_total_when_dict_key_str_raises(self) -> None:
        class _BadStrKey:
            def __hash__(self) -> int:
                return 1

            def __str__(self) -> str:
                raise RuntimeError("no str")

        exc = ValueError()
        exc.__dict__[_BadStrKey()] = "v"
        exc.good = "kept"  # type: ignore[attr-defined]
        info, dropped = _harvest_exception(exc)  # must not raise (str(k) coercion guarded)
        assert dropped == 1
        assert info.attrs.get("good") == "kept"

    def test_total_when_dict_key_eq_raises(self) -> None:
        class _BadEqKey:
            def __hash__(self) -> int:
                return 1

            def __eq__(self, other: object) -> bool:
                raise RuntimeError("no eq")

        exc = ValueError()
        exc.__dict__[_BadEqKey()] = "v"
        info, dropped = _harvest_exception(exc)  # must not raise (the k == "message" compare guarded)
        assert dropped == 1

    def test_total_when_vars_raises(self) -> None:
        class _NoDictError(Exception):
            @property
            def __dict__(self):  # type: ignore[override]
                raise RuntimeError("no dict")

        info, dropped = _harvest_exception(_NoDictError())  # must not raise
        assert info.attrs == {}

    def test_total_when_items_raises(self) -> None:
        class _HostileItems(dict):
            def items(self):  # type: ignore[override]
                raise RuntimeError("no items")

        class _BadItemsError(Exception):
            @property
            def __dict__(self):  # type: ignore[override]
                return _HostileItems()

        info, dropped = _harvest_exception(_BadItemsError())  # must not raise
        assert info.attrs == {}

    def test_construction_failure_tallies_dropped_attrs(self) -> None:
        # A hostile metaclass whose __name__ RETURNS a non-str (no raise) makes ExceptionInfo(type:
        # str) construction reject it; the harvested attrs are dropped — but TALLIED, never silently
        # (the count surfaces as exception_attrs_dropped, the feature's never-silent invariant).
        class _IntNameMeta(type):
            @property
            def __name__(cls):  # type: ignore[override]
                return 999  # non-str, not a raise

        class _IntNameError(Exception, metaclass=_IntNameMeta):
            pass

        exc = _IntNameError()
        exc.status_code = 400  # type: ignore[attr-defined]  # a cleanly-harvestable attr
        info, dropped = _harvest_exception(exc)  # must not raise
        assert info.type == "<unharvestable>"
        assert info.attrs == {}
        assert dropped == 1  # the attr was tallied with the construction drop, not silently discarded


class TestBuildSafeExceptionBounds:
    """build_safe(exception=, exception_attrs_dropped=, chain_truncated=) — the new field's
    carriage bound lives at the single total-construction chokepoint (spec §7)."""

    def test_carries_exception_slot(self) -> None:
        report = ErrorReport.build_safe(error_type="x", exception=ExceptionInfo(type="ModelHTTPError", attrs={"status_code": 400}))
        assert report.exception is not None
        assert report.exception.type == "ModelHTTPError"
        assert report.exception.attrs["status_code"] == 400

    def test_oversized_attrs_bounded_type_module_survive(self) -> None:
        # attrs over the 16 KB bound are dropped to {} with an exception_attrs_bytes breadcrumb,
        # but the slot is KEPT and type/module survive the model_copy (spec §7 / §5.D).
        big = ExceptionInfo(type="Big", module="m", attrs={"blob": "x" * 20_000})
        report = ErrorReport.build_safe(error_type="x", exception=big)
        assert report.exception is not None
        assert report.exception.type == "Big"
        assert report.exception.module == "m"
        assert report.exception.attrs == {}
        assert report.details[FaultTypes.ELIDED]["exception_attrs_bytes"] > 0

    def test_exception_attrs_dropped_breadcrumb(self) -> None:
        report = ErrorReport.build_safe(error_type="x", exception=ExceptionInfo(type="X"), exception_attrs_dropped=3)
        assert report.details[FaultTypes.ELIDED]["exception_attrs_dropped"] == 3

    def test_chain_truncated_breadcrumb(self) -> None:
        # The depth-cap-drop breadcrumb (the param's elided branch is not exercised by
        # from_exception until the chain lands, so unit-test it directly) (spec §7).
        report = ErrorReport.build_safe(error_type="x", exception=ExceptionInfo(type="X"), chain_truncated=True)
        assert report.details[FaultTypes.ELIDED]["chain_truncated"] is True

    def test_no_breadcrumbs_when_nothing_elided(self) -> None:
        report = ErrorReport.build_safe(error_type="x", exception=ExceptionInfo(type="X", attrs={"k": "v"}))
        assert FaultTypes.ELIDED not in report.details

    def test_unserializable_attrs_defensive_breadcrumb(self) -> None:
        # Defensive uniformity (spec §7): if build_safe is handed an ExceptionInfo whose attrs are
        # NOT jsonsafe'd (a direct caller, not the harvest), the unserializable case empties attrs and
        # breadcrumbs exception_attrs_unserializable — total and never silent, never a raise.
        report = ErrorReport.build_safe(error_type="x", exception=ExceptionInfo(type="X", attrs={"k": object()}))
        assert report.exception is not None
        assert report.exception.attrs == {}
        assert report.details[FaultTypes.ELIDED]["exception_attrs_unserializable"] is True


class TestFromException:
    """PR-6: ErrorReport.from_exception — the factory the rail's chokepoint uses to
    synthesize a fault from an arbitrary (non-NodeFaultError) exception (spec §6.7)."""

    def test_generic_exception_maps_to_calf_exception(self) -> None:
        report = ErrorReport.from_exception(ValueError("boom"))
        assert report.error_type == FaultTypes.EXCEPTION
        # the structured projection is harvested into the typed exception slot
        assert report.exception is not None
        assert report.exception.type == "ValueError"
        # the clamped exception message rides the report's message field
        assert "boom" in report.message

    def test_is_total_on_a_broken_str_exception(self) -> None:
        # The error path must never itself raise (spec §6.7/§4.3): an exception whose
        # __str__ raises still produces a report, not a second exception.
        class HostileError(Exception):
            def __str__(self) -> str:
                raise RuntimeError("no str for you")

        report = ErrorReport.from_exception(HostileError())
        assert report.error_type == FaultTypes.EXCEPTION
        assert report.exception is not None
        assert report.exception.type == "HostileError"

    def test_is_total_when_both_str_and_repr_raise(self) -> None:
        # The innermost fallback of ``_safe_exc_str``: reached only when BOTH ``str(exc)``
        # AND ``repr(exc)`` raise. The message degrades to the bounded sentinel — and NO
        # exception escapes (the error path's totality contract, §6.7/§4.3).
        class TotallyUnprintableError(Exception):
            def __str__(self) -> str:
                raise RuntimeError("no str")

            def __repr__(self) -> str:
                raise RuntimeError("no repr")

        report = ErrorReport.from_exception(TotallyUnprintableError())
        assert report.message == "<unprintable TotallyUnprintableError>"
        assert report.error_type == FaultTypes.EXCEPTION
        assert report.exception is not None
        assert report.exception.type == "TotallyUnprintableError"

    def test_chains_a_cause(self) -> None:
        # The §6.8 recovery-then-failure case: the second error chains the original.
        prior = ErrorReport(error_type="upstream")
        report = ErrorReport.from_exception(RuntimeError("x"), cause=prior)
        assert [c.report_id for c in report.causes] == [prior.report_id]

    def test_sources_origin_node_id_from_node(self) -> None:
        # origin breadcrumb (scenario 1): node.node_id → origin_node_id when given.
        class FakeNode:
            node_id = "orchestrator"

        report = ErrorReport.from_exception(RuntimeError("x"), node=FakeNode())
        assert report.origin_node_id == "orchestrator"

    def test_populates_exception_slot_with_attrs(self) -> None:
        # The harvest is wired through from_exception: structured attrs reach report.exception.
        exc = ValueError("boom")
        exc.status_code = 429  # type: ignore[attr-defined]
        report = ErrorReport.from_exception(exc)
        assert report.exception is not None
        assert report.exception.type == "ValueError"
        assert report.exception.attrs["status_code"] == 429

    def test_oversized_attrs_elided_with_breadcrumb_slot_kept(self) -> None:
        # attrs over the 16 KB bound: the slot is kept with type, attrs emptied, breadcrumbed (§7/§5.D).
        exc = ValueError("boom")
        exc.blob = "x" * 20_000  # type: ignore[attr-defined]
        report = ErrorReport.from_exception(exc)
        assert report.exception is not None
        assert report.exception.type == "ValueError"
        assert report.exception.attrs == {}
        assert report.details[FaultTypes.ELIDED]["exception_attrs_bytes"] > 0

    def test_residual_attr_drop_breadcrumb(self) -> None:
        # A deep attr is dropped-and-tallied; the count surfaces as a non-silent breadcrumb (§7/§5.D).
        deep: dict = {"leaf": 1}
        for _ in range(2000):
            deep = {"x": deep}
        exc = ValueError("boom")
        exc.deep = deep  # type: ignore[attr-defined]
        report = ErrorReport.from_exception(exc)
        assert report.details[FaultTypes.ELIDED]["exception_attrs_dropped"] >= 1

    def test_total_when_metaclass_name_raises_degrades_slot_to_sentinel(self) -> None:
        # from_exception must stay total even when type(exc).__name__ raises: the harvest's
        # type-access guard degrades the slot's type to the sentinel rather than escaping (§8).
        class _NoNameMeta(type):
            @property
            def __name__(cls):  # type: ignore[override]
                raise RuntimeError("no name")

        class _NoNameError(Exception, metaclass=_NoNameMeta):
            pass

        report = ErrorReport.from_exception(_NoNameError())  # must not raise
        assert report.error_type == FaultTypes.EXCEPTION
        assert report.exception is not None
        assert report.exception.type == "<unharvestable>"


class TestExceptionChain:
    """The __cause__-only language chain mapped onto causes (spec §6) — each link a lean
    ErrorReport carrying its own exception slot; linear, cycle-safe, depth-capped."""

    def test_cause_mapped_to_chain_link(self) -> None:
        # raise B() from A() → root exception.type=="B", causes[0].exception.type=="A".
        a = ValueError("a")
        b = RuntimeError("b")
        b.__cause__ = a
        report = ErrorReport.from_exception(b)
        assert report.exception is not None and report.exception.type == "RuntimeError"
        assert len(report.causes) == 1
        link = report.causes[0]
        assert link.error_type == FaultTypes.EXCEPTION
        assert link.exception is not None and link.exception.type == "ValueError"
        # a mid-chain link carries no calfkit origin — it happened at no calfkit frame (spec §6).
        assert link.origin_node_id is None
        assert link.frame_chain == []

    def test_chain_link_carries_its_own_attrs(self) -> None:
        a = ValueError("a")
        a.status_code = 503  # type: ignore[attr-defined]
        b = RuntimeError("b")
        b.__cause__ = a
        report = ErrorReport.from_exception(b)
        assert report.causes[0].exception is not None
        assert report.causes[0].exception.attrs["status_code"] == 503

    def test_end_of_chain_no_cause(self) -> None:
        report = ErrorReport.from_exception(ValueError("solo"))
        assert report.causes == []

    def test_multi_hop_cycle_to_root_terminates(self) -> None:
        # A.__cause__=B, B.__cause__=A — terminates via the threaded _seen (correct dedup, not a drop).
        a = ValueError("a")
        b = RuntimeError("b")
        a.__cause__ = b
        b.__cause__ = a
        report = ErrorReport.from_exception(a)  # must terminate
        assert report.exception is not None and report.exception.type == "ValueError"
        assert len(report.causes) == 1
        assert report.causes[0].exception is not None and report.causes[0].exception.type == "RuntimeError"
        assert report.causes[0].causes == []  # the cycle back to A is deduped, not re-walked

    def test_cycle_onto_mid_chain_link_terminates(self) -> None:
        # A → B → C → B (a cycle onto a NON-root link) terminates via threaded _seen.
        a = ValueError("a")
        b = RuntimeError("b")
        c = KeyError("c")
        a.__cause__ = b
        b.__cause__ = c
        c.__cause__ = b  # cycle back to B
        report = ErrorReport.from_exception(a)  # must terminate
        types = [r.exception.type for r in report.walk() if r.exception]
        assert types == ["ValueError", "RuntimeError", "KeyError"]  # B is not re-walked

    def test_chain_at_kept_depth_has_no_truncation_flag(self) -> None:
        # A chain of exactly _MAX_CAUSES_DEPTH reports (deepest __cause__ is None) → end-of-chain,
        # NOT truncated: `nxt is None` is checked BEFORE the depth cap (the off-by-one pin, spec §6).
        excs: list[Exception] = [ValueError(f"e{i}") for i in range(_MAX_CAUSES_DEPTH)]
        for i in range(len(excs) - 1):
            excs[i].__cause__ = excs[i + 1]
        report = ErrorReport.from_exception(excs[0])
        assert len(list(report.walk())) == _MAX_CAUSES_DEPTH
        assert "chain_truncated" not in report.details.get(FaultTypes.ELIDED, {})

    def test_chain_one_deeper_sets_truncation_flag(self) -> None:
        # One link deeper than the cap → the deepest is dropped and the drop is breadcrumbed
        # (never silent); a cycle would NOT set the flag, only a real depth-cap drop does.
        excs: list[Exception] = [ValueError(f"e{i}") for i in range(_MAX_CAUSES_DEPTH + 1)]
        for i in range(len(excs) - 1):
            excs[i].__cause__ = excs[i + 1]
        report = ErrorReport.from_exception(excs[0])
        assert report.details[FaultTypes.ELIDED]["chain_truncated"] is True

    def test_walk_and_exception_type_locates_a_deep_link(self) -> None:
        a = ValueError("a")
        b = RuntimeError("b")
        c = KeyError("c")
        a.__cause__ = b
        b.__cause__ = c
        report = ErrorReport.from_exception(a)
        key_link = next((r for r in report.walk() if r.exception and r.exception.type == "KeyError"), None)
        assert key_link is not None

    def test_find_does_not_discriminate_chain_links(self) -> None:
        # Every link shares calf.exception, so find() returns the shallowest match (the root),
        # NOT a deeper link — find() is not a chain discriminator here (spec §11.1).
        a = ValueError("a")
        b = RuntimeError("b")
        a.__cause__ = b
        report = ErrorReport.from_exception(a)
        assert report.find(FaultTypes.EXCEPTION) is report

    def test_recovery_cause_and_chain_comingle_in_causes(self) -> None:
        # On a recovery-then-failure synthesis (cause= passed), causes holds BOTH the recovery-prior
        # report AND the language-chain link, co-mingled — discriminate by CONTENT, not position (spec §6).
        prior = ErrorReport(error_type="recovery.prior")
        b = RuntimeError("b")
        b.__cause__ = ValueError("a")
        report = ErrorReport.from_exception(b, cause=prior)
        assert any(c.error_type == "recovery.prior" for c in report.causes)
        assert any(c.exception is not None and c.exception.type == "ValueError" for c in report.causes)

    def test_context_only_chain_is_not_walked(self) -> None:
        # __cause__-ONLY: an implicit __context__ chain (raised in an except with no `raise from`) is
        # NOT walked — pin the dropped behavior (spec §6.1).
        try:
            try:
                raise ValueError("inner")
            except ValueError:
                raise RuntimeError("outer")  # noqa: B904 — intentionally no `from`, sets __context__ only
        except RuntimeError as e:
            report = ErrorReport.from_exception(e)
        assert report.exception is not None and report.exception.type == "RuntimeError"
        assert report.causes == []  # the __context__ ValueError is NOT walked

    def test_total_when_cause_descriptor_raises_at_root(self) -> None:
        # A hostile/raising __cause__ descriptor must not break totality — the chain is dropped and
        # from_exception stays total (spec §8).
        class _RaisingCauseError(Exception):
            @property
            def __cause__(self):  # type: ignore[override]
                raise RuntimeError("no cause")

        report = ErrorReport.from_exception(_RaisingCauseError("x"))  # must not raise
        assert report.exception is not None
        assert report.causes == []

    def test_total_when_cause_descriptor_raises_mid_chain(self) -> None:
        class _RaisingCauseError(Exception):
            @property
            def __cause__(self):  # type: ignore[override]
                raise RuntimeError("no cause")

        root = ValueError("root")
        root.__cause__ = _RaisingCauseError("mid")  # the mid link's own __cause__ raises
        report = ErrorReport.from_exception(root)  # must not raise
        assert report.exception is not None and report.exception.type == "ValueError"
        assert len(report.causes) == 1
        assert report.causes[0].exception is not None
        assert report.causes[0].exception.type == "_RaisingCauseError"
        assert report.causes[0].causes == []  # mid's raising __cause__ drops its onward chain

    def test_wire_json_decodes_with_no_producer_class(self) -> None:
        # Decoupling by construction (spec §11): a fault wire JSON authored as a STRING LITERAL —
        # referencing no producer class — decodes to a plain ErrorReport whose exception slot and
        # __cause__-chain link read as JSON-native data. The receiver needs no openai / pydantic_ai
        # class importable; the decode is pure pydantic validation.
        wire = (
            '{"error_type":"calf.exception","message":"status_code: 400 ...",'
            '"exception":{"type":"ModelHTTPError",'
            '"module":"calfkit._vendor.pydantic_ai.exceptions",'
            '"attrs":{"status_code":400,"body":{"code":"context_length_exceeded"}}},'
            '"causes":[{"error_type":"calf.exception","message":"bad request",'
            '"exception":{"type":"BadRequestError","module":"openai","attrs":{"status_code":400}}}]}'
        )
        report = ErrorReport.model_validate_json(wire)
        assert report.exception is not None
        assert report.exception.type == "ModelHTTPError"
        assert report.exception.attrs["status_code"] == 400
        assert report.exception.attrs["body"]["code"] == "context_length_exceeded"
        link = next((r for r in report.walk() if r.exception and r.exception.type == "BadRequestError"), None)
        assert link is not None and link.exception is not None
        assert link.exception.attrs["status_code"] == 400
