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
from calfkit.models.error_report import ErrorReport, FaultTypes, FrameRef
from calfkit.models.reply import FaultMessage, _ReplyBase


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
        inner = ErrorReport(error_type="calf.unhandled", message="inner")
        group = ErrorReport(error_type="calf.fault_group", causes=[inner, inner])
        back = ErrorReport.model_validate_json(group.model_dump_json())
        assert len(back.causes) == 2
        assert back.causes[0].error_type == "calf.unhandled"
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
        # coerced to str, NOT rewritten to calf.unhandled by the fallback arm, and not
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
        assert r.error_type != FaultTypes.UNHANDLED  # not collapsed into the fallback
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
        nested = ErrorReport(error_type="calf.unhandled")
        group = ErrorReport(error_type="calf.fault_group", causes=[nested])
        found = group.find("calf.unhandled")
        assert found is nested

    def test_find_returns_none_when_absent(self) -> None:
        assert ErrorReport(error_type="x").find("calf.unhandled") is None

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
            error=ErrorReport(error_type="calf.fault_group", causes=[ErrorReport(error_type="calf.unhandled")]),
        )
        back = FaultMessage.model_validate_json(fm.model_dump_json())
        assert back == fm
        assert back.error.causes[0].error_type == "calf.unhandled"


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
        report = ErrorReport(error_type=FaultTypes.UNHANDLED)
        assert NodeFaultError(report).report.error_type == FaultTypes.UNHANDLED

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
        assert FaultTypes.UNHANDLED == "calf.unhandled"
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


class TestFromException:
    """PR-6: ErrorReport.from_exception — the factory the rail's chokepoint uses to
    synthesize a fault from an arbitrary (non-NodeFaultError) exception (spec §6.7)."""

    def test_generic_exception_maps_to_calf_unhandled(self) -> None:
        report = ErrorReport.from_exception(ValueError("boom"))
        assert report.error_type == FaultTypes.UNHANDLED
        # the exception class name is recorded as a framework details breadcrumb
        assert report.details[FaultTypes.EXCEPTION_TYPE] == "ValueError"
        # the clamped exception message rides the report's message field
        assert "boom" in report.message

    def test_is_total_on_a_broken_str_exception(self) -> None:
        # The error path must never itself raise (spec §6.7/§4.3): an exception whose
        # __str__ raises still produces a report, not a second exception.
        class HostileError(Exception):
            def __str__(self) -> str:
                raise RuntimeError("no str for you")

        report = ErrorReport.from_exception(HostileError())
        assert report.error_type == FaultTypes.UNHANDLED
        assert report.details[FaultTypes.EXCEPTION_TYPE] == "HostileError"

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
