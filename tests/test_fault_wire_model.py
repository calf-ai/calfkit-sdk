"""PR-5 — the fault wire model (fault half of the §4 wire vocabulary).

Additive vocabulary the rail (PR-6) speaks; nothing here produces, routes, or
catches a fault. All unit-level, no broker. See
notes/pr5-fault-wire-model-implementation-plan.md.
"""

from __future__ import annotations

import typing

import pytest

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
