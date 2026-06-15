"""The typed fault value and its vocabulary (spec §4.3).

Additive wire model introduced by PR-5: ``ErrorReport`` (the fault carried on
the reply slot's ``FaultMessage``), its topology breadcrumb ``FrameRef``, the
``FaultTypes`` constants, and the total-construction ``build_safe`` builder.
Nothing here produces or routes a fault — that is the rail (PR-6).

A plain ``BaseModel`` (not the long-gone ``CompactBaseModel``): every field
always serializes, because a fault's absence/presence is load-bearing and
``exclude_unset`` gotchas must not apply.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pydantic_core
import uuid_utils
from pydantic import BaseModel, ConfigDict, Field, field_validator

# Carriage budgets (spec §4.3): construction-time bounds so a fault never grows
# unboundedly. The total 256 KB serialized cap and the strip-and-retry-then-floor
# loop are publish-time concerns and live with the fault-publish path (PR-6); what
# lives here is the per-field bounding applied at synthesis (build_safe) plus the
# minimal strip target (to_minimal).
_MAX_MESSAGE_CHARS = 2000
_MAX_CAUSES_DEPTH = 8
_MAX_CAUSES_TOTAL = 64
_MAX_FRAME_CHAIN = 64
_MAX_DETAILS_BYTES = 16 * 1024


class FaultTypes:
    """Known fault vocabulary (spec §4.3): framework-minted ``error_type`` codes
    under the reserved ``calf.`` prefix, plus the load-bearing ``details`` key
    ``build_safe`` writes. Shipped as constants so consumers and the framework
    never typo magic strings.

    Only the codes this PR can reference are declared; codes produced solely by
    the rail's synthesis (e.g. the exception-class key, ``details.reason`` values)
    land with their producer (PR-6), so no constant ships without a user.
    """

    # error_type codes
    MODEL_CONTEXT_WINDOW_EXCEEDED = "calf.model.context_window_exceeded"
    FAULT_GROUP = "calf.fault_group"
    UNHANDLED = "calf.unhandled"
    DELIVERY_REJECTED = "calf.delivery.rejected"
    DELIVERY_UNDECODABLE = "calf.delivery.undecodable"
    SLOT_MATERIALIZATION_FAILED = "calf.slot.materialization_failed"
    AGENT_SELF_RETRY_EXHAUSTED = "calf.agent.self_retry_exhausted"

    # details key: a non-silent breadcrumb recording what build_safe elided to
    # stay within the carriage budget. Maps to a small dict, e.g.
    # ``{"causes": 6, "frames": 6, "details_bytes": 20012}`` (only non-zero parts).
    ELIDED = "calf.elided"


class FrameRef(BaseModel):
    """Topology-only breadcrumb of one call frame (spec §4.3).

    Deliberately excludes input payloads/overrides: frame inputs may carry user
    data, so shipping them in every fault is a leak vector. With ``origin_payload``
    gone from the model, faults carry no user payloads by construction.
    """

    frame_id: str
    target_topic: str


class ErrorReport(BaseModel):
    """A terminal failure as a typed wire value (spec §4.3).

    Travels on the reply slot inside a ``FaultMessage``; the same value reaches
    seams (the ``fault`` argument), the client (``NodeFaultError.report``) and
    sinks (``ConsumerContext.fault``). Exception identity never crosses the wire —
    ``error_type`` (a dotted string code) is the contract, not a class name.
    """

    model_config = ConfigDict(extra="ignore")

    report_id: str = Field(default_factory=lambda: uuid_utils.uuid7().hex)
    """Framework-minted UUID7 at synthesis — stable across hops and mirrors; the
    dedup key for ops taps and the future DLT."""

    error_type: str
    """Dotted code, e.g. ``calf.model.context_window_exceeded``. An open string
    code, not a closed enum; consumers must tolerate unknown values."""

    message: str = ""
    """Human summary, clamped (never rejected) to keep inbound decode total."""

    retryable: bool = False
    """ADVISORY — consumers may act on it; the framework does not."""

    origin_node_id: str | None = None
    origin_frame_id: str | None = None
    frame_chain: list[FrameRef] = Field(default_factory=list)
    """Full chain topology at fault time (subject to the carriage budget)."""

    details: dict[str, Any] = Field(default_factory=dict)
    """Open, JSON-serializable extension slot."""

    causes: list[ErrorReport] = Field(default_factory=list)
    """Recursive — non-empty means this report is composed of other faults
    (a fault group, a deliberate conversion, or a recovery-then-failure)."""

    @field_validator("message", mode="before")
    @classmethod
    def _clamp_message(cls, v: Any) -> Any:
        """Clamp an over-long ``message`` rather than reject it.

        A rejecting constraint would poison inbound decode of an otherwise-valid
        report; a BEFORE-mode clamp keeps construction total on every path,
        including deserialization. Mirrors ``FailedToolCall``'s clamp discipline.
        """
        if isinstance(v, str) and len(v) > _MAX_MESSAGE_CHARS:
            return v[:_MAX_MESSAGE_CHARS]
        return v

    def walk(self) -> Iterator[ErrorReport]:
        """Yield this report, then every nested cause (pre-order)."""
        yield self
        for cause in self.causes:
            yield from cause.walk()

    def find(self, error_type: str) -> ErrorReport | None:
        """The first report in ``walk()`` order matching ``error_type``, or ``None``.

        Use this rather than a bare ``error_type ==`` when faults may compose: a
        fan-out wraps unhandled siblings in a fault group, so a top-level equality
        check would silently stop matching the day an agent fans out (spec §4.4).
        """
        for report in self.walk():
            if report.error_type == error_type:
                return report
        return None

    def to_minimal(self) -> ErrorReport:
        """The strip-and-retry floor target (spec §4.3): identity only — no
        ``causes``, ``details``, or ``frame_chain`` — so an oversized fault can
        still be published instead of becoming a new silent drop. (The publish-time
        size check and retry loop that call this live with the rail, PR-6.)"""
        return ErrorReport(
            report_id=self.report_id,
            error_type=self.error_type,
            message=self.message,
            retryable=self.retryable,
            origin_node_id=self.origin_node_id,
            origin_frame_id=self.origin_frame_id,
        )

    @classmethod
    def build_safe(
        cls,
        *,
        error_type: str,
        message: str = "",
        retryable: bool = False,
        origin_node_id: str | None = None,
        origin_frame_id: str | None = None,
        frame_chain: list[FrameRef] | None = None,
        details: dict[str, Any] | None = None,
        causes: list[ErrorReport] | None = None,
    ) -> ErrorReport:
        """Synthesize a report that **never raises** (spec §4.3).

        The fault path must never itself raise — a fault that throws while being
        built re-opens the silent-drop hole this feature closes (mirrors
        ``FailedToolCall.build_safe``). Applies the per-field carriage bounds
        (``causes`` depth/total, ``frame_chain`` head+tail, ``details`` size),
        recording any elision under ``details[FaultTypes.ELIDED]`` so nothing is
        dropped silently. On any unexpected construction error, falls back to a
        minimal report keeping only the scalar identity that is safe to coerce.
        """
        try:
            budget = _CauseBudget()
            bounded_causes = _bound_cause_list(list(causes or []), depth=2, budget=budget)
            bounded_chain, frames_dropped = _bound_frame_chain(list(frame_chain or []))
            bounded_details, details_bytes_dropped = _bound_details(dict(details or {}))
            elided: dict[str, int] = {}
            if budget.dropped:
                elided["causes"] = budget.dropped
            if frames_dropped:
                elided["frames"] = frames_dropped
            if details_bytes_dropped:
                elided["details_bytes"] = details_bytes_dropped
            if elided:
                bounded_details = {**bounded_details, FaultTypes.ELIDED: elided}
            return cls(
                error_type=error_type,
                message=message,
                retryable=retryable,
                origin_node_id=origin_node_id,
                origin_frame_id=origin_frame_id,
                frame_chain=bounded_chain,
                details=bounded_details,
                causes=bounded_causes,
            )
        except Exception:
            return cls(
                error_type=error_type if isinstance(error_type, str) else FaultTypes.UNHANDLED,
                message=message if isinstance(message, str) else "",
                retryable=retryable if isinstance(retryable, bool) else False,
                origin_node_id=origin_node_id if isinstance(origin_node_id, str) else None,
                origin_frame_id=origin_frame_id if isinstance(origin_frame_id, str) else None,
            )


# ---------------------------------------------------------------------------
# Carriage-bound helpers (used by ErrorReport.build_safe)
# ---------------------------------------------------------------------------
class _CauseBudget:
    """Shared accounting across the recursive cause bounding: how many reports may
    still be kept (``remaining``) and how many were dropped (``dropped``)."""

    __slots__ = ("remaining", "dropped")

    def __init__(self) -> None:
        self.remaining = _MAX_CAUSES_TOTAL
        self.dropped = 0


def _count_tree(report: ErrorReport) -> int:
    """Total number of reports in a cause subtree, ``report`` included."""
    return 1 + sum(_count_tree(c) for c in report.causes)


def _bound_cause_list(causes: list[ErrorReport], *, depth: int, budget: _CauseBudget) -> list[ErrorReport]:
    """Copy ``causes`` keeping the tree within the depth (≤ 8 levels below the
    root) and total-count (≤ 64) budgets; everything dropped is tallied on
    ``budget.dropped`` so the caller can record it (never a silent drop)."""
    kept: list[ErrorReport] = []
    for cause in causes:
        if depth > _MAX_CAUSES_DEPTH or budget.remaining <= 0:
            budget.dropped += _count_tree(cause)
            continue
        budget.remaining -= 1
        kept.append(cause.model_copy(update={"causes": _bound_cause_list(cause.causes, depth=depth + 1, budget=budget)}))
    return kept


def _bound_frame_chain(chain: list[FrameRef]) -> tuple[list[FrameRef], int]:
    """Bound ``chain`` to ≤ 64 frames with head+tail elision (the ends carry the
    origin and the most-recent hop). Returns the bounded chain and the count
    dropped from the middle."""
    if len(chain) <= _MAX_FRAME_CHAIN:
        return list(chain), 0
    head = _MAX_FRAME_CHAIN // 2
    tail = _MAX_FRAME_CHAIN - head
    return [*chain[:head], *chain[-tail:]], len(chain) - _MAX_FRAME_CHAIN


def _bound_details(details: dict[str, Any]) -> tuple[dict[str, Any], int]:
    """Bound ``details`` to ≤ 16 KB serialized. If it does not fit (or cannot be
    serialized at all), drop it wholesale and return the dropped byte count so the
    caller can leave a breadcrumb. Returns ``(bounded_details, bytes_dropped)``."""
    try:
        size = len(pydantic_core.to_json(details))
    except Exception:
        # Unserializable (NodeFaultError checks this at mint; build_safe stays
        # total regardless): drop, marking the drop without a measurable size.
        return {}, -1
    if size <= _MAX_DETAILS_BYTES:
        return dict(details), 0
    return {}, size
