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
# The details field budget covers user content AND the ELIDED breadcrumb appended
# afterwards; reserve headroom for the breadcrumb so it can never push the field
# over budget (the breadcrumb is a few small int/bool keys, well under the reserve).
_ELIDED_RESERVE_BYTES = 512
# Framework-reserved prefix for details keys (mirrors the error_type reservation),
# so a consumer can trust a calf.* details key was written by the framework.
_CALF_PREFIX = "calf."


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
    # A fan-out batch could not complete (a node-own infra failure mid-batch, not a callee fault): the
    # durable store died, its basestate was missing, a sibling/re-entry publish failed, or the node's
    # own work raised while slots were outstanding. The batch is tombstoned and the caller faulted ONCE
    # (in-node spec §4.4). ``details.reason`` ∈ {store_unavailable, basestate_missing, reentry_failed,
    # dispatch_failed}; a node-own raise mid-batch escalates the exception itself (``calf.unhandled``).
    FANOUT_ABORTED = "calf.fanout.aborted"
    REASON_STORE_UNAVAILABLE = "store_unavailable"
    REASON_BASESTATE_MISSING = "basestate_missing"
    REASON_REENTRY_FAILED = "reentry_failed"
    REASON_DISPATCH_FAILED = "dispatch_failed"

    # details key: a non-silent breadcrumb recording what build_safe elided to stay
    # within the carriage budget. Maps to a small dict carrying only the parts that
    # applied: ``causes`` (int dropped), ``frames`` (int dropped), ``details_bytes``
    # (int, oversized details dropped wholesale), ``details_unserializable`` (True),
    # and ``fallback`` (the exception class name, when build_safe's last-resort arm
    # dropped causes/frame_chain/details after an unexpected construction error).
    ELIDED = "calf.elided"

    # details key: the breadcrumb the on_node_error chain runner records when a
    # recovery handler *accidentally* raises (spec §6.5 / scenario 9). The handler
    # is treated as declining, but its failure is noted here — a list of
    # ``{"handler", "exc_type", "exc_message"}`` dicts — so the escalating original
    # fault carries the trace of every recovery attempt that itself broke.
    SEAM_ERRORS = "calf.seam_errors"

    # details key: the exception class name :meth:`ErrorReport.from_exception` records
    # when synthesizing a ``calf.unhandled`` fault from an arbitrary exception (spec
    # §6.7) — exception identity does not cross the wire, but the class name is a useful
    # forensic breadcrumb in ``details``.
    EXCEPTION_TYPE = "calf.exception_type"

    # details key + values: why a reply-owing delivery declined its body, written by the route
    # dispatcher (§10) and carried on the ``DELIVERY_REJECTED`` auto-fault so a consumer can branch
    # without typing a magic string. ``REASON`` is a plain (non-``calf.``) key by design — it is the
    # framework's own field on a framework-minted report (the spec writes ``details.reason``).
    REASON = "reason"
    REASON_SCHEMA_REJECTED = "schema_rejected"
    REASON_ALL_DECLINED = "all_declined"

    # details key: the per-slot fan-out topology a closing batch's fault group carries (spec §4.4/§7) —
    # a list of ``{tag, target_topic, status}`` plus ``ok``/``failed`` counts, so partial-success
    # visibility survives without shipping the success VALUES (lean faults, §4.3 leak posture). On a
    # SINGLETON-flattened group it is copied onto the bare child's ``details``. Framework-written, so it
    # is a reserved ``calf.`` key (set after ``build_safe``, which strips caller-supplied ``calf.*``).
    FANOUT_TOPOLOGY = "calf.fanout_topology"


class FrameRef(BaseModel):
    """Topology-only breadcrumb of one call frame (spec §4.3).

    Deliberately excludes input payloads/overrides: frame inputs may carry user
    data, so shipping them in every fault is a leak vector. With ``origin_payload``
    gone from the model, faults carry no user payloads by construction.
    """

    model_config = ConfigDict(frozen=True)

    frame_id: str
    target_topic: str


class ErrorReport(BaseModel):
    """A terminal failure as a typed wire value (spec §4.3).

    Travels on the reply slot inside a ``FaultMessage``; in PR-6 the value reaches
    the seam ``fault`` argument and the broadcast mirror, and is read at a *mint*
    via ``NodeFaultError.report``. Both fault-RECEPTION surfaces — the client
    raising ``NodeFaultError(report)`` on a ``kind=fault`` reply, and the consumer
    ``ConsumerContext`` fault/``delivery_kind`` field — are DEFERRED to the reception
    PR and are not present here. Exception identity never crosses the wire —
    ``error_type`` (a dotted string code) is the contract, not a class name.

    Frozen: the report travels and is read at many surfaces (a seam's ``fault``
    arg, a mint's ``NodeFaultError.report``, the broadcast mirror, and — once the
    deferred reception surfaces land — the client/consumer reads) and, once the
    rail escalates it, passes up a frame chain untouched.
    Freezing makes the "stable across hops" promise on ``report_id`` real and
    matches the codebase's frozen wire values (e.g. ``CallFrame``).
    The freeze is **shallow** — field *reassignment* is blocked, but the
    ``causes``/``details``/``frame_chain`` containers remain mutable in place, so
    transform a report you've handed off with ``model_copy(update=...)``, never by
    mutating a container (e.g. the rail's batch-closure flatten copies metadata
    onto a child's ``details`` via ``model_copy``).
    Bounds note: the carriage budgets are applied by :meth:`build_safe` at
    synthesis; the plain constructor (and inbound decode) are NOT re-bounded.
    """

    model_config = ConfigDict(extra="ignore", frozen=True)

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
        """Coerce a non-str ``message`` to str, then clamp it — never reject.

        A rejecting constraint would poison inbound decode of an otherwise-valid
        report; a BEFORE-mode clamp keeps construction total on every path,
        including deserialization. The clamp-don't-reject discipline keeps the error path total.
        A non-str scalar (e.g. an int the rail passed by mistake) is coerced via ``str(v)``
        instead of falling through to the ``message: str`` constraint, whose rejection would
        otherwise drop the WHOLE ``build_safe`` into its last-resort fallback — discarding the
        report's causes and frame_chain. Only the offending scalar is normalized.
        """
        if not isinstance(v, str):
            v = str(v)
        if len(v) > _MAX_MESSAGE_CHARS:
            return v[:_MAX_MESSAGE_CHARS]
        return v

    @field_validator("error_type", mode="before")
    @classmethod
    def _coerce_error_type(cls, v: Any) -> Any:
        """Coerce a non-str ``error_type`` to str rather than reject it.

        Mirrors :meth:`_clamp_message`: a non-str ``error_type`` would otherwise hit the
        ``error_type: str`` constraint and collapse ``build_safe`` into its last-resort
        fallback, which rewrites ``error_type`` to ``calf.unhandled`` and drops causes and
        frame_chain. Coercing the offending scalar keeps the primary build path total and
        preserves the original code's string form."""
        if not isinstance(v, str):
            return str(v)
        return v

    def walk(self) -> Iterator[ErrorReport]:
        """Yield this report, then every nested cause (pre-order).

        Iterative and cycle-guarded: a report off the wire is acyclic (JSON has no
        cycles), but the shallow freeze leaves ``causes`` mutable in place, so an
        in-process cycle (or a cause reached by two paths) is possible. An ``id()``-keyed
        visited set yields each report once and terminates, instead of recursing without
        bound — ``walk`` is the public traversal a consumer calls on a possibly
        attacker-influenced ``NodeFaultError.report``.
        """
        seen: set[int] = set()
        stack: list[ErrorReport] = [self]
        while stack:
            report = stack.pop()
            if id(report) in seen:
                continue
            seen.add(id(report))
            yield report
            stack.extend(reversed(report.causes))  # reversed ⇒ first cause pops next (pre-order)

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
        built re-opens the silent-drop hole this feature closes (the same
        build-safe discipline). Applies the per-field carriage bounds
        (``causes`` depth/total, ``frame_chain`` head+tail, ``details`` size),
        recording any elision under ``details[FaultTypes.ELIDED]`` so nothing is
        dropped silently. On any unexpected construction error, falls back to a
        minimal report keeping only the scalar identity that is safe to coerce.
        """
        try:
            budget = _CauseBudget()
            bounded_causes = _bound_cause_list(list(causes or []), depth=2, budget=budget)
            bounded_chain, frames_dropped = _bound_frame_chain(list(frame_chain or []))
            # calf.* details keys are framework-reserved; drop any a caller supplied
            # BEFORE bounding, so the framework's ELIDED breadcrumb stays authoritative
            # and a reserved key's size can never evict legitimate user keys.
            user_details = {k: v for k, v in dict(details or {}).items() if not k.startswith(_CALF_PREFIX)}
            bounded_details, details_bytes_dropped = _bound_details(user_details)
            elided: dict[str, Any] = {}
            if budget.dropped:
                elided["causes"] = budget.dropped
            if frames_dropped:
                elided["frames"] = frames_dropped
            if details_bytes_dropped is None:
                elided["details_unserializable"] = True
            elif details_bytes_dropped:
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
        except Exception as exc:
            # Last-resort total fallback: keep only the scalar identity that is safe
            # to coerce, and record the wholesale drop of causes/frame_chain/details
            # under ELIDED — the fallback must not itself become a silent drop. Use
            # the exception class name (never str(exc), which can raise) and avoid
            # importing safe_exc_message to keep this module calfkit-import-free.
            # ``type(x) is str`` (not isinstance): a str subclass with a hostile
            # ``__len__``/``__getitem__`` is what likely broke the primary path's
            # clamp, so re-passing it would re-break the fallback — coerce it out.
            return cls(
                error_type=error_type if type(error_type) is str else FaultTypes.UNHANDLED,
                message=message if type(message) is str else "",
                retryable=retryable if type(retryable) is bool else False,
                origin_node_id=origin_node_id if type(origin_node_id) is str else None,
                origin_frame_id=origin_frame_id if type(origin_frame_id) is str else None,
                details={FaultTypes.ELIDED: {"fallback": type(exc).__name__[:200]}},
            )

    @classmethod
    def from_exception(
        cls,
        exc: BaseException,
        *,
        node: Any = None,
        ctx: Any = None,
        cause: ErrorReport | None = None,
        frame_chain: list[FrameRef] | None = None,
        origin_frame_id: str | None = None,
    ) -> ErrorReport:
        """Synthesize a fault from an arbitrary exception (spec §6.7).

        A non-``NodeFaultError`` maps to ``error_type="calf.unhandled"`` with the
        exception's class name (``details[FaultTypes.EXCEPTION_TYPE]``) and a clamped
        message; ``build_safe`` keeps it total — the error path must never itself raise.
        ``node``/``ctx`` (optional, keyword) source the origin breadcrumb when present
        (``node.node_id`` / ``ctx.frame_id``); ``frame_chain`` and ``origin_frame_id`` capture the
        call-stack topology at synthesis (§4.3/§4.4, ADR-0003 — the traceback analog), passed
        explicitly by the rail from the pre-mutation stack snapshot since ``ctx`` carries no stack;
        ``cause`` chains a prior report (the §6.8 recovery-then-failure case). ``node``/``ctx`` are
        typed ``Any`` to keep this module calfkit-import-free (the same reason ``build_safe`` inlines
        its own coercions).

        A ``NodeFaultError`` is NOT routed here — the chokepoint converts it verbatim,
        bypassing ``on_node_error`` (the mint rule, §6.5).
        """
        report = cls.build_safe(
            error_type=FaultTypes.UNHANDLED,
            message=_safe_exc_str(exc),
            origin_node_id=getattr(node, "node_id", None),
            origin_frame_id=origin_frame_id if origin_frame_id is not None else getattr(ctx, "frame_id", None),
            frame_chain=frame_chain,
            causes=[cause] if cause is not None else None,
        )
        # The exception-class breadcrumb is a framework-reserved calf.* details key, so it
        # is set AFTER build_safe (which strips caller-supplied calf.* keys); the report is
        # freshly built and owned here, so the in-place add is safe.
        report.details[FaultTypes.EXCEPTION_TYPE] = type(exc).__name__[:200]
        return report


# ---------------------------------------------------------------------------
# Carriage-bound helpers (used by ErrorReport.build_safe)
# ---------------------------------------------------------------------------
class _CauseBudget:
    """Shared accounting across the cause bounding: how many reports may still be kept
    (``remaining``), how many were dropped (``dropped``), and the report ids already
    visited (``seen``) so the walk is cycle/DAG-safe and always terminates."""

    __slots__ = ("remaining", "dropped", "seen")

    def __init__(self) -> None:
        self.remaining = _MAX_CAUSES_TOTAL
        self.dropped = 0
        self.seen: set[int] = set()


def _bound_cause_list(causes: list[ErrorReport], *, depth: int, budget: _CauseBudget) -> list[ErrorReport]:
    """Copy ``causes`` within the depth (≤ 8 levels, root included) and total-count (≤ 64)
    budgets; everything dropped is tallied on ``budget.dropped`` so the caller records it
    (never a silent drop).

    The kept-build recursion is bounded by ``_MAX_CAUSES_DEPTH`` — it descends only while a
    node is *kept* (``depth <= _MAX_CAUSES_DEPTH``), so it cannot exhaust Python's recursion
    limit. A *dropped* subtree, by contrast, can be arbitrarily deep, so it is counted
    **iteratively** (:func:`_count_subtree`), never by recursion: otherwise a deep ``causes``
    chain (nested fault groups, §4.4) would ``RecursionError`` into ``build_safe``'s fallback
    and silently downgrade the precise ``causes=N`` breadcrumb to a bare ``fallback``.
    ``budget.seen`` (report ids) makes the whole walk cycle/DAG-safe — a report reached by
    more than one path, or a self-referential cycle (possible only via in-process mutation of
    the shallow-frozen ``causes``, never off the wire), is kept once and its re-occurrences
    counted — so the transform always terminates."""
    kept: list[ErrorReport] = []
    for cause in causes:
        if depth > _MAX_CAUSES_DEPTH or budget.remaining <= 0 or id(cause) in budget.seen:
            budget.dropped += _count_subtree(cause, budget)  # iterative, cycle-safe tally
            continue
        budget.seen.add(id(cause))
        budget.remaining -= 1
        kept.append(cause.model_copy(update={"causes": _bound_cause_list(cause.causes, depth=depth + 1, budget=budget)}))
    return kept


def _count_subtree(root: ErrorReport, budget: _CauseBudget) -> int:
    """Count every not-yet-seen report in ``root``'s subtree (root included), iteratively and
    cycle-safe, marking each on ``budget.seen``. The dropped-subtree tally behind the ``causes``
    elision breadcrumb — the true number of reports elided, however deep or cyclic the dropped
    input, with no recursion."""
    count = 0
    stack: list[ErrorReport] = [root]
    while stack:
        node = stack.pop()
        if id(node) in budget.seen:
            continue
        budget.seen.add(id(node))
        count += 1
        stack.extend(node.causes)
    return count


def _bound_frame_chain(chain: list[FrameRef]) -> tuple[list[FrameRef], int]:
    """Bound ``chain`` to ≤ 64 frames with head+tail elision (the ends carry the
    origin and the most-recent hop). Returns the bounded chain and the count
    dropped from the middle."""
    if len(chain) <= _MAX_FRAME_CHAIN:
        return list(chain), 0
    head = _MAX_FRAME_CHAIN // 2
    tail = _MAX_FRAME_CHAIN - head
    return [*chain[:head], *chain[-tail:]], len(chain) - _MAX_FRAME_CHAIN


def _bound_details(details: dict[str, Any]) -> tuple[dict[str, Any], int | None]:
    """Bound ``details`` to ≤ 16 KB serialized. Returns ``(bounded_details, dropped)``
    where ``dropped`` is the byte count dropped (``0`` if it fit), or ``None`` if the
    details could not be serialized at all (dropped, size unmeasurable). The caller
    turns each case into a distinct, non-magic breadcrumb."""
    try:
        size = len(pydantic_core.to_json(details))
    except Exception:
        # Unserializable. NodeFaultError checks this at mint, so this path is only
        # reachable when something else (e.g. the rail) calls build_safe directly;
        # build_safe stays total regardless.
        return {}, None
    # Keep user content within the field budget LESS the breadcrumb reserve, so the
    # ELIDED breadcrumb appended afterwards cannot push the field over budget.
    if size <= _MAX_DETAILS_BYTES - _ELIDED_RESERVE_BYTES:
        return dict(details), 0
    return {}, size


def _safe_exc_str(exc: BaseException) -> str:
    """Best-effort string of an exception, robust against a broken ``__str__``.

    A local twin of ``calfkit.exceptions.safe_exc_message`` — duplicated deliberately
    so this module stays calfkit-import-free (importing ``exceptions`` would be circular;
    ``exceptions`` imports ``ErrorReport``). Used only by :meth:`ErrorReport.from_exception`,
    whose contract is totality on the error path, so the ``str``/``repr`` calls are guarded.
    """
    try:
        return str(exc)
    except Exception:
        try:
            return repr(exc)
        except Exception:
            return f"<unprintable {type(exc).__name__}>"
