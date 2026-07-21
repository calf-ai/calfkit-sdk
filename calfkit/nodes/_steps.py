"""Caller-side step emission — the fact vocabulary, the widened body return, and the hop ledger
(caller-side step-emission spec §3.1).

Three moving parts, all framework-internal (nothing here is re-exported from ``calfkit``):

- **``StepFact``** — the closed union of body-declared semantic facts (``Said`` / ``DeniedCall`` /
  ``HandedOff``). Frozen dataclasses of plain data: a fact structurally cannot carry or become a
  wire event. The fact → step translation table lives ONLY in the ledger.
- **``Observed(action, facts)``** — the widened body return (Writer-style pairing of the body's
  value with its telemetry log). Only framework node kinds construct it; the kernel unwraps it in
  ``_execute`` immediately after the body returns, BEFORE ``after_node`` — seams run on a plain
  ``NodeResult`` and never see facts (I13).
- **``HopStepLedger``** — the sole constructor and sole batch-holder of wire step events (I7). A
  local of ``BaseNodeDef._handle_delivery``, threaded per hop; dies with the hop (I2).
  Capability-sealed: it exposes only fact-/marker-shaped mint methods — no public ``append(event)``
  exists, so possession of a hand-built ``*Step`` is useless.

**Totality (spec §3.1, normative).** Mint methods and fact constructors run OUTSIDE the kernel's
best-effort wrap (``folded``/``fold_failed`` on the run's own rail inside ``_aggregate``; fact
construction inside the body; ``absorb`` at the ``_execute`` unwrap; ``note_dispatch`` at the
chokepoint). A raise there faults
— or, mid-batch, aborts — a run that should have proceeded: the one thing telemetry must never do.
Every field is therefore coerced at build (``DeniedCall`` clamps ``args`` and renders foreign
``reason_parts`` elements); a future field sourced from un-coerced data must preserve totality or
move under a guard. ``flush`` itself is TOTAL by construction — the log-and-drop guard (I1) lives
at the kernel's exit-flush call site, which also owns argument derivation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Generic, Literal, Protocol

import pydantic_core
from typing_extensions import assert_never

from calfkit._protocol import HDR_EMITTER, HDR_EMITTER_KIND, HDR_WIRE, NodeKind
from calfkit._types import StateT
from calfkit.keying import partition_key
from calfkit.models.actions import Call, NodeResult, ReturnCall
from calfkit.models.marker import ToolCallMarker
from calfkit.models.payload import ContentPart, DataPart, FilePart, TextPart, ToolCallPart, is_retry
from calfkit.models.step import AgentMessageStep, HandoffStep, StepEvent, StepMessage, ToolCallStep, ToolResultStep

_CONTENT_PART_TYPES = (TextPart, FilePart, DataPart, ToolCallPart)


@dataclass(frozen=True)
class Said:
    """The hop's model preamble: ``(TextPart(text=extracted),)`` from the ``step_preamble``
    extractor (spec §5.4) — never declared when the extractor yields nothing. The extractor is
    already total, so no build-time coercion is needed."""

    parts: tuple[ContentPart, ...]


@dataclass(frozen=True)
class DeniedCall:
    """A call the caller refused to dispatch — schema/args rejection, invalid ``message_agent``
    target, handoff losers and rejected handoffs (spec §5.1). Carries the identity triple INLINE
    (not a ``ToolCallMarker``: a denied call may hold raw, off-spec args the marker's validated
    dict typing would reject) plus the rejection content the model sees.

    Coerced at build (mint totality, spec §3.1): ``args`` outside ``str | dict | None`` clamp to
    their string form; a ``reason_parts`` element that is not a ``ContentPart`` renders to a
    ``TextPart`` via ``pydantic_core.to_json(..., fallback=str)`` — the mint's pydantic validation
    runs outside any guard, so a foreign element must never survive to raise there."""

    tool_call_id: str
    tool_name: str
    args: str | dict[str, Any] | None
    reason_parts: tuple[ContentPart, ...]

    def __post_init__(self) -> None:
        if not (self.args is None or isinstance(self.args, (str, dict))):
            object.__setattr__(self, "args", str(self.args))
        if not all(isinstance(part, _CONTENT_PART_TYPES) for part in self.reason_parts):
            rendered = tuple(
                part if isinstance(part, _CONTENT_PART_TYPES) else TextPart(text=pydantic_core.to_json(part, fallback=str).decode())
                for part in self.reason_parts
            )
            object.__setattr__(self, "reason_parts", rendered)


@dataclass(frozen=True)
class HandedOff:
    """The arbitration WINNER only (spec §7) — losers and rejections are ``DeniedCall`` facts. No
    pair: a handoff is a ``TailCall``, it pushes no frame, nothing ever unwinds (the frame-mirror
    rule, spec §6)."""

    target: str
    message: str


StepFact = Said | DeniedCall | HandedOff
"""The closed union of body-declared semantic facts (I8). Grows only here, framework-owned."""


@dataclass(frozen=True)
class Observed(Generic[StateT]):
    """The widened body return: the body's ``action`` paired with the semantic facts it observed
    (spec §3.1b). A bare ``NodeResult`` is permitted everywhere and carries no facts — every user
    handler, every tool body, every seam substitute stays fact-free by construction."""

    action: NodeResult[StateT]
    facts: tuple[StepFact, ...] = ()


class _StepPublisher(Protocol):
    """The ledger's one broker touchpoint, duck-typed (unit fakes; the kernel passes the real
    ``KafkaBroker``)."""

    async def publish(self, message: Any, *, topic: str, correlation_id: str, key: bytes, headers: dict[str, str]) -> Any: ...


@dataclass
class HopStepLedger:
    """The per-hop mint and batch-holder (spec §3.1c). Created per delivery in
    ``_handle_delivery``, threaded as a framework-internal parameter, flushed once at every hop
    exit (I6), dies with the hop (I2).

    ``outcome`` is never a caller-supplied argument — each value has exactly one, type-distinct
    production site: ``folded`` derives from the retry marker, ``fold_failed`` and the
    ``DeniedCall`` translation hard-code. A wrong outcome is unrepresentable (I9 made structural).
    """

    _events: list[StepEvent] = field(default_factory=list, init=False)  # not injectable — the seal holds by construction

    def folded(self, marker: ToolCallMarker, parts: list[ContentPart]) -> None:
        """Mint the result step for a resolved marked reply: the resolved slot parts VERBATIM
        (post-``on_callee_error`` substitution, pre-materialization — I10); ``failed`` iff
        retry-marked, else ``success`` (spec §5.1 — a plain-value seam substitute reads success,
        deliberately: the stream mirrors the model's view)."""
        outcome: Literal["success", "failed"] = "failed" if is_retry(parts) else "success"
        self._events.append(ToolResultStep(tool_call_id=marker.tool_call_id, name=marker.tool_name, parts=parts, outcome=outcome))

    def fold_failed(self, marker: ToolCallMarker) -> None:
        """Mint the result step for an UNHANDLED marked fault: ``parts=[]``, ``failed``. No report
        parameter exists — the stream carries no raw error data (I10); forensics ride the fault
        rail's own publishes."""
        self._events.append(ToolResultStep(tool_call_id=marker.tool_call_id, name=marker.tool_name, parts=[], outcome="failed"))

    def absorb(self, facts: tuple[StepFact, ...]) -> None:
        """Translate body-declared facts into wire events — the normative fact table (spec §3.1a).
        A ``DeniedCall`` expands ATOMICALLY into its born-closed pair; a half-pair is
        unrepresentable."""
        for fact in facts:
            if isinstance(fact, Said):
                self._events.append(AgentMessageStep(parts=list(fact.parts)))
            elif isinstance(fact, DeniedCall):
                self._events.append(ToolCallStep(tool_call_id=fact.tool_call_id, name=fact.tool_name, args=fact.args))
                self._events.append(
                    ToolResultStep(tool_call_id=fact.tool_call_id, name=fact.tool_name, parts=list(fact.reason_parts), outcome="denied")
                )
            elif isinstance(fact, HandedOff):
                self._events.append(HandoffStep(target=fact.target, reason=fact.message))
            else:
                # Exhaustive over the closed union: a NEW fact member missing its translation arm is
                # a TYPE-CHECK failure here (spec §5.5's maintenance story), not a latent run fault
                # at the un-guarded unwrap.
                assert_never(fact)

    def note_dispatch(self, action: NodeResult[Any]) -> None:
        """Mint the call half for every OUTGOING marked ``Call`` (the pair law's dispatch side,
        I4): a bare marked ``Call`` → one mint; a fan-out list → one per marked element;
        ``TailCall`` / ``ReturnCall`` / unmarked calls → nothing (marker-gating scopes emission by
        data, not node type)."""
        calls = [action] if isinstance(action, Call) else action if isinstance(action, list) else []
        for call in calls:
            if isinstance(call, Call) and call.marker is not None:
                self._events.append(ToolCallStep(tool_call_id=call.marker.tool_call_id, name=call.marker.tool_name, args=call.marker.args))

    async def flush(
        self,
        broker: _StepPublisher | None,
        *,
        disposition: NodeResult[Any] | None,
        depth: int,
        frame_id: str,
        correlation_id: str,
        task_id: str,
        emitter: str,
        emitter_kind: NodeKind,
        root_callback: str | None,
    ) -> None:
        """Publish the hop's minted steps as ONE ``StepMessage`` (I6), identity stamped only here
        (I12). DRAINS unconditionally (L18j): a second flush on the same hop — e.g. the fault exit
        after a failed action publish — is an empty no-op, so at-most-one holds by drain. No-ops on
        an empty ledger or a rootless (fire-and-forget) run BEFORE touching the broker. The
        terminal gate (§3.4) drops ``agent_message`` events whenever ``disposition`` is a
        ``ReturnCall`` — any depth; a fault exit passes ``disposition=None``, so the gate does not
        fire. TOTAL by construction: no internal wrap — the kernel's exit-flush helper owns the
        log-and-drop guard (I1)."""
        events, self._events = self._events, []
        if not events or root_callback is None:
            return
        if isinstance(disposition, ReturnCall):
            events = [event for event in events if not isinstance(event, AgentMessageStep)]
            if not events:
                return
        if broker is None:  # a broker-less delivery flow (unit harnesses); total — clamp, don't raise
            return
        await broker.publish(
            StepMessage(correlation_id=correlation_id, emitter=emitter, depth=depth, frame_id=frame_id, events=events),
            topic=root_callback,
            correlation_id=correlation_id,
            key=partition_key(task_id),  # co-partitions with the terminal (task-keying cutover)
            # A step's OWN header dict — NOT _headers() (which stamps wire="envelope" + a business
            # x-calf-kind): a step is a side-effect-free notification, no kind. The x-calf-task
            # HEADER stamp on step messages is the durable PR's (keyed-but-headerless until then).
            headers={HDR_WIRE: StepMessage.WIRE, HDR_EMITTER: emitter, HDR_EMITTER_KIND: emitter_kind},
        )
