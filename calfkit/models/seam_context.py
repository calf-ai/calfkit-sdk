"""The capability-scoped seam context handed to policy-seam handlers (spec §6.3).

``SeamContext`` is the façade the four seams (``before_node``/``after_node``/
``on_node_error``/``on_callee_error``) receive — deliberately *not* the full
``SessionRunContext`` (whose transport stamping, frame id, and resource plumbing
invite workflow-breaking mutation). It mirrors the in-house ``ConsumerContext``
idiom (ADK's ``CallbackContext`` vs ``InvocationContext`` split). ``CalleeResult``
is the transport view of one resolved slot, with a convenience ``value`` projected
from its reply parts.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Generic

from pydantic import BaseModel, JsonValue

from calfkit._protocol import MessageKind
from calfkit._types import StateT
from calfkit.models.error_report import ErrorReport
from calfkit.models.marker import CallMarker
from calfkit.models.node_result import extract_lenient
from calfkit.models.payload import ContentPart

# The shared policy-seam return contract (spec §6.9): a substitute value the base coerces to reply
# ``parts`` (``_coerce_to_parts``), or ``None`` to decline. Its canonical home is the seam layer; the
# ``on_tool_error`` surface (``calfkit.nodes._tool_error``) imports it. ``None`` is formally in
# ``JsonValue`` but is always the decline gesture, never a substitute.
SeamReturn = ContentPart | list[ContentPart] | JsonValue | None


class CalleeResult(BaseModel):
    """The transport view of one callee slot (spec §6.3).

    Carried in :attr:`SeamContext.callee_results` (all resolved slots) and exposed
    as :attr:`SeamContext.failing_call` during ``on_callee_error``. ``value`` is a
    convenience projection of ``parts`` (raw ``parts`` kept alongside); ``fault`` is
    set when the slot failed (handled or not); ``handled`` is True once
    ``on_callee_error`` resolved it.
    """

    frame_id: str
    """The reply's ``in_reply_to`` — the framework slot key."""
    tag: str | None = None
    """The caller's correlation token, echoed back (§4.2; the agent's tool_call_id)."""
    target_topic: str | None = None
    """The callee's topic — sourced from the matched ``SlotRef`` for a fan-out sibling; ``None`` for a
    single (non-fan-out) call, where there is no slot registration to source it from (decision 5)."""
    marker: CallMarker | None = None
    """The echoed :class:`~calfkit.models.marker.CallMarker` of the failing slot (echo-rail spec D7),
    populated on the ``on_callee_error`` fault arm from ``reply.marker``. ``resolve_failing_tool_call``
    reads it (carriage-first) to reconstruct the failing tool's identity — for the ``message_agent`` peer
    arm, whose reply state is foreign. ``None`` for a slot answering an unstamped call."""
    parts: list[ContentPart] | None = None
    """Raw reply content (a return, or a handled substitute); ``None`` on a fault."""
    fault: ErrorReport | None = None
    """Set when this slot faulted (handled or not)."""
    handled: bool = False
    """True if ``on_callee_error`` resolved this slot with a substitute."""

    @property
    def value(self) -> Any:
        """Convenience view of ``parts``: ``DataPart.data`` first, then
        ``TextPart.text``, else ``None`` (spec §6.3) — the shared lenient extraction
        (``node_result.extract_lenient``), so a seam handler reads a callee's output
        without a strict projection that could raise."""
        return extract_lenient(self.parts)


@dataclass
class SeamContext(Generic[StateT]):
    """The capability-scoped context handed to every policy-seam handler (spec §6.3).

    Deliberately **not** frozen and **not** the full ``SessionRunContext``: ``state``
    is the official input-transform channel (mutate in place, return ``None``), and
    the framework sets the per-stage fields (``failing_call`` during ``on_callee_error``,
    ``exception`` during ``on_node_error``) on the live object. Withheld by design:
    workflow state / the call stack, frame mutation, and transport stamping.
    """

    state: StateT
    """The app state — MUTABLE; the official input-transform channel."""
    deps: Mapping[str, Any]
    """User-provided dependencies (read-only by type)."""
    resources: Mapping[str, Any]
    """The node's lifecycle-managed resources (read-only by type)."""
    payload: Any | None
    """The inbound frame payload (a tool sees its ``ToolCallRef``); read-only."""
    node_id: str
    correlation_id: str
    emitter_node_id: str | None
    """The immediate hop sender (``x-calf-emitter``)."""
    route: str | None
    """Inbound route key — set on call-kind ingress only (``None`` off-ingress)."""
    delivery_kind: MessageKind
    """``call`` | ``return`` | ``fault`` — distinguishes ingress from continuation/closure firings."""
    awaiting_reply: bool
    """True iff THIS node is the addressee of a reply-owing frame (the §10 hard-vs-soft-reject discriminator)."""
    callee_results: list[CalleeResult] = field(default_factory=list)
    """All resolved slots in dispatch order; ``[]`` on ingress."""
    failing_call: CalleeResult | None = None
    """The slot being handled — set during ``on_callee_error`` ONLY, else ``None``."""
    exception: BaseException | None = None
    """The live in-process exception — set during ``on_node_error`` ONLY, never serialized."""

    @property
    def callee_result(self) -> CalleeResult | None:
        """Convenience for the single-call case: the one resolved slot, or ``None``
        when there is not exactly one (ingress, or a fan-out batch)."""
        return self.callee_results[0] if len(self.callee_results) == 1 else None
