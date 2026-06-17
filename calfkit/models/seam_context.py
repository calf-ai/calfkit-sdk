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

from pydantic import BaseModel

from calfkit._protocol import MessageKind
from calfkit._types import StateT
from calfkit.models.error_report import ErrorReport
from calfkit.models.payload import ContentPart, DataPart, TextPart


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
    target_topic: str
    parts: list[ContentPart] | None = None
    """Raw reply content (a return, or a handled substitute); ``None`` on a fault."""
    fault: ErrorReport | None = None
    """Set when this slot faulted (handled or not)."""
    handled: bool = False
    """True if ``on_callee_error`` resolved this slot with a substitute."""

    @property
    def value(self) -> Any:
        """Convenience view of ``parts``: ``DataPart.data`` first, then
        ``TextPart.text``, else ``None`` (spec §6.3).

        The lenient mirror of ``node_result._extract_auto`` (which raises on no
        match) — a seam handler reads a callee's output without a strict
        projection. (Step 4 extracts a shared ``extract_lenient`` once the agent's
        ``_resolve_slot`` becomes the second consumer.)
        """
        parts = self.parts or []
        for part in parts:
            if isinstance(part, DataPart):
                return part.data
        for part in parts:
            if isinstance(part, TextPart):
                return part.text
        return None


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
