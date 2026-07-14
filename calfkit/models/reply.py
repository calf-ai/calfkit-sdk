from typing import Literal

from pydantic import BaseModel

from calfkit.models.error_report import ErrorReport
from calfkit.models.marker import CallMarker
from calfkit.models.payload import ContentPart


class _ReplyBase(BaseModel):
    """Common per-delivery reply correlation (spec §4.2).

    The shared parent of the two reply shapes: :class:`ReturnMessage` (a
    successful output, content as parts) and :class:`FaultMessage` (a terminal
    failure, an :class:`~calfkit.models.error_report.ErrorReport`) — ``result``
    XOR ``error``, the JSON-RPC split.

    The ``Envelope.reply`` slot now carries ``ReturnMessage | FaultMessage | None``,
    discriminated on ``kind`` (``Field(default=None, discriminator="kind")``). This PR
    is the rail that produces faults, so a fault shape rides the slot directly — the
    projection/dispatch side floors a fault at its producing hop rather than re-deriving
    it at a reader.
    """

    in_reply_to: str | None
    """``frame_id`` of the frame this reply answers, echoed by the framework at
    unwind. ``None`` only on frameless terminals (no caller frame to answer)."""

    tag: str | None
    """Echo of ``CallFrame.tag`` — the caller's opaque correlation token (the agent
    sets ``tool_call_id``). Transport metadata, never interpreted as content."""

    marker: CallMarker | None = None
    """Verbatim echo of the answered ``CallFrame.marker`` (echo-rail spec D3), minted from the
    frame at the two mints that answer a Call-pushed frame (the ``ReturnCall`` arm and the fault
    arm). Callee-opaque, like ``tag`` — never interpreted or rewritten by the callee. ``None`` on a
    reply answering an unstamped frame, and on the fan-out re-entry (a control signal that answers no
    Call, so it mints none). Decode-tolerant default (``None``) — old→new safe (D10)."""


class ReturnMessage(_ReplyBase):
    """A node's successful output, carried in the per-delivery reply slot.

    The content travels as the existing :data:`~calfkit.models.payload.ContentPart`
    vocabulary; the reader projects its concrete type (schema-on-read, spec §4.5).
    """

    kind: Literal["return"] = "return"
    parts: list[ContentPart]


class FaultMessage(_ReplyBase):
    """A terminal failure, carried in the per-delivery reply slot (spec §4.2).

    In PR-6 the same :class:`~calfkit.models.error_report.ErrorReport` reaches a
    caller's seam (the ``fault`` argument) and the broadcast mirror. Both
    fault-RECEPTION surfaces — the client raising ``NodeFaultError(report)`` on a
    ``kind=fault`` reply, and the consumer ``ConsumerContext`` fault/``delivery_kind``
    field — are DEFERRED to the reception PR and are not present here.
    """

    kind: Literal["fault"] = "fault"
    error: ErrorReport

    state_elided: bool = False
    """The oversized-fault degradation signal (state-elision spec D3): ``True`` means the run
    state this fault would normally carry (``context.state``/``deps``, frame payloads/overrides,
    workflow metadata) was elided at some hop so the fault could fit the producer's size limit
    instead of being floored. Stamped ONLY by the producer chokepoint (``_publish_fault``): the
    lean rungs stamp ``True``, and a rung-1 publish re-stamps ``True`` when the inbound delivery
    being answered was itself a state-elided fault (the mirror carries that inbound's — elided —
    context, so an unmarked flag would lie on multi-hop escalation). It does NOT propagate
    through a fan-out close, where the durable basestate re-establishes genuine state. An empty
    ``State`` is legitimate on many deliveries, so absence-of-state cannot signal elision — this
    field is what preserves the (deliberately undecided) receiver-policy choice without a wire
    migration. Decode-tolerant default: an old producer's fault reads as not-elided."""
