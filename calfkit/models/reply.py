from typing import Literal

from pydantic import BaseModel

from calfkit.models.error_report import ErrorReport
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


class ReturnMessage(_ReplyBase):
    """A node's successful output, carried in the per-delivery reply slot.

    The content travels as the existing :data:`~calfkit.models.payload.ContentPart`
    vocabulary; the reader projects its concrete type (schema-on-read, spec §4.5).
    """

    kind: Literal["return"] = "return"
    parts: list[ContentPart]


class FaultMessage(_ReplyBase):
    """A terminal failure, carried in the per-delivery reply slot (spec §4.2).

    The same :class:`~calfkit.models.error_report.ErrorReport` reaches every
    fault-aware surface this PR produces — a caller's seam (the ``fault``
    argument) and the client (``NodeFaultError.report``). The consumer
    fault-reception surface (a ``ConsumerContext`` fault/``delivery_kind``
    field) is DEFERRED to the reception PR and is not present here.
    """

    kind: Literal["fault"] = "fault"
    error: ErrorReport
