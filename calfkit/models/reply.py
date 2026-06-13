from typing import Literal

from pydantic import BaseModel

from calfkit.models.payload import ContentPart


class _ReplyBase(BaseModel):
    """Common per-delivery reply correlation (spec §4.2).

    Shipped now so PR-C's ``FaultMessage`` is a pure sibling addition
    (``class FaultMessage(_ReplyBase)``) rather than a re-parenting of
    :class:`ReturnMessage`, and the ``Envelope.reply`` union widens to
    ``ReturnMessage | FaultMessage`` with ``Field(discriminator="kind")``.
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
