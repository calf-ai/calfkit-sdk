from typing import ClassVar

from pydantic import BaseModel, Field

from calfkit.models.reply import FaultMessage, ReturnMessage
from calfkit.models.session_context import SessionRunContext, WorkflowState


# ---------------------------------------------------------------------------
# Wire format (framework-internal)
# ---------------------------------------------------------------------------
class Envelope(BaseModel):
    """Wire format — framework internal. Carries routing metadata + developer context.

    Uses plain BaseModel so all fields are always serialized — no exclude_unset
    gotchas.
    """

    WIRE: ClassVar[str] = "envelope"
    """The ``x-calf-wire`` value for an Envelope body (spec §2.4) — the single source for both the
    outbound stamp (``nodes/base.py`` ``_headers`` + the client ingress) and the inbound
    ``wire_filter(Envelope)``. ``MessageKind`` (the business kind) and ``reply`` are unchanged."""

    context: SessionRunContext
    internal_workflow_state: WorkflowState = Field(description="The internal, framework-level state tracking workflow")
    reply: ReturnMessage | FaultMessage | None = Field(default=None, discriminator="kind")
    """Per-delivery response carriage (spec §4), discriminated on ``kind``: ``None`` on
    call-kind deliveries; a :class:`~calfkit.models.reply.ReturnMessage` on return-kind;
    a :class:`~calfkit.models.reply.FaultMessage` on fault-kind. Readers that only handle
    success (``output_parts``, ``project_output``, ``ConsumerContext``) guard on
    ``isinstance(reply, ReturnMessage)`` so a fault never makes them raise — the fault is
    floored at the producing hop, not re-derived at a reader."""
