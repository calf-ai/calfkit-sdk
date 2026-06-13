from pydantic import BaseModel, Field

from calfkit.models.reply import ReturnMessage
from calfkit.models.session_context import SessionRunContext, WorkflowState


# ---------------------------------------------------------------------------
# Wire format (framework-internal)
# ---------------------------------------------------------------------------
class Envelope(BaseModel):
    """Wire format — framework internal. Carries routing metadata + developer context.

    Uses plain BaseModel so all fields are always serialized — no exclude_unset
    gotchas.
    """

    context: SessionRunContext
    internal_workflow_state: WorkflowState = Field(description="The internal, framework-level state tracking workflow")
    reply: ReturnMessage | None = None
    """Per-delivery response carriage (spec §4). ``None`` on call-kind deliveries;
    a :class:`~calfkit.models.reply.ReturnMessage` on return-kind deliveries. PR-C
    widens this to ``ReturnMessage | FaultMessage`` with a ``kind`` discriminator."""
