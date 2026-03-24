from typing import Generic

from pydantic import BaseModel, Field

from calfkit.experimental._types import DepsT
from calfkit.experimental.base_models.session_context import SessionRunContext, WorkflowState


# ---------------------------------------------------------------------------
# Wire format (framework-internal)
# ---------------------------------------------------------------------------
class BaseEnvelope(BaseModel, Generic[DepsT]):
    """Wire format — framework internal. Carries routing metadata + developer context.

    Uses plain BaseModel (not CompactBaseModel) so all fields are always
    serialized — no exclude_unset gotchas with reply_stack.
    """

    context: SessionRunContext[DepsT]
    internal_workflow_state: WorkflowState = Field(description="The internal, framework-level state tracking workflow")


Envelope = BaseEnvelope[DepsT]
