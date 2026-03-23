from collections.abc import Sequence
from typing import Any, Generic

from pydantic import BaseModel, Field

from calfkit.experimental._types import DepsT, StateT
from calfkit.experimental.base_models.session_context import BaseSessionRunContext, WorkflowState


# ---------------------------------------------------------------------------
# Wire format (framework-internal)
# ---------------------------------------------------------------------------
class Envelope(BaseModel, Generic[StateT, DepsT]):
    """Wire format — framework internal. Carries routing metadata + developer context.

    Uses plain BaseModel (not CompactBaseModel) so all fields are always
    serialized — no exclude_unset gotchas with reply_stack.
    """

    context: BaseSessionRunContext[StateT, DepsT]
    internal_workflow_state: WorkflowState = Field(
        description="The internal, framework-level state tracking workflow"
    )
