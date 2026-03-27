from typing_extensions import TypeAliasType

from calfkit.experimental._types import AgentDepsT
from calfkit.experimental.base_models.session_context import SessionRunContext

AgentSessionRunContext = TypeAliasType(
    "AgentSessionRunContext",
    SessionRunContext,
    type_params=(AgentDepsT,),
)
"""Developer-facing context for a session.
Generic on dependency as it is developer supplied type."""
