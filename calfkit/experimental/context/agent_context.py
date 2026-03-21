from typing_extensions import TypeAliasType

from calfkit.experimental._types import AgentDepsT
from calfkit.experimental.context.session_context import BaseSessionRunContext
from calfkit.experimental.data_model.state_deps import Deps, State

AgentSessionRunContext = TypeAliasType(
    "AgentSessionRunContext",
    BaseSessionRunContext[State, Deps[AgentDepsT]],
    type_params=(AgentDepsT,),
)
"""Developer-facing context for a session.
Generic on dependency as it is developer supplied type."""
