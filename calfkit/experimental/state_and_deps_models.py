from typing import Any, Generic

from pydantic import BaseModel, ConfigDict, Field
from typing_extensions import TypeVar

from calfkit._vendor.pydantic_ai.messages import ModelMessage
from calfkit.experimental.payload_model import Payload

DataT = TypeVar("DataT", default=dict[str, Any])

AgentOutputT = TypeVar("AgentOutputT", default=Any)

AgentInputT = TypeVar("AgentInputT", default=Any)

AgentDepsT = TypeVar("AgentDepsT", default=Any)


class State(BaseModel):
    """mutable data model to track state of an execution among one or more agents,
    sharing correlation id"""

    model_config = ConfigDict(extra="ignore")
    todo_stack: list[Payload] = Field(
        default_factory=list,
        description=(
            "Stack of agents' completed actions, executions, objects. "
            "When an agent finishes its turn, a new payload is produced and pushed to this stack."
        ),
    )
    message_history: list[ModelMessage] = Field(
        default_factory=list, description="Append-only message history list"
    )

    # Map of tool call IDs to tool return results
    uncommited_tool_results: dict[str, Any] | None = None


class Deps(BaseModel, Generic[AgentDepsT]):
    """immutable dependencies for agent executions"""

    model_config = ConfigDict(extra="ignore", frozen=True)
    correlation_id: str
    agent_deps: AgentDepsT  # user-provided agent dependencies
