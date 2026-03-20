from typing import Annotated, Any, Generic

from pydantic import BaseModel, ConfigDict, Discriminator, Field
from typing_extensions import TypeVar

from calfkit._vendor.pydantic_ai.messages import ModelMessage, ToolCallPart
from calfkit._vendor.pydantic_ai.tools import DeferredToolCallResult as ToolCallResult
from calfkit.experimental.payload_model import Payload

DataT = TypeVar("DataT", default=dict[str, Any])

AgentOutputT = TypeVar("AgentOutputT", default=Any)

AgentInputT = TypeVar("AgentInputT", default=Any)

AgentDepsT = TypeVar("AgentDepsT", default=Any)


class WorkflowState(BaseModel):
    """The current control state for the routing within the workflow."""

    model_config = ConfigDict(extra="ignore")
    metadata: Any = Field(
        default=None,
        description="Additional data that can be accessed programmatically by the application.",
    )


class BaseAgentActivityState(BaseModel):
    model_config = ConfigDict(extra="ignore")


class CoreMessageState(BaseAgentActivityState):
    """The state for committed messages and objects"""

    model_config = ConfigDict(extra="ignore")
    message_history: list[ModelMessage] = Field(
        default_factory=list, description="Append-only message history list"
    )
    todo_stack: list[Payload] = Field(
        default_factory=list,
        description=(
            "Stack of agents' completed actions, executions, objects. "
            "When an agent finishes its turn, a new payload is produced and pushed to this stack."
        ),
    )


class InFlightToolsState(BaseAgentActivityState):
    """State of all in-progress tool calls and results.
    Tool calls and results are in-progress when all tool calls have not been completed and committed to message history."""  # noqa: E501

    model_config = ConfigDict(extra="ignore")

    # Map of tool call IDs to tool results
    tool_results: dict[str, ToolCallResult | Any] = Field(default_factory=dict)

    # Map of tool call IDS to tool calls
    tool_calls: dict[str, ToolCallPart] = Field(default_factory=dict)

    @property
    def all_calls_completed(self) -> bool:
        for tool_call_id in self.tool_calls:
            if tool_call_id not in self.tool_results:
                return False
        return True


class AgentActivityState(CoreMessageState, InFlightToolsState):
    """A flat unified state tracking all current states within an agent's loop.
    Inheritance is ordered by priority, so important state fields take precedence."""

    model_config = ConfigDict(extra="ignore")
    metadata: Any = Field(
        default=None,
        description="Additional data that can be accessed programmatically by the application but is not sent to the LLM.",  # noqa: E501
    )


class State(BaseModel):
    """mutable data model to track state of an execution among one or more agents,
    sharing correlation id"""

    model_config = ConfigDict(extra="ignore")
    workflow_state: WorkflowState = Field(default_factory=WorkflowState)
    run_state: AgentActivityState = Field(default_factory=AgentActivityState)


AgentStateSubsetT = TypeVar("AgentStateSubsetT", CoreMessageState, InFlightToolsState)

PartialState = Annotated[CoreMessageState | InFlightToolsState | Any, Discriminator("kind")]


class NodeConsumeState(BaseModel, Generic[AgentStateSubsetT]):
    """Used to partially consume a subset of the complete agent state"""

    model_config = ConfigDict(extra="ignore")
    run_state: AgentStateSubsetT


class Deps(BaseModel, Generic[AgentDepsT]):
    """immutable dependencies for agent executions"""

    model_config = ConfigDict(extra="ignore", frozen=True)
    correlation_id: str
    agent_deps: AgentDepsT = Field(description="user-provided agent dependencies")
    metadata: Any = Field(
        default=None,
        description="Additional dependency metadata.",
    )
