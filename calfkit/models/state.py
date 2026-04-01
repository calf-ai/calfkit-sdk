import logging
from dataclasses import dataclass, field
from typing import Annotated, Any, Generic

from pydantic import BaseModel, ConfigDict, Discriminator, Field
from typing_extensions import TypeVar

from calfkit._vendor.pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ToolCallPart,
)
from calfkit._vendor.pydantic_ai.tools import DeferredToolCallResult as ToolCallResult
from calfkit.models.payload import ContentPart


class BaseAgentActivityState(BaseModel):
    model_config = ConfigDict(extra="ignore")


class CoreMessageState(BaseAgentActivityState):
    """The state for committed messages and structured objects"""

    model_config = ConfigDict(extra="ignore")
    uncommitted_message: ModelMessage | None = None
    message_history: list[ModelMessage] = Field(default_factory=list, description="Append-only message history list")
    final_output_parts: list[ContentPart] = Field(default_factory=list)

    temp_instructions: str | None = None

    def latest_tool_calls(self) -> list[ToolCallPart]:
        pending_tool_calls = list()
        for msg in reversed(self.message_history):
            if isinstance(msg, ModelRequest):
                break
            elif isinstance(msg.tool_calls, list):
                pending_tool_calls.extend(msg.tool_calls)
        return pending_tool_calls

    def stage_message(self, message: ModelMessage) -> None:
        self.uncommitted_message = message

    def commit_message_to_history(self) -> None:
        if self.uncommitted_message is None:
            err_msg = "The staged message(uncommitted_message) is None, can't be committed to history."
            logging.error(err_msg)
            raise RuntimeError(err_msg)
        self.message_history.append(self.uncommitted_message)
        self.uncommitted_message = None


class InFlightToolsState(BaseAgentActivityState):
    """State of all in-progress tool calls and results.
    Tool calls and results are in-progress when all tool calls have not been completed and committed to message history."""  # noqa: E501

    model_config = ConfigDict(extra="ignore")

    # Map of tool call IDS to tool calls
    tool_calls: dict[str, ToolCallPart] = Field(default_factory=dict)

    # Map of tool call IDs to tool results
    tool_results: dict[str, ToolCallResult | Any] = Field(default_factory=dict)

    def add_tool_call(self, tool_call: ToolCallPart) -> None:
        self.tool_calls[tool_call.tool_call_id] = tool_call

    def add_tool_result(self, tool_call_id: str, tool_result: ToolCallResult | Any) -> None:
        self.tool_results[tool_call_id] = tool_result

    def get_tool_call(self, tool_call_id: str) -> ToolCallPart | None:
        return self.tool_calls.get(tool_call_id)

    def get_tool_result(self, tool_call_id: str) -> ToolCallResult | Any | None:
        return self.tool_results.get(tool_call_id)

    def all_call_ids_complete(self, *call_ids: str) -> bool:
        for call_id in call_ids:
            _ = self.tool_calls[call_id]
            if call_id not in self.tool_results:
                return False
        return True


class State(CoreMessageState, InFlightToolsState):
    """A flat unified state tracking all current states within an agent's loop.
    Inheritance is ordered by priority, so important state fields take precedence."""

    model_config = ConfigDict(extra="ignore")
    metadata: Any = Field(
        default=None,
        description="Additional data that can be accessed programmatically by the application but is not sent to the LLM.",  # noqa: E501
    )


# class State(BaseModel):
#     """mutable data model to track state of an execution among one or more agents,
#     sharing correlation id"""

#     model_config = ConfigDict(extra="ignore")
#     run_state: AgentActivityState = Field(default_factory=AgentActivityState)


# ---------------------------------------------------------------------------
# Experimental PartialState implementation
# ---------------------------------------------------------------------------
AgentStateSubsetT = TypeVar("AgentStateSubsetT", CoreMessageState, InFlightToolsState)

PartialState = Annotated[CoreMessageState | InFlightToolsState | Any, Discriminator("kind")]


class NodeConsumeState(BaseModel, Generic[AgentStateSubsetT]):
    """Used to partially consume a subset of the complete agent state"""

    model_config = ConfigDict(extra="ignore")
    run_state: AgentStateSubsetT


@dataclass
class PendingToolBatch:
    """Tracks an in-flight parallel tool call batch for one correlation chain."""

    expected_tool_call_ids: frozenset[str]

    # State snapshot at fan-out time, with tool_calls registered
    base_state: State

    # map of tool call IDs to tool call results
    collected_results: dict[str, ToolCallResult | Any] = field(default_factory=dict)

    @property
    def is_complete(self) -> bool:
        return self.expected_tool_call_ids == frozenset(self.collected_results.keys())
