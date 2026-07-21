import logging
from typing import Annotated, Any, Generic

from pydantic import BaseModel, ConfigDict, Discriminator, Field, Tag
from typing_extensions import TypeVar

from calfkit._vendor.pydantic_ai.exceptions import ModelRetry
from calfkit._vendor.pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    RetryPromptPart,
    ToolCallPart,
    ToolReturn,
)


class BaseAgentActivityState(BaseModel):
    model_config = ConfigDict(extra="ignore")


class CoreMessageState(BaseAgentActivityState):
    """The state for committed messages and structured objects"""

    model_config = ConfigDict(extra="ignore")
    uncommitted_message: ModelMessage | None = None
    message_history: list[ModelMessage] = Field(default_factory=list, description="Append-only message history list")

    temp_instructions: str | None = None

    def latest_tool_calls(self) -> list[ToolCallPart]:
        pending_tool_calls = list()
        for msg in reversed(self.message_history):
            if isinstance(msg, ModelRequest):
                break
            elif isinstance(msg.tool_calls, list):
                pending_tool_calls.extend(msg.tool_calls)
        return pending_tool_calls

    def extend_with_responses(self, messages: list[ModelMessage], author: str) -> None:
        """Append run-produced messages, stamping author identity on un-named responses.

        Every ``ModelResponse`` in ``messages`` whose ``name`` is still ``None`` is
        tagged with ``author`` (the producing agent's id) before the messages are
        appended to the canonical ``message_history``. The ``if m.name is None``
        guard makes this idempotent — re-stamping an already-stamped list is a no-op
        (no current pydantic-ai provider sets ``ModelResponse.name``, so any non-None
        name here is one calfkit already applied; §4). ``ModelRequest``s are untouched.
        """
        for m in messages:
            if isinstance(m, ModelResponse) and m.name is None:
                m.name = author
        self.message_history.extend(messages)

    def stage_message(self, message: ModelMessage) -> None:
        self.uncommitted_message = message

    def commit_message_to_history(self) -> None:
        if self.uncommitted_message is None:
            err_msg = "The staged message(uncommitted_message) is None, can't be committed to history."
            logging.error(err_msg)
            raise RuntimeError(err_msg)
        self.message_history.append(self.uncommitted_message)
        self.uncommitted_message = None


def _calf_tool_result_discriminator(x: Any) -> str | None:
    """Tag extractor for the ``CalfToolResult`` discriminated union.

    Mirrors ``calfkit._vendor.pydantic_ai.tools._deferred_tool_call_result_discriminator``
    (the union holds only pydantic-ai's own result types now — the rail's carriage switch
    retired the ``FailedToolCall`` marker, §6.9). Returns only string tags; non-string values
    are treated as missing so the union falls through to the ``Any`` arm rather than silently
    misrouting. Keep this in sync with the upstream helper if a new pydantic-ai tag is added.
    """
    for key in ("kind", "part_kind"):
        v = x.get(key) if isinstance(x, dict) else getattr(x, key, None)
        if isinstance(v, str):
            return v
    return None


CalfToolResult = Annotated[
    Annotated[ToolReturn, Tag("tool-return")] | Annotated[ModelRetry, Tag("model-retry")] | Annotated[RetryPromptPart, Tag("retry-prompt")],
    Discriminator(_calf_tool_result_discriminator),
]
"""Calfkit-side tagged union for values stored in ``State.tool_results`` — the agent's PRIVATE
conversation-assembly record (I4), no longer an inter-node wire protocol (the carriage switch).

Flattens pydantic-ai's ``DeferredToolCallResult`` (``ToolReturn`` / ``ModelRetry`` /
``RetryPromptPart``). Flattening (rather than re-using ``DeferredToolCallResult``) is required
because Pydantic's discriminated-union machinery does not compose nested ``Discriminator``
annotations reliably. These come from the agent's own in-process writes: a slot's materialized
``ToolReturn`` (§6.9) and the agent's validation/``calf.retry`` ``RetryPromptPart``s.
"""


class InFlightToolsState(BaseAgentActivityState):
    """State of all in-progress tool calls and results.
    Tool calls and results are in-progress when all tool calls have not been completed and committed to message history."""  # noqa: E501

    model_config = ConfigDict(extra="ignore")

    tool_calls: dict[str, ToolCallPart] = Field(default_factory=dict)

    tool_results: dict[str, CalfToolResult | Any] = Field(default_factory=dict)

    def add_tool_call(self, tool_call: ToolCallPart) -> None:
        self.tool_calls[tool_call.tool_call_id] = tool_call

    def add_tool_result(self, tool_call_id: str, tool_result: CalfToolResult | Any) -> None:
        self.tool_results[tool_call_id] = tool_result

    def get_tool_result(self, tool_call_id: str) -> CalfToolResult | Any | None:
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


AgentStateSubsetT = TypeVar("AgentStateSubsetT", CoreMessageState, InFlightToolsState)

PartialState = Annotated[CoreMessageState | InFlightToolsState | Any, Discriminator("kind")]


class NodeConsumeState(BaseModel, Generic[AgentStateSubsetT]):
    """Used to partially consume a subset of the complete agent state"""

    model_config = ConfigDict(extra="ignore")
    run_state: AgentStateSubsetT
