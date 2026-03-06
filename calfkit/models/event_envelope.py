from typing import Any, Generic

from pydantic import Field

from calfkit._vendor.pydantic_ai import ModelMessage
from calfkit._vendor.pydantic_ai.models import ModelRequestParameters
from calfkit.models.delegation import DelegationFrame
from calfkit.models.groupchat import GroupchatDataModel
from calfkit.models.types import (
    CompactBaseModel,
    PayloadT,
    ToolCallRequest,
)


class EnvelopeState(CompactBaseModel):
    """Shared mutable runtime state that persists across hops."""

    # Running message history
    message_history: list[ModelMessage] = Field(default_factory=list)

    # Uncommitted messages, often coming out from a node and not yet persisted in message history
    uncommitted_messages: list[ModelMessage] = Field(default_factory=list)

    # Pending tool calls to enforce sequential tool calling when thread_id
    # is not provided or when there is no memory history store configured
    pending_tool_calls: list[ToolCallRequest] = Field(default_factory=list)

    # Whether the current state of messages is the final response from the AI to the user
    final_response: bool = False

    # Stack of DelegationFrames tracking nested agent-to-agent delegations.
    # Each frame records the return address and tool-call metadata so that
    # the response can be routed back to the correct caller.
    delegation_stack: list[DelegationFrame] = Field(default_factory=list)

    # For holding groupchat data and config. Only to directly be accessed by the groupchat node.
    groupchat_data: GroupchatDataModel | None = None

    # Per-request session config; set by router from RouterPayload, persists across tool-call cycles
    instructions: str | None = None
    agent_name: str | None = None
    model_request_params: ModelRequestParameters | None = None

    @property
    def latest_message_in_history(self) -> ModelMessage | None:
        return self.message_history[-1] if self.message_history else None

    @property
    def is_groupchat(self) -> bool:
        return self.groupchat_data is not None

    @property
    def is_end_of_turn(self) -> bool:
        return self.final_response

    @property
    def has_uncommitted_messages(self) -> bool:
        """Check if there are uncommitted, unprocessed messages."""
        return bool(self.uncommitted_messages)

    def mark_as_end_of_turn(self) -> None:
        self.final_response = True

    def mark_as_start_of_turn(self) -> None:
        self.final_response = False

    def add_to_uncommitted_messages(self, message: ModelMessage) -> None:
        """Add message to uncommitted list when returning out of a node, if it exists.

        Args:
            message (ModelMessage): new message
        """
        self.uncommitted_messages.append(message)
        if self.groupchat_data is not None:
            self.groupchat_data.add_uncommitted_message_to_turn(message)

    def prepare_uncommitted_agent_messages(self, messages: list[ModelMessage]) -> None:
        """Prepare and set the agent-level uncommitted messages with provided messages.

        Args:
            messages (list[ModelMessage]): list of messages
        """
        self.uncommitted_messages = messages

    def pop_all_uncommited_agent_messages(self) -> list[ModelMessage]:
        """Clears the list of uncommitted agent-level messages and returns them"""
        messages = self.uncommitted_messages
        self.uncommitted_messages = []
        return messages

    def replace_uncommitted_with_turn_context(self) -> None:
        """Replace uncommitted messages with conversation context from the groupchat turns queue."""
        self.uncommitted_messages = (
            self.groupchat_data.flat_messages_from_turns_queue
            if self.groupchat_data is not None
            else []
        )

    def push_delegation_frame(self, frame: DelegationFrame) -> None:
        """Push a delegation frame onto the stack.

        Uses assignment (not in-place mutation) so that Pydantic's
        ``exclude_unset`` serialization includes the field after modification.
        """
        self.delegation_stack = [*self.delegation_stack, frame]

    def pop_delegation_frame(self) -> DelegationFrame:
        """Pop the top delegation frame from the stack.

        Uses assignment (not in-place mutation) so that Pydantic's
        ``exclude_unset`` serialization includes the field after modification.

        Raises:
            IndexError: If the delegation stack is empty.
        """
        if not self.delegation_stack:
            raise IndexError("Cannot pop from an empty delegation stack")
        *rest, frame = self.delegation_stack
        self.delegation_stack = rest
        return frame


class EventEnvelope(CompactBaseModel, Generic[PayloadT]):
    # Routing
    trace_id: str | None = None
    thread_id: str | None = None
    final_response_topic: str | None = None

    # Node-specific data (direction-agnostic, handler interprets)
    payload: PayloadT | None = None

    # User context — persists across payload transformations.
    # Must be JSON-serializable (e.g. dict, str, int, list) since the envelope
    # travels over the Kafka wire as JSON.
    deps: Any = None

    # Shared mutable runtime state
    state: EnvelopeState = Field(default_factory=EnvelopeState)

    def model_dump(self, **kwargs: Any) -> dict[str, Any]:
        """Override to always include ``state`` even when created via default_factory.

        Pydantic v2's ``exclude_unset=True`` drops fields populated by
        ``default_factory`` because they were never explicitly set.  Since
        ``state`` is critical for wire serialization we force-include it.
        """
        data = super().model_dump(**kwargs)
        if "state" not in data:
            data["state"] = self.state.model_dump(**kwargs)
        return data

    def model_dump_json(self, **kwargs: Any) -> str:
        """Override to ensure ``state`` is always present in JSON output.

        Converts via ``model_dump`` (which force-includes ``state``) then
        serializes to JSON so the field is never dropped.
        """
        import json as _json

        return _json.dumps(self.model_dump(**kwargs))
