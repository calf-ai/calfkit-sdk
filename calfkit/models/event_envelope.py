from pydantic import Field

from calfkit._vendor.pydantic_ai import ModelMessage, ModelRequest
from calfkit._vendor.pydantic_ai.models import ModelRequestParameters
from calfkit.models.groupchat import GroupchatDataModel
from calfkit.models.handoff import HandoffFrame
from calfkit.models.types import CompactBaseModel, SerializableModelSettings, ToolCallRequest


class EventEnvelope(CompactBaseModel):
    trace_id: str | None = None

    # Used to surface the tool call from latest message so tool call workers do not have to dig
    # For tool node eyes only
    tool_call_request: ToolCallRequest | None = None

    # Pending tool calls to enforce sequential tool calling when thread_id
    # is not provided or when there is no memory history store configured
    pending_tool_calls: list[ToolCallRequest] = Field(default_factory=list)

    # Optional inference-time patch in settings and parameters
    patch_model_request_params: ModelRequestParameters | None = None
    patch_model_settings: SerializableModelSettings | None = None

    # Running message history
    message_history: list[ModelMessage] = Field(default_factory=list)

    # Optional name to save LLM generated messages under
    name: str | None = None

    @property
    def latest_message_in_history(self) -> ModelMessage | None:
        return self.message_history[-1] if self.message_history else None

    # Uncommitted messages, often coming out from a node and not yet persisted in message history
    uncommitted_messages: list[ModelMessage] = Field(default_factory=list)

    # thread id / conversation identifier
    thread_id: str | None = None

    # Allow client to dynamically patch system message at runtime
    # Intentionally kept separate from message_history in order to simplify patch logic
    system_message: ModelRequest | None = None

    # Where the final response from AI should be published to
    final_response_topic: str | None = None

    # Whether the current state of messages is the final response from the AI to the user
    final_response: bool = False

    # For holding groupchat data and config. Only to directly be accessed by the groupchat node.
    groupchat_data: GroupchatDataModel | None = None

    # Stack of HandoffFrames tracking nested agent-to-agent delegations.
    # Each frame records the return address and tool-call metadata so that
    # the response can be routed back to the correct caller.
    handoff_stack: list[HandoffFrame] = Field(default_factory=list)

    @property
    def is_groupchat(self) -> bool:
        return self.groupchat_data is not None

    @property
    def is_end_of_turn(self) -> bool:
        return self.final_response

    @property
    def has_uncommitted_messages(self) -> bool:
        """Check if the envelope has uncommitted, unprocessed messages."""
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

    def push_handoff_frame(self, frame: HandoffFrame) -> None:
        """Push a handoff frame onto the stack.

        Uses assignment (not in-place mutation) so that Pydantic's
        ``exclude_unset`` serialization includes the field after modification.
        """
        self.handoff_stack = [*self.handoff_stack, frame]

    def pop_handoff_frame(self) -> HandoffFrame:
        """Pop the top handoff frame from the stack.

        Uses assignment (not in-place mutation) so that Pydantic's
        ``exclude_unset`` serialization includes the field after modification.

        Raises:
            IndexError: If the handoff stack is empty.
        """
        if not self.handoff_stack:
            raise IndexError("Cannot pop from an empty handoff stack")
        *rest, frame = self.handoff_stack
        self.handoff_stack = rest
        return frame
