from calfkit.models.types import CompactBaseModel


class HandoffFrame(CompactBaseModel):
    """A single frame on the handoff stack, representing one delegation hop.

    Pushed onto EventEnvelope.handoff_stack when a HandoffTool delegates
    to a sub-agent, and popped when the sub-agent's response returns.
    """

    caller_private_topic: str
    """Return address — the topic where the ToolReturnPart should be published."""

    caller_final_response_topic: str | None = None
    """The caller's original final_response_topic, restored on return."""

    tool_call_id: str
    """ID of the tool call that triggered the delegation (for constructing ToolReturnPart)."""

    tool_name: str
    """Name of the tool that triggered the delegation (for constructing ToolReturnPart)."""
