from typing import Annotated, Any, cast

from faststream import Context
from faststream.kafka.annotations import (
    KafkaBroker as BrokerAnnotation,
)

from calfkit._vendor.pydantic_ai import ModelRequest, ModelResponse, ToolReturnPart
from calfkit._vendor.pydantic_ai.tools import Tool, ToolDefinition
from calfkit.models.event_envelope import EventEnvelope
from calfkit.models.handoff import HandoffFrame
from calfkit.nodes.base_node import BaseNode, subscribe_private
from calfkit.nodes.base_tool_node import BaseToolNode


class HandoffTool(BaseToolNode):
    tool_name = "handoff_tool"

    def __init__(self, nodes: list[BaseNode], **kwargs: Any):
        description = f"""Use this tool to handoff the task or conversation to another agent.
The names of the agents that you can handoff to are as listed:
{", ".join(node.name for node in nodes if node.name)}

Args:
    name (str): The name of the agent to handoff to.
    message (str): The message to provide the handoff agent in order to provide context."""

        self._handoffs_topic_registry: dict[str, str] = {
            handoff.name: handoff.private_subscribed_topic
            for handoff in nodes
            if handoff.private_subscribed_topic is not None and handoff.name is not None
        }

        def handoff_tool(name: str, message: str):
            """Use this tool to handoff the task or conversation to another agent.

            Args:
                name (str): The name of the agent to handoff to.
                message (str): The message to provide the handoff agent in order to provide context.
            """
            return

        self.tool = Tool(
            handoff_tool,
            name=self.tool_name,
            description=description,
        )
        node_names = sorted(n.name for n in nodes if n.name)
        resolved_name = f"handoff_{'_'.join(node_names)}"
        super().__init__(name=resolved_name, **kwargs)
        # Resolved topic for Phase 2 — must match the @subscribe_private template below
        self._delegation_response_topic = f"tool_node.handoff.response.{resolved_name}"

    # --- Phase 1: Delegation entry point ---
    # IMPORTANT: on_enter must be defined before on_delegation_response so that
    # private_subscribed_topic (which returns the first private topic) resolves
    # to Phase 1's topic. The router uses that for tools_topic_registry.
    @subscribe_private("tool_node.handoff.{name}")
    async def on_enter(
        self,
        event_envelope: EventEnvelope,
        correlation_id: Annotated[str, Context()],
        reply_to: Annotated[str, Context("message.reply_to")],
        broker: BrokerAnnotation,
    ) -> EventEnvelope:
        if not event_envelope.tool_call_request:
            raise RuntimeError("No tool call request found")

        tool_call_req = event_envelope.tool_call_request
        kw_args = tool_call_req.args_as_dict()
        target_name = kw_args.get("name", "")
        message = kw_args.get("message", "")

        # Validate target agent exists — error case returns normally via reply_to
        target_topic = self._handoffs_topic_registry.get(target_name)
        if target_topic is None:
            tool_result = ToolReturnPart(
                tool_name=tool_call_req.tool_name,
                content=f"Error: agent '{target_name}' not found. Available agents: {', '.join(self._handoffs_topic_registry.keys())}",
                tool_call_id=tool_call_req.tool_call_id,
            )
            event_envelope.tool_call_request = None
            event_envelope.add_to_uncommitted_messages(ModelRequest(parts=[tool_result]))
            return event_envelope

        # Validate reply_to — caller must publish with reply_to set
        if not reply_to:
            tool_result = ToolReturnPart(
                tool_name=tool_call_req.tool_name,
                content="Error: handoff requires reply_to (caller must publish with reply_to set).",
                tool_call_id=tool_call_req.tool_call_id,
            )
            event_envelope.tool_call_request = None
            event_envelope.add_to_uncommitted_messages(ModelRequest(parts=[tool_result]))
            return event_envelope

        # Validate thread_id — error case returns normally via reply_to
        if event_envelope.thread_id is None:
            tool_result = ToolReturnPart(
                tool_name=tool_call_req.tool_name,
                content="Error: handoff requires a thread_id (message history store must be configured).",
                tool_call_id=tool_call_req.tool_call_id,
            )
            event_envelope.tool_call_request = None
            event_envelope.add_to_uncommitted_messages(ModelRequest(parts=[tool_result]))
            return event_envelope

        # --- Success path: delegate to sub-agent ---

        # Build the handoff frame capturing the caller's return address
        frame = HandoffFrame(
            caller_private_topic=reply_to,
            caller_final_response_topic=event_envelope.final_response_topic,
            tool_call_id=tool_call_req.tool_call_id,
            tool_name=tool_call_req.tool_name,
        )

        # Create delegation envelope (deep copy, clean slate for sub-agent)
        delegation = event_envelope.model_copy(deep=True)
        delegation.push_handoff_frame(frame)
        delegation.message_history = []
        delegation.final_response_topic = self._delegation_response_topic
        delegation.pending_tool_calls = []
        delegation.tool_call_request = None
        delegation.patch_model_request_params = None
        delegation.system_message = None
        delegation.name = None

        # Prepare the user prompt for the sub-agent, attributed to the caller
        delegation.prepare_uncommitted_agent_messages(
            [ModelRequest.user_text_prompt(message, name=event_envelope.name)]
        )

        await broker.publish(
            delegation,
            topic=target_topic,
            correlation_id=correlation_id,
        )

        # Return a minimal empty envelope so that FastStream's reply_to
        # serialization succeeds (returning None produces empty bytes which
        # fail Pydantic validation). The router's defensive guard skips
        # processing since there are no uncommitted messages. No ToolReturnPart
        # is sent yet — Phase 2 sends it once the sub-agent actually completes.
        return EventEnvelope()

    # --- Phase 2: Delegation response ---
    @subscribe_private("tool_node.handoff.response.{name}")
    async def on_delegation_response(
        self,
        event_envelope: EventEnvelope,
        correlation_id: Annotated[str, Context()],
        broker: BrokerAnnotation,
    ) -> None:
        frame = event_envelope.pop_handoff_frame()

        # Extract the sub-agent's final response text
        last_msg = event_envelope.latest_message_in_history
        response_text = (
            last_msg.text if isinstance(last_msg, ModelResponse) and last_msg.text else ""
        )

        # Wrap the sub-agent's response as a ToolReturnPart for the caller's LLM
        tool_result = ToolReturnPart(
            tool_name=frame.tool_name,
            content=response_text,
            tool_call_id=frame.tool_call_id,
        )

        # Build a clean response envelope for the caller's router
        response = EventEnvelope(
            trace_id=event_envelope.trace_id,
            thread_id=event_envelope.thread_id,
            final_response_topic=frame.caller_final_response_topic,
            handoff_stack=event_envelope.handoff_stack,
        )
        response.prepare_uncommitted_agent_messages([ModelRequest(parts=[tool_result])])

        await broker.publish(
            response,
            topic=frame.caller_private_topic,
            correlation_id=correlation_id,
        )

    @property
    def tool_schema(self) -> ToolDefinition:
        return cast(ToolDefinition, self.tool.tool_def)
