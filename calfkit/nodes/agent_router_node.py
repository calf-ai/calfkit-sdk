from typing import Annotated, Any, overload

from faststream import Context
from faststream.kafka.annotations import (
    KafkaBroker as BrokerAnnotation,
)
from pydantic_ai import ModelRequest, ModelResponse, SystemPromptPart
from pydantic_ai.models import ModelRequestParameters

from calfkit.broker.broker import BrokerClient
from calfkit.messages import patch_system_prompts, validate_tool_call_pairs
from calfkit.models.event_envelope import EventEnvelope
from calfkit.models.types import ToolCallRequest
from calfkit.nodes.base_node import BaseNode, publish_to, subscribe_to
from calfkit.nodes.base_tool_node import BaseToolNode
from calfkit.stores.base import MessageHistoryStore


class AgentRouterNode(BaseNode):
    """Logic for the internal routing to operate agents"""

    _router_sub_topic_name = "agent_router.input"
    _router_pub_topic_name = "agent_router.output"

    @overload
    def __init__(
        self,
        chat_node: BaseNode,
        *,
        system_prompt: str,
        tool_nodes: list[BaseToolNode],
        handoff_nodes: list[type[BaseNode]] = [],
        message_history_store: MessageHistoryStore,
        **kwargs: Any,
    ): ...

    @overload
    def __init__(
        self,
        chat_node: BaseNode,
        *,
        system_prompt: str | None = None,
        tool_nodes: list[BaseToolNode] = [],
        handoff_nodes: list[type[BaseNode]] = [],
        message_history_store: MessageHistoryStore | None = None,
        **kwargs: Any,
    ): ...

    @overload
    def __init__(self) -> None: ...

    @overload
    def __init__(
        self,
        *,
        system_prompt: str | None = None,
        tool_nodes: list[BaseToolNode] = [],
        handoff_nodes: list[type[BaseNode]] = [],
    ): ...

    def __init__(
        self,
        chat_node: BaseNode | None = None,
        *,
        system_prompt: str | None = None,
        tool_nodes: list[BaseToolNode] = [],
        handoff_nodes: list[type[BaseNode]] = [],
        message_history_store: MessageHistoryStore | None = None,
        **kwargs: Any,
    ):
        """Initialize an AgentRouterNode.

        The AgentRouterNode supports multiple initialization patterns depending on use case:

        1. **Deployable Router NodesService** (with required parameters):
           Use when deploying the router as a service with all dependencies explicitly provided.
           Requires: chat_node, system_prompt (str), tool_nodes, message_history_store

        2. **Deployable Router NodesService** (with optional parameters):
           Use when deploying with optional or runtime-configurable dependencies.
           Requires: chat_node
           Optional: system_prompt, tool_nodes, handoff_nodes, message_history_store

        3. **Minimal Client**:
           Use when creating a client to invoke an already-deployed router service.
           No parameters needed - connects to the deployed service via the broker.

        4. **Client with Runtime Patches**:
           Use when creating a client that provides its own tools/system prompt at runtime,
           overriding or supplementing what the deployed router service provides.
           Optional: system_prompt, tool_nodes, handoff_nodes

        Args:
            chat_node: The chat node for LLM interactions. Required for deployable services.
            system_prompt: Optional system prompt to override the default. Must be str for
                deployable service, optional for client with runtime patches.
            tool_nodes: List of tool nodes that the agent can call. Optional for all forms.
            handoff_nodes: List of node types for agent handoff scenarios. Optional.
            message_history_store: Store for persisting conversation history across requests.
                Required for deployable service, optional otherwise.
            **kwargs: Additional keyword arguments passed to BaseNode.
        """
        self.chat = chat_node
        self.tools = tool_nodes
        self.handoffs = handoff_nodes
        self.system_prompt = system_prompt
        self.system_message = (
            ModelRequest(parts=[SystemPromptPart(self.system_prompt)])
            if self.system_prompt
            else None
        )
        self.message_history_store = message_history_store

        self.tools_topic_registry: dict[str, str] = {
            tool.tool_schema().name: tool.subscribed_topic
            for tool in tool_nodes
            if tool.subscribed_topic is not None
        }

        self.tool_response_topics = [tool.publish_to_topic for tool in self.tools]

        super().__init__(**kwargs)

    @subscribe_to(_router_sub_topic_name)
    @publish_to(_router_pub_topic_name)
    async def _router(
        self,
        ctx: EventEnvelope,
        correlation_id: Annotated[str, Context()],
        broker: BrokerAnnotation,
    ) -> EventEnvelope:
        if not ctx.uncommitted_messages:
            raise RuntimeError("There is no response message to process")

        # One central place where message history is updated
        if self.message_history_store is not None and ctx.thread_id is not None:
            await self.message_history_store.append_many(
                thread_id=ctx.thread_id, messages=ctx.uncommitted_messages
            )
            ctx.message_history = await self.message_history_store.get(thread_id=ctx.thread_id)
        else:
            ctx.message_history.extend(ctx.uncommitted_messages)
        print("message history:", ctx.message_history)
        # Apply system prompts with priority: incoming > self.system_message > existing history
        # First, apply self.system_message as fallback (replaces existing history)
        if ctx.system_message is not None:
            ctx.message_history = patch_system_prompts(ctx.message_history, [ctx.system_message])
        elif self.system_message is not None:
            ctx.message_history = patch_system_prompts(
                ctx.message_history,
                [self.system_message],
            )

        if isinstance(ctx.latest_message_in_history, ModelResponse):
            if (
                ctx.latest_message_in_history.finish_reason == "tool_call"
                or ctx.latest_message_in_history.tool_calls
            ):
                await self._route_tool_calls(
                    ctx, ctx.latest_message_in_history.tool_calls, correlation_id, broker
                )
            else:
                await self._reply_to_sender(ctx, correlation_id, broker)
        elif ctx.pending_tool_calls:
            await self._route_tool_calls(ctx, ctx.pending_tool_calls, correlation_id, broker)
        elif validate_tool_call_pairs(ctx.message_history):
            await self._call_model(ctx, correlation_id, broker)

        return ctx

    async def _route_tool(
        self,
        event_envelope: EventEnvelope,
        generated_tool_call: ToolCallRequest,
        correlation_id: str,
        broker: Any,
    ) -> None:
        """Route a tool call request to the appropriate tool node.

        Modifies event_envelope in place by setting the tool_call_request field.

        Args:
            event_envelope: The event envelope to route. Modified in place.
            generated_tool_call: The tool call request from the model response.
            correlation_id: The correlation ID for request tracking.
            broker: The message broker for publishing.
        """
        tool_topic = self.tools_topic_registry.get(generated_tool_call.tool_name)
        if tool_topic is None:
            # TODO: implement a short circuit to respond with an
            # error message for when provided tool does not exist.
            return
        event_envelope.tool_call_request = generated_tool_call
        await broker.publish(
            event_envelope,
            topic=tool_topic,
            correlation_id=correlation_id,
            reply_to=self.subscribed_topic,
        )

    def _requires_sequential_tool_calls(self, ctx: EventEnvelope) -> bool:
        """Check if sequential tool calling is required.

        Sequential mode is needed when there's no message history store or no
        thread_id, because concurrent tool results cannot be aggregated without
        a central store keyed by thread_id.
        """
        return self.message_history_store is None or ctx.thread_id is None

    async def _route_tool_calls(
        self,
        ctx: EventEnvelope,
        tool_calls: list[ToolCallRequest],
        correlation_id: str,
        broker: Any,
    ) -> None:
        """Route tool calls, using sequential mode when no central store is available.

        In sequential mode, only the first tool call is routed and the rest are
        queued in pending_tool_calls. In concurrent mode, all tool calls are
        routed at once.

        Args:
            ctx: The event envelope. Modified in place to set pending_tool_calls.
            tool_calls: List of tool calls to route.
            correlation_id: The correlation ID for request tracking.
            broker: The message broker for publishing.
        """
        if self._requires_sequential_tool_calls(ctx) and len(tool_calls) > 1:
            first, *rest = tool_calls
            ctx.pending_tool_calls = rest
            await self._route_tool(ctx, first, correlation_id, broker)
        else:
            ctx.pending_tool_calls = []
            for tool_call in tool_calls:
                await self._route_tool(ctx, tool_call, correlation_id, broker)

    async def _reply_to_sender(
        self, event_envelope: EventEnvelope, correlation_id: str, broker: Any
    ) -> None:
        """Send the final response back to the client.

        Modifies event_envelope in place by setting final_response to True.

        Args:
            event_envelope: The event envelope containing the final response. Modified in place.
            correlation_id: The correlation ID for request tracking.
            broker: The message broker for publishing.
        """
        event_envelope.final_response = True
        await broker.publish(
            event_envelope,
            topic=event_envelope.final_response_topic or self.publish_to_topic,
            correlation_id=correlation_id,
        )

    async def _call_model(
        self,
        event_envelope: EventEnvelope,
        correlation_id: str,
        broker: Any,
    ) -> None:
        """Send the message history to the chat node for LLM inference.

        Modifies event_envelope in place by setting patch_model_request_params
        if not already set.

        Args:
            event_envelope: The event envelope to send. Modified in place.
            correlation_id: The correlation ID for request tracking.
            broker: The message broker for publishing.
        """
        patch_model_request_params = event_envelope.patch_model_request_params
        if patch_model_request_params is None:
            patch_model_request_params = ModelRequestParameters(
                function_tools=[tool.tool_schema() for tool in self.tools]
            )
        event_envelope.patch_model_request_params = patch_model_request_params
        await broker.publish(
            event_envelope,
            topic=self.chat.subscribed_topic,  # type: ignore
            correlation_id=correlation_id,
            reply_to=self.subscribed_topic,
        )

    async def invoke(
        self,
        *,
        user_prompt: str,
        broker: BrokerClient,
        final_response_topic: str | None = None,
        correlation_id: str,
        thread_id: str | None = None,
    ) -> str:
        """Invoke the agent

        Args:
            user_prompt (str): User prompt to request the model
            broker (BrokerClient): The broker to connect to
            correlation_id (str | None, optional): Optionally provide a correlation ID
            for this request. Defaults to None.

        Returns:
            str: The correlation ID for this request
        """

        patch_model_request_params = (
            ModelRequestParameters(function_tools=[tool.tool_schema() for tool in self.tools])
            if self.tools
            else None
        )
        new_node_messages = [ModelRequest.user_text_prompt(user_prompt)]
        if not broker._connection:
            await broker.start()
        await broker.publish(
            EventEnvelope(
                trace_id=correlation_id,
                patch_model_request_params=patch_model_request_params,
                thread_id=thread_id,
                uncommitted_messages=new_node_messages,
                system_message=self.system_message,
                final_response_topic=final_response_topic,
            ),
            topic=self.subscribed_topic or "",
            correlation_id=correlation_id,
        )
        return correlation_id
