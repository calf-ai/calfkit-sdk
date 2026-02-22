from typing import Annotated, Any, cast, overload

from faststream import Context
from faststream.kafka.annotations import (
    KafkaBroker as BrokerAnnotation,
)

from calfkit._vendor.pydantic_ai import ModelRequest, ModelResponse, SystemPromptPart
from calfkit._vendor.pydantic_ai.models import ModelRequestParameters
from calfkit.broker.broker import BrokerClient
from calfkit.messages import patch_system_prompts, validate_tool_call_pairs
from calfkit.models.event_envelope import EventEnvelope
from calfkit.models.types import ToolCallRequest
from calfkit.nodes.base_node import BaseNode, entrypoint, publish_to, subscribe_to
from calfkit.nodes.base_tool_node import BaseToolNode
from calfkit.stores.base import MessageHistoryStore


# TODO: consider a pattern where public input
# and output topics are configured via init as runtime passed dynamic variable.
class AgentRouterNode(BaseNode):
    """Logic for the internal routing to operate agents"""

    _router_sub_topic_name = "agent_router.input"
    _router_pub_topic_name = "agent_router.output"

    @overload
    def __init__(
        self,
        chat_node: BaseNode,
        *,
        name: str | None = None,
        system_prompt: str,
        tool_nodes: list[BaseToolNode],
        message_history_store: MessageHistoryStore,
        **kwargs: Any,
    ): ...

    @overload
    def __init__(
        self,
        chat_node: BaseNode,
        *,
        name: str | None = None,
        system_prompt: str | None = None,
        tool_nodes: list[BaseToolNode] | None = None,
        message_history_store: MessageHistoryStore | None = None,
        **kwargs: Any,
    ): ...

    @overload
    def __init__(self, *, name: str | None = None) -> None: ...

    @overload
    def __init__(
        self,
        *,
        name: str | None = None,
        system_prompt: str | None = None,
        tool_nodes: list[BaseToolNode] | None = None,
    ): ...

    def __init__(
        self,
        chat_node: BaseNode | None = None,
        *,
        system_prompt: str | None = None,
        name: str | None = None,
        tool_nodes: list[BaseToolNode] | None = None,
        message_history_store: MessageHistoryStore | None = None,
        deps_type: type | None = None,
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
           Optional: system_prompt, tool_nodes, message_history_store

        3. **Minimal Client**:
           Use when creating a client to invoke an already-deployed router service.
           No parameters needed - connects to the deployed service via the broker.

        4. **Client with Runtime Patches**:
           Use when creating a client that provides its own tools/system prompt at runtime,
           overriding or supplementing what the deployed router service provides.
           Optional: system_prompt, tool_nodes

        Args:
            chat_node: The chat node for LLM interactions. Required for deployable services.
            system_prompt: Optional system prompt to override the default. Must be str for
                deployable service, optional for client with runtime patches.
            tool_nodes: List of tool nodes that the agent can call. Includes any HandoffTool
                instances — the router treats them like any other tool. Optional for all forms.
            message_history_store: Store for persisting conversation history across requests.
                Required for deployable service, optional otherwise.
            **kwargs: Additional keyword arguments passed to BaseNode.
        """
        self.chat = chat_node
        self.tools = tool_nodes
        self.system_prompt = system_prompt
        self.system_message = (
            ModelRequest(parts=[SystemPromptPart(self.system_prompt)])
            if self.system_prompt
            else None
        )
        self.message_history_store = message_history_store
        self.deps_type = deps_type

        self.tools_topic_registry: dict[str, str] | None = (
            {
                tool.tool_schema.name: cast(str, tool.subscribed_topic or tool.entrypoint_topic)
                for tool in tool_nodes
                if tool.subscribed_topic is not None or tool.entrypoint_topic is not None
            }
            if tool_nodes is not None
            else None
        )

        super().__init__(name=name, **kwargs)

    @subscribe_to(_router_sub_topic_name)
    @entrypoint("agent_router.private.{name}")
    @publish_to(_router_pub_topic_name)
    async def _router(
        self,
        ctx: EventEnvelope,
        correlation_id: Annotated[str, Context()],
        broker: BrokerAnnotation,
    ) -> EventEnvelope:
        if not ctx.has_uncommitted_messages:
            return ctx

        ctx.agent_name = self.name

        # One central place where message history is updated
        uncommitted_messages = ctx.pop_all_uncommited_agent_messages()
        if self.message_history_store is not None and ctx.thread_id is not None:
            await self.message_history_store.append_many(
                thread_id=ctx.thread_id,
                messages=uncommitted_messages,
                scope=self.name,
            )
            ctx.message_history = await self.message_history_store.get(
                thread_id=ctx.thread_id, scope=self.name
            )
        else:
            ctx.message_history.extend(uncommitted_messages)

        # Apply system prompts w/ priority: incoming patch > self.system_message > existing history
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

        Pure topic-based routing — no content inspection.

        Args:
            event_envelope: The event envelope to route. Modified in place.
            generated_tool_call: The tool call request from the model response.
            correlation_id: The correlation ID for request tracking.
            broker: The message broker for publishing.
        """
        if self.tools_topic_registry is None:
            raise RuntimeError("No tools configured on this router node, but tool was still called")
        tool_topic = self.tools_topic_registry.get(generated_tool_call.tool_name)
        if tool_topic is None:
            return
        event_envelope.tool_call_request = generated_tool_call
        await broker.publish(
            event_envelope,
            topic=tool_topic,
            correlation_id=correlation_id,
            reply_to=self.entrypoint_topic or self.subscribed_topic,
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
        event_envelope.mark_as_end_of_turn()
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

        Args:
            event_envelope: The event envelope to send. Modified in place.
            correlation_id: The correlation ID for request tracking.
            broker: The message broker for publishing.
        """
        patch_model_request_params = event_envelope.patch_model_request_params
        if patch_model_request_params is None:
            patch_model_request_params = ModelRequestParameters(
                function_tools=[tool.tool_schema for tool in self.tools]
                if self.tools is not None
                else []
            )
        event_envelope.patch_model_request_params = patch_model_request_params
        if event_envelope.name is None:
            event_envelope.name = self.name

        await broker.publish(
            event_envelope,
            topic=self.chat.subscribed_topic,  # type: ignore
            correlation_id=correlation_id,
            reply_to=self.entrypoint_topic or self.subscribed_topic,
        )

    async def invoke(
        self,
        *,
        user_prompt: str,
        broker: BrokerClient,
        final_response_topic: str | None = None,
        correlation_id: str,
        thread_id: str | None = None,
        deps: Any = None,
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
            ModelRequestParameters(function_tools=[tool.tool_schema for tool in self.tools])
            if self.tools is not None
            else None
        )
        if not broker._connection:
            await broker.start()

        event_envelope = EventEnvelope(
            trace_id=correlation_id,
            patch_model_request_params=patch_model_request_params,
            thread_id=thread_id,
            system_message=self.system_message,
            final_response_topic=final_response_topic,
            deps=deps,
        )
        event_envelope.mark_as_start_of_turn()
        event_envelope.prepare_uncommitted_agent_messages(
            [ModelRequest.user_text_prompt(user_prompt)]
        )
        if self.name is not None:
            event_envelope.name = self.name
        await broker.publish(
            event_envelope,
            topic=self.subscribed_topic or "",
            correlation_id=correlation_id,
        )
        return correlation_id
