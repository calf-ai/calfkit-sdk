import logging
from typing import Annotated, Any, cast, overload

from faststream import Context
from faststream.kafka.annotations import (
    KafkaBroker as BrokerAnnotation,
)
from pydantic import BaseModel

from calfkit._vendor.pydantic_ai import ModelResponse
from calfkit._vendor.pydantic_ai.models import ModelRequestParameters
from calfkit.broker.broker import BrokerClient
from calfkit.messages import validate_tool_call_pairs
from calfkit.models.event_envelope import EventEnvelope
from calfkit.models.payloads import ChatPayload, RouterPayload, ToolPayload
from calfkit.models.types import ToolCallRequest
from calfkit.nodes.base_node import BaseNode, entrypoint, publish_to, returnpoint, subscribe_to
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
        name: str,
        input_topic: str | list[str] | None = None,
        output_topic: str | None = None,
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
        name: str,
        input_topic: str | list[str] | None = None,
        output_topic: str | None = None,
        system_prompt: str | None = None,
        tool_nodes: list[BaseToolNode] | None = None,
        message_history_store: MessageHistoryStore | None = None,
        **kwargs: Any,
    ): ...

    @overload
    def __init__(
        self,
        *,
        name: str,
        input_topic: str | list[str] | None = None,
        output_topic: str | None = None,
    ) -> None: ...

    @overload
    def __init__(
        self,
        *,
        name: str,
        input_topic: str | list[str] | None = None,
        output_topic: str | None = None,
        system_prompt: str | None = None,
        tool_nodes: list[BaseToolNode] | None = None,
    ): ...

    def __init__(
        self,
        chat_node: BaseNode | None = None,
        *,
        system_prompt: str | None = None,
        name: str,
        input_topic: str | list[str] | None = None,
        output_topic: str | None = None,
        tool_nodes: list[BaseToolNode] | None = None,
        message_history_store: MessageHistoryStore | None = None,
        deps_type: type | None = None,
        **kwargs: Any,
    ):
        """Initialize an AgentRouterNode.

        Args:
            chat_node: The chat node for LLM interactions. Required for deployable services.
            name: Required name for the router. Used for topic resolution and scoped history.
            system_prompt: Optional system prompt to override the default.
            input_topic: Override the default input topic(s).
            output_topic: Override the default output topic.
            tool_nodes: List of tool nodes that the agent can call.
            message_history_store: Store for persisting conversation history across requests.
            deps_type: Optional type for validating structured deps payloads.
            **kwargs: Additional keyword arguments passed to BaseNode.
        """
        self.chat = chat_node
        self.tools = tool_nodes
        self.system_prompt = system_prompt
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

        super().__init__(name=name, input_topic=input_topic, output_topic=output_topic, **kwargs)

    @subscribe_to(_router_sub_topic_name)
    @entrypoint("agent_router.private.{name}")
    async def on_request(
        self,
        ctx: EventEnvelope,
        correlation_id: Annotated[str, Context()],
        broker: BrokerAnnotation,
    ) -> EventEnvelope:
        """Handle initial requests from clients or delegating agents.

        Receives messages with RouterPayload (normal invocation) or
        uncommitted messages (delegation handoff / tool-call re-entry).
        """
        # Consume RouterPayload if present
        router_payload = RouterPayload.model_validate(ctx.payload) if ctx.payload else None

        if router_payload is None and not ctx.state.has_uncommitted_messages:
            return ctx  # Guard

        # Store session config from RouterPayload onto state (persists across tool-call cycles)
        if router_payload is not None:
            ctx.state.instructions = router_payload.instructions
            ctx.state.agent_name = router_payload.name
            ctx.state.model_request_params = router_payload.patch_model_request_params
            ctx.payload = None  # consumed

        # Validate deps against deps_type if configured
        if ctx.deps is not None and self.deps_type is not None:
            if issubclass(self.deps_type, BaseModel):
                ctx.deps = self.deps_type.model_validate(ctx.deps)
            else:
                if not isinstance(ctx.deps, self.deps_type):
                    logging.error(
                        "incoming deps does not match defined deps_type: \n"
                        + f"deps={ctx.deps}\n\ndeps_type={self.deps_type}"
                    )

        # Load existing history from store (for multi-turn conversations)
        if self.message_history_store is not None and ctx.thread_id is not None:
            ctx.state.message_history = await self.message_history_store.get(
                thread_id=ctx.thread_id, scope=self.name
            )

        # Commit any pre-existing uncommitted messages (e.g., from delegation handoff
        # where the DelegationTool prepares a user prompt as an uncommitted message)
        if ctx.state.has_uncommitted_messages:
            uncommitted_messages = ctx.state.pop_all_uncommited_agent_messages()
            if self.message_history_store is not None and ctx.thread_id is not None:
                await self.message_history_store.append_many(
                    thread_id=ctx.thread_id,
                    messages=uncommitted_messages,
                    scope=self.name,
                )
                ctx.state.message_history = await self.message_history_store.get(
                    thread_id=ctx.thread_id, scope=self.name
                )
            else:
                ctx.state.message_history.extend(uncommitted_messages)

        # Send to ChatNode
        await self._call_model(
            ctx,
            correlation_id,
            broker,
            user_prompt=router_payload.user_prompt if router_payload else None,
        )
        return ctx

    @returnpoint("agent_router.return.{name}")
    @publish_to(_router_pub_topic_name)
    async def on_return(
        self,
        ctx: EventEnvelope,
        correlation_id: Annotated[str, Context()],
        broker: BrokerAnnotation,
    ) -> EventEnvelope:
        """Handle responses from ChatNode and ToolNodes."""
        if not ctx.state.has_uncommitted_messages:
            return ctx  # Guard

        # Commit messages
        uncommitted_messages = ctx.state.pop_all_uncommited_agent_messages()
        if self.message_history_store is not None and ctx.thread_id is not None:
            await self.message_history_store.append_many(
                thread_id=ctx.thread_id,
                messages=uncommitted_messages,
                scope=self.name,
            )
            ctx.state.message_history = await self.message_history_store.get(
                thread_id=ctx.thread_id, scope=self.name
            )
        else:
            ctx.state.message_history.extend(uncommitted_messages)

        # Structured output: ChatNode set payload — reply immediately
        if ctx.payload is not None:
            await self._reply_to_sender(ctx, correlation_id, broker)
            return ctx

        # Route based on latest message
        if isinstance(ctx.state.latest_message_in_history, ModelResponse):
            if (
                ctx.state.latest_message_in_history.finish_reason == "tool_call"
                or ctx.state.latest_message_in_history.tool_calls
            ):
                await self._route_tool_calls(
                    ctx, ctx.state.latest_message_in_history.tool_calls, correlation_id, broker
                )
            else:
                await self._reply_to_sender(ctx, correlation_id, broker)
        elif ctx.state.pending_tool_calls:
            await self._route_tool_calls(ctx, ctx.state.pending_tool_calls, correlation_id, broker)
        elif validate_tool_call_pairs(ctx.state.message_history):
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
            # TODO: verify if this is possible.
            # i.e. does pydantic_ai already internally do tool validation/checking?
            logging.error(
                f"[{self.name}] Tool '{generated_tool_call.tool_name}' not found in "
                f"tools_topic_registry. Available tools: "
                f"{list(self.tools_topic_registry.keys())}. "
                f"tool_call_id={generated_tool_call.tool_call_id}"
            )
            return
        event_envelope.payload = ToolPayload(
            tool_call_request=generated_tool_call,
            agent_name=event_envelope.state.agent_name or self.name,
        )
        await broker.publish(
            event_envelope,
            topic=tool_topic,
            correlation_id=correlation_id,
            reply_to=self.returnpoint_topic or self.entrypoint_topic or self.subscribed_topic,
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
            ctx.state.pending_tool_calls = rest
            await self._route_tool(ctx, first, correlation_id, broker)
        else:
            ctx.state.pending_tool_calls = []
            for tool_call in tool_calls:
                await self._route_tool(ctx, tool_call, correlation_id, broker)

    async def _reply_to_sender(
        self, event_envelope: EventEnvelope, correlation_id: str, broker: Any
    ) -> None:
        """Send the final response back to the client.

        Args:
            event_envelope: The event envelope containing the final response. Modified in place.
            correlation_id: The correlation ID for request tracking.
            broker: The message broker for publishing.
        """
        event_envelope.state.mark_as_end_of_turn()
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
        user_prompt: str | None = None,
    ) -> None:
        """Send the message history to the chat node for LLM inference.

        Reads session config from state (set by on_request from RouterPayload)
        with fallbacks to self attributes.

        Args:
            event_envelope: The event envelope to send. Modified in place.
            correlation_id: The correlation ID for request tracking.
            broker: The message broker for publishing.
            user_prompt: Optional user prompt for the initial request.
        """
        # Read config from state (persisted across tool-call cycles), fall back to self
        instructions = event_envelope.state.instructions or self.system_prompt
        name = event_envelope.state.agent_name or self.name
        params = event_envelope.state.model_request_params
        if params is None:
            params = ModelRequestParameters(
                function_tools=[tool.tool_schema for tool in self.tools]
                if self.tools is not None
                else []
            )

        event_envelope.payload = ChatPayload(
            user_prompt=user_prompt,
            instructions=instructions,
            name=name,
            patch_model_request_params=params,
        )

        await broker.publish(
            event_envelope,
            topic=self.chat.entrypoint_topic or self.chat.subscribed_topic,  # type: ignore
            correlation_id=correlation_id,
            reply_to=self.returnpoint_topic or self.entrypoint_topic or self.subscribed_topic,
        )

    async def invoke(
        self,
        *,
        user_prompt: str | None = None,
        broker: BrokerClient,
        final_response_topic: str | None = None,
        correlation_id: str,
        thread_id: str | None = None,
        deps: Any = None,
    ) -> str:
        """Invoke the agent

        Args:
            user_prompt: User prompt to request the model.
            broker: The broker to connect to.
            final_response_topic: Topic to publish the final response to.
            correlation_id: Correlation ID for this request.
            thread_id: Conversation ID for multi-turn memory.
            deps: Optional runtime dependencies forwarded to tool functions.

        Returns:
            str: The correlation ID for this request
        """

        if not broker._connection:
            await broker.start()

        router_payload = RouterPayload(
            user_prompt=user_prompt,
            instructions=self.system_prompt,
            name=self.name,
            patch_model_request_params=(
                ModelRequestParameters(function_tools=[t.tool_schema for t in self.tools])
                if self.tools
                else None
            ),
        )
        event_envelope = EventEnvelope(
            trace_id=correlation_id,
            thread_id=thread_id,
            final_response_topic=final_response_topic,
            deps=deps,
            payload=router_payload,
        )
        event_envelope.state.mark_as_start_of_turn()
        await broker.publish(
            event_envelope,
            topic=self.entrypoint_topic or self.subscribed_topic or "",
            correlation_id=correlation_id,
        )
        return correlation_id
