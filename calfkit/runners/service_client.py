import asyncio
import math
from collections.abc import AsyncGenerator, Callable
from typing import Annotated, Any, Generic, overload

import uuid_utils
from anyio import create_memory_object_stream
from faststream import Context
from typing_extensions import TypeVar

from calfkit._vendor.pydantic_ai import ModelMessage
from calfkit.broker.broker import BrokerClient
from calfkit.models.event_envelope import EventEnvelope
from calfkit.nodes.agent_router_node import AgentRouterNode

AgentDepsT = TypeVar("AgentDepsT", default=None)


class InvokeResponse:
    def __init__(
        self,
        correlation_id: str,
    ):
        self.send, self.receive = create_memory_object_stream[EventEnvelope](
            max_buffer_size=math.inf
        )
        self._done = asyncio.Event()
        self._final_response: ModelMessage | None = None
        self.correlation_id = correlation_id
        self._cleanup_task: asyncio.Task[None] | None = None

    async def _put(self, item: EventEnvelope) -> None:
        if self.finished:
            return
        await self.send.send(item)
        if item.is_end_of_turn:
            self._final_response = item.latest_message_in_history
            await self.send.aclose()
            self._done.set()

    async def messages_stream(self) -> AsyncGenerator[ModelMessage, None]:
        """Can be used to stream all agent's actions and thinking prior to the final response

        Returns:
            ModelMessage: request/response object from the model client
        """
        async for item in self.receive:
            if item.latest_message_in_history:
                yield item.latest_message_in_history

    async def get_final_response(self) -> ModelMessage:
        """Blocks until final response is received and returns it.

        Returns:
            ModelMessage: The final response message from the model
        """
        if not self.finished:
            await self._done.wait()
        if self._final_response is None:
            raise RuntimeError("Final response not available")
        return self._final_response

    @property
    def finished(self) -> bool:
        return self._final_response is not None


class RouterServiceClient(Generic[AgentDepsT]):
    """Client for invoking a deployed AgentRouterNode.

    Generic in `AgentDepsT` — the type of runtime dependencies passed to tool
    functions via ``ToolContext``.  When ``deps_type`` is provided, type checkers
    can verify that the ``deps`` value passed to :meth:`request` / :meth:`invoke`
    matches the expected type.

    Examples::

        # Without deps (default)
        client = RouterServiceClient(broker, router_node)
        response = await client.request(user_prompt="Hello")

        # With typed deps
        client = RouterServiceClient(broker, router_node, deps_type=MyDeps)
        response = await client.request(user_prompt="Buy BTC", deps=MyDeps(...))
    """

    @overload
    def __init__(
        self,
        broker: BrokerClient,
        node: AgentRouterNode,
        *,
        deps_type: type[AgentDepsT],
    ) -> None: ...

    @overload
    def __init__(
        self,
        broker: BrokerClient,
        node: AgentRouterNode,
    ) -> None: ...

    def __init__(
        self,
        broker: BrokerClient,
        node: AgentRouterNode,
        *,
        deps_type: type[AgentDepsT] | None = None,
    ) -> None:
        self._broker = broker
        self._node = node
        self._deps_type = deps_type

    def _get_ephemeral_handler(
        self,
        match_correlation_id: str,
    ) -> tuple[Callable[..., Any], InvokeResponse]:
        response_pipe = InvokeResponse(match_correlation_id)

        async def _handle_responses(
            event_envelope: EventEnvelope,
            correlation_id: Annotated[str, Context()],
        ) -> None:
            if match_correlation_id == correlation_id:
                await response_pipe._put(event_envelope)

        return _handle_responses, response_pipe

    async def request(
        self,
        user_prompt: str,
        *,
        deps: AgentDepsT | None = None,
        final_response_topic: str | None = None,
        thread_id: str | None = None,
        correlation_id: str | None = None,
    ) -> InvokeResponse:
        """Invoke the service via a request and wait for a response.
        Synchronous request->response communication model.

        Args:
            user_prompt: User prompt to request the model.
            deps: Optional runtime dependencies forwarded to tool functions
                via ``ToolContext``.
            final_response_topic: The topic to publish the final response to.
            thread_id: The conversation ID for multi-turn memory.
            correlation_id: Optionally provide a correlation ID for this request.

        Returns:
            InvokeResponse: The response stream for the request.
        """
        if correlation_id is None:
            correlation_id = uuid_utils.uuid7().hex
        subscriber = self._broker.subscriber(
            self._node.publish_to_topic or "", persistent=False, group_id=uuid_utils.uuid4().hex
        )

        handler, response_pipe = self._get_ephemeral_handler(correlation_id)
        subscriber(handler)

        # Only start broker if not already connected, otherwise just start the new subscriber
        if not self._broker._connection:
            await self._broker.start()
        else:
            await subscriber.start()
        await self._node.invoke(
            user_prompt=user_prompt,
            broker=self._broker,
            final_response_topic=final_response_topic,
            thread_id=thread_id,
            correlation_id=correlation_id,
            deps=deps,
        )

        async def cleanup_when_done() -> None:
            await response_pipe._done.wait()
            await subscriber.stop()

        # Store reference to prevent GC before cleanup completes
        response_pipe._cleanup_task = asyncio.create_task(cleanup_when_done())

        return response_pipe

    async def invoke(
        self,
        user_prompt: str,
        *,
        deps: AgentDepsT | None = None,
        final_response_topic: str | None = None,
        thread_id: str | None = None,
        correlation_id: str | None = None,
    ) -> str:
        """Invoke the agent asynchronously, following fire-and-forget pattern.

        Args:
            user_prompt: User prompt to request the model.
            deps: Optional runtime dependencies forwarded to tool functions
                via ``ToolContext``.
            final_response_topic: The final topic to respond to when
                the agent node is done.
            thread_id: The conversation ID for multi-turn memory.
            correlation_id: Optionally provide a correlation ID for this request.

        Returns:
            The correlation ID for this request.
        """
        if correlation_id is None:
            correlation_id = uuid_utils.uuid7().hex
        # Only start broker if not already connected, otherwise just start the new subscriber
        if not self._broker._connection:
            await self._broker.start()
        return await self._node.invoke(
            user_prompt=user_prompt,
            broker=self._broker,
            final_response_topic=final_response_topic,
            thread_id=thread_id,
            correlation_id=correlation_id,
            deps=deps,
        )
