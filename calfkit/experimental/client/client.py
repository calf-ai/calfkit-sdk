from collections.abc import Sequence
from typing import Any

import uuid_utils

from calfkit._vendor.pydantic_ai.messages import ModelMessage, ModelRequest
from calfkit.experimental.base_models.envelope import Envelope
from calfkit.experimental.client.base import BaseClient
from calfkit.experimental.client.invocation_handle import InvocationHandle
from calfkit.experimental.data_model.state_deps import State


class Client(BaseClient):
    """High-level client for invoking Calf agent nodes.

    Builds on :class:`BaseClient` to provide user-facing methods that accept a
    natural-language prompt, construct the session state, and dispatch to a
    target node topic. Supports both fire-and-forget (:meth:`invoke_node`) and
    request/reply (:meth:`execute_node`) patterns.
    """

    async def invoke_node(
        self,
        user_prompt: str,
        topic: str,
        *,
        reply_topic: str | None = None,
        correlation_id: str | None = None,
        temp_instructions: str | None = None,
        message_history: list[ModelMessage] | None = None,
        run_args: Sequence[Any] | None = None,
        deps: dict[str, Any] | None = None,
    ) -> InvocationHandle:
        """Invoke an agent node asynchronously and return a handle for the reply.

        Constructs a :class:`State` from the provided prompt and message history,
        publishes it to *topic*, and returns an :class:`InvocationHandle` whose
        :meth:`~InvocationHandle.result` method can be awaited for the reply.

        Args:
            user_prompt: The user message to send to the agent node.
            topic: The Kafka topic the target node subscribes to.
            reply_topic: Topic the node should publish its reply to.
                Defaults to the client's auto-generated reply topic.
            correlation_id: Unique identifier to correlate this request with its
                reply. Auto-generated (uuid7) when ``None``.
            temp_instructions: Optional system-level instructions injected into
                the user prompt as a one-shot override.
            message_history: Prior conversation messages to include for
                multi-turn context. Defaults to an empty history.
            run_args: Positional arguments forwarded to the node's ``run()``
                method.
            deps: A mapping of dependency keys to values, made available to
                the node's tools at runtime via the session context.

        Returns:
            An :class:`InvocationHandle` with an associated future that resolves
            to the node's reply :class:`~calfkit.experimental.base_models.envelope.Envelope`.
        """
        if correlation_id is None:
            correlation_id = uuid_utils.uuid7().hex
        if reply_topic is None:
            reply_topic = self._reply_topic

        state = State(message_history=message_history or list())
        state.stage_message(ModelRequest.user_text_prompt(user_prompt, instructions=temp_instructions))
        return await self._invoke(
            topic=topic,
            reply_topic=reply_topic,
            correlation_id=correlation_id,
            run_args=run_args,
            state=state,
            deps=deps,
        )

    async def execute_node(
        self,
        user_prompt: str,
        topic: str,
        *,
        reply_topic: str | None = None,
        correlation_id: str | None = None,
        temp_instructions: str | None = None,
        message_history: list[ModelMessage] | None = None,
        run_args: Sequence[Any] | None = None,
        deps: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> Envelope:
        """Invoke an agent node and await the reply in a single call.

        Convenience wrapper equivalent to::

            handle = await client.invoke_node(...)
            envelope = await handle.result(timeout=timeout)

        Accepts the same arguments as :meth:`invoke_node`, plus *timeout*.

        Args:
            user_prompt: The user message to send to the agent node.
            topic: The Kafka topic the target node subscribes to.
            reply_topic: Topic the node should publish its reply to.
                Defaults to the client's auto-generated reply topic.
            correlation_id: Unique identifier to correlate this request with its
                reply. Auto-generated (uuid7) when ``None``.
            temp_instructions: Optional system-level instructions injected into
                the user prompt as a one-shot override.
            message_history: Prior conversation messages to include for
                multi-turn context. Defaults to an empty history.
            run_args: Positional arguments forwarded to the node's ``run()``
                method.
            deps: A mapping of dependency keys to values, made available to
                the node's tools at runtime via the session context.
            timeout: Maximum seconds to wait for the reply. ``None`` means
                wait indefinitely.

        Returns:
            The reply :class:`~calfkit.experimental.base_models.envelope.Envelope`
            containing the node's session context and workflow state.

        Raises:
            asyncio.TimeoutError: If *timeout* elapses before a reply arrives.
        """
        handle = await self.invoke_node(
            user_prompt,
            topic,
            reply_topic=reply_topic,
            correlation_id=correlation_id,
            temp_instructions=temp_instructions,
            message_history=message_history,
            run_args=run_args,
            deps=deps,
        )
        return await handle.result(timeout=timeout)
