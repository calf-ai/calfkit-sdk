from __future__ import annotations

from collections.abc import Sequence
from typing import Any, overload

import uuid_utils

from calfkit._types import OutputT
from calfkit._vendor.pydantic_ai.messages import ModelMessage, ModelRequest
from calfkit.client.base import BaseClient
from calfkit.client.invocation_handle import InvocationHandle
from calfkit.client.node_result import NodeResult
from calfkit.client.deserialize import _UNSET
from calfkit.models import State


class Client(BaseClient):
    """High-level client for invoking Calf agent nodes.

    Builds on :class:`BaseClient` to provide user-facing methods that accept a
    natural-language prompt, construct the session state, and dispatch to a
    target node topic. Supports both fire-and-forget (:meth:`invoke_node`) and
    request/reply (:meth:`execute_node`) patterns.
    """

    @overload
    async def invoke_node(
        self,
        user_prompt: str,
        topic: str,
        *,
        output_type: type[OutputT],
        reply_topic: str | None = ...,
        correlation_id: str | None = ...,
        temp_instructions: str | None = ...,
        message_history: list[ModelMessage] | None = ...,
        run_args: Sequence[Any] | None = ...,
        deps: dict[str, Any] | None = ...,
    ) -> InvocationHandle[OutputT]: ...

    @overload
    async def invoke_node(
        self,
        user_prompt: str,
        topic: str,
        *,
        reply_topic: str | None = ...,
        correlation_id: str | None = ...,
        temp_instructions: str | None = ...,
        message_history: list[ModelMessage] | None = ...,
        run_args: Sequence[Any] | None = ...,
        deps: dict[str, Any] | None = ...,
    ) -> InvocationHandle[Any]: ...

    async def invoke_node(
        self,
        user_prompt: str,
        topic: str,
        *,
        output_type: type[Any] = _UNSET,
        reply_topic: str | None = None,
        correlation_id: str | None = None,
        temp_instructions: str | None = None,
        message_history: list[ModelMessage] | None = None,
        run_args: Sequence[Any] | None = None,
        deps: dict[str, Any] | None = None,
    ) -> InvocationHandle[Any]:
        """Invoke an agent node asynchronously and return a handle for the reply.

        Constructs a :class:`State` from the provided prompt and message history,
        publishes it to *topic*, and returns an :class:`InvocationHandle` whose
        :meth:`~InvocationHandle.result` method can be awaited for the reply.

        Args:
            user_prompt: The user message to send to the agent node.
            topic: The Kafka topic the target node subscribes to.
            output_type: The expected Python type for deserializing the agent's
                output. When omitted, auto-detection is used (``DataPart`` →
                ``TextPart`` fallback).
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
            An :class:`InvocationHandle` whose ``result()`` resolves to a
            :class:`NodeResult`.
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
            output_type=output_type,
        )

    @overload
    async def execute_node(
        self,
        user_prompt: str,
        topic: str,
        *,
        output_type: type[OutputT],
        reply_topic: str | None = ...,
        correlation_id: str | None = ...,
        temp_instructions: str | None = ...,
        message_history: list[ModelMessage] | None = ...,
        run_args: Sequence[Any] | None = ...,
        deps: dict[str, Any] | None = ...,
        timeout: float | None = ...,
    ) -> NodeResult[OutputT]: ...

    @overload
    async def execute_node(
        self,
        user_prompt: str,
        topic: str,
        *,
        reply_topic: str | None = ...,
        correlation_id: str | None = ...,
        temp_instructions: str | None = ...,
        message_history: list[ModelMessage] | None = ...,
        run_args: Sequence[Any] | None = ...,
        deps: dict[str, Any] | None = ...,
        timeout: float | None = ...,
    ) -> NodeResult[Any]: ...

    async def execute_node(
        self,
        user_prompt: str,
        topic: str,
        *,
        output_type: type[Any] = _UNSET,
        reply_topic: str | None = None,
        correlation_id: str | None = None,
        temp_instructions: str | None = None,
        message_history: list[ModelMessage] | None = None,
        run_args: Sequence[Any] | None = None,
        deps: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> NodeResult[Any]:
        """Invoke an agent node and await the reply in a single call.

        Convenience wrapper equivalent to::

            handle = await client.invoke_node(...)
            result = await handle.result(timeout=timeout)

        Accepts the same arguments as :meth:`invoke_node`, plus *timeout*.

        Args:
            user_prompt: The user message to send to the agent node.
            topic: The Kafka topic the target node subscribes to.
            output_type: The expected Python type for deserializing the agent's
                output. When omitted, auto-detection is used.
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
            A :class:`NodeResult` containing the deserialized output and
            session metadata.

        Raises:
            asyncio.TimeoutError: If *timeout* elapses before a reply arrives.
        """
        handle = await self.invoke_node(
            user_prompt,
            topic,
            output_type=output_type,
            reply_topic=reply_topic,
            correlation_id=correlation_id,
            temp_instructions=temp_instructions,
            message_history=message_history,
            run_args=run_args,
            deps=deps,
        )
        return await handle.result(timeout=timeout)
