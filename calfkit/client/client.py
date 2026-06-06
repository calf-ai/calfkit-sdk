from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any, overload

import uuid_utils

from calfkit._types import OutputT
from calfkit._vendor.pydantic_ai.messages import ModelMessage, ModelRequest
from calfkit._vendor.pydantic_ai.settings import ModelSettings
from calfkit.client.base import BaseClient
from calfkit.client.deserialize import _UNSET
from calfkit.client.invocation_handle import InvocationHandle
from calfkit.client.node_result import NodeResult
from calfkit.models import State
from calfkit.models.node_schema import BaseToolNodeSchema
from calfkit.models.state import OverridesState


class Client(BaseClient):
    """High-level client for invoking Calf agent nodes.

    Builds on :class:`BaseClient` to provide user-facing methods that accept a
    natural-language prompt, construct the session state, and dispatch to a
    target node topic. Three invocation patterns are supported:

    * **One-way fire-and-forget** (:meth:`emit_to_node`) — publish and return the
      ``correlation_id`` immediately; no reply future, no reply traffic.
    * **Async with handle** (:meth:`invoke_node`) — publish and return an
      :class:`InvocationHandle` whose :meth:`~InvocationHandle.result` is awaited
      later for the reply.
    * **Sync request/reply** (:meth:`execute_node`) — publish and await the reply
      in a single call.

    See :meth:`emit_to_node` for the fire-and-forget traceability model (the
    target node's ``publish_topic`` broadcast stream).
    """

    def _build_state_and_overrides(
        self,
        user_prompt: str,
        *,
        correlation_id: str | None,
        temp_instructions: str | None,
        message_history: list[ModelMessage] | None,
        tool_overrides: list[BaseToolNodeSchema] | None,
        model_settings: ModelSettings | dict[str, Any] | None,
        author: str | None,
    ) -> tuple[str, State, OverridesState | None]:
        """Shared input-shaping for :meth:`invoke_node` / :meth:`emit_to_node`.

        Validates that *model_settings* is JSON-serializable (it crosses the Kafka
        boundary), defaults *correlation_id* to a fresh uuid7 hex, builds and
        stages the :class:`State` from *user_prompt* + *message_history* (stamping
        *author* as the staged message's participant name), and builds the
        :class:`OverridesState` (or ``None`` when neither *tool_overrides* nor
        *model_settings* is set). Returns ``(correlation_id, state, overrides)``.

        Raises:
            ValueError: If *model_settings* is not JSON-serializable.
        """
        if model_settings is not None:
            try:
                json.dumps(model_settings, allow_nan=False)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"model_settings is not JSON-serializable: {exc}. Payload: {model_settings!r}") from exc

        if correlation_id is None:
            correlation_id = uuid_utils.uuid7().hex

        state = State(message_history=message_history or list(), temp_instructions=temp_instructions)
        state.stage_message(ModelRequest.user_text_prompt(user_prompt, name=author))
        overrides = (
            OverridesState(
                override_agent_tools=tool_overrides,
                model_settings=dict(model_settings) if model_settings is not None else None,
            )
            if tool_overrides is not None or model_settings is not None
            else None
        )
        return correlation_id, state, overrides

    @overload
    async def invoke_node(
        self,
        user_prompt: str,
        topic: str,
        *,
        output_type: type[OutputT],
        author: str | None = ...,
        tool_overrides: list[BaseToolNodeSchema] | None = ...,
        reply_topic: str | None = ...,
        correlation_id: str | None = ...,
        temp_instructions: str | None = ...,
        message_history: list[ModelMessage] | None = ...,
        run_args: Sequence[Any] | None = ...,
        deps: dict[str, Any] | None = ...,
        model_settings: ModelSettings | dict[str, Any] | None = ...,
    ) -> InvocationHandle[OutputT]: ...

    @overload
    async def invoke_node(
        self,
        user_prompt: str,
        topic: str,
        *,
        author: str | None = ...,
        tool_overrides: list[BaseToolNodeSchema] | None = ...,
        reply_topic: str | None = ...,
        correlation_id: str | None = ...,
        temp_instructions: str | None = ...,
        message_history: list[ModelMessage] | None = ...,
        run_args: Sequence[Any] | None = ...,
        deps: dict[str, Any] | None = ...,
        model_settings: ModelSettings | dict[str, Any] | None = ...,
    ) -> InvocationHandle[Any]: ...

    async def invoke_node(
        self,
        user_prompt: str,
        topic: str,
        *,
        author: str | None = None,
        tool_overrides: list[BaseToolNodeSchema] | None = None,
        output_type: type[Any] = _UNSET,
        reply_topic: str | None = None,
        correlation_id: str | None = None,
        temp_instructions: str | None = None,
        message_history: list[ModelMessage] | None = None,
        run_args: Sequence[Any] | None = None,
        deps: dict[str, Any] | None = None,
        model_settings: ModelSettings | dict[str, Any] | None = None,
    ) -> InvocationHandle[Any]:
        """Invoke an agent node asynchronously and return a handle for the reply.

        The **async-with-handle** pattern: constructs a :class:`State` from the
        provided prompt and message history, publishes it to *topic*, and returns
        an :class:`InvocationHandle` whose :meth:`~InvocationHandle.result` method
        can be awaited for the reply. For a true one-way send that registers no
        reply future, use :meth:`emit_to_node`; to publish and await in one call,
        use :meth:`execute_node`.

        Args:
            user_prompt: The user message to send to the agent node.
            topic: The Kafka topic the target node subscribes to.
            author: Optional name for the human author of *user_prompt*. When set,
                it is stamped onto the staged ``UserPromptPart.name`` and surfaces
                as a ``<user:author>`` attribution prefix once two or more named
                humans share a channel. Defaults to ``None`` (anonymous ``<user>``).
            tool_overrides: Runtime agent tool overrides.
            output_type: The expected Python type for deserializing the agent's
                output. When omitted, auto-detection is used (``DataPart`` →
                ``TextPart`` fallback).
            reply_topic: Topic the node should publish its reply to.
                Defaults to the client's auto-generated reply topic.
            correlation_id: Unique identifier to correlate this request with its
                reply. Auto-generated (uuid7) when ``None``.
            temp_instructions: Optional system-level instructions injected into
                the user prompt as a temporary prompt addition.
            message_history: Prior conversation messages to include for
                multi-turn context. Defaults to an empty history.
            run_args: Positional arguments forwarded to the node's ``run()``
                method.
            deps: A mapping of dependency keys to values, made available to
                the node's tools at runtime via the session context.
            model_settings: Per-call model settings (e.g. ``{"temperature": 0.0}``)
                that merge over the agent's constructor defaults, which in turn
                merge over the model client's defaults. Must be JSON-serializable
                since it travels over Kafka.

        Returns:
            An :class:`InvocationHandle` whose ``result()`` resolves to a
            :class:`NodeResult`.
        """
        correlation_id, state, overrides = self._build_state_and_overrides(
            user_prompt,
            correlation_id=correlation_id,
            temp_instructions=temp_instructions,
            message_history=message_history,
            tool_overrides=tool_overrides,
            model_settings=model_settings,
            author=author,
        )
        if reply_topic is None:
            reply_topic = self._reply_topic
        return await self._invoke(
            topic=topic,
            reply_topic=reply_topic,
            correlation_id=correlation_id,
            run_args=run_args,
            state=state,
            overrides=overrides,
            deps=deps,
            output_type=output_type,
        )

    async def emit_to_node(
        self,
        user_prompt: str,
        topic: str,
        *,
        tool_overrides: list[BaseToolNodeSchema] | None = None,
        correlation_id: str | None = None,
        temp_instructions: str | None = None,
        message_history: list[ModelMessage] | None = None,
        run_args: Sequence[Any] | None = None,
        deps: dict[str, Any] | None = None,
        model_settings: ModelSettings | dict[str, Any] | None = None,
        author: str | None = None,
    ) -> str:
        """Emit a true one-way (fire-and-forget) invocation to an agent node.

        The **one-way fire-and-forget** pattern: constructs a :class:`State` from
        the provided prompt and message history and publishes it to *topic*,
        returning immediately. Unlike :meth:`invoke_node`, this registers **no**
        reply future and the worker suppresses the point-to-point callback on the
        terminal hop, so the call allocates zero per-call client state and
        triggers zero reply traffic.

        Because no reply is collected, there is no ``reply_topic`` or
        ``output_type``. The terminal result still rides the target node's
        ``publish_topic`` broadcast channel for traceability — wire a
        ``@consumer`` to that topic to observe results. A target node with no
        ``publish_topic`` leaves no trace of the invocation.

        Args:
            user_prompt: The user message to send to the agent node.
            topic: The Kafka topic the target node subscribes to.
            tool_overrides: Runtime agent tool overrides.
            correlation_id: Unique identifier to correlate this request with any
                downstream traces. Auto-generated (uuid7) when ``None``.
            temp_instructions: Optional system-level instructions injected into
                the user prompt as a temporary prompt addition.
            message_history: Prior conversation messages to include for
                multi-turn context. Defaults to an empty history.
            run_args: Positional arguments forwarded to the node's ``run()``
                method.
            deps: A mapping of dependency keys to values, made available to
                the node's tools at runtime via the session context.
            model_settings: Per-call model settings (e.g. ``{"temperature": 0.0}``)
                that merge over the agent's constructor defaults, which in turn
                merge over the model client's defaults. Must be JSON-serializable
                since it travels over Kafka.
            author: Optional name for the human author of *user_prompt*. When set,
                it rides on the staged message and surfaces to multi-agent agents
                as a ``<user:author>`` attribution prefix once two or more named
                humans are present; otherwise human messages read as ``<user>``.

        Returns:
            The ``correlation_id`` of the emitted invocation, for tracing.

        Raises:
            ValueError: If *model_settings* is not JSON-serializable.
        """
        correlation_id, state, overrides = self._build_state_and_overrides(
            user_prompt,
            correlation_id=correlation_id,
            temp_instructions=temp_instructions,
            message_history=message_history,
            tool_overrides=tool_overrides,
            model_settings=model_settings,
            author=author,
        )
        return await self._emit(
            topic=topic,
            correlation_id=correlation_id,
            run_args=run_args,
            state=state,
            overrides=overrides,
            deps=deps,
        )

    @overload
    async def execute_node(
        self,
        user_prompt: str,
        topic: str,
        *,
        output_type: type[OutputT],
        author: str | None = ...,
        tool_overrides: list[BaseToolNodeSchema] | None = ...,
        reply_topic: str | None = ...,
        correlation_id: str | None = ...,
        temp_instructions: str | None = ...,
        message_history: list[ModelMessage] | None = ...,
        run_args: Sequence[Any] | None = ...,
        deps: dict[str, Any] | None = ...,
        model_settings: ModelSettings | dict[str, Any] | None = ...,
        timeout: float | None = ...,
    ) -> NodeResult[OutputT]: ...

    @overload
    async def execute_node(
        self,
        user_prompt: str,
        topic: str,
        *,
        author: str | None = ...,
        tool_overrides: list[BaseToolNodeSchema] | None = ...,
        reply_topic: str | None = ...,
        correlation_id: str | None = ...,
        temp_instructions: str | None = ...,
        message_history: list[ModelMessage] | None = ...,
        run_args: Sequence[Any] | None = ...,
        deps: dict[str, Any] | None = ...,
        model_settings: ModelSettings | dict[str, Any] | None = ...,
        timeout: float | None = ...,
    ) -> NodeResult[Any]: ...

    async def execute_node(
        self,
        user_prompt: str,
        topic: str,
        *,
        author: str | None = None,
        tool_overrides: list[BaseToolNodeSchema] | None = None,
        output_type: type[Any] = _UNSET,
        reply_topic: str | None = None,
        correlation_id: str | None = None,
        temp_instructions: str | None = None,
        message_history: list[ModelMessage] | None = None,
        run_args: Sequence[Any] | None = None,
        deps: dict[str, Any] | None = None,
        model_settings: ModelSettings | dict[str, Any] | None = None,
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
            author: Optional name for the human author of *user_prompt*. See
                :meth:`invoke_node` for details.
            tool_overrides: Runtime agent tool overrides.
            output_type: The expected Python type for deserializing the agent's
                output. When omitted, auto-detection is used.
            reply_topic: Topic the node should publish its reply to.
                Defaults to the client's auto-generated reply topic.
            correlation_id: Unique identifier to correlate this request with its
                reply. Auto-generated (uuid7) when ``None``.
            temp_instructions: Optional system-level instructions injected into
                the user prompt as a temporary prompt addition.
            message_history: Prior conversation messages to include for
                multi-turn context. Defaults to an empty history.
            run_args: Positional arguments forwarded to the node's ``run()``
                method.
            deps: A mapping of dependency keys to values, made available to
                the node's tools at runtime via the session context.
            model_settings: Per-call model settings (e.g. ``{"temperature": 0.0}``)
                that merge over the agent's constructor defaults, which in turn
                merge over the model client's defaults. Must be JSON-serializable
                since it travels over Kafka.
            timeout: Maximum seconds to wait for the reply. ``None`` means
                wait indefinitely.

        Returns:
            A :class:`NodeResult` containing the deserialized output and
            session metadata.

        Raises:
            asyncio.TimeoutError: If *timeout* elapses before a reply arrives.
            ReplyExpiredError: If the client was created with a ``reply_ttl`` and
                the reply is evicted before arriving (propagated from
                :meth:`InvocationHandle.result`). Not raised when ``reply_ttl`` is
                ``None`` (the default).
            ValueError: If *model_settings* is not JSON-serializable.
        """
        handle = await self.invoke_node(
            user_prompt,
            topic,
            author=author,
            tool_overrides=tool_overrides,
            output_type=output_type,
            reply_topic=reply_topic,
            correlation_id=correlation_id,
            temp_instructions=temp_instructions,
            message_history=message_history,
            run_args=run_args,
            deps=deps,
            model_settings=model_settings,
        )
        return await handle.result(timeout=timeout)
