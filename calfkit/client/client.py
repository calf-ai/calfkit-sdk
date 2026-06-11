from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any, overload

import uuid_utils

from calfkit._types import OutputT
from calfkit._vendor.pydantic_ai.messages import ModelMessage, ModelRequest
from calfkit._vendor.pydantic_ai.settings import ModelSettings
from calfkit.client.base import BaseClient
from calfkit.client.invocation_handle import InvocationHandle
from calfkit.models import State
from calfkit.models.node_result import _UNSET, NodeResult
from calfkit.models.state import OverridesState
from calfkit.models.tool_dispatch import ToolBinding, ToolProvider, normalize_tool_bindings


class Client(BaseClient):
    """High-level client for invoking Calf agent nodes.

    Builds on :class:`BaseClient` to provide user-facing methods that accept a
    natural-language prompt, construct the session state, and dispatch to a
    target node topic. Three invocation patterns are supported:

    * **One-way send** (:meth:`send`) — publish and return the
      ``correlation_id`` immediately; no reply future. Optionally carries a
      ``reply_to`` return address where the worker delivers the terminal
      result for someone else to consume.
    * **Start with handle** (:meth:`start`) — publish and return an
      :class:`InvocationHandle` whose :meth:`~InvocationHandle.result` is awaited
      later for the reply, delivered to this client's own reply inbox.
    * **Execute** (:meth:`execute`) — publish and await the reply in a
      single call.

    See :meth:`send` for the one-way traceability model (the ``reply_to``
    return address and the target node's ``publish_topic`` broadcast stream).
    """

    def _build_state_and_overrides(
        self,
        user_prompt: str,
        *,
        correlation_id: str | None,
        temp_instructions: str | None,
        message_history: list[ModelMessage] | None,
        tool_overrides: Sequence[ToolBinding | ToolProvider] | None,
        model_settings: ModelSettings | dict[str, Any] | None,
        author: str | None,
    ) -> tuple[str, State, OverridesState | None]:
        """Shared input-shaping for :meth:`start` / :meth:`send`.

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
                # Providers (e.g. tool nodes) flatten into ToolBindings, same
                # surface as ``Agent(tools=...)``; the bindings' validators are
                # process-local and stripped when the state serializes.
                override_agent_tools=normalize_tool_bindings(tool_overrides) if tool_overrides is not None else None,
                model_settings=dict(model_settings) if model_settings is not None else None,
            )
            if tool_overrides is not None or model_settings is not None
            else None
        )
        return correlation_id, state, overrides

    @overload
    async def start(
        self,
        user_prompt: str,
        topic: str,
        *,
        output_type: type[OutputT],
        author: str | None = ...,
        tool_overrides: Sequence[ToolBinding | ToolProvider] | None = ...,
        correlation_id: str | None = ...,
        temp_instructions: str | None = ...,
        message_history: list[ModelMessage] | None = ...,
        deps: dict[str, Any] | None = ...,
        model_settings: ModelSettings | dict[str, Any] | None = ...,
        route: str | None = ...,
        body: Any | None = ...,
    ) -> InvocationHandle[OutputT]: ...

    @overload
    async def start(
        self,
        user_prompt: str,
        topic: str,
        *,
        author: str | None = ...,
        tool_overrides: Sequence[ToolBinding | ToolProvider] | None = ...,
        correlation_id: str | None = ...,
        temp_instructions: str | None = ...,
        message_history: list[ModelMessage] | None = ...,
        deps: dict[str, Any] | None = ...,
        model_settings: ModelSettings | dict[str, Any] | None = ...,
        route: str | None = ...,
        body: Any | None = ...,
    ) -> InvocationHandle[Any]: ...

    async def start(
        self,
        user_prompt: str,
        topic: str,
        *,
        author: str | None = None,
        tool_overrides: Sequence[ToolBinding | ToolProvider] | None = None,
        output_type: type[Any] = _UNSET,
        correlation_id: str | None = None,
        temp_instructions: str | None = None,
        message_history: list[ModelMessage] | None = None,
        deps: dict[str, Any] | None = None,
        model_settings: ModelSettings | dict[str, Any] | None = None,
        route: str | None = None,
        body: Any | None = None,
    ) -> InvocationHandle[Any]:
        """Start an agent node invocation and return a handle for the reply.

        The **start-with-handle** pattern (cf. Temporal's ``start_workflow``):
        constructs a :class:`State` from the provided prompt and message history,
        publishes it to *topic*, and returns an :class:`InvocationHandle` whose
        :meth:`~InvocationHandle.result` method can be awaited for the reply. The
        reply is always delivered to this client's own reply inbox — the only
        address whose reply future can resolve. For a one-way send (optionally
        with a ``reply_to`` return address for someone else to consume), use
        :meth:`send`; to publish and await in one call, use :meth:`execute`.

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
            correlation_id: Unique identifier to correlate this request with its
                reply. Auto-generated (uuid7) when ``None``.
            temp_instructions: Optional system-level instructions injected into
                the user prompt as a temporary prompt addition.
            message_history: Prior conversation messages to include for
                multi-turn context. Defaults to an empty history.
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
        return await self._start(
            topic=topic,
            correlation_id=correlation_id,
            state=state,
            overrides=overrides,
            deps=deps,
            route=route,
            body=body,
            output_type=output_type,
        )

    async def send(
        self,
        user_prompt: str,
        topic: str,
        *,
        reply_to: str | None = None,
        tool_overrides: Sequence[ToolBinding | ToolProvider] | None = None,
        correlation_id: str | None = None,
        temp_instructions: str | None = None,
        message_history: list[ModelMessage] | None = None,
        deps: dict[str, Any] | None = None,
        model_settings: ModelSettings | dict[str, Any] | None = None,
        author: str | None = None,
        route: str | None = None,
        body: Any | None = None,
    ) -> str:
        """Send a one-way invocation to an agent node, with an optional return address.

        The **one-way send** pattern: constructs a :class:`State` from the
        provided prompt and message history and publishes it to *topic*,
        returning immediately. Unlike :meth:`start`, this registers **no**
        reply future, so the call allocates zero per-call client state.

        Where the terminal result goes is controlled by *reply_to*:

        * ``reply_to=None`` (default) — true fire-and-forget: the worker
          suppresses the point-to-point callback on the terminal hop entirely.
        * ``reply_to="some.topic"`` — the Return Address pattern: the worker
          delivers the terminal result to that topic, point-to-point, for
          **someone else** to consume (a ``@consumer`` node, a sink service —
          never this client, which registers no future). The consumer of that
          topic owns deserialization and, on auto-create-off brokers, the
          topic's existence.

        Either way the terminal result also rides the target node's
        ``publish_topic`` broadcast channel for traceability — wire a
        ``@consumer`` to that topic to observe results. With ``reply_to=None``,
        a target node with no ``publish_topic`` leaves no trace of the
        invocation.

        Because no reply returns to this client, there is no ``output_type`` —
        deserialization belongs to whoever consumes the result.

        Args:
            user_prompt: The user message to send to the agent node.
            topic: The Kafka topic the target node subscribes to.
            reply_to: Optional topic the terminal result is delivered to,
                point-to-point, for a consumer other than this client.
                ``None`` (default) suppresses the terminal callback entirely.
                Not a chaining mechanism — the call stack is unwound at the
                terminal, so address consumers/sinks, not agent input topics.
            tool_overrides: Runtime agent tool overrides.
            correlation_id: Unique identifier to correlate this request with any
                downstream traces. Auto-generated (uuid7) when ``None``.
            temp_instructions: Optional system-level instructions injected into
                the user prompt as a temporary prompt addition.
            message_history: Prior conversation messages to include for
                multi-turn context. Defaults to an empty history.
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
            The ``correlation_id`` of the sent invocation, for tracing.

        Raises:
            ValueError: If *model_settings* is not JSON-serializable, if
                *reply_to* is blank, or if *reply_to* is this client's own reply
                inbox (no future is registered, so the reply would arrive at the
                dispatcher and be dropped — use :meth:`start` / :meth:`execute`
                to await a reply).
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
        return await self._send(
            topic=topic,
            correlation_id=correlation_id,
            state=state,
            overrides=overrides,
            deps=deps,
            route=route,
            body=body,
            reply_to=reply_to,
        )

    @overload
    async def execute(
        self,
        user_prompt: str,
        topic: str,
        *,
        output_type: type[OutputT],
        author: str | None = ...,
        tool_overrides: Sequence[ToolBinding | ToolProvider] | None = ...,
        correlation_id: str | None = ...,
        temp_instructions: str | None = ...,
        message_history: list[ModelMessage] | None = ...,
        deps: dict[str, Any] | None = ...,
        model_settings: ModelSettings | dict[str, Any] | None = ...,
        route: str | None = ...,
        body: Any | None = ...,
        timeout: float | None = ...,
    ) -> NodeResult[OutputT]: ...

    @overload
    async def execute(
        self,
        user_prompt: str,
        topic: str,
        *,
        author: str | None = ...,
        tool_overrides: Sequence[ToolBinding | ToolProvider] | None = ...,
        correlation_id: str | None = ...,
        temp_instructions: str | None = ...,
        message_history: list[ModelMessage] | None = ...,
        deps: dict[str, Any] | None = ...,
        model_settings: ModelSettings | dict[str, Any] | None = ...,
        route: str | None = ...,
        body: Any | None = ...,
        timeout: float | None = ...,
    ) -> NodeResult[Any]: ...

    async def execute(
        self,
        user_prompt: str,
        topic: str,
        *,
        author: str | None = None,
        tool_overrides: Sequence[ToolBinding | ToolProvider] | None = None,
        output_type: type[Any] = _UNSET,
        correlation_id: str | None = None,
        temp_instructions: str | None = None,
        message_history: list[ModelMessage] | None = None,
        deps: dict[str, Any] | None = None,
        model_settings: ModelSettings | dict[str, Any] | None = None,
        route: str | None = None,
        body: Any | None = None,
        timeout: float | None = None,
    ) -> NodeResult[Any]:
        """Invoke an agent node and await the reply in a single call.

        The **execute** pattern (cf. Temporal's ``execute_workflow``):
        convenience wrapper equivalent to::

            handle = await client.start(...)
            result = await handle.result(timeout=timeout)

        Accepts the same arguments as :meth:`start`, plus *timeout*.

        Args:
            user_prompt: The user message to send to the agent node.
            topic: The Kafka topic the target node subscribes to.
            author: Optional name for the human author of *user_prompt*. See
                :meth:`start` for details.
            tool_overrides: Runtime agent tool overrides.
            output_type: The expected Python type for deserializing the agent's
                output. When omitted, auto-detection is used.
            correlation_id: Unique identifier to correlate this request with its
                reply. Auto-generated (uuid7) when ``None``.
            temp_instructions: Optional system-level instructions injected into
                the user prompt as a temporary prompt addition.
            message_history: Prior conversation messages to include for
                multi-turn context. Defaults to an empty history.
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
        handle = await self.start(
            user_prompt,
            topic,
            author=author,
            tool_overrides=tool_overrides,
            output_type=output_type,
            correlation_id=correlation_id,
            temp_instructions=temp_instructions,
            message_history=message_history,
            deps=deps,
            model_settings=model_settings,
            route=route,
            body=body,
        )
        return await handle.result(timeout=timeout)
