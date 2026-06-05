import logging
import os
from collections.abc import Iterable, Sequence
from typing import Any

import uuid_utils
from faststream.kafka import KafkaBroker
from typing_extensions import Self

from calfkit._protocol import CLIENT_KIND, HDR_EMITTER, HDR_EMITTER_KIND
from calfkit.client.deserialize import _UNSET
from calfkit.client.invocation_handle import InvocationHandle
from calfkit.client.middleware import ContextInjectionMiddleware
from calfkit.client.reply_dispatcher import _ReplyDispatcher
from calfkit.models import State
from calfkit.models.envelope import Envelope
from calfkit.models.session_context import (
    CallFrame,
    CallFrameStack,
    SessionRunContext,
    WorkflowState,
)
from calfkit.models.state import OverridesState

logger = logging.getLogger(__name__)


def _new_client_emitter_id(client_id: str | None = None) -> str:
    """Return a stable ``client.<uuid7-hex>`` emitter id used as the ``x-calf-emitter``
    header on every client publish. Pass *client_id* to reuse an existing hex id
    (e.g. the one used for the reply topic) instead of minting a fresh one.
    """
    return f"client.{client_id if client_id is not None else uuid_utils.uuid7().hex}"


class BaseClient:
    """Base client for communicating with Calf agent nodes over Kafka.

    Manages a Kafka broker connection and a shared reply dispatcher that
    correlates outgoing invocations with their asynchronous replies. Subclasses
    should build higher-level invocation methods on top of :meth:`_invoke`.

    Supports use as an async context manager for automatic cleanup::

        async with Client.connect("localhost:9092") as client:
            result = await client.execute_node(...)
    """

    def __init__(self, connection: KafkaBroker, reply_topic: str, dispatcher: _ReplyDispatcher, emitter_id: str | None = None) -> None:
        """Initialize the client with pre-configured components.

        Prefer :meth:`connect` for constructing a fully wired client instance.

        Args:
            connection: A configured ``KafkaBroker`` instance.
            reply_topic: The Kafka topic this client listens on for replies.
            dispatcher: The reply dispatcher that routes incoming envelopes
                to their corresponding futures by ``correlation_id``.
            emitter_id: Stable identifier stamped onto every outbound message as
                the ``x-calf-emitter`` Kafka header. When ``None``, a random
                uuid7-based id is generated.
        """
        if emitter_id is not None and not emitter_id.strip():
            raise ValueError("emitter_id must be a non-empty string or None")
        self._connection = connection
        self._reply_topic = reply_topic
        self._dispatcher = dispatcher
        self._emitter_id = emitter_id if emitter_id is not None else _new_client_emitter_id()

    @classmethod
    def connect(
        cls,
        server_urls: str | Iterable[str] | None = None,
        reply_topic: str | None = None,
        reply_ttl: float | None = None,
        **broker_kwargs: Any,
    ) -> Self:
        """Create a new client connected to a Kafka broker.

        This is the primary factory method. It sets up the broker, generates a
        unique reply topic and consumer group, and registers the reply dispatcher.

        Args:
            server_urls: Kafka bootstrap server URL(s). Falls back to the
                ``CALF_HOST_URL`` environment variable, then ``"localhost"``.
            reply_topic: Explicit reply topic name. When ``None`` (default), a
                unique topic is generated using a uuid7 client ID.
            reply_ttl: Optional seconds after which an un-answered reply future
                (from :meth:`invoke_node` / :meth:`execute_node`) is evicted with
                a :class:`~calfkit.exceptions.ReplyExpiredError`. ``None``
                (default) disables eviction entirely — a deliberate
                caller-responsibility choice, not a default safety ceiling.
                Callers who need a bounded pending map under lost replies or
                abandoned handles must opt in by setting a TTL.
            **broker_kwargs: Additional keyword arguments forwarded to
                ``KafkaBroker`` (e.g. ``security``, ``client_id``).

        Returns:
            A new client instance ready for use. Call ``broker.start()`` or use
            the client as an async context manager before invoking nodes.
        """
        if server_urls is None:
            server_urls = os.getenv("CALF_HOST_URL") or "localhost"

        client_id = uuid_utils.uuid7().hex
        if reply_topic is None:
            reply_topic = f"calf-client-reply-{client_id}"
        group_id = f"calf-client-reply-{client_id}"

        broker_connection = KafkaBroker(
            server_urls,
            middlewares=[ContextInjectionMiddleware],
            **broker_kwargs,
        )

        dispatcher = _ReplyDispatcher(reply_ttl=reply_ttl)
        dispatcher.register(broker_connection, reply_topic, group_id)

        return cls(broker_connection, reply_topic, dispatcher, emitter_id=_new_client_emitter_id(client_id))

    @property
    def broker(self) -> KafkaBroker:
        """The underlying ``KafkaBroker`` connection."""
        return self._connection

    @property
    def reply_topic(self) -> str:
        """The Kafka topic this client subscribes to for receiving replies."""
        return self._reply_topic

    async def _invoke(
        self,
        topic: str,
        reply_topic: str,
        correlation_id: str,
        state: State,
        overrides: OverridesState | None = None,
        run_args: Sequence[Any] | None = None,
        deps: dict[str, Any] | None = None,
        output_type: type[Any] = _UNSET,
    ) -> InvocationHandle:
        """Invoke the node asynchronously.

        Args:
            topic: Topic to send args to.
            reply_topic: Topic the node should reply to.
            correlation_id: Correlation ID for this request.
            state: The session state.
            run_args: The args to send to the node's run() method.
            deps: Provided dependencies.

        Returns:
            An invocation handle with an associated future for the reply.
        """
        future = self._dispatcher.expect(correlation_id)
        logger.debug("[%s] invoke topic=%s reply=%s", correlation_id[:8], topic, reply_topic)
        await self._publish_call(
            topic=topic,
            correlation_id=correlation_id,
            callback_topic=reply_topic,
            state=state,
            overrides=overrides,
            run_args=run_args,
            deps=deps,
        )
        return InvocationHandle(
            correlation_id=correlation_id,
            topic=topic,
            reply_topic=reply_topic,
            _future=future,
            _output_type=output_type,
        )

    async def _publish_call(
        self,
        *,
        topic: str,
        correlation_id: str,
        callback_topic: str | None,
        state: State,
        overrides: OverridesState | None,
        run_args: Sequence[Any] | None,
        deps: dict[str, Any] | None,
    ) -> None:
        """Build and publish one client-originated call envelope.

        Single-sources the wire shape shared by :meth:`_invoke` (*callback_topic*
        is the reply topic) and :meth:`_emit` (*callback_topic* is ``None``, so the
        worker suppresses the terminal point-to-point reply): the lazy
        connect-guard, the ``CallFrame`` push, the ``Envelope`` build, and the
        emitter headers. Callers own dispatcher registration — ``_invoke`` calls
        ``expect()`` *before* this so a reply can never race an unregistered future.
        """
        if not self._connection._connection:
            await self._connection.start()

        call_stack = CallFrameStack()
        call_stack.push(
            CallFrame(
                target_topic=topic,
                callback_topic=callback_topic,
                input_args=run_args,
                overrides=overrides,
            )
        )
        envelope = Envelope(
            internal_workflow_state=WorkflowState(call_stack=call_stack),
            context=SessionRunContext(state=state, deps={} if deps is None else deps),
        )
        await self._connection.publish(
            envelope,
            topic=topic,
            correlation_id=correlation_id,
            headers={HDR_EMITTER: self._emitter_id, HDR_EMITTER_KIND: CLIENT_KIND},
        )

    async def _emit(
        self,
        topic: str,
        correlation_id: str,
        state: State,
        overrides: OverridesState | None = None,
        run_args: Sequence[Any] | None = None,
        deps: dict[str, Any] | None = None,
    ) -> str:
        """Emit a true one-way (fire-and-forget) invocation to a node.

        Mirrors :meth:`_invoke` but allocates **zero** per-call client state and
        triggers **zero** reply traffic: it does not register a reply future with
        the dispatcher, does not build an :class:`InvocationHandle`, and pushes a
        :class:`CallFrame` with ``callback_topic=None`` so the worker suppresses
        the point-to-point reply on the terminal hop. The result still rides the
        target node's ``publish_topic`` broadcast channel for traceability.

        Args:
            topic: Topic to send args to.
            correlation_id: Correlation ID for this request, returned for tracing.
            state: The session state.
            overrides: Runtime overrides (agent tools, model settings).
            run_args: The args to send to the node's run() method.
            deps: Provided dependencies.

        Returns:
            The ``correlation_id`` of the emitted invocation, for tracing.
        """
        logger.debug("[%s] emit topic=%s", correlation_id[:8], topic)
        await self._publish_call(
            topic=topic,
            correlation_id=correlation_id,
            callback_topic=None,
            state=state,
            overrides=overrides,
            run_args=run_args,
            deps=deps,
        )
        return correlation_id

    async def close(self) -> None:
        """Shut down the client gracefully.

        Cancels all pending reply futures via the dispatcher and stops the
        underlying Kafka broker connection. Safe to call multiple times.
        """
        self._dispatcher.close()
        await self._connection.stop()

    async def __aenter__(self) -> Self:
        """Enter the async context manager. Returns ``self``."""
        return self

    async def __aexit__(self, *exc: object) -> None:
        """Exit the async context manager, calling :meth:`close`."""
        await self.close()
