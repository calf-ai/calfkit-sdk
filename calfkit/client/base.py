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
from calfkit.client.kafka_config import KafkaConfig
from calfkit.client.middleware import ContextInjectionMiddleware
from calfkit.client.reply_dispatcher import _ReplyDispatcher
from calfkit.models import State
from calfkit.models.envelope import Envelope
from calfkit.models.session_context import (
    CallFrame,
    CallFrameStack,
    Deps,
    SessionRunContext,
    WorkflowState,
)
from calfkit.models.state import OverridesState
from calfkit.nodes.aggregator._partitioner import FanOutAggregatorPartitioner, has_composite_delimiter

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

    def __init__(
        self,
        connection: KafkaBroker,
        reply_topic: str,
        dispatcher: _ReplyDispatcher,
        emitter_id: str | None = None,
        kafka_config: KafkaConfig | None = None,
    ) -> None:
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
            kafka_config: Snapshot of the Kafka client kwargs (bootstrap
                servers + SASL/SSL/security settings) used to build the
                ``KafkaBroker``. The :class:`~calfkit.worker.worker.Worker`
                threads this forward to the fan-out aggregator's state
                store so its transient :class:`AIOKafkaConsumer`
                (rehydration only) shares the broker's auth/transport
                configuration. ``None`` for backwards compatibility with
                callers that construct ``BaseClient`` directly (tests);
                in that case, :class:`~calfkit.worker.worker.Worker`
                raises if it needs the config.
        """
        if emitter_id is not None and not emitter_id.strip():
            raise ValueError("emitter_id must be a non-empty string or None")
        self._connection = connection
        self._reply_topic = reply_topic
        self._dispatcher = dispatcher
        self._emitter_id = emitter_id if emitter_id is not None else _new_client_emitter_id()
        self._kafka_config = kafka_config

    @classmethod
    def connect(
        cls,
        server_urls: str | Iterable[str] | None = None,
        reply_topic: str | None = None,
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

        if "partitioner" in broker_kwargs:
            raise ValueError(
                "Calfkit installs its own partitioner (FanOutAggregatorPartitioner) "
                "to preserve co-partitioning across fan-out aggregator topics. "
                "Remove the 'partitioner' kwarg."
            )

        # Snapshot kwargs BEFORE constructing the broker so the aggregator's
        # transient AIOKafkaConsumer can reuse the same security/SASL/SSL
        # config. dict(...) copies; the splat into KafkaBroker(...) below
        # is unaffected. Narrow Iterable[str] to list[str] so KafkaConfig's
        # str | list[str] field type is honoured.
        bootstrap_for_config: str | list[str]
        bootstrap_for_config = server_urls if isinstance(server_urls, str) else list(server_urls)
        kafka_config = KafkaConfig(
            bootstrap_servers=bootstrap_for_config,
            client_kwargs=dict(broker_kwargs),
        )

        broker_connection = KafkaBroker(
            server_urls,
            middlewares=[ContextInjectionMiddleware],
            partitioner=FanOutAggregatorPartitioner(),
            **broker_kwargs,
        )

        dispatcher = _ReplyDispatcher()
        dispatcher.register(broker_connection, reply_topic, group_id)

        return cls(
            broker_connection,
            reply_topic,
            dispatcher,
            emitter_id=_new_client_emitter_id(client_id),
            kafka_config=kafka_config,
        )

    @property
    def broker(self) -> KafkaBroker:
        """The underlying ``KafkaBroker`` connection."""
        return self._connection

    @property
    def reply_topic(self) -> str:
        """The Kafka topic this client subscribes to for receiving replies."""
        return self._reply_topic

    @property
    def kafka_config(self) -> KafkaConfig | None:
        """Snapshot of the Kafka client kwargs used to construct the broker.

        Captured by :meth:`connect` (the public factory). ``None`` when the
        client was constructed directly via :meth:`__init__` without an
        explicit ``kafka_config`` -- typically tests. The
        :class:`~calfkit.worker.worker.Worker` uses this to thread
        bootstrap + security settings into the fan-out aggregator's
        state store and raises if it's ``None`` when needed.
        """
        return self._kafka_config

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
        if has_composite_delimiter(correlation_id):
            raise ValueError(
                f"correlation_id must not contain '|' (fan-out aggregator composite-key delimiter): {correlation_id!r}"
            )

        future = self._dispatcher.expect(correlation_id)

        logger.debug("[%s] invoke topic=%s reply=%s", correlation_id[:8], topic, reply_topic)

        if not self._connection._connection:
            await self._connection.start()

        call_stack = CallFrameStack()
        call_stack.push(
            CallFrame(
                target_topic=topic,
                callback_topic=reply_topic,
                input_args=run_args,
                overrides=overrides,
            )
        )

        envelope = Envelope(
            internal_workflow_state=WorkflowState(call_stack=call_stack),
            context=SessionRunContext(state=state, deps=Deps(correlation_id=correlation_id, provided_deps=deps or dict())),
        )
        await self._connection.publish(
            envelope,
            topic=topic,
            correlation_id=correlation_id,
            headers={HDR_EMITTER: self._emitter_id, HDR_EMITTER_KIND: CLIENT_KIND},
        )

        return InvocationHandle(
            correlation_id=correlation_id,
            topic=topic,
            reply_topic=reply_topic,
            _future=future,
            _output_type=output_type,
        )

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
