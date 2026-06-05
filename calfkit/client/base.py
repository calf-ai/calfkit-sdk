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
from calfkit.provisioning import ProvisioningConfig, TopicProvisioner

logger = logging.getLogger(__name__)

# Raw aiokafka connection kwargs that are security-relevant for topic
# provisioning but which ``KafkaBroker`` does not itself accept (FastStream
# sources these from the ``security=`` object). They are captured for the admin
# client and stripped from what is forwarded to ``KafkaBroker``. The
# ``sasl_kerberos_*`` / ``sasl_oauth_token_provider`` kwargs are intentionally
# NOT listed here: ``KafkaBroker`` does accept those, so they continue to flow
# to the broker while also being captured for provisioning.
_BROKER_ONLY_SECURITY_PREFIXES = ("sasl_plain_", "sasl_mechanism")
_RAW_SECURITY_KEYS = frozenset({"security_protocol", "ssl_context"})


def _is_security_kwarg(key: str) -> bool:
    """Return ``True`` if *key* is a security-relevant connection kwarg.

    Covers the FastStream ``security=`` object and the raw aiokafka kwargs
    (``security_protocol``, ``ssl_context``, and any ``sasl_*``) that the topic
    provisioner forwards to the admin client.
    """
    return key == "security" or key in _RAW_SECURITY_KEYS or key.startswith("sasl_")


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
        *,
        provisioning: ProvisioningConfig | None = None,
        server_urls: str | Iterable[str] | None = None,
        security_kwargs: dict[str, Any] | None = None,
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
            provisioning: Opt-in Kafka topic auto-creation config. When ``None``
                (the default) it is normalized to a disabled
                :class:`~calfkit.provisioning.ProvisioningConfig`, so
                ``self._provisioning`` is never ``None`` and ``.enabled`` is
                always safe to read.
            server_urls: The Kafka bootstrap server URL(s) this client connected
                to. Captured so the topic provisioner can reach the same broker.
            security_kwargs: Security-relevant connection kwargs (the
                FastStream ``security=`` object plus raw ``sasl_*`` /
                ``ssl_context`` / ``security_protocol``) captured from
                ``connect`` and forwarded to the admin client when provisioning.
        """
        if emitter_id is not None and not emitter_id.strip():
            raise ValueError("emitter_id must be a non-empty string or None")
        self._connection = connection
        self._reply_topic = reply_topic
        self._dispatcher = dispatcher
        self._emitter_id = emitter_id if emitter_id is not None else _new_client_emitter_id()
        # Never None: a disabled config makes `.enabled` always safe to read.
        self._provisioning = provisioning or ProvisioningConfig()
        self._server_urls = server_urls
        self._security_kwargs: dict[str, Any] = dict(security_kwargs) if security_kwargs else {}
        # One-shot guard: the reply topic is provisioned at most once across the
        # lifetime of this client, regardless of how many times `_invoke` runs.
        self._reply_topic_provisioned = False

    @classmethod
    def connect(
        cls,
        server_urls: str | Iterable[str] | None = None,
        reply_topic: str | None = None,
        *,
        provisioning: ProvisioningConfig | None = None,
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
            provisioning: Opt-in Kafka topic auto-creation config. When enabled,
                the client provisions its reply topic on first invocation.
                Defaults to ``None``, which is normalized to a disabled config
                (a no-op). See :class:`~calfkit.provisioning.ProvisioningConfig`
                for the dev-safe / review-for-prod caveats.
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

        # Capture security-relevant kwargs for the topic provisioner's admin
        # client. Raw `sasl_plain_*` / `sasl_mechanism` / `security_protocol` /
        # `ssl_context` are sourced by FastStream from the `security=` object,
        # so `KafkaBroker` does not accept them as kwargs — capture them for
        # provisioning but strip them from what is forwarded to the broker.
        security_kwargs: dict[str, Any] = {}
        broker_forwarded: dict[str, Any] = {}
        for key, value in broker_kwargs.items():
            if _is_security_kwarg(key):
                security_kwargs[key] = value
                broker_rejects = key in _RAW_SECURITY_KEYS or key.startswith(_BROKER_ONLY_SECURITY_PREFIXES)
                if broker_rejects:
                    continue
            broker_forwarded[key] = value

        broker_connection = KafkaBroker(
            server_urls,
            middlewares=[ContextInjectionMiddleware],
            **broker_forwarded,
        )

        dispatcher = _ReplyDispatcher()
        dispatcher.register(broker_connection, reply_topic, group_id)

        return cls(
            broker_connection,
            reply_topic,
            dispatcher,
            emitter_id=_new_client_emitter_id(client_id),
            provisioning=provisioning,
            server_urls=server_urls,
            security_kwargs=security_kwargs,
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
    def provisioning(self) -> ProvisioningConfig:
        """The Kafka topic provisioning config (never ``None``; disabled by default)."""
        return self._provisioning

    @property
    def server_urls(self) -> str | Iterable[str] | None:
        """The Kafka bootstrap server URL(s) this client connected to."""
        return self._server_urls

    @property
    def security_kwargs(self) -> dict[str, Any]:
        """Security-relevant connection kwargs captured for topic provisioning.

        Includes the FastStream ``security=`` object (under the ``security`` key)
        and any raw ``sasl_*`` / ``ssl_context`` / ``security_protocol`` kwargs
        passed to :meth:`connect`. A defensive copy is returned.
        """
        return dict(self._security_kwargs)

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

        if not self._connection._connection:
            # Best-effort, opt-in reply-topic provisioning. This is a dev-safe
            # convenience (rf=1, no ACLs) — review before relying on it in
            # production, where topics are typically ops-governed. See
            # ProvisioningConfig for the full caveats. Provision exactly ONCE,
            # before the broker connects, so the reply consumer has its inbox.
            if self._provisioning.enabled and not self._reply_topic_provisioned:
                self._reply_topic_provisioned = True
                await self._provision_reply_topic()
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
            context=SessionRunContext(state=state, deps={} if deps is None else deps),
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

    async def _provision_reply_topic(self) -> None:
        """Best-effort creation of this client's reply topic via the admin client.

        The reply topic is a framework inbox (correlation-keyed request/reply
        traffic), so it is passed in ``framework_topics`` to ensure user
        ``topic_configs`` (retention / compaction) are never applied to it.
        """
        provisioner = TopicProvisioner.from_connection(
            server_urls=self._server_urls,
            config=self._provisioning,
            security_kwargs=self._security_kwargs,
        )
        report = await provisioner.provision(
            [self._reply_topic],
            framework_topics={self._reply_topic},
        )
        logger.info(
            "provisioned topics: %d created, %d existing, %d unauthorized",
            len(report.created),
            len(report.existing),
            len(report.unauthorized),
        )
        if report.unauthorized:
            logger.warning(
                "topic provisioning: %d unauthorized topic(s) NOT created "
                "(producers/consumers will stall): %s",
                len(report.unauthorized),
                ", ".join(report.unauthorized),
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
