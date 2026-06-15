import logging
import os
from collections.abc import Iterable
from typing import Any

import uuid_utils
from faststream.kafka import KafkaBroker
from typing_extensions import Self

from calfkit._protocol import CLIENT_KIND, HDR_EMITTER, HDR_EMITTER_KIND, HDR_KIND, HDR_ROUTE, is_topic_safe
from calfkit._routing import is_concrete_route_key
from calfkit.client._broker import _PreStartHookBroker
from calfkit.client.invocation_handle import InvocationHandle
from calfkit.client.middleware import ContextInjectionMiddleware
from calfkit.client.reply_dispatcher import _ReplyDispatcher
from calfkit.models import State
from calfkit.models.envelope import Envelope
from calfkit.models.node_result import _UNSET
from calfkit.models.session_context import (
    CallFrame,
    CallFrameStack,
    SessionRunContext,
    WorkflowState,
)
from calfkit.models.state import OverridesState
from calfkit.provisioning import ProvisioningConfig, StartupTopicEnsurer

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
    should build higher-level invocation methods on top of :meth:`_start`.

    Supports use as an async context manager for automatic cleanup::

        async with Client.connect("localhost:9092") as client:
            result = await client.execute(...)
    """

    def __init__(
        self,
        connection: KafkaBroker,
        reply_topic: str,
        dispatcher: _ReplyDispatcher,
        emitter_id: str | None = None,
        *,
        provisioning: ProvisioningConfig | None = None,
        startup_ensurer: StartupTopicEnsurer | None = None,
        server_urls: str | None = None,
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
            startup_ensurer: The topic ensurer wired as the broker's pre-start
                hook (see :meth:`connect`). When ``None``, a fresh empty one is
                created from *provisioning*. NOTE: provisioning only actually
                runs when the broker is the ``connect()``-built
                ``_PreStartHookBroker`` (whose ``start()`` invokes the hook);
                constructing a client directly with a plain ``KafkaBroker``
                leaves provisioning a no-op even when enabled.
        """
        if emitter_id is not None and not emitter_id.strip():
            raise ValueError("emitter_id must be a non-empty string or None")
        self._connection = connection
        self._reply_topic = reply_topic
        self._dispatcher = dispatcher
        self._emitter_id = emitter_id if emitter_id is not None else _new_client_emitter_id()
        # Never None: a disabled config makes `.enabled` always safe to read.
        self._provisioning = provisioning or ProvisioningConfig()
        # Startup topic ensurer: declares + (when provisioning is enabled)
        # creates this client's inbox topics at broker start, before any
        # subscriber consumes. ``connect`` builds it with the reply topic already
        # declared; direct construction gets a fresh, empty one.
        self._startup_ensurer = startup_ensurer if startup_ensurer is not None else StartupTopicEnsurer(config=self._provisioning)
        # Retained for zero-config control-plane consumers (e.g. MCP capability
        # discovery): the one URL the user provided, normalized. None when the
        # client was hand-built rather than created via connect().
        self._server_urls = server_urls

    @classmethod
    def connect(
        cls,
        server_urls: str | Iterable[str] | None = None,
        reply_topic: str | None = None,
        reply_ttl: float | None = None,
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
            reply_ttl: Optional seconds after which an un-answered reply future
                (from :meth:`start` / :meth:`execute`) is evicted with
                a :class:`~calfkit.exceptions.ReplyExpiredError`. ``None``
                (default) disables eviction entirely — a deliberate
                caller-responsibility choice, not a default safety ceiling.
                Callers who need a bounded pending map under lost replies or
                abandoned handles must opt in by setting a TTL.
            provisioning: **Experimental** (may change or be removed in a minor
                release; calfkit is pre-1.0). Opt-in Kafka topic auto-creation
                config. When enabled, the client creates its reply topic at
                broker start (before the reply consumer subscribes), reusing the
                broker's admin client. Defaults to ``None``, which is normalized
                to a disabled config (a no-op). See
                :class:`~calfkit.provisioning.ProvisioningConfig` for the
                dev-safe / review-for-prod caveats.
            **broker_kwargs: Additional keyword arguments forwarded to
                ``KafkaBroker`` (e.g. ``security``, ``client_id``). Configure
                broker/admin security with a FastStream ``security=`` object.
                The shared producer defaults to ``acks="all"`` +
                ``enable_idempotence=True`` (durable, non-duplicating publishes);
                pass either here to override.

        Returns:
            A new client instance ready for use. Call ``broker.start()`` or use
            the client as an async context manager before invoking nodes.
        """
        if server_urls is None:
            server_urls = os.getenv("CALF_HOST_URL") or "localhost"
        # Materialize ONCE into a list (a one-shot iterable must not be drained
        # twice), and keep the two consumers' forms straight:
        # - the broker gets the LIST — FastStream wraps a str into [str], and
        #   aiokafka never comma-splits inside a list element, so a joined
        #   string here would mangle multi-host into one bad address;
        # - the retained property is the comma-joined STRING — correct for
        #   consumers that hand it to aiokafka directly (which splits strings),
        #   e.g. the ktables control plane.
        server_list = [server_urls] if isinstance(server_urls, str) else list(server_urls)

        # Raw security kwargs were accepted pre-#180 (for calfkit's own admin
        # client); now all kwargs flow straight to KafkaBroker, which rejects
        # these. Fail with an actionable migration message instead of a cryptic
        # "unexpected keyword argument" TypeError from KafkaBroker.
        rejected_security = [k for k in broker_kwargs if k in ("security_protocol", "ssl_context") or k.startswith(("sasl_plain_", "sasl_mechanism"))]
        if rejected_security:
            raise ValueError(
                f"Client.connect() no longer accepts raw security kwargs {rejected_security}; "
                "configure security with a FastStream `security=` object "
                "(e.g. `security=faststream.security.SASLPlaintext(username=..., password=...)`), "
                "which is applied to the producer, consumer, and the admin client used for provisioning."
            )

        client_id = uuid_utils.uuid7().hex
        if reply_topic is None:
            reply_topic = f"calf-client-reply-{client_id}"
        elif not is_topic_safe(reply_topic):
            # The reply topic is the wire callback on every start()/execute()
            # frame and this client's own subscription — same legality rule,
            # same loud client-side rejection as send(reply_to=...).
            raise ValueError(
                f"reply_topic {reply_topic!r} is not a valid Kafka topic name "
                "(allowed: letters, digits, '.', '_', '-'; max 249 chars; not '.' or '..')"
            )
        group_id = f"calf-client-reply-{client_id}"

        # The reply topic is a framework inbox: declare it (framework=True so
        # user topic_configs never apply) into the startup ensurer, wired as the
        # broker's one-shot pre-start hook. The ensurer creates it — when
        # provisioning is enabled — after connect() and before the reply consumer
        # subscribes, on every start path (issue #180). Provisioning reuses the
        # broker's own admin client, so security is configured the FastStream way
        # (a ``security=`` object in ``broker_kwargs``) — no separate capture.
        provisioning = provisioning or ProvisioningConfig()
        ensurer = StartupTopicEnsurer(config=provisioning)
        ensurer.declare([reply_topic], framework=True)

        # calfkit hardens the shared producer: acks=all + idempotence, so a
        # broker-acked publish survives leader failover and producer retries
        # can't duplicate or reorder (fault-rail spec §13 — the fault rail's
        # escalation hops and the in-node fan-out re-entry self-publish both
        # depend on it). These are defaults only: a user may override either via
        # ``Client.connect(**broker_kwargs)`` (document, don't police).
        producer_posture = {"acks": "all", "enable_idempotence": True}
        broker_connection = _PreStartHookBroker(
            server_list,
            middlewares=[ContextInjectionMiddleware],
            pre_start=ensurer.run,
            **{**producer_posture, **broker_kwargs},
        )

        dispatcher = _ReplyDispatcher(reply_ttl=reply_ttl)
        dispatcher.register(broker_connection, reply_topic, group_id)

        return cls(
            broker_connection,
            reply_topic,
            dispatcher,
            emitter_id=_new_client_emitter_id(client_id),
            provisioning=provisioning,
            startup_ensurer=ensurer,
            server_urls=",".join(server_list),
        )

    @property
    def broker(self) -> KafkaBroker:
        """The underlying ``KafkaBroker`` connection."""
        return self._connection

    @property
    def server_urls(self) -> str | None:
        """The bootstrap server URL(s) this client was connected with.

        Normalized to a comma-joined string. ``None`` for clients built
        directly via ``__init__`` rather than :meth:`connect`.
        """
        return self._server_urls

    @property
    def reply_topic(self) -> str:
        """The Kafka topic this client subscribes to for receiving replies."""
        return self._reply_topic

    @property
    def provisioning(self) -> ProvisioningConfig:
        """The Kafka topic provisioning config (never ``None``; disabled by default)."""
        return self._provisioning

    async def _start(
        self,
        topic: str,
        correlation_id: str,
        state: State,
        overrides: OverridesState | None = None,
        deps: dict[str, Any] | None = None,
        route: str | None = None,
        body: Any | None = None,
        output_type: type[Any] = _UNSET,
    ) -> InvocationHandle:
        """Start a node invocation asynchronously and register its reply future.

        The wire callback is always ``self._reply_topic`` — the one topic the
        dispatcher consumes, and therefore the only address whose reply future
        can ever resolve. There is deliberately no parameter for it: a foreign
        callback with a registered future is the dangling-future bug the
        send/start/execute redesign removed (ADR-0005).

        Args:
            topic: Topic to send args to.
            correlation_id: Correlation ID for this request.
            state: The session state.
            deps: Provided dependencies.

        Returns:
            An invocation handle with an associated future for the reply.
        """
        future = self._dispatcher.expect(correlation_id)
        logger.debug("[%s] start topic=%s reply=%s", correlation_id[:8], topic, self._reply_topic)
        await self._publish_call(
            topic=topic,
            correlation_id=correlation_id,
            callback_topic=self._reply_topic,
            state=state,
            overrides=overrides,
            deps=deps,
            route=route,
            body=body,
        )
        return InvocationHandle(
            correlation_id=correlation_id,
            topic=topic,
            reply_topic=self._reply_topic,
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
        deps: dict[str, Any] | None,
        route: str | None = None,
        body: Any | None = None,
    ) -> None:
        """Build and publish one client-originated call envelope.

        Single-sources the wire shape shared by :meth:`_start` (*callback_topic*
        is the client's reply inbox) and :meth:`_send` (*callback_topic* is the
        caller's ``reply_to`` or ``None``, in which case the worker suppresses the
        terminal point-to-point reply): the lazy connect-guard, the ``CallFrame``
        push, the ``Envelope`` build, and the emitter headers. Callers own
        dispatcher registration — ``_start`` calls ``expect()`` *before* this so a
        reply can never race an unregistered future.
        """
        if route is not None and not is_concrete_route_key(route):
            raise ValueError(
                f"producer route {route!r} must be a concrete key — non-empty, '.'-delimited words, no empty "
                "segments, no wildcard. ('*' is a route pattern for @handler, not a producer route key.)"
            )
        # A routeless ``body`` is allowed — it rides ``CallFrame.payload`` and is read by
        # the target node's inherited ``@handler('*')`` ``run`` when that ``run`` declares a schema.
        if not self._connection._connection:
            # First publish before an explicit start(): bring the broker up. The
            # broker's pre-start hook (the startup ensurer) provisions the reply
            # topic before the reply consumer subscribes — see ``connect``.
            await self._connection.start()

        call_stack = CallFrameStack()
        call_stack.push(
            CallFrame(
                target_topic=topic,
                callback_topic=callback_topic,
                overrides=overrides,
                payload=body,
            )
        )
        envelope = Envelope(
            internal_workflow_state=WorkflowState(call_stack=call_stack),
            context=SessionRunContext(state=state, deps={} if deps is None else deps),
        )
        headers = {HDR_EMITTER: self._emitter_id, HDR_EMITTER_KIND: CLIENT_KIND, HDR_KIND: "call"}
        if route is not None:
            headers[HDR_ROUTE] = route
        await self._connection.publish(
            envelope,
            topic=topic,
            correlation_id=correlation_id,
            headers=headers,
        )

    async def _send(
        self,
        topic: str,
        correlation_id: str,
        state: State,
        overrides: OverridesState | None = None,
        deps: dict[str, Any] | None = None,
        route: str | None = None,
        body: Any | None = None,
        reply_to: str | None = None,
    ) -> str:
        """Send a one-way invocation to a node, with an optional return address.

        Mirrors :meth:`_start` but allocates **zero** per-call client state: it
        does not register a reply future with the dispatcher and does not build
        an :class:`InvocationHandle`. The pushed :class:`CallFrame` carries
        *reply_to* as its ``callback_topic``: ``None`` (the default) makes the
        worker suppress the point-to-point reply on the terminal hop entirely; a
        topic name makes the worker deliver the terminal result there — a return
        address for **someone else** to consume, never this client. Either way
        the result still rides the target node's ``publish_topic`` broadcast
        channel for traceability.

        Args:
            topic: Topic to send args to.
            correlation_id: Correlation ID for this request, returned for tracing.
            state: The session state.
            overrides: Runtime overrides (agent tools, model settings).
            deps: Provided dependencies.
            reply_to: Optional topic the terminal result is delivered to,
                point-to-point. ``None`` suppresses the terminal callback.

        Returns:
            The ``correlation_id`` of the sent invocation, for tracing.

        Raises:
            ValueError: If *reply_to* is blank, or is this client's own reply
                inbox (no future is registered, so the reply would arrive at the
                dispatcher and be dropped — use ``start``/``execute`` to await).
        """
        if reply_to is not None:
            if not reply_to.strip():
                raise ValueError("reply_to must be a non-empty topic name or None")
            if not is_topic_safe(reply_to):
                raise ValueError(
                    f"reply_to {reply_to!r} is not a valid Kafka topic name (allowed: letters, digits, '.', '_', '-'; max 249 chars; not '.' or '..')"
                )
            if reply_to == self._reply_topic:
                raise ValueError(
                    "reply_to is this client's own reply inbox; send() registers no future, "
                    "so the reply would arrive and be dropped ('no pending future'). "
                    "Use start()/execute() to await a reply."
                )
        logger.debug("[%s] send topic=%s reply_to=%s", correlation_id[:8], topic, reply_to)
        await self._publish_call(
            topic=topic,
            correlation_id=correlation_id,
            callback_topic=reply_to,
            state=state,
            overrides=overrides,
            deps=deps,
            route=route,
            body=body,
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
