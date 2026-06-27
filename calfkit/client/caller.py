"""The new caller-surface ``Client`` (spec §2) — built standalone in this module.

This is the redesigned client: a lazy+sync :meth:`Client.connect`, a typed :meth:`agent` gateway
(Commit 5), the per-run handle (Commit 4), and the cross-run :meth:`events` firehose. It owns one
unstarted broker + one :class:`~calfkit.client.hub._Hub` (the single groupless inbox reader, tee'd
to per-run channels and firehose outlets).

It lives **alongside** the shipped ``calfkit.client.base.BaseClient`` / ``client.Client`` until the
Commit-6 cutover repoints ``calfkit.Client`` here and deletes the old surface (so the old client's
reply subscriber and this hub never both consume the inbox during the transition).
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Callable, Iterable, Sequence
from typing import Any, overload

import uuid_utils
from faststream.kafka import KafkaBroker

from calfkit._protocol import CLIENT_KIND, HDR_EMITTER, HDR_EMITTER_KIND, HDR_KIND, HDR_ROUTE, is_topic_safe
from calfkit._routing import is_concrete_route_key
from calfkit._types import OutputT
from calfkit._vendor.pydantic_ai.messages import ModelMessage, ModelRequest
from calfkit._vendor.pydantic_ai.settings import ModelSettings
from calfkit.client._broker import _PreStartHookBroker
from calfkit.client.events import DEFAULT_FIREHOSE_BUFFER_SIZE, EventStream
from calfkit.client.gateway import AgentGateway
from calfkit.client.hub import _Hub
from calfkit.client.middleware import ContextInjectionMiddleware, DecodeFloorMiddleware
from calfkit.models.agents import derive_input_topic
from calfkit.models.envelope import Envelope
from calfkit.models.session_context import CallFrame, CallFrameStack, SessionRunContext, WorkflowState
from calfkit.models.state import OverridesState, State
from calfkit.models.tool_dispatch import ToolBinding, ToolProvider, normalize_tool_bindings
from calfkit.provisioning import ProvisioningConfig, StartupTopicEnsurer

logger = logging.getLogger(__name__)


def _new_client_emitter_id(client_id: str | None = None) -> str:
    """Return a stable ``client.<uuid7-hex>`` emitter id stamped on the ``x-calf-emitter`` header of
    every client publish. Pass *client_id* to reuse an existing hex id (e.g. the inbox's)."""
    return f"client.{client_id if client_id is not None else uuid_utils.uuid7().hex}"


class Client:
    """The caller-side entry point (spec §2.1). Connect once per app, mint a typed gateway per agent.

    ``connect()`` is **lazy and synchronous** — it builds the config, the *unstarted* broker, and the
    hub, and registers the hub's reply subscriber; it does **no I/O**, so a connection failure surfaces
    from the first dispatch / ``events()`` (which brings the broker up), not from ``connect()``.
    """

    def __init__(
        self,
        broker: KafkaBroker,
        hub: _Hub,
        inbox_topic: str,
        *,
        emitter_id: str,
        firehose_buffer_size: int,
        deps_factory: Callable[[], dict[str, Any]] | None,
        provisioning: ProvisioningConfig,
        startup_ensurer: StartupTopicEnsurer,
        server_urls: str | None,
    ) -> None:
        self._broker = broker
        self._hub = hub
        self._inbox_topic = inbox_topic
        self._emitter_id = emitter_id
        self._firehose_buffer_size = firehose_buffer_size
        self._deps_factory = deps_factory
        self._server_urls = server_urls
        # EXPERIMENTAL opt-in topic provisioning (issue #180), kept as a removable unit (see connect()'s
        # _make_provisioned_broker). The co-located Worker reuses these for its own node-topic
        # provisioning (worker.py reads `_provisioning` / `_startup_ensurer`).
        self._provisioning = provisioning
        self._startup_ensurer = startup_ensurer
        # broker.start() is NOT self-idempotent; the base.py:334 check-then-await is non-atomic vs a
        # co-located Worker's app.start(), so guard the first start with a lock (re-check inside).
        self._start_lock = asyncio.Lock()

    @classmethod
    def connect(
        cls,
        server_urls: str | Iterable[str] | None = None,
        *,
        inbox_topic: str | None = None,
        deps_factory: Callable[[], dict[str, Any]] | None = None,
        firehose_buffer_size: int = DEFAULT_FIREHOSE_BUFFER_SIZE,
        provisioning: ProvisioningConfig | None = None,
        **broker_kwargs: Any,
    ) -> Client:
        """Build the client — **sync, lazy, no I/O** (spec §2.1/§2.7). Registers the hub's groupless
        reply subscriber + the decode-floor undecodable seam on the inbox; the broker is started by the
        first ``events()`` / dispatch (or a co-located ``Worker``'s ``app.start()``).

        ``inbox_topic`` defaults to an ephemeral per-client name; set it for a durable, shareable inbox
        (§6). Topic existence is an operational contract — the client never *boot-checks* it (§2.7).
        Security is configured the broker's way (a FastStream ``security=`` object in ``broker_kwargs``);
        raw security kwargs are rejected with an actionable error.

        ``provisioning`` is an **experimental** opt-in (issue #180; default disabled): when enabled,
        topics are auto-created at broker start. It is a separate, removable concern from the §2.7
        boot-check posture — see :meth:`_make_provisioned_broker`.
        """
        if server_urls is None:
            server_urls = os.getenv("CALF_HOST_URL") or "localhost"
        # FastStream wraps a str into [str] and aiokafka never comma-splits inside a list element, so a
        # one-shot iterable is materialized once into a list for the broker.
        server_list = [server_urls] if isinstance(server_urls, str) else list(server_urls)

        rejected_security = [k for k in broker_kwargs if k in ("security_protocol", "ssl_context") or k.startswith(("sasl_plain_", "sasl_mechanism"))]
        if rejected_security:
            raise ValueError(
                f"Client.connect() does not accept raw security kwargs {rejected_security}; configure "
                "security with a FastStream `security=` object (e.g. "
                "`security=faststream.security.SASLPlaintext(username=..., password=...)`), applied to "
                "the producer, consumer, and any admin client."
            )

        client_id = uuid_utils.uuid7().hex
        if inbox_topic is None:
            inbox_topic = f"calf-client-inbox-{client_id}"
        elif not is_topic_safe(inbox_topic):
            raise ValueError(
                f"inbox_topic {inbox_topic!r} is not a valid Kafka topic name "
                "(allowed: letters, digits, '.', '_', '-'; max 249 chars; not '.' or '..')"
            )

        hub = _Hub()
        # The broker hardens the shared producer (acks=all + idempotence) so a broker-acked publish
        # survives leader failover and retries can't duplicate/reorder. Defaults only — a user may
        # override via broker_kwargs. The decode floor is OUTERMOST, carrying the undecodable-sink
        # registry {inbox -> hub.fail_run} (spec §5.8); ContextInjection populates correlation_id.
        producer_posture = {"acks": "all", "enable_idempotence": True}
        middlewares = [DecodeFloorMiddleware.builder({inbox_topic: hub.fail_run}), ContextInjectionMiddleware]
        broker, ensurer, provisioning = cls._make_provisioned_broker(
            server_list, middlewares, {**producer_posture, **broker_kwargs}, inbox_topic, provisioning
        )
        hub.register(broker, inbox_topic)

        return cls(
            broker,
            hub,
            inbox_topic,
            emitter_id=_new_client_emitter_id(client_id),
            firehose_buffer_size=firehose_buffer_size,
            deps_factory=deps_factory,
            provisioning=provisioning,
            startup_ensurer=ensurer,
            server_urls=",".join(server_list),
        )

    @staticmethod
    def _make_provisioned_broker(
        server_list: list[str],
        middlewares: list[Any],
        broker_kwargs: dict[str, Any],
        inbox_topic: str,
        provisioning: ProvisioningConfig | None,
    ) -> tuple[KafkaBroker, StartupTopicEnsurer, ProvisioningConfig]:
        """EXPERIMENTAL opt-in topic provisioning (issue #180), isolated here as a **removable unit**.

        To drop provisioning later: delete this method, the ``provisioning=`` param, and the
        ``_provisioning`` / ``_startup_ensurer`` fields, then have ``connect()`` build a plain
        ``KafkaBroker(server_list, middlewares=middlewares, **broker_kwargs)``. The co-located ``Worker``
        reuses ``_startup_ensurer`` + ``_provisioning`` for its own node-topic provisioning (``worker.py``),
        so removing it here is a coordinated change with the Worker.
        """
        provisioning = provisioning or ProvisioningConfig()
        ensurer = StartupTopicEnsurer(config=provisioning)
        ensurer.declare([inbox_topic], framework=True)  # the inbox is a framework topic
        # _PreStartHookBroker runs the ensurer at start (FastStream 0.7.x has no broker-level start hook).
        broker = _PreStartHookBroker(server_list, middlewares=middlewares, pre_start=ensurer.run, **broker_kwargs)
        return broker, ensurer, provisioning

    @property
    def inbox_topic(self) -> str:
        """The named topic this client receives its runs' events + terminal replies on (spec §6)."""
        return self._inbox_topic

    @property
    def broker(self) -> KafkaBroker:
        """The underlying ``KafkaBroker`` (shared with a co-located ``Worker``)."""
        return self._broker

    @property
    def _connection(self) -> KafkaBroker:
        # Worker-compat alias for the broker: the Worker reads ``client._connection`` to register node
        # subscribers/publishers on the shared broker. (Distinct from ``broker._connection``, the
        # broker's own started-indicator.)
        return self._broker

    @property
    def server_urls(self) -> str | None:
        """The bootstrap server URL(s), comma-joined; ``None`` for a directly-constructed client."""
        return self._server_urls

    # Four overloads (spec §2.2): a dedicated no-output_type form per address so the str default is
    # actually bound — a `= str` parameter default does NOT bind a TypeVar (OutT would fall back to Any).
    @overload
    def agent(self, name: str) -> AgentGateway[str]: ...
    @overload
    def agent(self, name: str, *, output_type: type[OutputT]) -> AgentGateway[OutputT]: ...
    @overload
    def agent(self, *, topic: str) -> AgentGateway[str]: ...
    @overload
    def agent(self, *, topic: str, output_type: type[OutputT]) -> AgentGateway[OutputT]: ...
    def agent(self, name: str | None = None, *, topic: str | None = None, output_type: type[Any] = str) -> AgentGateway[Any]:
        """Mint a typed gateway to **one** destination (spec §2.2), addressed **exactly one** of two
        ways: by ``name`` (a deployed agent — derives its Private input topic, ADR-0017) or by
        ``topic=`` (the escape hatch for a non-derived topic). ``output_type`` binds once at mint
        (default ``str`` — extract the text reply; a structured-output agent requires
        ``output_type=Model``, else a ``DataPart`` reply is a loud ``DeserializationError``)."""
        if name is not None and topic is not None:
            raise ValueError("agent() takes exactly one of `name` (a deployed agent) or `topic=` (the escape hatch), not both.")
        if name is not None:
            resolved = derive_input_topic(name)
        elif topic is not None:
            resolved = topic
        else:
            raise ValueError("agent() requires exactly one of `name` (a deployed agent) or `topic=` (the escape hatch).")
        return AgentGateway(self, resolved, output_type)

    def events(self, *, terminal_only: bool = False) -> EventStream:
        """The cross-run firehose (spec §3.2) over this client's one configured inbox — every reply on
        it while open, demuxed by the caller. Best-effort (bounded drop-oldest, ``firehose_buffer_size``);
        for guaranteed delivery hold the run's handle or run a ``@consumer`` node. Entering the stream
        brings the broker up if it isn't already (a pure-observer client's first use)."""
        return EventStream(
            self._hub,
            terminal_only=terminal_only,
            buffer_size=self._firehose_buffer_size,
            on_enter=self._ensure_started,
        )

    async def _ensure_started(self) -> None:
        """Bring the shared broker up once, idempotently. ``broker.start()`` is not self-idempotent and
        the check-then-await is non-atomic vs a co-located ``Worker``'s ``app.start()``, so guard it
        with a lock and re-check the started flag inside (spec §2.7 / plan §6)."""
        if self._broker._connection:  # fast path: already started (by us, a publish, or a Worker)
            return
        async with self._start_lock:
            if self._broker._connection:  # re-check inside the lock — closes the concurrent-start race
                return
            await self._broker.start()

    async def aclose(self) -> None:
        """Graceful shutdown (spec §5.8): resolve every pending ``result()`` with ``ClientClosedError``,
        then stop the broker's reader."""
        self._hub.close()
        if self._broker._connection:
            await self._broker.stop()

    async def __aenter__(self) -> Client:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    # ── shared verb machinery (called by AgentGateway.send/start/execute) ──

    def _merge_deps(self, deps: dict[str, Any] | None) -> dict[str, Any] | None:
        """Merge per-call ``deps`` **over** the ambient ``deps_factory`` seed (spec §2.3)."""
        if self._deps_factory is None:
            return deps
        merged = self._deps_factory()
        if deps:
            merged.update(deps)
        return merged

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
        """Shape the per-call input: default ``correlation_id`` to a fresh uuid7, build the ``State``
        from the prompt + history (stamping ``author``), and build the ``OverridesState`` (or ``None``).
        **No ``model_settings`` JSON pre-flight** (dropped per spec §2.5): a non-serializable ``deps`` /
        ``model_settings`` bubbles from ``publish``, not a call-site check."""
        if correlation_id is None:
            correlation_id = uuid_utils.uuid7().hex
        state = State(message_history=message_history or [], temp_instructions=temp_instructions)
        state.stage_message(ModelRequest.user_text_prompt(user_prompt, name=author))
        overrides = (
            OverridesState(
                override_agent_tools=normalize_tool_bindings(tool_overrides) if tool_overrides is not None else None,
                model_settings=dict(model_settings) if model_settings is not None else None,
            )
            if tool_overrides is not None or model_settings is not None
            else None
        )
        return correlation_id, state, overrides

    async def _publish_call(
        self,
        *,
        topic: str,
        correlation_id: str,
        state: State,
        overrides: OverridesState | None,
        deps: dict[str, Any] | None,
        route: str | None = None,
        body: Any | None = None,
    ) -> None:
        """Publish ONE call envelope to *topic* (spec §2.6): an ``Envelope`` carrying the session
        ``State`` + ``deps`` and a pushed ``CallFrame`` whose ``callback_topic`` is this client's inbox,
        plus the emitter headers + ``x-calf-kind=call``. Keyed by ``correlation_id``.

        ``route`` / ``body`` are an **internal, non-public lower-level surface** (NOT the agent gateway,
        spec §9.2): the gateway verbs never pass them. They let framework-internal callers target a
        ``@handler`` route (``x-calf-route``) or carry a typed run ``payload`` — left unprioritized/hidden.
        """
        if route is not None and not is_concrete_route_key(route):
            raise ValueError(
                f"producer route {route!r} must be a concrete key — non-empty, '.'-delimited words, no "
                "empty segments, no wildcard. ('*' is a route pattern for @handler, not a producer key.)"
            )
        call_stack = CallFrameStack()
        call_stack.push(CallFrame(target_topic=topic, callback_topic=self._inbox_topic, overrides=overrides, payload=body))
        envelope = Envelope(
            internal_workflow_state=WorkflowState(call_stack=call_stack),
            context=SessionRunContext(state=state, deps={} if deps is None else deps),
        )
        headers = {HDR_EMITTER: self._emitter_id, HDR_EMITTER_KIND: CLIENT_KIND, HDR_KIND: "call"}
        if route is not None:
            headers[HDR_ROUTE] = route
        await self._broker.publish(envelope, topic=topic, correlation_id=correlation_id, headers=headers)
