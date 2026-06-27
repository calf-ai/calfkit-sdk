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
from collections.abc import Callable, Iterable
from typing import Any

import uuid_utils
from faststream.kafka import KafkaBroker

from calfkit._protocol import is_topic_safe
from calfkit.client.base import _new_client_emitter_id
from calfkit.client.events import DEFAULT_FIREHOSE_BUFFER_SIZE, EventStream
from calfkit.client.hub import _Hub
from calfkit.client.middleware import ContextInjectionMiddleware, DecodeFloorMiddleware

logger = logging.getLogger(__name__)


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
    ) -> None:
        self._broker = broker
        self._hub = hub
        self._inbox_topic = inbox_topic
        self._emitter_id = emitter_id
        self._firehose_buffer_size = firehose_buffer_size
        self._deps_factory = deps_factory
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
        **broker_kwargs: Any,
    ) -> Client:
        """Build the client — **sync, lazy, no I/O** (spec §2.1/§2.7). Registers the hub's groupless
        reply subscriber + the decode-floor undecodable seam on the inbox; the broker is started by the
        first ``events()`` / dispatch (or a co-located ``Worker``'s ``app.start()``).

        ``inbox_topic`` defaults to an ephemeral per-client name (nothing to provision); set it for a
        durable, shareable inbox (§6). Topic existence is an operational contract — documented, not
        policed here (§2.7). Security is configured the broker's way (a FastStream ``security=`` object
        in ``broker_kwargs``); raw security kwargs are rejected with an actionable error.
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
        broker = KafkaBroker(
            server_list,
            middlewares=[DecodeFloorMiddleware.builder({inbox_topic: hub.fail_run}), ContextInjectionMiddleware],
            **{**producer_posture, **broker_kwargs},
        )
        hub.register(broker, inbox_topic)

        return cls(
            broker,
            hub,
            inbox_topic,
            emitter_id=_new_client_emitter_id(client_id),
            firehose_buffer_size=firehose_buffer_size,
            deps_factory=deps_factory,
        )

    @property
    def inbox_topic(self) -> str:
        """The named topic this client receives its runs' events + terminal replies on (spec §6)."""
        return self._inbox_topic

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
