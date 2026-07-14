"""A ``KafkaBroker`` with a one-shot pre-start hook (issue #180).

FastStream 0.7.x has no broker-level startup hook, and a bare
``client.broker.start()`` has no FastStream app to host ``on_startup`` hooks.
``KafkaBroker.start()`` is the single choke point every start path funnels
through (bare ``broker.start()``, ``Worker.run()``/``start()``/``async with``,
``ck run``, and auto-start on first publish). This subclass adds a generic
seam there: run an injected async hook **after** ``connect()`` (so the broker's
admin client and the cluster are reachable) and **before** subscribers begin
consuming.

It is deliberately generic — it knows nothing about provisioning. The calfkit
client wires a :class:`~calfkit.provisioning.StartupTopicEnsurer` as the hook.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from types import TracebackType
from typing import Any

from faststream.kafka import KafkaBroker

from calfkit._faststream_ext import KeyOrderedRegistratorMixin
from calfkit.client._mesh_url import DEFAULT_MESH_URL

PreStartHook = Callable[[KafkaBroker], Awaitable[None]]


class _PreStartHookBroker(KeyOrderedRegistratorMixin, KafkaBroker):
    """``KafkaBroker`` that runs ``pre_start(self)`` once, after ``connect()`` and
    before ``super().start()`` starts the subscribers.

    The hook receives the broker so it can reach the broker-managed admin client
    and the registered subscribers without the broker holding any extra state.

    Also composes the standalone key-ordered dispatch extension's registration mixin
    (``key_ordered_subscriber()`` — additive; the stock ``subscriber()`` builder is
    untouched), which the worker uses for caller-capable nodes.
    """

    def __init__(
        self,
        bootstrap_servers: str | Iterable[str] = DEFAULT_MESH_URL,
        *,
        pre_start: PreStartHook | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(bootstrap_servers, **kwargs)
        self._pre_start = pre_start
        self._pre_start_done = False

    async def start(self) -> None:
        # connect() is idempotent; calling it here ensures the broker's admin
        # client exists for the hook before subscribers start consuming.
        await self.connect()
        if self._pre_start is not None and not self._pre_start_done:
            await self._pre_start(self)
            # Mark done only AFTER the hook succeeds: a hook that RAISES must be
            # retried on the next start() — silently skipping provisioning would
            # re-introduce the #180 hang. A concurrent double-run is harmless
            # (topic provisioning is idempotent), and start() is not a
            # concurrent-call surface anyway.
            self._pre_start_done = True
        await super().start()

    async def stop(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: TracebackType | None = None,
    ) -> None:
        await super().stop(exc_type, exc_val, exc_tb)
        # A fresh start() after stop() must re-run the hook: the topics could
        # have been deleted (or the in-memory broker restarted) in between.
        self._pre_start_done = False
