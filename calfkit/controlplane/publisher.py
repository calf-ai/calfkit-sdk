"""Write side of the control-plane substrate: the worker-owned publisher (spec §7).

One publisher per worker runs **one** heartbeat loop and **one** ordered tombstone
pass for every hosted node's adverts (ADR-0011). Startup is **fail-loud** (a publish
failure aborts boot); loop ticks are **per-advert resilient**. Content is *pulled*
from each advert factory every tick (pull-only); all writes funnel through here, so
a future ``publish_now()`` push plugs into the same shutdown-flag serialization.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from calfkit.controlplane.records import ControlPlaneIdentity

if TYPE_CHECKING:
    from pydantic import AwareDatetime

    from calfkit.controlplane.advert import AdvertInfo
    from calfkit.controlplane.config import ControlPlaneConfig
    from calfkit.nodes.base import BaseNodeDef
    from calfkit.worker.lifecycle import ServingContext

logger = logging.getLogger(__name__)


class ControlPlanePublisher:
    """Worker-owned heartbeat publisher + ordered tombstone (spec §7, ADR-0011)."""

    def __init__(
        self,
        *,
        worker_id: str,
        adverts: list[tuple[BaseNodeDef, AdvertInfo]],
        config: ControlPlaneConfig,
    ) -> None:
        self._worker_id = worker_id
        self._adverts = adverts
        self._config = config
        self._task: asyncio.Task[None] | None = None
        self._shutting_down = False
        self._started_at: AwareDatetime | None = None

    async def start(self, ctx: ServingContext) -> None:
        """``after_startup``: capture boot time, publish each advert once (FAIL-LOUD), spawn the loop.

        A publish failure here propagates and aborts worker startup — a node that
        cannot advertise must surface, not start silently degraded. The lifecycle
        then runs no ``on_shutdown``, so a partially-published record simply goes
        stale (crash-equivalent, accepted).
        """
        self._started_at = datetime.now(tz=timezone.utc)
        for node, info in self._adverts:
            await self._publish_one(ctx, node, info, self._started_at)
        self._task = asyncio.create_task(self._loop(ctx), name="control-plane-heartbeat")

    async def stop(self, ctx: ServingContext) -> None:
        """``on_shutdown``: flag first, cancel+await the loop, then tombstone the cross-product.

        Cancel-before-delete is what makes the tombstone win: after ``await task``
        the loop (and any in-flight ``set``) is fully resolved/cancelled, so no
        ``set`` can land after a ``delete``. The tombstone target is the declared
        cross-product ``{(node_id, topic)}`` — deletes are idempotent, so no
        per-key success bookkeeping is needed.
        """
        self._shutting_down = True
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        for node, info in self._adverts:
            writer = ctx.resources.get(self._writer_key(info.topic))
            if writer is not None:
                await writer.delete(node.node_id, self._worker_id)

    async def _loop(self, ctx: ServingContext) -> None:
        """Per-tick, per-advert resilient: a bad advert is logged; the rest still publish."""
        while True:
            await asyncio.sleep(self._config.heartbeat_interval)
            now = datetime.now(tz=timezone.utc)
            for node, info in self._adverts:
                try:
                    await self._publish_one(ctx, node, info, now)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception(
                        "control-plane publish failed node=%s topic=%s; retry next tick",
                        node.node_id,
                        info.topic,
                    )

    async def _publish_one(self, ctx: ServingContext, node: BaseNodeDef, info: AdvertInfo, now: AwareDatetime) -> None:
        assert self._started_at is not None  # set in start() before any publish
        writer = ctx.resources[self._writer_key(info.topic)]  # KeyError => wiring bug (fail-loud at start)
        identity = ControlPlaneIdentity(
            node_id=node.node_id,
            worker_id=self._worker_id,
            started_at=self._started_at,
            last_heartbeat_at=now,
            heartbeat_interval=self._config.heartbeat_interval,
        )
        record = getattr(node, info.name)(identity)  # opaque ControlPlaneRecord
        await writer.set(node.node_id, self._worker_id, record)

    @staticmethod
    def _writer_key(topic: str) -> str:
        return f"calfkit.controlplane.writer.{topic}"
