from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from calfkit.experimental.base_models.envelope import Envelope


@dataclass
class InvocationHandle:
    correlation_id: str
    topic: str
    reply_topic: str
    _future: asyncio.Future[Envelope] = field(repr=False, compare=False, default=None)  # type: ignore[assignment]

    async def result(self, timeout: float | None = None) -> Envelope:
        """Await the invocation result.

        Args:
            timeout: Maximum seconds to wait. ``None`` means wait indefinitely.

        Returns:
            The reply ``Envelope``.

        Raises:
            asyncio.TimeoutError: If *timeout* elapses before a reply arrives.
            RuntimeError: If this handle was created without a future (fire-and-forget).
        """
        if self._future is None:
            raise RuntimeError("This handle has no associated future — was the client's reply dispatcher configured?")
        if timeout is not None:
            return await asyncio.wait_for(self._future, timeout=timeout)
        return await self._future
