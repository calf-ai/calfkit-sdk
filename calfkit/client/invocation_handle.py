from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Generic

from calfkit._types import OutputT
from calfkit.client.deserialize import _UNSET, deserialize_to_node_result
from calfkit.client.node_result import NodeResult
from calfkit.models.envelope import Envelope


@dataclass
class InvocationHandle(Generic[OutputT]):
    correlation_id: str
    topic: str
    reply_topic: str
    _future: asyncio.Future[Envelope] = field(repr=False, compare=False)
    _output_type: type[Any] = field(default=_UNSET, repr=False, compare=False)

    async def result(self, timeout: float | None = None) -> NodeResult[OutputT]:
        """Await the invocation result.

        Args:
            timeout: Maximum seconds to wait. ``None`` means wait indefinitely.

        Returns:
            A ``NodeResult`` with the deserialized output and session metadata.

        Raises:
            asyncio.TimeoutError: If *timeout* elapses before a reply arrives.
            RuntimeError: If this handle was created without a future (fire-and-forget).
            DeserializationError: If the expected output part type is missing
                from ``final_output_parts``.
            pydantic.ValidationError: If ``output_type`` is provided and the
                reply's ``DataPart.data`` doesn't validate against it.
            pydantic.PydanticSchemaGenerationError: If ``output_type`` cannot
                be schematized by :class:`pydantic.TypeAdapter` (e.g. an
                unsupported generic or non-type value).
        """
        if self._future is None:
            raise RuntimeError("This handle has no associated future — was the client's reply dispatcher configured?")
        if timeout is not None:
            envelope = await asyncio.wait_for(self._future, timeout=timeout)
        else:
            envelope = await self._future
        return deserialize_to_node_result(envelope, self._output_type)
