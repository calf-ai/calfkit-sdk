import logging
from collections.abc import Awaitable, Callable
from typing import Any

from faststream import BaseMiddleware, PublishCommand
from faststream.message import StreamMessage
from faststream.types import AsyncFuncAny
from pydantic import ValidationError

from calfkit.exceptions import safe_exc_message
from calfkit.models.error_report import ErrorReport, FaultTypes

logger = logging.getLogger(__name__)

_MAX_DECODE_ERROR_CHARS = 1000


class ContextInjectionMiddleware(BaseMiddleware):
    async def consume_scope(
        self,
        call_next: AsyncFuncAny,
        msg: StreamMessage[Any],
    ) -> Any:
        with self.context.scope("correlation_id", msg.correlation_id):
            return await super().consume_scope(call_next, msg)

    async def publish_scope(
        self,
        call_next: Callable[[PublishCommand], Awaitable[Any]],
        cmd: PublishCommand,
    ) -> Any:
        return await call_next(cmd)


class DecodeFloorMiddleware(BaseMiddleware):
    """Broker-level decode floor (fault-rail spec §6.7 / §13).

    An inbound message whose body fails to decode/validate never reaches a handler — FastStream
    captures the error, logs generically, and the ack-first offset means the message is gone (the
    silent-drop hole on the decode path, below the chokepoint ``BaseNodeDef.handler``). This shim
    catches the decode ``ValidationError`` in :meth:`consume_scope` (verified empirically on
    FastStream 0.7.1: BOTH a non-JSON parser failure and a missing-field validator failure surface
    there as a pydantic ``ValidationError`` — so a single ``consume_scope`` override suffices, and
    the §6.7 "parser-stage failures bypass ``consume_scope`` / hook both paths" caveat does not
    hold on 0.7.1), FLOORS a typed ``calf.delivery.undecodable`` event (ERROR + the transport
    metadata that survives an unreadable body — the correlation id + the clamped decode error),
    then **re-raises**.

    Re-raising is load-bearing (verified): suppressing lets FastStream treat the hop as success and
    push an empty body (``b''``) through any attached ``@publisher`` (the §6.7 empty-payload trap).
    Routing is impossible — the return address is inside the unreadable body — so **floor-only is
    the ceiling** at nodes (P1, stated honestly); the client reply subscriber additionally fails
    the pending future (the deferred reception PR, §11). Installed broker-wide so every node
    subscriber (and the client reply subscriber) is covered by one shim.
    """

    async def consume_scope(
        self,
        call_next: AsyncFuncAny,
        msg: StreamMessage[Any],
    ) -> Any:
        try:
            return await super().consume_scope(call_next, msg)
        except ValidationError as exc:
            correlation_id = getattr(msg, "correlation_id", None)
            report = ErrorReport.build_safe(
                error_type=FaultTypes.DELIVERY_UNDECODABLE,
                message="inbound delivery body failed to decode/validate",
                details={"correlation_id": correlation_id, "decode_error": safe_exc_message(exc)[:_MAX_DECODE_ERROR_CHARS]},
            )
            logger.error(
                "[%s] inbound delivery floored (undecodable body); error_type=%s report=%s",
                (correlation_id or "n/a")[:8],
                report.error_type,
                report.model_dump_json(),
            )
            raise
