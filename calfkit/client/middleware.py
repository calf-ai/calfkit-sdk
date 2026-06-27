import json
import logging
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from faststream import BaseMiddleware, PublishCommand
from faststream.message import StreamMessage
from faststream.types import AsyncFuncAny
from pydantic import ValidationError

from calfkit.exceptions import safe_exc_message
from calfkit.models.error_report import ErrorReport, FaultTypes

logger = logging.getLogger(__name__)

_MAX_DECODE_ERROR_CHARS = 1000

UndecodableSink = Callable[[str, ErrorReport], None]
"""A topic's undecodable-reply sink (spec §5.8): the decode floor calls it ``(correlation_id, report)``
with the surviving transport id + the synthesized ``calf.delivery.undecodable`` report. The client
registers ``inbox_topic → hub.fail_run``."""


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
    catches decode failures in :meth:`consume_scope` — on FastStream 0.7.1 (verified) these are a
    pydantic ``ValidationError`` (a schema mismatch, or a non-JSON body with no JSON content-type,
    which the content-type-less decode path hands to pydantic) **and** a ``json.JSONDecodeError`` /
    ``UnicodeDecodeError`` (a malformed body carrying a JSON/text content-type, which FastStream
    decodes un-suppressed before the handler). A single ``consume_scope`` override catching that union
    suffices, so the §6.7 "parser-stage failures bypass ``consume_scope``" caveat does not bite here.
    It FLOORS a typed ``calf.delivery.undecodable`` event (ERROR + the transport metadata that
    survives an unreadable body — the correlation id + the clamped decode error), then **re-raises**.

    Re-raising is load-bearing (verified): suppressing lets FastStream treat the hop as success and
    push an empty body (``b''``) through any attached ``@publisher`` (the §6.7 empty-payload trap).
    Routing is impossible — the return address is inside the unreadable body — so **floor-only is
    the ceiling** at nodes (P1, stated honestly); the client reply subscriber additionally fails
    the pending future (the deferred reception PR, §11). Installed broker-wide so every node
    subscriber (and the client reply subscriber) is covered by one shim.

    **Option-B undecodable-sink seam (spec §5.8, additive).** When constructed with a ``registry``
    (a ``{topic: sink}`` map), a floored decode failure also hands the *same* synthesized report to the
    sink registered for the delivery's topic, then re-raises — so an undecodable reply **on the client
    inbox** surfaces to the awaiting ``result()`` as a fault instead of hanging. The bare class (no
    registry — the broker-wide install for every node subscriber) keeps the exact synthesize+log+re-raise
    behavior, no seam. Carry the registry with :meth:`builder` (the ``BrokerMiddleware`` factory protocol).
    """

    def __init__(self, msg: Any, /, *, context: Any, registry: Mapping[str, UndecodableSink] | None = None) -> None:
        super().__init__(msg, context=context)
        self._registry = registry

    @classmethod
    def builder(cls, registry: Mapping[str, UndecodableSink]) -> "_DecodeFloorBuilder":
        """A configured factory carrying the ``{topic: sink}`` registry for the undecodable seam.

        FastStream instantiates each broker middleware per message as ``factory(msg, *, context)``; this
        returns a factory that injects the registry, while the bare class stays a valid no-registry
        factory. Pass it to ``KafkaBroker(middlewares=[DecodeFloorMiddleware.builder(registry), …])``."""
        return _DecodeFloorBuilder(registry)

    async def consume_scope(
        self,
        call_next: AsyncFuncAny,
        msg: StreamMessage[Any],
    ) -> Any:
        try:
            return await super().consume_scope(call_next, msg)
        except (ValidationError, json.JSONDecodeError, UnicodeDecodeError) as exc:
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
            # Option-B seam (spec §5.8): hand the SAME synthesized report to the sink registered for
            # this delivery's topic, iff any. Topic-key scoping confines the push to the client inbox
            # (a node-hop undecodable on a node topic has no sink, so it never reaches the client hub
            # even though it carries the run's correlation_id). Read the topic defensively — a batch
            # subscriber's raw_message is a tuple — so the floor stays total (never raises here). A
            # surviving correlation_id is required: with none, no run can be routed (the ERROR log above
            # is the whole story). Bare class (registry is None) skips the seam entirely.
            if self._registry is not None and correlation_id is not None:
                topic = getattr(getattr(msg, "raw_message", None), "topic", None)
                if topic is not None:
                    sink = self._registry.get(topic)
                    if sink is not None:
                        sink(correlation_id, report)
            raise


class _DecodeFloorBuilder:
    """A configured factory for :class:`DecodeFloorMiddleware` carrying the undecodable-sink registry.

    FastStream instantiates each broker middleware per message as ``factory(msg, *, context)`` (see
    ``subscriber/usecase.py``), so a plain callable returning the per-message middleware is a valid
    factory — the ``BrokerMiddleware`` protocol. This builder closes over the ``{topic: sink}`` registry
    and injects it; the bare ``DecodeFloorMiddleware`` class stays usable as a no-registry factory."""

    def __init__(self, registry: Mapping[str, UndecodableSink]) -> None:
        self._registry = registry

    def __call__(self, msg: Any, *, context: Any) -> DecodeFloorMiddleware:
        return DecodeFloorMiddleware(msg, context=context, registry=self._registry)
