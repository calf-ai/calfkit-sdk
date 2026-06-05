from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable
from typing import Annotated, Any, ClassVar, Generic

from faststream import Context, Response
from faststream.kafka.annotations import KafkaBroker as BrokerAnnotation
from pydantic import TypeAdapter, ValidationError

from calfkit._protocol import HDR_EMITTER, HDR_EMITTER_KIND, NodeKind, decode_header_str
from calfkit._types import OutputT
from calfkit.client.deserialize import _UNSET, deserialize_to_node_result
from calfkit.client.node_result import NodeResult
from calfkit.exceptions import DeserializationError
from calfkit.models import State
from calfkit.models.actions import NodeResult as NodeAction
from calfkit.models.envelope import Envelope
from calfkit.models.session_context import SessionRunContext
from calfkit.nodes.base import BaseNodeDef, GateFunction

logger = logging.getLogger(__name__)


ConsumerFn = Callable[[NodeResult[OutputT]], None | Awaitable[None]]
"""Signature of a consumer function: receives the same client-facing
:class:`~calfkit.client.node_result.NodeResult` that
:meth:`Client.execute_node` returns. Sync or async.

``result.output`` is ``None`` on intermediate hops (e.g. tool completions, or
agent state transitions that haven't yet produced a final output). User code
must tolerate ``None`` or filter via gates. ``result.message_history``,
``result.correlation_id``, ``result.emitter_node_id``, and
``result.emitter_node_kind`` are always populated. ``result.deps`` carries the
inbound producer ``deps`` (``{}`` if none were set) — read it as
``result.deps["key"]``, the same data tools see via ``ctx.deps["key"]``."""


def _validate_consume_fn(consume_fn: Any) -> None:
    """Reject consume_fn shapes that would silently no-op at runtime.

    Why: a plain generator or async-generator function passed here would be
    invoked, return an iterator/asyncgen object (which is neither None nor
    awaitable), and the user's body would never execute — the consumer would
    appear to run forever without doing anything. Surface that at construction.
    """
    if inspect.isgeneratorfunction(consume_fn):
        raise TypeError(
            f"consume_fn must be a regular function or coroutine function; got generator function {getattr(consume_fn, '__name__', consume_fn)!r}"
        )
    if inspect.isasyncgenfunction(consume_fn):
        raise TypeError(
            f"consume_fn must be a regular function or coroutine function; "
            f"got async generator function {getattr(consume_fn, '__name__', consume_fn)!r}"
        )


class ConsumerNodeDef(Generic[OutputT], BaseNodeDef):
    """Terminal sink node: listens on one or more topics and runs arbitrary user
    logic against a :class:`NodeResult` projected from each inbound envelope.

    Use :func:`consumer` for the fast-path decorator. Instantiate this class
    directly for class-based composition (mirrors :class:`Agent` /
    :class:`ToolNodeDef`).

    The consumer is wired to any topic carrying calfkit ``Envelope`` payloads —
    typically the ``publish_topic`` of an upstream agent or tool. Because that
    publisher fires on **every** handler hop (intermediate ``Call`` envelopes,
    tool completions, and final ``ReturnCall`` envelopes alike), the consumer
    sees the entire transition stream by design. ``result.output`` is populated
    only when ``final_output_parts`` is present on the envelope (agent
    terminals); on every other hop it is ``None``. Use a gate to filter:

        gates=[lambda ctx: bool(ctx.state.final_output_parts)]

    Behavior:
        * Sync and async user functions are both supported.
        * Gates inherited from :class:`BaseNodeDef` filter pre-run.
        * Exceptions from the user function are logged and swallowed by
          default (offset commits, processing continues). See ``re_raise``.
        * ``asyncio.CancelledError`` always propagates.
    """

    _node_kind: ClassVar[NodeKind] = "consumer"

    def __init__(
        self,
        *,
        node_id: str,
        subscribe_topics: str | list[str],
        consume_fn: ConsumerFn[OutputT],
        output_type: type[OutputT] = _UNSET,
        gates: list[GateFunction] | None = None,
        re_raise: bool = False,
    ) -> None:
        """Initialize a consumer node definition.

        Args:
            node_id: Unique identifier for the node.
            subscribe_topics: One or more topics to consume from.
            consume_fn: The user-provided sink function. Receives a single
                :class:`NodeResult` argument; may be sync or async. Must not
                be a generator or async-generator function.
            output_type: Expected Python type for ``result.output`` on hops
                that carry final output parts. Defaults to auto-detect
                (``DataPart`` → ``TextPart`` fallback). On hops without
                ``final_output_parts``, ``result.output`` is ``None``
                regardless of this setting.
            gates: Optional pre-run predicates with AND semantics.
            re_raise: When ``True``, exceptions raised by ``consume_fn``
                propagate out of the handler. **Important caveat**: the
                :class:`Worker` registers subscribers with FastStream's
                default ``AckPolicy.ACK_FIRST``, which commits the Kafka
                offset *before* the handler runs. So ``re_raise=True`` only
                restarts the FastStream consumer task and surfaces the
                exception in logs — the offending message is **not**
                redelivered, and there is no built-in DLQ. Use this flag for
                fail-loud development semantics. For true retry/DLQ
                behavior, configure the broker's ``ack_policy`` explicitly
                on the :class:`Worker`.
        """
        _validate_consume_fn(consume_fn)
        if not isinstance(subscribe_topics, (list, tuple)):
            subscribe_topics = [subscribe_topics]
        super().__init__(
            node_id=node_id,
            subscribe_topics=list(subscribe_topics),
            publish_topic=None,
            gates=gates,
        )
        self._consume_fn = consume_fn
        self._output_type = output_type
        self._re_raise = re_raise
        # Pre-build the TypeAdapter at construction so a schema-generation
        # error (e.g. an unschematizable output_type) surfaces once at wiring
        # time rather than per envelope at runtime. Auto-detect and str paths
        # don't need an adapter.
        self._type_adapter: TypeAdapter[Any] | None = None
        if output_type is not _UNSET and output_type is not str:
            self._type_adapter = TypeAdapter(output_type)

    @property
    def publish_topic(self) -> None:
        """Consumers are terminal sinks; always ``None``."""
        return None

    @publish_topic.setter
    def publish_topic(self, value: str | None) -> None:
        # Accept None silently so BaseNodeSchema's dataclass __init__ can run.
        # Reject everything else: the no-publish invariant must hold for the
        # life of the instance (Worker would otherwise wire up a publisher).
        if value is not None:
            raise AttributeError(f"Cannot set publish_topic={value!r} on {type(self).__name__}: consumers are terminal sinks.")

    async def run(self, ctx: SessionRunContext) -> NodeAction[State]:
        # Defensive: handler() is overridden and never calls run(). If a future
        # refactor reaches here, fail loud rather than silently returning Silent.
        raise AssertionError(f"{type(self).__name__}.run() should never be invoked; handler() is overridden.")

    async def handler(
        self,
        envelope: Envelope,
        correlation_id: Annotated[str, Context()],
        headers: Annotated[dict[str, Any], Context("message.headers")],
        broker: BrokerAnnotation,
        message: Annotated[Any, Context("message")] = None,
    ) -> Response:
        raw_emitter = headers.get(HDR_EMITTER)
        emitter = decode_header_str(raw_emitter)
        if emitter is None:
            logger.warning(
                "[%s] inbound to consumer=%s has no usable %s header (got %s); emitter unknown",
                correlation_id[:8],
                self.node_id,
                HDR_EMITTER,
                "None" if raw_emitter is None else type(raw_emitter).__name__,
            )
        emitter_kind = decode_header_str(headers.get(HDR_EMITTER_KIND))
        # Pull raw-broker message metadata (topic / partition / offset) for
        # error-log context. ``getattr`` keeps direct handler() calls (tests)
        # working even when no broker message is present.
        raw_msg = getattr(message, "raw_message", None) if message is not None else None
        in_topic = getattr(raw_msg, "topic", None)
        in_partition = getattr(raw_msg, "partition", None)
        in_offset = getattr(raw_msg, "offset", None)
        logger.debug(
            "[%s] consumer handler entered node=%s emitter=%s kind=%s topic=%s partition=%s offset=%s",
            correlation_id[:8],
            self.node_id,
            emitter,
            emitter_kind,
            in_topic,
            in_partition,
            in_offset,
        )
        # Consumer is outside the call/return flow, so the inbound call_stack
        # may be empty (e.g. when tapping publish_topic post-ReturnCall) — we
        # bypass BaseNodeDef.prepare_context which peeks the stack. The stamped
        # ids are PrivateAttr (calfkit.models.session_context), so they never
        # ride on the wire; stamping the inbound context directly is safe.
        envelope.context._stamp_transport(correlation_id=correlation_id, emitter_node_id=emitter, emitter_node_kind=emitter_kind)
        ctx = envelope.context

        if not await self._evaluate_gates(ctx, correlation_id):
            return Response(envelope, headers=self._emitter_headers())

        try:
            result: NodeResult[OutputT] = deserialize_to_node_result(
                envelope,
                self._output_type,
                correlation_id=correlation_id,
                strict=False,
                type_adapter=self._type_adapter,
                resources=self._effective_resources(),
            )
        except (DeserializationError, ValidationError):
            logger.exception(
                "[%s] consumer=%s deserialize failed; skipping (emitter=%s kind=%s output_type=%s topic=%s partition=%s offset=%s)",
                correlation_id[:8],
                self.node_id,
                emitter,
                emitter_kind,
                getattr(self._output_type, "__name__", repr(self._output_type)),
                in_topic,
                in_partition,
                in_offset,
            )
            return Response(envelope, headers=self._emitter_headers())
        except Exception:
            # Safety net: anything that slips past the expected exception
            # tuple (e.g. an unforeseen adapter failure, a Pydantic edge case,
            # a third-party model integration error) is logged and skipped so
            # a single bad envelope can't poison-pill the Kafka offset.
            logger.exception(
                "[%s] consumer=%s unexpected deserialize error; skipping (emitter=%s kind=%s output_type=%s topic=%s partition=%s offset=%s)",
                correlation_id[:8],
                self.node_id,
                emitter,
                emitter_kind,
                getattr(self._output_type, "__name__", repr(self._output_type)),
                in_topic,
                in_partition,
                in_offset,
            )
            return Response(envelope, headers=self._emitter_headers())

        try:
            ret = self._consume_fn(result)
            # Catch the runtime-only silent-noop shapes that escape
            # _validate_consume_fn — callable-class generators, plain functions
            # that return generator/asyncgen objects, etc. In all these cases
            # ``ret`` is a generator/asyncgen object that is NOT awaitable, so
            # ``inspect.isawaitable`` would skip it and the body would never run.
            if inspect.isgenerator(ret) or inspect.isasyncgen(ret):
                raise TypeError(
                    f"consume_fn returned a {type(ret).__name__} object; the function body would never execute. "
                    f"Iterate inside the function or refactor to a coroutine."
                )
            if inspect.isawaitable(ret):
                await ret
        except asyncio.CancelledError:
            # Never swallow cooperative cancellation — let the event loop unwind.
            raise
        except Exception:
            logger.exception(
                "[%s] consumer=%s consume_fn raised (emitter=%s kind=%s topic=%s partition=%s offset=%s)",
                correlation_id[:8],
                self.node_id,
                emitter,
                emitter_kind,
                in_topic,
                in_partition,
                in_offset,
            )
            if self._re_raise:
                raise

        return Response(envelope, headers=self._emitter_headers())


def consumer(
    *,
    subscribe_topics: str | list[str],
    output_type: type[OutputT] = _UNSET,
    node_id: str | None = None,
    gates: list[GateFunction] | None = None,
    re_raise: bool = False,
) -> Callable[[ConsumerFn[OutputT]], ConsumerNodeDef[OutputT]]:
    """Decorator turning a function into a deployable consumer node.

    The decorated function receives a single argument: the same
    :class:`NodeResult` returned by :meth:`Client.execute_node`. Both sync
    and async functions are supported. Generators / async generators are
    rejected at decoration time.

    Example::

        @consumer(subscribe_topics="weather_agent.output", output_type=WeatherReport)
        async def save_weather(result: NodeResult[WeatherReport]) -> None:
            if result.output is None:
                return  # intermediate hop — no final output yet
            await db.save(result.output)

    Args:
        subscribe_topics: One or more topics to consume from. Typically the
            ``publish_topic`` of an upstream agent or tool node. The consumer
            receives the entire transition stream on these topics —
            intermediate hops surface as ``NodeResult`` with ``output=None``.
        output_type: Expected Python type for ``result.output`` on hops that
            carry final output parts. Defaults to auto-detect.
        node_id: Optional override; defaults to ``consumer_<function name>``.
        gates: Optional pre-run predicates. A useful idiom for "final outputs
            only" is ``gates=[lambda ctx: bool(ctx.state.final_output_parts)]``.
        re_raise: When ``True``, exceptions propagate. See
            :class:`ConsumerNodeDef` for the ack-policy caveat.

    Returns:
        A :class:`ConsumerNodeDef` instance ready to register with a
        :class:`Worker`.
    """

    def _wrap(fn: ConsumerFn[OutputT]) -> ConsumerNodeDef[OutputT]:
        return ConsumerNodeDef[OutputT](
            node_id=node_id or f"consumer_{fn.__name__}",
            subscribe_topics=subscribe_topics,
            consume_fn=fn,
            output_type=output_type,
            gates=gates,
            re_raise=re_raise,
        )

    return _wrap
