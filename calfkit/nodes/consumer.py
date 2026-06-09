import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable
from typing import Any, ClassVar, Generic

from pydantic import TypeAdapter, ValidationError

from calfkit._protocol import NodeKind
from calfkit._types import OutputT
from calfkit.exceptions import DeserializationError
from calfkit.models.consumer_context import ConsumerContext
from calfkit.models.node_result import _UNSET
from calfkit.models.session_context import SessionRunContext
from calfkit.nodes.base import BaseNodeDef, GateFunction

logger = logging.getLogger(__name__)


ConsumerFn = Callable[[ConsumerContext[OutputT]], None | Awaitable[None]]
"""Signature of a consumer function: receives a single
:class:`~calfkit.models.consumer_context.ConsumerContext`. Sync or async.

``ctx.output`` is ``None`` on intermediate hops (tool completions, mid-loop agent
hops). ``ctx.deps`` / ``ctx.resources`` / ``ctx.correlation_id`` / ``ctx.state``
are always populated — the same ``ctx.*`` vocabulary tools use."""


def _validate_consume_fn(consume_fn: Any) -> None:
    """Reject consume_fn shapes that would silently no-op at runtime.

    A plain generator / async-generator function would be invoked, return an
    iterator/asyncgen object (neither ``None`` nor awaitable), and the user's body
    would never execute. Surface that at construction.
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


class ConsumerNode(Generic[OutputT], BaseNodeDef):
    """Terminal sink node: listens on one or more topics and runs arbitrary user
    logic against a :class:`ConsumerContext` projected from each inbound envelope.

    Unlike the original ``ConsumerNodeDef``, this rides the shared
    :meth:`BaseNodeDef.handler`: ``run`` is the inherited ``@handler('*')``
    catch-all, so a frameless inbound dispatches straight to it. It overrides
    :meth:`prepare_context` to stamp the inbound context **in place** (no deep
    copy), so ``ctx.state``/``ctx.deps`` stay identical to the envelope's and the
    full ``State`` isn't deep-copied on every stream envelope.

    Behavior:
        * Sync and async consume functions are both supported.
        * Gates (inherited from :class:`BaseNodeDef`) filter pre-run.
        * Exceptions from the consume function are logged at ERROR and swallowed
          (processing continues). ``asyncio.CancelledError`` always propagates.
    """

    _node_kind: ClassVar[NodeKind] = "consumer"

    def __init__(
        self,
        *,
        node_id: str,
        consume_fn: ConsumerFn[OutputT],
        subscribe_topics: str | list[str],
        gates: list[GateFunction] | None = None,
        output_type: type[OutputT] = _UNSET,
    ) -> None:
        _validate_consume_fn(consume_fn)
        if not isinstance(subscribe_topics, (list, tuple)):
            subscribe_topics = [subscribe_topics]
        super().__init__(node_id=node_id, subscribe_topics=list(subscribe_topics), gates=gates)
        self._func: ConsumerFn[OutputT] = consume_fn
        self._output_type = output_type
        # Pre-build the TypeAdapter once so a schema-generation error surfaces at
        # wiring time (not per envelope) and the adapter is reused. Auto-detect and
        # str paths need no adapter.
        self._type_adapter: TypeAdapter[Any] | None = None
        if output_type is not _UNSET and output_type is not str:
            self._type_adapter = TypeAdapter(output_type)

    async def prepare_context(
        self,
        envelope: Any,
        emitter_node_id: str | None = None,
        emitter_node_kind: str | None = None,
        correlation_id: str | None = None,
    ) -> SessionRunContext:
        # Terminal sink, outside the call/return flow: stamp the inbound context in
        # place (no deep copy). This keeps ctx.state IS envelope.context.state (the
        # no-copy contract) and avoids deep-copying the full State — message history
        # included — on every stream envelope. The stamped ids/resources are
        # PrivateAttr, so they never ride the wire. A sink ignores frame overrides /
        # frame_id (it has no frame).
        ctx: SessionRunContext = envelope.context
        ctx._stamp_transport(correlation_id=correlation_id, emitter_node_id=emitter_node_id, emitter_node_kind=emitter_node_kind)
        ctx._resources = self._effective_resources()
        return ctx

    async def run(self, ctx: SessionRunContext) -> None:
        try:
            cctx: ConsumerContext[OutputT] = ConsumerContext.from_run_context(ctx, self._output_type, type_adapter=self._type_adapter)
        except (DeserializationError, ValidationError):
            logger.exception(
                "[%s] consumer=%s projection failed; skipping (emitter=%s kind=%s output_type=%s)",
                ctx.correlation_id[:8],
                self.node_id,
                ctx.emitter_node_id,
                ctx.emitter_node_kind,
                getattr(self._output_type, "__name__", repr(self._output_type)),
            )
            return
        except Exception:
            # Safety net: anything past the expected exceptions is logged and skipped
            # so one bad envelope can't poison-pill the offset.
            logger.exception(
                "[%s] consumer=%s unexpected projection error; skipping (emitter=%s kind=%s)",
                ctx.correlation_id[:8],
                self.node_id,
                ctx.emitter_node_id,
                ctx.emitter_node_kind,
            )
            return

        try:
            ret = self._func(cctx)
            # Catch runtime-only silent-noop shapes that escape _validate_consume_fn
            # (callable-class generators, functions returning generator/asyncgen
            # objects): ``ret`` is a generator/asyncgen that is NOT awaitable, so the
            # body would never run.
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
                "[%s] consumer=%s consume_fn raised (emitter=%s kind=%s)",
                ctx.correlation_id[:8],
                self.node_id,
                ctx.emitter_node_id,
                ctx.emitter_node_kind,
            )


def consumer(
    *,
    subscribe_topics: str | list[str],
    output_type: type[OutputT] = _UNSET,
    node_id: str | None = None,
    gates: list[GateFunction] | None = None,
) -> Callable[[ConsumerFn[OutputT]], ConsumerNode[OutputT]]:
    """Decorator turning a function into a deployable consumer node.

    The decorated function receives a single
    :class:`~calfkit.models.consumer_context.ConsumerContext`. Sync and async are
    both supported; generators / async generators are rejected at decoration time.

    Example::

        @consumer_v2(subscribe_topics="weather_agent.output", output_type=WeatherReport)
        async def save_weather(ctx: ConsumerContext[WeatherReport]) -> None:
            if ctx.output is None:
                return  # intermediate hop
            await ctx.resources["db"].save(ctx.output)
    """

    def _wrap(fn: ConsumerFn[OutputT]) -> ConsumerNode[OutputT]:
        return ConsumerNode[OutputT](
            node_id=node_id or f"consumer_{fn.__name__}",
            subscribe_topics=subscribe_topics,
            consume_fn=fn,
            output_type=output_type,
            gates=gates,
        )

    return _wrap
