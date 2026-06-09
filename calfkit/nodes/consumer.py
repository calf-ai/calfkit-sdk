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
    _node_kind: ClassVar[NodeKind] = "consumer"

    def __init__(
        self,
        *,
        node_id: str,
        consume_fn: ConsumerFn[OutputT],
        subscribe_topics: str | list[str],
        gates: list[GateFunction] | None = None,
        agent_output_type: type[OutputT] = _UNSET,
    ) -> None:
        _validate_consume_fn(consume_fn)
        if not isinstance(subscribe_topics, (list, tuple)):
            subscribe_topics = [subscribe_topics]
        super().__init__(node_id=node_id, subscribe_topics=list(subscribe_topics), gates=gates)
        self._func: ConsumerFn[OutputT] = consume_fn
        self._output_type = agent_output_type

        self._type_adapter: TypeAdapter[Any] | None = None
        if agent_output_type is not _UNSET and agent_output_type is not str:
            self._type_adapter = TypeAdapter(agent_output_type)

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
    agent_output_type: type[OutputT] = _UNSET,
    node_id: str | None = None,
    gates: list[GateFunction] | None = None,
) -> Callable[[ConsumerFn[OutputT]], ConsumerNode[OutputT]]:
    """Decorator turning a function into a deployable consumer node.

    The decorated function receives a single
    :class:`~calfkit.models.consumer_context.ConsumerContext`. Sync and async are
    both supported; generators / async generators are rejected at decoration time.

    Example::

        @consumer(subscribe_topics="weather_agent.output", agent_output_type=WeatherReport)
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
            agent_output_type=agent_output_type,
            gates=gates,
        )

    return _wrap
