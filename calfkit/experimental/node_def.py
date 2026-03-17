import logging
import warnings
from abc import abstractmethod
from dataclasses import dataclass
from typing import Annotated, Any, Generic

from faststream import Context
from faststream.kafka.annotations import (
    KafkaBroker as BrokerAnnotation,
)
from pydantic import BaseModel, Field
from typing_extensions import TypeAliasType, TypeVar

from calfkit.experimental._types import DepsT, StateT
from calfkit.experimental.context_models import BaseSessionRunContext
from calfkit.experimental.payload_model import Payload

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Developer-facing context
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Return types
# ---------------------------------------------------------------------------


@dataclass
class Reply(Generic[StateT]):
    """Terminal: send value back to whoever called this node (pops reply_stack)."""

    value: StateT


@dataclass
class Delegate(Generic[StateT]):
    """Terminal: forward to another node, expect result back (pushes reply_stack)."""

    topic: str
    value: StateT | None = None


@dataclass
class Emit(Generic[StateT]):
    """Terminal: fire-and-forget publish to a topic. No reply expected."""

    value: StateT
    topic: str


@dataclass
class Parallel(Generic[StateT]):
    """Parallel fan-out of delegates. Developer manages result aggregation via store."""

    delegates: list[Delegate[StateT]]


_T = TypeVar("_T")

NodeResult = TypeAliasType(
    "NodeResult",
    Reply[_T] | Emit[_T] | Delegate[_T] | list[Emit[_T]] | list[Delegate[_T]] | Parallel[_T],
    type_params=(_T,),
)
"""All possible return types from a node's ``run`` method."""


# ---------------------------------------------------------------------------
# Wire format (framework-internal)
# ---------------------------------------------------------------------------


@dataclass
class Hop:
    input_payload: Payload  # payload to pass into node as input
    result_topic: str  # publish result of node execution to this topic


class Envelope(BaseModel, Generic[StateT, DepsT]):
    """Wire format — framework internal. Carries routing metadata + developer context.

    Uses plain BaseModel (not CompactBaseModel) so all fields are always
    serialized — no exclude_unset gotchas with reply_stack.
    """

    context: BaseSessionRunContext[StateT, DepsT]
    reply_stack: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Base node definition
# ---------------------------------------------------------------------------


class BaseNodeDef(Generic[StateT, DepsT]):
    def __init__(
        self,
        node_id: str,
        *,
        subscribe_topics: str | list[str] | None = None,
        publish_topic: str | None = None,
    ):
        self._node_id = node_id
        if subscribe_topics is None:
            warnings.warn(
                f"node {node_id} is not subscribed to any topics. It is unreachable.",
                RuntimeWarning,
                stacklevel=2,
            )
        self.subscribe_topics = (
            [subscribe_topics] if isinstance(subscribe_topics, str) else subscribe_topics
        )
        self.publish_topic = publish_topic
        self._return_topic = f"{node_id}.private.return"

    @abstractmethod
    async def run(self, ctx: BaseSessionRunContext[StateT, DepsT]) -> NodeResult[StateT]:
        raise NotImplementedError()

    async def prepare_context(self, envelope: Envelope[StateT, DepsT]) -> BaseSessionRunContext:
        ctx = envelope.context
        return ctx

    async def handler(
        self,
        envelope: Envelope[StateT, DepsT],
        correlation_id: Annotated[str, Context()],
        broker: BrokerAnnotation,
    ) -> None:
        ctx = await self.prepare_context(envelope)
        output = await self.run(ctx)

        if isinstance(output, Reply):
            if not envelope.reply_stack:
                logger.warning(
                    "Node %s: Reply returned but reply_stack is empty — response dropped",
                    self._node_id,
                )
                return
            topic = envelope.reply_stack[-1]
            remaining_stack = envelope.reply_stack[:-1]
            await broker.publish(
                Envelope(
                    context=BaseSessionRunContext(state=output.value, deps=envelope.context.deps),
                    reply_stack=remaining_stack,
                ),
                topic=topic,
                correlation_id=correlation_id,
            )

        elif isinstance(output, Delegate):
            new_stack = [*envelope.reply_stack, self._return_topic]
            await broker.publish(
                Envelope(
                    context=BaseSessionRunContext(state=output.value, deps=envelope.context.deps),
                    reply_stack=new_stack,
                ),
                topic=output.topic,
                correlation_id=correlation_id,
            )

        elif isinstance(output, Emit):
            await broker.publish(
                BaseSessionRunContext(state=output.value, deps=envelope.context.deps),
                topic=output.topic,
                correlation_id=correlation_id,
            )

        elif isinstance(output, Parallel):
            new_stack = [*envelope.reply_stack, self._return_topic]
            for delegate in output.delegates:
                await broker.publish(
                    Envelope(
                        context=BaseSessionRunContext(
                            state=delegate.value, deps=envelope.context.deps
                        ),
                        reply_stack=new_stack,
                    ),
                    topic=delegate.topic,
                    correlation_id=correlation_id,
                )

        elif isinstance(output, list) and output and all(isinstance(e, Emit) for e in output):
            # mypy can't narrow list element types from all(isinstance(...))
            emits: list[Emit[Any]] = output  # type: ignore[assignment]
            for emit in emits:
                await broker.publish(
                    BaseSessionRunContext(state=emit.value, deps=envelope.context.deps),
                    topic=emit.topic,
                    correlation_id=correlation_id,
                )

        elif isinstance(output, list) and output and all(isinstance(d, Delegate) for d in output):
            # Sequential multi-delegate: chain via reply_stack.
            # Push self._return_topic (bottom), then remaining delegate
            # topics in reverse, so popping goes:
            #   first.topic → output[1].topic → ... → self._return_topic
            delegates: list[Delegate[Any]] = output  # type: ignore[assignment]
            remaining_topics = [d.topic for d in reversed(delegates[1:])]
            new_stack = [
                *envelope.reply_stack,
                self._return_topic,
                *remaining_topics,
            ]
            first = delegates[0]
            await broker.publish(
                Envelope(
                    context=BaseSessionRunContext(state=first.value, deps=envelope.context.deps),
                    reply_stack=new_stack,
                ),
                topic=first.topic,
                correlation_id=correlation_id,
            )

    @property
    def id(self):
        return self._node_id

    @property
    def name(self):
        return self._node_id
