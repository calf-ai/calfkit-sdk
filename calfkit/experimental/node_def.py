import logging
import warnings
from abc import abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from typing import Annotated, Any, Generic

from faststream import Context
from faststream.kafka.annotations import (
    KafkaBroker as BrokerAnnotation,
)
from faststream.types import SendableMessage
from pydantic import BaseModel, Field
from typing_extensions import TypeVar

logger = logging.getLogger(__name__)

StateT = TypeVar("StateT", default=Any)
DepsT = TypeVar("DepsT", default=Any)
OutputT = TypeVar("OutputT", bound=SendableMessage, default=Any)


# ---------------------------------------------------------------------------
# Developer-facing context
# ---------------------------------------------------------------------------


class SessionRunContext(BaseModel, Generic[StateT, DepsT]):
    """Developer-facing context for a session — just state + deps."""

    state: StateT
    """The state of the graph. Mutable."""
    deps: DepsT
    """Dependencies for the graph. Immutable."""


# ---------------------------------------------------------------------------
# Return types
# ---------------------------------------------------------------------------


@dataclass
class Reply(Generic[OutputT]):
    """Terminal: send value back to whoever called this node (pops reply_stack)."""

    value: OutputT


@dataclass
class Delegate(Generic[OutputT]):
    """Terminal: forward to another node, expect result back (pushes reply_stack)."""

    value: OutputT
    topic: str


@dataclass
class Emit(Generic[OutputT]):
    """Terminal: fire-and-forget publish to a topic. No reply expected."""

    payload: OutputT
    topic: str


@dataclass
class Parallel(Generic[OutputT]):
    """Parallel fan-out of delegates. Developer manages result aggregation via store."""

    delegates: list[Delegate[OutputT]]


# ---------------------------------------------------------------------------
# Wire format (framework-internal)
# ---------------------------------------------------------------------------


class Envelope(BaseModel, Generic[StateT, DepsT]):
    """Wire format — framework internal. Carries routing metadata + developer context.

    Uses plain BaseModel (not CompactBaseModel) so all fields are always
    serialized — no exclude_unset gotchas with reply_stack.
    """

    context: SessionRunContext[StateT, DepsT]
    reply_stack: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Base node definition
# ---------------------------------------------------------------------------


class BaseNodeDef(Generic[StateT, DepsT, OutputT]):
    def __init__(
        self,
        node_id: str,
        *,
        subscribe_topics: str | list[str] | None = None,
        input_to_prompt_func: Callable[[SessionRunContext[StateT, DepsT]], str] | None = None,
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
        self._return_topic = f"{node_id}.private.return"
        self._input_to_prompt_func = input_to_prompt_func

    @abstractmethod
    async def run(
        self, ctx: SessionRunContext[StateT, DepsT]
    ) -> (
        Reply[OutputT]
        | Emit[OutputT]
        | Delegate[OutputT]
        | list[Emit[OutputT]]
        | list[Delegate[OutputT]]
        | Parallel[OutputT]
    ):
        raise NotImplementedError()

    async def handler(
        self,
        envelope: Envelope[StateT, DepsT],
        correlation_id: Annotated[str, Context()],
        broker: BrokerAnnotation,
    ) -> None:
        output = await self.run(envelope.context)

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
                    context=envelope.context,
                    reply_stack=remaining_stack,
                ),
                topic=topic,
                correlation_id=correlation_id,
            )

        elif isinstance(output, Delegate):
            new_stack = [*envelope.reply_stack, self._return_topic]
            await broker.publish(
                Envelope(
                    context=SessionRunContext(state=output.value, deps=envelope.context.deps),
                    reply_stack=new_stack,
                ),
                topic=output.topic,
                correlation_id=correlation_id,
            )

        elif isinstance(output, Emit):
            await broker.publish(
                output.payload,
                topic=output.topic,
                correlation_id=correlation_id,
            )

        elif isinstance(output, Parallel):
            new_stack = [*envelope.reply_stack, self._return_topic]
            for delegate in output.delegates:
                await broker.publish(
                    Envelope(
                        context=SessionRunContext(state=delegate.value, deps=envelope.context.deps),
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
                    emit.payload,
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
                    context=SessionRunContext(state=first.value, deps=envelope.context.deps),
                    reply_stack=new_stack,
                ),
                topic=first.topic,
                correlation_id=correlation_id,
            )

    # TODO: move to baseagent subclass.
    # The concept of structured input to prompt string is agent-level concept
    def input_to_prompt(
        self, func: Callable[[SessionRunContext[StateT, DepsT]], str]
    ) -> Callable[[SessionRunContext[StateT, DepsT]], str]:
        """decorator to define function to parse structured input to string"""
        self._input_to_prompt_func = func
        return func
