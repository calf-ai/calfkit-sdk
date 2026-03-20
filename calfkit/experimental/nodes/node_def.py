import inspect
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

from calfkit.experimental._types import DepsT, InputT, StateT
from calfkit.experimental.context.context_models import BaseSessionRunContext

logger = logging.getLogger(__name__)


@dataclass
class Reply(Generic[StateT]):
    """Terminal: send value back to whoever called this node (pops reply_stack).
    Does not require a topic address to reply to, it is handled by the framework.
    Similar mental model to completing an async Promise or Future with a result."""

    value: StateT


@dataclass
class Delegate(Generic[StateT]):
    """Terminal: forward to another node, expect result back (pushes reply_stack)."""

    topic: str
    value: StateT | None = None
    input: Any | None = None


@dataclass
class Sequential(Generic[StateT]):
    """Sequentially forward a shared state from node to node,
    where the final state is returned to caller."""

    topics: list[str]  # passed to topics in this list in order index 0 -> n
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


@dataclass
class Silent:
    """Silent end of node execution, no explicit publish. End of event stream."""


_T = TypeVar("_T")

NodeResult = TypeAliasType(
    "NodeResult",
    Reply[_T]
    | Emit[_T]
    | Delegate[_T]
    | list[Emit[_T]]
    | list[Delegate[_T]]
    | Parallel[_T]
    | Silent
    | Sequential[_T],
    type_params=(_T,),
)
"""All possible return types from a node's ``run`` method."""


# ---------------------------------------------------------------------------
# Wire format (framework-internal)
# ---------------------------------------------------------------------------


class Envelope(BaseModel, Generic[StateT, DepsT]):
    """Wire format — framework internal. Carries routing metadata + developer context.

    Uses plain BaseModel (not CompactBaseModel) so all fields are always
    serialized — no exclude_unset gotchas with reply_stack.
    """

    context: BaseSessionRunContext[StateT, DepsT]
    reply_stack: list[str] = Field(default_factory=list)
    input: Any | None = None


# ---------------------------------------------------------------------------
# Base node definition
# ---------------------------------------------------------------------------


class BaseNodeDef(Generic[StateT, DepsT, InputT]):
    _run_accepts_input: bool

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        sig = inspect.signature(cls.run)
        # Unbound method signature includes self, so self + ctx = 2 params.
        # If > 2, the subclass declared an input parameter (any name).
        cls._run_accepts_input = len(sig.parameters) > 2

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

    # TODO: consider multiple abstract methods per node based on the incoming communication pattern,
    # like a delgation or emit. So the communication-specific handler can properly handle it
    @abstractmethod
    async def run(self, ctx: BaseSessionRunContext[StateT, DepsT]) -> NodeResult[StateT]:
        """Runs the node's logic using provided context.

        Subclasses that need per-invocation input can add an optional parameter
        (any name). The framework inspects the signature at class definition time
        and will pass ``envelope.input`` automatically if the parameter is declared.

        Args:
            ctx: Session context containing mutable state and immutable dependencies.

        Raises:
            NotImplementedError: If node subclass does not implement the run() method.

        Returns:
            NodeResult[StateT]: Execution results persisted via modified state wrapped
            in the NodeResult type. Different NodeResult types define how results
            should be communicated.
        """
        raise NotImplementedError()

    async def prepare_context(self, envelope: Envelope[StateT, DepsT]) -> BaseSessionRunContext:
        ctx = envelope.context.model_copy(deep=True)
        return ctx

    async def handler(
        self,
        envelope: Envelope[StateT, DepsT],
        correlation_id: Annotated[str, Context()],
        broker: BrokerAnnotation,
    ) -> None:
        ctx = await self.prepare_context(envelope)
        if self._run_accepts_input:
            output = await self.run(ctx, envelope.input)  # type: ignore[call-arg]
        else:
            output = await self.run(ctx)

        if isinstance(output, Silent):
            logging.warning(
                f"node ({self.name}) ran and was silent with no explicit publish. This is the end of this event-stream, any state modifications will not be carried downstream."  # noqa: E501
            )
            return

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
                    input=output.input,
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
                        input=delegate.input,
                    ),
                    topic=delegate.topic,
                    correlation_id=correlation_id,
                )

        elif isinstance(output, list) and output and all(isinstance(e, Emit) for e in output):
            # mypy can't narrow list element types from all(isinstance(...))
            for emit in output:
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
            remaining_topics = [d.topic for d in reversed(output[1:])]  # type: ignore[attr-defined]
            new_stack = [
                *envelope.reply_stack,
                self._return_topic,
                *remaining_topics,
            ]
            first = output[0]
            await broker.publish(
                Envelope(
                    context=BaseSessionRunContext(state=first.value, deps=envelope.context.deps),
                    reply_stack=new_stack,
                    input=first.input,  # type: ignore[union-attr]
                ),
                topic=first.topic,
                correlation_id=correlation_id,
            )

    @property
    def id(self) -> str:
        return self._node_id

    @property
    def name(self) -> str:
        return self._node_id
