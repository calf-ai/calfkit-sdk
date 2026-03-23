import inspect
import logging
from abc import abstractmethod
from typing import Annotated, Any, Generic

from faststream import Context
from faststream.kafka.annotations import (
    KafkaBroker as BrokerAnnotation,
)

from calfkit.experimental._types import DepsT, InputT, StateT
from calfkit.experimental.base_models.actions import (
    Call,
    NodeResult,
    ReturnCall,
    Silent,
    TailCall,
)
from calfkit.experimental.base_models.envelope import Envelope
from calfkit.experimental.base_models.session_context import BaseSessionRunContext

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Base node definition
# ---------------------------------------------------------------------------


class BaseNodeDef(Generic[StateT, DepsT]):
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
        if isinstance(subscribe_topics, str):
            self.subscribe_topics = [subscribe_topics]
        elif subscribe_topics is not None:
            self.subscribe_topics = list(subscribe_topics)
        else:
            logging.error(
                f"node {node_id} is not subscribed to any topics. It is unreachable.",
            )
            raise RuntimeError("node {node_id} is not subscribed to any topics. It is unreachable.")
        self.publish_topic = publish_topic
        self._return_topic = f"{node_id}.private.return"

    # TODO: consider multiple abstract methods per node based on the incoming communication pattern,
    # like a delgation or emit. So the communication-specific handler can properly handle it
    @abstractmethod
    async def run(
        self, ctx: BaseSessionRunContext[StateT, DepsT], *args: Any, **kwargs: Any
    ) -> NodeResult[StateT]:
        """Runs the node's logic using provided context.

        Subclasses that need per-invocation input can add an optional parameter
        (any name). The framework inspects the signature at class definition time
        and will pass ``current_frame.input_args`` automatically if the parameter is declared.

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
        if (
            self._run_accepts_input
            and envelope.internal_workflow_state.current_frame.input_args is not None
        ):
            output = await self.run(ctx, *envelope.internal_workflow_state.current_frame.input_args)
        else:
            output = await self.run(ctx)

        if isinstance(output, Call):
            # push to callstack and call the target topic
            envelope.internal_workflow_state.invoke_frame(output, self.subscribe_topics[0])
            await broker.publish(
                Envelope(
                    context=BaseSessionRunContext(state=output.state, deps=envelope.context.deps),
                    internal_workflow_state=envelope.internal_workflow_state,
                ),
                topic=envelope.internal_workflow_state.current_frame.target_topic,
                correlation_id=correlation_id,
            )
        elif isinstance(output, ReturnCall):
            # unwind current frame and return to previous topic
            frame = envelope.internal_workflow_state.unwind_frame()
            await broker.publish(
                Envelope(
                    context=BaseSessionRunContext(state=output.state, deps=envelope.context.deps),
                    internal_workflow_state=envelope.internal_workflow_state,
                ),
                topic=frame.callback_topic,
                correlation_id=correlation_id,
            )
            return

        elif isinstance(output, TailCall):
            # tailcall optimization: replace current call frame with new tailcall
            frame = envelope.internal_workflow_state.unwind_frame()
            envelope.internal_workflow_state.invoke_frame(output, frame.callback_topic)
            await broker.publish(
                Envelope(
                    context=BaseSessionRunContext(state=output.state, deps=envelope.context.deps),
                    internal_workflow_state=envelope.internal_workflow_state,
                ),
                topic=envelope.internal_workflow_state.current_frame.target_topic,
                correlation_id=correlation_id,
            )
            return

        elif isinstance(output, Silent):
            logging.warning(
                f"node ({self.name}) ran and was silent with no explicit publish. This is the end of this event-stream, any state modifications will not be carried downstream."  # noqa: E501
            )
            return
        else:
            logging.error("Return type is unknown so the message was not published anywhere.")

        # elif isinstance(output, Reply):
        #     if not envelope.reply_stack:
        #         logger.warning(
        #             "Node %s: Reply returned but reply_stack is empty — response dropped",
        #             self._node_id,
        #         )
        #         return
        #     topic = envelope.reply_stack[-1]
        #     remaining_stack = envelope.reply_stack[:-1]
        #     await broker.publish(
        #         Envelope(
        #             context=BaseSessionRunContext(state=output.value, deps=envelope.context.deps),
        #             reply_stack=remaining_stack,
        #         ),
        #         topic=topic,
        #         correlation_id=correlation_id,
        #     )

        # elif isinstance(output, Delegate):
        #     new_stack = [*envelope.reply_stack, self._return_topic]
        #     await broker.publish(
        #         Envelope(
        #             context=BaseSessionRunContext(state=output.value, deps=envelope.context.deps),
        #             reply_stack=new_stack,
        #             input_args=output.input_args,
        #         ),
        #         topic=output.topic,
        #         correlation_id=correlation_id,
        #     )

        # elif isinstance(output, Emit):
        #     await broker.publish(
        #         BaseSessionRunContext(state=output.value, deps=envelope.context.deps),
        #         topic=output.topic,
        #         correlation_id=correlation_id,
        #     )

        # elif isinstance(output, Parallel):
        #     new_stack = [*envelope.reply_stack, self._return_topic]
        #     for delegate in output.delegates:
        #         await broker.publish(
        #             Envelope(
        #                 context=BaseSessionRunContext(
        #                     state=delegate.value, deps=envelope.context.deps
        #                 ),
        #                 reply_stack=new_stack,
        #                 input_args=delegate.input_args,
        #             ),
        #             topic=delegate.topic,
        #             correlation_id=correlation_id,
        #         )

        # elif isinstance(output, list) and output and all(isinstance(e, Emit) for e in output):
        #     # mypy can't narrow list element types from all(isinstance(...))
        #     for emit in output:
        #         await broker.publish(
        #             BaseSessionRunContext(state=emit.value, deps=envelope.context.deps),
        #             topic=emit.topic,
        #             correlation_id=correlation_id,
        #         )

        # elif isinstance(output, list) and output and all(isinstance(d, Delegate) for d in output):
        #     # Sequential multi-delegate: chain via reply_stack.
        #     # Push self._return_topic (bottom), then remaining delegate
        #     # topics in reverse, so popping goes:
        #     #   first.topic → output[1].topic → ... → self._return_topic
        #     remaining_topics = [d.topic for d in reversed(output[1:])]
        #     new_stack = [
        #         *envelope.reply_stack,
        #         self._return_topic,
        #         *remaining_topics,
        #     ]
        #     first = output[0]
        #     if not isinstance(first, Delegate):  # just to silence the type checker
        #         return
        #     await broker.publish(
        #         Envelope(
        #             context=BaseSessionRunContext(state=first.value, deps=envelope.context.deps),
        #             reply_stack=new_stack,
        #             input_args=first.input_args,
        #         ),
        #         topic=first.topic,
        #         correlation_id=correlation_id,
        #     )

    @property
    def id(self) -> str:
        return self._node_id

    @property
    def name(self) -> str:
        return self._node_id
