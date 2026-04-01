import inspect
import logging
from abc import abstractmethod
from typing import Annotated, Any

from faststream import Context
from faststream.kafka.annotations import (
    KafkaBroker as BrokerAnnotation,
)

from calfkit.models import (
    Call,
    NodeResult,
    ReturnCall,
    Silent,
    State,
    TailCall,
)
from calfkit.models.envelope import Envelope
from calfkit.models.session_context import SessionRunContext

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Base node definition
# ---------------------------------------------------------------------------


class BaseNodeDef:
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
        subscribe_topics: str | list[str],
        publish_topic: str | None,
    ):
        self._node_id = node_id
        if isinstance(subscribe_topics, str):
            self.subscribe_topics = [subscribe_topics]
        elif subscribe_topics is not None:
            self.subscribe_topics = list(subscribe_topics)
        self.publish_topic = publish_topic
        self._return_topic = f"{node_id}.private.return"

    # TODO: consider multiple abstract methods per node based on the incoming communication pattern,
    # like a delgation or emit. So the communication-specific handler can properly handle it
    @abstractmethod
    async def run(self, ctx: SessionRunContext, *args: Any, **kwargs: Any) -> NodeResult[State]:
        """Runs the node's logic using provided context.

        Subclasses that need per-invocation input can add an optional parameter
        (any name). The framework inspects the signature at class definition time
        and will pass ``current_frame.input_args`` automatically if the parameter is declared.

        Args:
            ctx: Session context containing mutable state and immutable dependencies.

        Raises:
            NotImplementedError: If node subclass does not implement the run() method.

        Returns:
            NodeResult[State]: Execution results persisted via modified state wrapped
            in the NodeResult type. Different NodeResult types define how results
            should be communicated.
        """
        raise NotImplementedError()

    async def prepare_context(self, envelope: Envelope) -> SessionRunContext:
        ctx = envelope.context.model_copy(deep=True)
        return ctx

    async def _publish_action(self, output: NodeResult[State], envelope: Envelope, correlation_id: str, broker: BrokerAnnotation) -> Envelope:
        publish_envelope: Envelope

        if isinstance(output, list) and all(isinstance(item, Call) for item in output):
            # Parallel fan-out: publish each Call with independent workflow_state
            for call in output:
                wf_copy = envelope.internal_workflow_state.model_copy(deep=True)
                wf_copy.invoke_frame(call, self.subscribe_topics[0])
                publish_envelope = Envelope(
                    context=SessionRunContext(state=call.state, deps=envelope.context.deps),
                    internal_workflow_state=wf_copy,
                )
                await broker.publish(
                    publish_envelope,
                    topic=wf_copy.current_frame.target_topic,
                    correlation_id=correlation_id,
                    key=correlation_id.encode(),
                )
            return envelope

        elif isinstance(output, Call):
            # push to callstack and call the target topic
            envelope.internal_workflow_state.invoke_frame(output, self.subscribe_topics[0])
            publish_envelope = Envelope(
                context=SessionRunContext(state=output.state, deps=envelope.context.deps),
                internal_workflow_state=envelope.internal_workflow_state,
            )
            target_topic = envelope.internal_workflow_state.current_frame.target_topic
            logger.debug("[%s] Call target=%s frame_pushed node=%s", correlation_id[:8], target_topic, self._node_id)
            await broker.publish(
                publish_envelope,
                topic=target_topic,
                correlation_id=correlation_id,
                key=correlation_id.encode(),
            )
        elif isinstance(output, ReturnCall):
            # unwind current frame and return to previous topic
            frame = envelope.internal_workflow_state.unwind_frame()
            publish_envelope = Envelope(
                context=SessionRunContext(state=output.state, deps=envelope.context.deps),
                internal_workflow_state=envelope.internal_workflow_state,
            )
            logger.debug("[%s] ReturnCall callback=%s node=%s", correlation_id[:8], frame.callback_topic, self._node_id)
            await broker.publish(
                publish_envelope,
                topic=frame.callback_topic,
                correlation_id=correlation_id,
                key=correlation_id.encode(),
            )

        elif isinstance(output, TailCall):
            # tailcall optimization: replace current call frame with new tailcall
            frame = envelope.internal_workflow_state.unwind_frame()
            envelope.internal_workflow_state.invoke_frame(output, frame.callback_topic)
            publish_envelope = Envelope(
                context=SessionRunContext(state=output.state, deps=envelope.context.deps),
                internal_workflow_state=envelope.internal_workflow_state,
            )
            target_topic = envelope.internal_workflow_state.current_frame.target_topic
            logger.debug("[%s] TailCall target=%s node=%s", correlation_id[:8], target_topic, self._node_id)
            await broker.publish(
                publish_envelope,
                topic=target_topic,
                correlation_id=correlation_id,
                key=correlation_id.encode(),
            )

        elif isinstance(output, Silent):
            logger.warning(
                "node (%s) ran and was silent with no explicit publish. This is the end of this event-stream, any state modifications will not be carried downstream.",  # noqa: E501
                self.name,
            )
            publish_envelope = envelope
        else:
            logger.error("Return type is unknown or invalid so the message was not published anywhere.")
            publish_envelope = envelope

        return publish_envelope

    async def handler(
        self,
        envelope: Envelope,
        correlation_id: Annotated[str, Context()],
        broker: BrokerAnnotation,
    ) -> Envelope:
        logger.debug("[%s] handler entered node=%s", correlation_id[:8], self._node_id)
        ctx = await self.prepare_context(envelope)
        if self._run_accepts_input and envelope.internal_workflow_state.current_frame.input_args is not None:
            output = await self.run(ctx, *envelope.internal_workflow_state.current_frame.input_args)
        else:
            output = await self.run(ctx)

        logger.debug("[%s] run() returned action=%s node=%s", correlation_id[:8], type(output).__name__, self._node_id)

        return await self._publish_action(output, envelope, correlation_id, broker)

    @property
    def id(self) -> str:
        return self._node_id

    @property
    def name(self) -> str:
        return self._node_id
