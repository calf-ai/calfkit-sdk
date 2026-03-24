import logging
import os
from collections.abc import Iterable, Sequence
from typing import Any, Generic

from faststream.kafka import KafkaBroker
from typing_extensions import Self

from calfkit.broker.middleware import ContextInjectionMiddleware
from calfkit.experimental._types import DepsT, StateT
from calfkit.experimental.base_models.envelope import Envelope
from calfkit.experimental.base_models.session_context import (
    CallFrame,
    CallFrameStack,
    Deps,
    SessionRunContext,
    WorkflowState,
)
from calfkit.experimental.client.invocation_handle import InvocationHandle
from calfkit.experimental.data_model.state_deps import State

logger = logging.getLogger(__name__)


class BaseClient(Generic[StateT]):
    def __init__(self, connection: KafkaBroker):
        self._connection = connection
        return

    @classmethod
    def connect(cls, server_urls: str | Iterable[str] | None = None, **broker_kwargs: Any) -> Self:
        if server_urls is None:
            server_urls = os.getenv("CALF_HOST_URL") or "localhost"
        broker_connection = KafkaBroker(
            server_urls,
            middlewares=[ContextInjectionMiddleware],
            **broker_kwargs,
        )
        return cls(broker_connection)

    @property
    def broker(self) -> KafkaBroker:
        return self._connection

    async def _invoke(
        self,
        topic: str,
        reply_topic: str,
        correlation_id: str,
        state: State,
        run_args: Sequence[Any] | None = None,
        deps: dict[str, Any] | None = None,
    ) -> InvocationHandle:
        """Invoke the node asynchronously, fire-and-forget.

        Args:
            topic: Topic to send args to.
            run_args: the args to send to the node's run() method
            correlation_id: Optionally provide a correlation ID for this request.
            result_type: The type of the output.

        Returns:
            The execution handle.
        """
        logger.debug("[%s] invoke topic=%s reply=%s", correlation_id[:8], topic, reply_topic)

        if not self._connection._connection:
            await self._connection.start()

        call_stack = CallFrameStack()
        call_stack.push(CallFrame(target_topic=topic, callback_topic=reply_topic, input_args=run_args))

        envelope = Envelope(
            internal_workflow_state=WorkflowState(call_stack=call_stack),
            context=SessionRunContext(state=state, deps=Deps(correlation_id=correlation_id, provided_deps=deps or dict())),
        )
        await self._connection.publish(envelope, topic=topic, correlation_id=correlation_id)

        return InvocationHandle(
            correlation_id=correlation_id,
        )
