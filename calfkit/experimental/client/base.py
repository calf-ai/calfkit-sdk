import os
from collections.abc import Iterable
from typing import Any, Sequence

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
from calfkit.experimental.nodes.node_def import BaseNodeDef


class BaseClient:
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

    async def invoke_node(
        self,
        node: BaseNodeDef[StateT, DepsT],
        topic: str,
        reply_topic: str,
        run_args: Sequence[Any],
        correlation_id: str,
        state: StateT,
        deps: DepsT | None = None,
    ) -> InvocationHandle[StateT]:
        """Invoke the agent asynchronously, fire-and-forget.

        Args:
            topic: Topic to send args to.
            run_args: the args to send to the node's run() method
            correlation_id: Optionally provide a correlation ID for this request.
            result_type: The type of the output.

        Returns:
            The execution handle.
        """
        if not self._connection._connection:
            await self._connection.start()

        call_stack = CallFrameStack()
        call_stack.push(CallFrame(topic, reply_topic, input_args=run_args))

        envelope = Envelope[StateT, DepsT](
            internal_workflow_state=WorkflowState(call_stack=call_stack),
            context=SessionRunContext(
                state=state, deps=Deps(correlation_id=correlation_id, agent_deps=deps)
            ),
        )
        await self._connection.publish(envelope, correlation_id=correlation_id)

        return InvocationHandle[StateT](
            correlation_id=correlation_id,
            state_type=type(state),
        )
