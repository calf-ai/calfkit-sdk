import logging
import os
from collections.abc import Iterable, Sequence
from typing import Any

import uuid_utils
from faststream.kafka import KafkaBroker
from typing_extensions import Self

from calfkit.broker.middleware import ContextInjectionMiddleware
from calfkit.experimental.base_models.envelope import Envelope
from calfkit.experimental.base_models.session_context import (
    CallFrame,
    CallFrameStack,
    Deps,
    SessionRunContext,
    WorkflowState,
)
from calfkit.experimental.client.invocation_handle import InvocationHandle
from calfkit.experimental.client.reply_dispatcher import _ReplyDispatcher
from calfkit.experimental.data_model.state_deps import State

logger = logging.getLogger(__name__)


class BaseClient:
    def __init__(self, connection: KafkaBroker, reply_topic: str, dispatcher: _ReplyDispatcher):
        self._connection = connection
        self._reply_topic = reply_topic
        self._dispatcher = dispatcher

    @classmethod
    def connect(
        cls,
        server_urls: str | Iterable[str] | None = None,
        reply_topic: str | None = None,
        **broker_kwargs: Any,
    ) -> Self:
        if server_urls is None:
            server_urls = os.getenv("CALF_HOST_URL") or "localhost"

        client_id = uuid_utils.uuid7().hex
        if reply_topic is None:
            reply_topic = f"calf-client-reply-{client_id}"
        group_id = f"calf-client-reply-{client_id}"

        broker_connection = KafkaBroker(
            server_urls,
            middlewares=[ContextInjectionMiddleware],
            **broker_kwargs,
        )

        dispatcher = _ReplyDispatcher()
        dispatcher.register(broker_connection, reply_topic, group_id)

        return cls(broker_connection, reply_topic, dispatcher)

    @property
    def broker(self) -> KafkaBroker:
        return self._connection

    @property
    def reply_topic(self) -> str:
        return self._reply_topic

    async def _invoke(
        self,
        topic: str,
        reply_topic: str,
        correlation_id: str,
        state: State,
        run_args: Sequence[Any] | None = None,
        deps: dict[str, Any] | None = None,
    ) -> InvocationHandle:
        """Invoke the node asynchronously.

        Args:
            topic: Topic to send args to.
            reply_topic: Topic the node should reply to.
            correlation_id: Correlation ID for this request.
            state: The session state.
            run_args: The args to send to the node's run() method.
            deps: Provided dependencies.

        Returns:
            An invocation handle with an associated future for the reply.
        """
        future = self._dispatcher.expect(correlation_id)

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
            topic=topic,
            reply_topic=reply_topic,
            _future=future,
        )

    async def close(self) -> None:
        """Close the dispatcher and stop the broker connection."""
        self._dispatcher.close()
        await self._connection.stop()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()
