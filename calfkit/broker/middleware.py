from typing import Any, Awaitable, Callable

from faststream import BaseMiddleware, PublishCommand
from faststream.message import StreamMessage
from faststream.types import AsyncFuncAny


class ContextInjectionMiddleware(BaseMiddleware):
    async def consume_scope(
        self,
        call_next: AsyncFuncAny,
        msg: StreamMessage[Any],
    ) -> Any:
        with self.context.scope("correlation_id", msg.correlation_id):
            return await super().consume_scope(call_next, msg)

    async def publish_scope(
        self,
        call_next: Callable[[PublishCommand], Awaitable[Any]],
        cmd: PublishCommand,
    ) -> Any:
        return await call_next(cmd)
