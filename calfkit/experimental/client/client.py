from collections.abc import Sequence
from typing import Any

from calfkit._vendor.pydantic_ai.messages import ModelMessage, ModelRequest
from calfkit.experimental.client.base import BaseClient
from calfkit.experimental.client.invocation_handle import InvocationHandle
from calfkit.experimental.data_model.state_deps import State


class Client(BaseClient[State]):
    async def invoke_node(
        self,
        user_prompt: str,
        topic: str,
        reply_topic: str,
        correlation_id: str,
        temp_instructions: str | None = None,
        message_history: list[ModelMessage] | None = None,
        run_args: Sequence[Any] | None = None,
        deps: dict[str, Any] | None = None,
    ) -> InvocationHandle:
        state = State(message_history=message_history or list())
        state.stage_message(ModelRequest.user_text_prompt(user_prompt, instructions=temp_instructions))
        return await self._invoke(
            topic=topic,
            reply_topic=reply_topic,
            correlation_id=correlation_id,
            run_args=run_args,
            state=state,
            deps=deps,
        )
