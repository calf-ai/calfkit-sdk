from collections.abc import Sequence
from typing import Any

import uuid_utils

from calfkit._vendor.pydantic_ai.messages import ModelMessage, ModelRequest
from calfkit.experimental.base_models.envelope import Envelope
from calfkit.experimental.client.base import BaseClient
from calfkit.experimental.client.invocation_handle import InvocationHandle
from calfkit.experimental.data_model.state_deps import State


class Client(BaseClient):
    async def invoke_node(
        self,
        user_prompt: str,
        topic: str,
        *,
        reply_topic: str | None = None,
        correlation_id: str | None = None,
        temp_instructions: str | None = None,
        message_history: list[ModelMessage] | None = None,
        run_args: Sequence[Any] | None = None,
        deps: dict[str, Any] | None = None,
    ) -> InvocationHandle:
        if correlation_id is None:
            correlation_id = uuid_utils.uuid7().hex
        if reply_topic is None:
            reply_topic = self._reply_topic

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

    async def execute_node(
        self,
        user_prompt: str,
        topic: str,
        *,
        reply_topic: str | None = None,
        correlation_id: str | None = None,
        temp_instructions: str | None = None,
        message_history: list[ModelMessage] | None = None,
        run_args: Sequence[Any] | None = None,
        deps: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> Envelope:
        """Invoke a node and await the result.

        Convenience wrapper: equivalent to ``(await invoke_node(...)).result(timeout)``.
        """
        handle = await self.invoke_node(
            user_prompt,
            topic,
            reply_topic=reply_topic,
            correlation_id=correlation_id,
            temp_instructions=temp_instructions,
            message_history=message_history,
            run_args=run_args,
            deps=deps,
        )
        return await handle.result(timeout=timeout)
