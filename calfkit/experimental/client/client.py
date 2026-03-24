from collections.abc import Sequence
from typing import Any, Generic

from calfkit._vendor.pydantic_ai.messages import ModelRequest
from calfkit.experimental._types import AgentDepsT
from calfkit.experimental.client.base import BaseClient
from calfkit.experimental.data_model.state_deps import State


class Client(Generic[AgentDepsT], BaseClient[State, AgentDepsT]):
    async def invoke_node(
        self,
        user_prompt: str,
        topic: str,
        reply_topic: str,
        correlation_id: str,
        run_args: Sequence[Any] | None = None,
        deps: AgentDepsT = None,
    ):
        state = State()
        state.stage_message(ModelRequest.user_text_prompt(user_prompt))
        await self._invoke(
            topic=topic,
            reply_topic=reply_topic,
            correlation_id=correlation_id,
            run_args=run_args,
            state=state,
            deps=deps,
        )
