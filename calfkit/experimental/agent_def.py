import logging
import time
from collections.abc import Callable
from typing import Any, Generic

from pydantic import BaseModel

from calfkit._vendor.pydantic_ai import Agent, DeferredToolRequests
from calfkit._vendor.pydantic_ai.toolsets.external import ExternalToolset
from calfkit.experimental.context_models import BaseSessionRunContext
from calfkit.experimental.node_def import (
    BaseNodeDef,
    Delegate,
    Envelope,
    NodeResult,
    Reply,
)
from calfkit.experimental.payload_model import ToolCallPart
from calfkit.experimental.state_and_deps_models import (
    AgentDepsT,
    AgentOutputT,
    Deps,
    Payload,
    State,
)
from calfkit.experimental.tool_def import ToolNodeDef
from calfkit.providers.pydantic_ai.model_client import PydanticModelClient


class BaseAgentNodeDef(
    Generic[AgentDepsT, AgentOutputT],
    BaseNodeDef[State, Deps[AgentDepsT], State],
):
    def __init__(
        self,
        agent_id: str,
        *,
        system_prompt: str = "You are a helpful AI assistant.",
        tools: list[ToolNodeDef] | None = None,
        model_client: PydanticModelClient,
        deps_type: type[AgentDepsT] | None = None,
        final_output_type: type[AgentOutputT] | type[str] = type[str],
        input_to_prompt_func: Callable[[BaseSessionRunContext[State, Deps[AgentDepsT]]], str]
        | None = None,
    ):
        if input_to_prompt_func is None:
            self._input_to_prompt_func = self._prepare_prompt
        else:
            self._input_to_prompt_func = input_to_prompt_func
        self.deps_type = deps_type
        self.final_output_type = final_output_type
        self.system_prompt = system_prompt
        self.tools = tools or list[ToolNodeDef]()
        super().__init__(agent_id)

        self._agent_loop: Agent[Any, Any] = Agent(
            model_client,
            name=self.name,
            output_type=[final_output_type, DeferredToolRequests],  # tool calling is always on
        )

    def input_to_prompt(
        self, func: Callable[[BaseSessionRunContext[State, Deps[AgentDepsT]]], str]
    ) -> Callable[[BaseSessionRunContext[State, Deps[AgentDepsT]]], str]:
        """decorator to define function to parse structured input to string"""
        self._input_to_prompt_func = func
        return func

    def _prepare_prompt(self, ctx: BaseSessionRunContext[State, Deps[AgentDepsT]]) -> str:
        return ctx.state.todo_stack[-1].text()

    async def run(self, ctx: BaseSessionRunContext[State, Deps[AgentDepsT]]) -> NodeResult[State]:
        prompt = self._input_to_prompt_func(ctx)
        if ctx.deps.agent_deps is not None and self.deps_type is not None:
            if issubclass(self.deps_type, BaseModel):
                ctx.deps.agent_deps = self.deps_type.model_validate(ctx.deps.agent_deps)
            else:
                if not isinstance(ctx.deps.agent_deps, self.deps_type):
                    logging.error(
                        "incoming deps does not match defined deps_type: \n"
                        + f"deps={ctx.deps.agent_deps}\n\ndeps_type={self.deps_type}"
                    )
        result = await self._agent_loop.run(
            user_prompt=prompt,
            message_history=ctx.state.message_history,
            instructions=self.system_prompt,
            toolsets=[ExternalToolset([tool.tool_schema for tool in self.tools])],
            deps=ctx.deps.agent_deps,
        )
        if isinstance(result.output, DeferredToolRequests):
            # The LLM called one or more tools
            messages = result.new_messages()  # preserve conversation history
            ctx.state.message_history.extend(messages)

            tool_state_delegations = list[Delegate[State]]()
            tool_call_state = State()
            for tool_call in result.output.calls:
                # TODO: fix multiple calls to same tool.
                # Multiple publishes to same topic, there may be duplication of work.
                tool_call_state.todo_stack.append(
                    Payload[AgentOutputT](
                        correlation_id=ctx.deps.correlation_id,
                        source_node_id=self.id,
                        timestamp=time.time(),
                        parts=[
                            ToolCallPart(
                                tool_call_id=tool_call.tool_call_id,
                                kwargs=tool_call.args_as_dict(),
                                tool_name=tool_call.tool_name,
                            )
                        ],
                    )
                )

                tool_state_delegations.append(
                    Delegate[State](
                        topic="test",
                    )
                )
            tool_state_delegations[-1].value = tool_call_state
            return tool_state_delegations

        elif isinstance(result.output, self.final_output_type):
            ctx.state.message_history.extend(result.new_messages())
            return Reply(value=ctx.state)

        else:
            raise RuntimeError(
                "Invalid point reached: model output was not the final output type nor a tool call."
            )
