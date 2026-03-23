import logging
from typing import Any, Generic

from pydantic import BaseModel

from calfkit._vendor.pydantic_ai import Agent, DeferredToolRequests
from calfkit._vendor.pydantic_ai.messages import RetryPromptPart
from calfkit._vendor.pydantic_ai.tools import DeferredToolResults
from calfkit._vendor.pydantic_ai.toolsets.external import ExternalToolset
from calfkit.experimental._types import AgentDepsT, AgentOutputT, InputT
from calfkit.experimental.base_models.actions import Call, NodeResult, ReturnCall, TailCall
from calfkit.experimental.context.agent_context import AgentSessionRunContext
from calfkit.experimental.data_model.state_deps import (
    Deps,
    State,
)
from calfkit.experimental.nodes.node_def import (
    BaseNodeDef,
)
from calfkit.experimental.nodes.tool_def import ToolNodeDef
from calfkit.providers.pydantic_ai.model_client import PydanticModelClient


class BaseAgentNodeDef(
    Generic[AgentDepsT, AgentOutputT, InputT],
    BaseNodeDef[State, Deps[AgentDepsT], InputT],
):
    def __init__(
        self,
        agent_id: str,
        *,
        system_prompt: str = "You are a helpful AI assistant.",
        subscribe_topics: str | list[str],
        publish_topic: str | None = None,
        tools: list[ToolNodeDef] | None = None,
        model_client: PydanticModelClient,
        deps_type: type[AgentDepsT] | None = None,
        final_output_type: type[AgentOutputT] | type[str] = str,
    ):
        self.deps_type = deps_type
        self.final_output_type = final_output_type
        self.system_prompt = system_prompt
        if tools:
            self.tools = {tool.tool_schema.name: tool for tool in tools}
        else:
            self.tools = dict[str, ToolNodeDef]()
        super().__init__(agent_id, subscribe_topics=subscribe_topics, publish_topic=publish_topic)

        self._agent_loop: Agent[Any, Any] = Agent(
            model_client,
            name=self.name,
            output_type=[final_output_type, DeferredToolRequests],  # tool calling is always on
        )

    async def run(self, ctx: AgentSessionRunContext[AgentDepsT]) -> NodeResult[State]:
        if ctx.deps.agent_deps is not None and self.deps_type is not None:
            if issubclass(self.deps_type, BaseModel):
                ctx.deps.agent_deps = self.deps_type.model_validate(ctx.deps.agent_deps)
            else:
                if not isinstance(ctx.deps.agent_deps, self.deps_type):
                    logging.error(
                        "incoming deps does not match defined deps_type: \n"
                        + f"deps={ctx.deps.agent_deps}\n\ndeps_type={self.deps_type}"
                    )

        latest_tool_calls = ctx.state.latest_tool_calls()
        tool_results = None

        if len(latest_tool_calls) > 0:
            if not ctx.state.all_call_ids_complete(*[tc.tool_call_id for tc in latest_tool_calls]):
                target_tool_call = next(
                    tc for tc in latest_tool_calls if tc.tool_call_id not in ctx.state.tool_results
                )
                return Call[State](
                    self.tools[target_tool_call.tool_name].subscribe_topics[0],
                    ctx.state,
                    target_tool_call.tool_call_id,
                    self.name,
                )

            tool_results = DeferredToolResults(
                calls={
                    tc.tool_call_id: ctx.state.get_tool_result(tc.tool_call_id)
                    for tc in latest_tool_calls
                }
            )

        if ctx.state.uncommitted_message is not None:
            ctx.state.commit_message_to_history()

        result = await self._agent_loop.run(
            message_history=ctx.state.message_history,
            instructions=self.system_prompt,
            toolsets=[ExternalToolset([tool.tool_schema for tool in self.tools.values()])],
            deps=ctx.deps.agent_deps,
            deferred_tool_results=tool_results,
        )
        if isinstance(result.output, DeferredToolRequests):
            # The LLM called one or more tools
            messages = result.new_messages()  # preserve conversation history
            ctx.state.message_history.extend(messages)
            latest_tool_calls = ctx.state.latest_tool_calls()

            tool_call_state = ctx.state.model_copy(deep=True)
            for tool_call in result.output.calls:
                tool_call_state.add_tool_call(tool_call)

                tool_node = self.tools.get(tool_call.tool_name)
                if tool_node is None:
                    logging.error(f"tool={tool_call.tool_name} does not exist.")
                    ctx.state.add_tool_result(
                        tool_call.tool_call_id,
                        RetryPromptPart(
                            content=f"There is no tool named {tool_call.tool_name}, it does not exist. Please ensure you are only calling tools you are provided.",  # noqa: E501
                            tool_name=tool_call.tool_name,
                            tool_call_id=tool_call.tool_call_id,
                        ),
                    )
                elif tool_node.subscribe_topics is None:
                    logging.error(
                        f"tool={tool_call.tool_name} is unreachable. No subscribe topics were provided for the tool node."  # noqa: E501
                    )
                    ctx.state.add_tool_result(
                        tool_call.tool_call_id,
                        RetryPromptPart(
                            content=f"This tool ({tool_call.tool_name}) is not callable and will not run. Please do not call this tool.",  # noqa: E501
                            tool_name=tool_call.tool_name,
                            tool_call_id=tool_call.tool_call_id,
                        ),
                    )

            if not tool_call_state.all_call_ids_complete(
                *[tc.tool_call_id for tc in latest_tool_calls]
            ):
                target_tool_call = next(
                    tc for tc in latest_tool_calls if tc.tool_call_id not in ctx.state.tool_results
                )
                return Call[State](
                    self.tools[target_tool_call.tool_name].subscribe_topics[0],
                    tool_call_state,
                    target_tool_call.tool_call_id,
                    self.name,
                )
            else:  # All tool calls were invalid, we need to retry.
                # TODO: consider a node retry return type that doesn't require round trip to itself.
                # Tailcall to itself is a roundtrip.
                return TailCall[State](target_topic=self.subscribe_topics[0], state=tool_call_state)

        elif isinstance(result.output, self.final_output_type):
            ctx.state.message_history.extend(result.new_messages())
            return ReturnCall[State](state=ctx.state)

        else:
            raise RuntimeError(
                "Invalid point reached: model output was not the final output type nor a tool call."
            )
