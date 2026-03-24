import logging
from typing import Generic, cast

from pydantic import BaseModel

from calfkit._vendor.pydantic_ai import Agent, DeferredToolRequests
from calfkit._vendor.pydantic_ai.messages import RetryPromptPart
from calfkit._vendor.pydantic_ai.tools import DeferredToolResults
from calfkit._vendor.pydantic_ai.toolsets.external import ExternalToolset
from calfkit.experimental._types import AgentDepsT, AgentOutputT
from calfkit.experimental.base_models.actions import Call, NodeResult, ReturnCall, TailCall
from calfkit.experimental.context.agent_context import AgentSessionRunContext
from calfkit.experimental.data_model.state_deps import State
from calfkit.experimental.nodes.base import (
    BaseNodeDef,
)
from calfkit.experimental.nodes.tool_def import ToolNodeDef
from calfkit.providers.pydantic_ai.model_client import PydanticModelClient

logger = logging.getLogger(__name__)

NoneType = type(None)


class BaseAgentNodeDef(
    Generic[AgentDepsT, AgentOutputT],
    BaseNodeDef[AgentDepsT],
):
    def __init__(
        self,
        node_id: str,
        *,
        system_prompt: str = "You are a helpful AI assistant.",
        subscribe_topics: str | list[str],
        publish_topic: str,
        tools: list[ToolNodeDef] | None = None,
        model_client: PydanticModelClient,
        deps_type: type[AgentDepsT] | type[None] = NoneType,
        final_output_type: type[AgentOutputT] | type[str] = str,
    ):
        self.deps_type = deps_type
        self.final_output_type = final_output_type
        self.system_prompt = system_prompt
        self.tools = tools
        self.tools_registry = {tool.tool_schema.name: tool for tool in tools} if tools else dict[str, ToolNodeDef]()
        super().__init__(node_id=node_id, subscribe_topics=subscribe_topics, publish_topic=publish_topic)

        output_types: list[type[AgentOutputT | DeferredToolRequests]] = [
            cast(type[AgentOutputT], final_output_type),
            DeferredToolRequests,
        ]
        self._agent_loop: Agent[AgentDepsT, AgentOutputT | DeferredToolRequests] = Agent(
            model_client,
            name=self.name,
            output_type=output_types,
            deps_type=cast(type[AgentDepsT], deps_type),
        )

    async def run(self, ctx: AgentSessionRunContext[AgentDepsT]) -> NodeResult[State]:
        agent_deps = ctx.deps.agent_deps
        logger.debug(
            "[%s] agent run entered node=%s pending_tool_calls=%d history_len=%d",
            ctx.deps.correlation_id[:8],
            self.name,
            len(ctx.state.latest_tool_calls()),
            len(ctx.state.message_history),
        )

        if agent_deps is not None and self.deps_type is not None:
            if issubclass(self.deps_type, BaseModel) and not isinstance(agent_deps, self.deps_type):
                agent_deps = self.deps_type.model_validate(agent_deps)
                ctx.deps = ctx.deps.model_copy(update={"agent_deps": agent_deps})
            elif not isinstance(agent_deps, self.deps_type):
                logger.error(
                    "incoming deps does not match defined deps_type: deps=%s deps_type=%s",
                    agent_deps,
                    self.deps_type,
                )

        latest_tool_calls = ctx.state.latest_tool_calls()
        tool_results = None

        if len(latest_tool_calls) > 0:
            if not ctx.state.all_call_ids_complete(*[tc.tool_call_id for tc in latest_tool_calls]):
                target_tool_call = next(tc for tc in latest_tool_calls if tc.tool_call_id not in ctx.state.tool_results)
                logger.debug(
                    "[%s] routing pending tool call=%s tool=%s node=%s",
                    ctx.deps.correlation_id[:8],
                    target_tool_call.tool_call_id,
                    target_tool_call.tool_name,
                    self.name,
                )
                return Call[State](
                    self.tools_registry[target_tool_call.tool_name].subscribe_topics[0],
                    ctx.state,
                    target_tool_call.tool_call_id,
                    self.name,
                )

            tool_results = DeferredToolResults(calls={tc.tool_call_id: ctx.state.get_tool_result(tc.tool_call_id) for tc in latest_tool_calls})

        if ctx.state.uncommitted_message is not None:
            ctx.state.commit_message_to_history()

        result = await self._agent_loop.run(
            message_history=ctx.state.message_history,
            instructions=self.system_prompt,
            toolsets=[ExternalToolset([tool.tool_schema for tool in self.tools_registry.values()])],
            deps=agent_deps,  # type: ignore[arg-type]  # None valid when AgentDepsT=NoneType
            deferred_tool_results=tool_results,
        )
        if isinstance(result.output, DeferredToolRequests):
            # The LLM called one or more tools
            logger.debug(
                "[%s] model returned DeferredToolRequests tool_count=%d node=%s",
                ctx.deps.correlation_id[:8],
                len(result.output.calls),
                self.name,
            )
            messages = result.new_messages()  # preserve conversation history
            ctx.state.message_history.extend(messages)
            latest_tool_calls = ctx.state.latest_tool_calls()

            tool_call_state = ctx.state.model_copy(deep=True)
            for tool_call in result.output.calls:
                tool_call_state.add_tool_call(tool_call)

                tool_node = self.tools_registry.get(tool_call.tool_name)
                if tool_node is None:
                    logger.error("tool=%s does not exist.", tool_call.tool_name)
                    ctx.state.add_tool_result(
                        tool_call.tool_call_id,
                        RetryPromptPart(
                            content=f"There is no tool named {tool_call.tool_name}, it does not exist. Please ensure you are only calling tools you are provided.",  # noqa: E501
                            tool_name=tool_call.tool_name,
                            tool_call_id=tool_call.tool_call_id,
                        ),
                    )
                elif tool_node.subscribe_topics is None:
                    logger.error(
                        "tool=%s is unreachable. No subscribe topics were provided for the tool node.",
                        tool_call.tool_name,
                    )
                    ctx.state.add_tool_result(
                        tool_call.tool_call_id,
                        RetryPromptPart(
                            content=f"This tool ({tool_call.tool_name}) is not callable and will not run. Please do not call this tool.",  # noqa: E501
                            tool_name=tool_call.tool_name,
                            tool_call_id=tool_call.tool_call_id,
                        ),
                    )

            if not tool_call_state.all_call_ids_complete(*[tc.tool_call_id for tc in latest_tool_calls]):
                target_tool_call = next(tc for tc in latest_tool_calls if tc.tool_call_id not in ctx.state.tool_results)
                logger.debug(
                    "[%s] routing new tool call=%s tool=%s node=%s",
                    ctx.deps.correlation_id[:8],
                    target_tool_call.tool_call_id,
                    target_tool_call.tool_name,
                    self.name,
                )
                return Call[State](
                    self.tools_registry[target_tool_call.tool_name].subscribe_topics[0],
                    tool_call_state,
                    target_tool_call.tool_call_id,
                    self.name,
                )
            else:  # All tool calls were invalid, we need to retry.
                # TODO: consider a node retry return type that doesn't require round trip to itself.
                # Tailcall to itself is a roundtrip.
                logger.debug("[%s] all tool calls invalid, TailCall retry node=%s", ctx.deps.correlation_id[:8], self.name)
                return TailCall[State](target_topic=self.subscribe_topics[0], state=tool_call_state)

        elif isinstance(result.output, self.final_output_type):
            logger.debug("[%s] final output reached, ReturnCall node=%s", ctx.deps.correlation_id[:8], self.name)
            ctx.state.message_history.extend(result.new_messages())
            return ReturnCall[State](state=ctx.state)

        else:
            raise RuntimeError("Invalid point reached: model output was not the final output type nor a tool call.")
