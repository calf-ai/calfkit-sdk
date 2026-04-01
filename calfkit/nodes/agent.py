import logging
from collections.abc import Callable
from typing import Any, Generic

from calfkit._types import AgentOutputT
from calfkit._vendor.pydantic_ai import Agent as InternalAgentLoop
from calfkit._vendor.pydantic_ai import DeferredToolRequests
from calfkit._vendor.pydantic_ai.messages import RetryPromptPart
from calfkit._vendor.pydantic_ai.output import OutputSpec
from calfkit._vendor.pydantic_ai.tools import DeferredToolResults
from calfkit._vendor.pydantic_ai.toolsets.external import ExternalToolset
from calfkit.models import Call, DataPart, NodeResult, ReturnCall, State, TailCall, TextPart
from calfkit.models.actions import Silent
from calfkit.models.session_context import SessionRunContext
from calfkit.models.state import PendingToolBatch
from calfkit.nodes.base import BaseNodeDef
from calfkit.nodes.tool import ToolNodeDef
from calfkit.providers.pydantic_ai.model_client import PydanticModelClient

logger = logging.getLogger(__name__)

NoneType = type(None)


class BaseAgentNodeDef(
    Generic[AgentOutputT],
    BaseNodeDef,
):
    def __init__(
        self,
        node_id: str,
        *,
        system_prompt: str = "You are a helpful AI assistant.",
        subscribe_topics: str | list[str],
        publish_topic: str | None = None,
        tools: list[ToolNodeDef] | None = None,
        model_client: PydanticModelClient,
        final_output_type: OutputSpec[AgentOutputT] = str,  # type: ignore[assignment]
        sequential_only_mode: bool = False,
    ):
        self.final_output_type = final_output_type
        self.system_prompt = system_prompt
        self.tools = tools or list()
        self.sequential_only_mode = sequential_only_mode
        self._pending_batches: dict[str, PendingToolBatch] = dict()

        super().__init__(node_id=node_id, subscribe_topics=subscribe_topics, publish_topic=publish_topic)

        self._agent_loop: InternalAgentLoop[dict[str, Any], AgentOutputT | DeferredToolRequests] = InternalAgentLoop(
            model_client, name=self.name, output_type=[final_output_type, DeferredToolRequests], deps_type=dict, instructions=system_prompt
        )

    def _parallel_state_aggregation(self, ctx: SessionRunContext) -> None:
        batch = self._pending_batches.get(ctx.deps.correlation_id)
        if batch is not None:
            for tool_call_id in batch.expected_tool_call_ids:
                if tool_call_id not in batch.collected_results and tool_call_id in ctx.state.tool_results:
                    batch.collected_results[tool_call_id] = ctx.state.tool_results[tool_call_id]

            if batch.is_complete:
                for tool_call_id, tool_call_result in batch.collected_results.items():
                    batch.base_state.add_tool_result(tool_call_id, tool_call_result)
                ctx.state = batch.base_state
                del self._pending_batches[ctx.deps.correlation_id]

    async def run(self, ctx: SessionRunContext) -> NodeResult[State]:
        tools_registry = {tool.tool_schema.name: tool for tool in self.tools} if self.tools else dict[str, ToolNodeDef]()

        logger.debug(
            "[%s] agent run entered node=%s pending_tool_calls=%d history_len=%d",
            ctx.deps.correlation_id[:8],
            self.name,
            len(ctx.state.latest_tool_calls()),
            len(ctx.state.message_history),
        )

        if not self.sequential_only_mode:
            self._parallel_state_aggregation(ctx)
            batch = self._pending_batches.get(ctx.deps.correlation_id)
            if batch and not batch.is_complete:
                return Silent()

        latest_tool_calls = ctx.state.latest_tool_calls()
        tool_results = None

        if len(latest_tool_calls) > 0:
            if not ctx.state.all_call_ids_complete(*[tc.tool_call_id for tc in latest_tool_calls]):
                if self.sequential_only_mode:
                    target_tool_call = next(tc for tc in latest_tool_calls if tc.tool_call_id not in ctx.state.tool_results)
                    logger.debug(
                        "[%s] routing pending tool call=%s tool=%s node=%s",
                        ctx.deps.correlation_id[:8],
                        target_tool_call.tool_call_id,
                        target_tool_call.tool_name,
                        self.name,
                    )
                    return Call[State](
                        tools_registry[target_tool_call.tool_name].subscribe_topics[0],
                        ctx.state,
                        target_tool_call.tool_call_id,
                        self.name,
                    )
                else:
                    remaining = [tc for tc in latest_tool_calls if tc.tool_call_id not in ctx.state.tool_results]
                    raise RuntimeError(
                        f"[{ctx.deps.correlation_id[:8]}] Parallel mode reached incomplete tool calls outside aggregation gate. "
                        f"node={self.name} remaining_ids={[tc.tool_call_id for tc in remaining]}. "
                        f"This indicates lost PendingToolBatch state (e.g. partition rebalance or process restart)."
                    )

            tool_results = DeferredToolResults(calls={tc.tool_call_id: ctx.state.get_tool_result(tc.tool_call_id) for tc in latest_tool_calls})

        if ctx.state.uncommitted_message is not None:
            ctx.state.commit_message_to_history()

        result = await self._agent_loop.run(
            message_history=ctx.state.message_history,
            instructions=ctx.state.temp_instructions,
            toolsets=[ExternalToolset([tool.tool_schema for tool in tools_registry.values()])],
            deps=ctx.deps.provided_deps,  # None valid when AgentDepsT=NoneType
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

            for tool_call in result.output.calls:
                ctx.state.add_tool_call(tool_call)

                tool_node = tools_registry.get(tool_call.tool_name)
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

            if ctx.state.all_call_ids_complete(*[tc.tool_call_id for tc in latest_tool_calls]):  # All tool calls were invalid, we need to retry.
                # TODO: maybe consider a node retry return type that doesn't require round trip to itself.
                # Tailcall to itself is a roundtrip.
                logger.debug("[%s] all tool calls invalid, TailCall retry node=%s", ctx.deps.correlation_id[:8], self.name)
                return TailCall[State](target_topic=self.subscribe_topics[0], state=ctx.state)

            pending_tool_calls = [tc for tc in latest_tool_calls if tc.tool_call_id not in ctx.state.tool_results]

            if self.sequential_only_mode or len(pending_tool_calls) == 1:
                target_tool_call = pending_tool_calls[0]
                logger.debug(
                    "[%s] routing new tool call=%s tool=%s node=%s",
                    ctx.deps.correlation_id[:8],
                    target_tool_call.tool_call_id,
                    target_tool_call.tool_name,
                    self.name,
                )
                return Call[State](
                    tools_registry[target_tool_call.tool_name].subscribe_topics[0],
                    ctx.state,
                    target_tool_call.tool_call_id,
                    self.name,
                )
            else:
                launch_tool_call_ids = [tc.tool_call_id for tc in pending_tool_calls]
                parallel_tool_calls = [
                    Call[State](
                        tools_registry[tc.tool_name].subscribe_topics[0],
                        ctx.state.model_copy(deep=True),
                        tc.tool_call_id,
                        self.name,
                    )
                    for tc in pending_tool_calls
                ]

                self._pending_batches[ctx.deps.correlation_id] = PendingToolBatch(
                    expected_tool_call_ids=frozenset(launch_tool_call_ids),
                    base_state=ctx.state.model_copy(deep=True),
                )

                return parallel_tool_calls

        else:
            logger.debug("[%s] final output reached, ReturnCall node=%s", ctx.deps.correlation_id[:8], self.name)
            ctx.state.message_history.extend(result.new_messages())
            if isinstance(result.output, str):
                ctx.state.final_output_parts = [TextPart(text=result.output)]
            else:
                ctx.state.final_output_parts = [DataPart(data=result.output)]
            return ReturnCall[State](state=ctx.state)

    def add_tools(self, *tools: ToolNodeDef) -> None:
        self.tools.extend(tools)

    def instructions(self, func: Callable[..., str | None]) -> Callable[..., str | None]:
        """Decorator to define dynamic instruction functions that can build instructions at runtime."""
        return self._agent_loop.instructions(func)


Agent = BaseAgentNodeDef
