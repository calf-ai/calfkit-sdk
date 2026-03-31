import logging
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import Any, cast

from calfkit._vendor.pydantic_ai import Tool, ToolDefinition
from calfkit._vendor.pydantic_ai.messages import ToolReturn
from calfkit.models import SessionRunContext, Silent, State, ToolContext
from calfkit.models.actions import NodeResult, ReturnCall
from calfkit.nodes.base import BaseNodeDef

logger = logging.getLogger(__name__)


class BaseToolNodeDef(BaseNodeDef, ABC):
    @property
    @abstractmethod
    def tool_schema(self) -> ToolDefinition: ...


class ToolNodeDef(BaseToolNodeDef):
    def __init__(self, func: Callable[..., Any], subscribe_topics: str | list[str], publish_topic: str):
        self._tool = Tool(func)
        super().__init__(
            node_id=f"tool_{func.__name__}",
            subscribe_topics=subscribe_topics,
            publish_topic=publish_topic,
        )

    async def run(self, ctx: SessionRunContext, tool_call_id: str, source_node_name: str) -> NodeResult[State]:
        logger.debug(
            "[%s] tool run entered tool=%s tool_call_id=%s source=%s",
            ctx.deps.correlation_id[:8],
            self.name,
            tool_call_id,
            source_node_name,
        )
        tool_call_part = ctx.state.get_tool_call(tool_call_id)
        if tool_call_part is None:
            logger.warning(
                "tool node reached but no matching tool call found in run state for tool_call_id=%s",
                tool_call_id,
            )
            return Silent()

        tool_call_ctx = ToolContext(
            deps=ctx.deps,
            agent_name=source_node_name,
            tool_call_id=tool_call_part.tool_call_id,
            tool_name=tool_call_part.tool_name,
            messages=ctx.state.message_history,
            run_id=ctx.deps.correlation_id,
        )

        # TODO: add some retry mechanism and max_retry logic here.
        # Note, retry logic should be configurable via client side
        result = await self._tool.function_schema.call(tool_call_part.args_as_dict(), tool_call_ctx)

        # tool_result = ToolReturnPart(
        #     tool_name=tool_call_part.tool_name,
        #     content=result,
        #     tool_call_id=tool_call_part.tool_call_id,
        # )

        # multimodal support is possible via `content`, for example:
        # ToolReturn(
        #       return_value="Screenshot captured successfully for https://example.com",
        #       content=[
        #           "Here is the screenshot:",
        #           BinaryContent(data=png_bytes, media_type="image/png"),
        #       ],
        #   )
        ctx.state.add_tool_result(
            tool_call_part.tool_call_id,
            ToolReturn(return_value=result, metadata={"tool_call_id": tool_call_part.tool_call_id}),
        )

        logger.debug("[%s] tool completed tool=%s", ctx.deps.correlation_id[:8], self.name)
        return ReturnCall[State](state=ctx.state)

    @property
    def tool_schema(self) -> ToolDefinition:
        return cast(ToolDefinition, self._tool.tool_def)


def agent_tool(func: Callable[..., Any] | Callable[..., Awaitable[Any]]) -> ToolNodeDef:
    """Decorator to turn a function into a deployable tool node that agents can call"""
    subscribe_topic = f"tool.{func.__name__}.input"
    publish_topic = f"tool.{func.__name__}.output"
    tool_node = ToolNodeDef(func=func, subscribe_topics=subscribe_topic, publish_topic=publish_topic)

    return tool_node
