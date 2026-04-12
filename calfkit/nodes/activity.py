import functools
import logging
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

from faststream.kafka import KafkaBroker
from typing_extensions import Self

from calfkit._vendor.pydantic_ai import Tool
from calfkit._vendor.pydantic_ai.messages import ToolReturn
from calfkit.models import SessionRunContext, Silent, State, ToolContext
from calfkit.models.actions import NodeResult, ReturnCall
from calfkit.nodes.base import BaseNodeDef

logger = logging.getLogger(__name__)


class ActivityNode(BaseNodeDef):
    def __init__(self, activity_func: Callable[..., Any] | Callable[..., Awaitable[Any]], **kwargs):
        self._func = activity_func
        self._executable = Tool(activity_func)
        super().__init__(**kwargs)

    @classmethod
    def define(
        cls,
        subscribe_topics: list[str] | str,
        publish_topic: str,
        *,
        node_id: str | None = None,
    ):
        def internal_decorator(func: Callable[..., Any] | Callable[..., Awaitable[Any]]) -> Self:
            return cls(
                activity_func=func, node_id=node_id or f"activity_{func.__name__}", subscribe_topics=subscribe_topics, publish_topic=publish_topic
            )

        return internal_decorator

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
        result = await self._executable.function_schema.call(tool_call_part.args_as_dict(), tool_call_ctx)

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
