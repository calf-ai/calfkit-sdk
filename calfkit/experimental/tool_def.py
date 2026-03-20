import logging
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import Any, cast

from calfkit._vendor.pydantic_ai import Tool, ToolDefinition
from calfkit._vendor.pydantic_ai.messages import ToolReturn
from calfkit.experimental.context_models import BaseSessionRunContext
from calfkit.experimental.node_def import BaseNodeDef, Envelope, NodeResult, Reply, Silent
from calfkit.experimental.payload_model import Payload
from calfkit.experimental.state_and_deps_models import (
    Deps,
    InFlightToolsState,
    NodeConsumeState,
    State,
)
from calfkit.experimental.utils import find_first_tool_call_part
from calfkit.models.tool_context import ToolContext


class BaseToolNodeDef(BaseNodeDef, ABC):
    @property
    @abstractmethod
    def tool_schema(self) -> ToolDefinition: ...


class ToolNodeDef(BaseToolNodeDef):
    def __init__(
        self, func: Callable[..., Any], subscribe_topics: str | list[str], publish_topic: str
    ):
        self._tool = Tool(func)
        super().__init__(
            node_id=f"tool_{func.__name__}",
            subscribe_topics=subscribe_topics,
            publish_topic=publish_topic,
        )

    # async def prepare_context(
    #     self, envelope: Envelope[State, Deps[Any]]
    # ) -> BaseSessionRunContext[NodeConsumeState[InFlightToolsState], Deps[Any]]:
    #     consume_state = NodeConsumeState[InFlightToolsState].model_validate(
    #         envelope.context.state.model_dump()
    #     )
    #     ctx = BaseSessionRunContext[NodeConsumeState[InFlightToolsState], Deps[Any]](
    #         state=consume_state, deps=envelope.context.deps
    #     )
    #     return ctx

    async def run(self, ctx: BaseSessionRunContext[State, Deps[Any]]) -> NodeResult[State]:
        # TODO: consider a more sophistcated or target way to store and retrieve payloads from state.  # noqa: E501
        # A targetted way would allow reciever nodes to know exactly what payload to run and process.  # noqa: E501
        payload = Payload.model_validate(ctx.state.run_state.todo_stack[-1])
        tool_call_part = find_first_tool_call_part(payload)
        if tool_call_part is None:
            logging.warning("tool node ran but no matching tool call found in payload.")
            return Silent()

        tool_call_ctx = ToolContext(
            deps=ctx.deps.agent_deps,
            agent_name=payload.source_node_id,
            tool_call_id=tool_call_part.tool_call_id,
            tool_name=tool_call_part.tool_name,
            messages=ctx.state.run_state.message_history,
            run_id=ctx.deps.correlation_id,
        )

        # TODO: add some retry mechanism and max_retry logic here.
        # Note, retry logic should be configurable via client side
        result = await self._tool.function_schema.call(tool_call_part.kwargs, tool_call_ctx)

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

        if ctx.state.run_state.tool_results is None:
            ctx.state.run_state.tool_results = {}
        ctx.state.run_state.tool_results[tool_call_part.tool_call_id] = ToolReturn(
            return_value=result, metadata={"tool_call_id": tool_call_part.tool_call_id}
        )

        return Reply[State](value=ctx.state)

    @property
    def tool_schema(self) -> ToolDefinition:
        return cast(ToolDefinition, self._tool.tool_def)


def agent_tool(func: Callable[..., Any] | Callable[..., Awaitable[Any]]) -> BaseToolNodeDef:
    """Tool decorator to turn a function into a deployable node that agents can call"""
    subscribe_topic = f"tool.{func.__name__}.input"
    publish_topic = f"tool.{func.__name__}.output"
    tool_node = ToolNodeDef(
        func=func, subscribe_topics=subscribe_topic, publish_topic=publish_topic
    )

    return tool_node
