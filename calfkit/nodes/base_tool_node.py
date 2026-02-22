from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import Annotated, Any, cast

from faststream import Context

from calfkit._vendor.pydantic_ai import ModelRequest, Tool, ToolDefinition, ToolReturnPart
from calfkit.models.event_envelope import EventEnvelope
from calfkit.models.tool_context import ToolContext
from calfkit.nodes.base_node import BaseNode, publish_to, subscribe_to


class BaseToolNode(BaseNode, ABC):
    @property
    @abstractmethod
    def tool_schema(self) -> ToolDefinition: ...


def agent_tool(func: Callable[..., Any] | Callable[..., Awaitable[Any]]) -> BaseToolNode:
    """Agent tool decorator to turn a function into a deployable node"""

    class ToolNode(BaseToolNode):
        def __init__(self, *args: Any, **kwargs: Any):
            self.tool = Tool(func)
            super().__init__(*args, **kwargs)

        @subscribe_to(f"tool_node.{func.__name__}.request")
        @publish_to(f"tool_node.{func.__name__}.result")
        async def on_enter(
            self,
            event_envelope: EventEnvelope,
            correlation_id: Annotated[str, Context()],
        ) -> EventEnvelope:
            if not event_envelope.tool_call_request:
                raise RuntimeError("No tool call request found")
            tool_cal_req = event_envelope.tool_call_request
            kw_args = tool_cal_req.args_as_dict()

            ctx = ToolContext(
                deps=event_envelope.deps,
                agent_name=event_envelope.agent_name,
                tool_call_id=tool_cal_req.tool_call_id,
                tool_name=tool_cal_req.tool_name,
                messages=list(event_envelope.message_history),
                run_id=correlation_id,
            )
            result = await self.tool.function_schema.call(kw_args, ctx)

            tool_result = ToolReturnPart(
                tool_name=tool_cal_req.tool_name,
                content=result,
                tool_call_id=tool_cal_req.tool_call_id,
            )
            event_envelope.add_to_uncommitted_messages(ModelRequest(parts=[tool_result]))
            return event_envelope

        @property
        def tool_schema(self) -> ToolDefinition:
            return cast(ToolDefinition, self.tool.tool_def)

    ToolNode.__name__ = func.__name__
    ToolNode.__qualname__ = func.__qualname__
    ToolNode.__doc__ = func.__doc__
    ToolNode.__module__ = func.__module__

    return ToolNode(name=ToolNode.__name__)
