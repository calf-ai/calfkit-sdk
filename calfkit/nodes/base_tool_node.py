import inspect
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import Any, cast

from calfkit._vendor.pydantic_ai import ModelRequest, Tool, ToolDefinition, ToolReturnPart
from calfkit.models.event_envelope import EventEnvelope
from calfkit.nodes.base_node import BaseNode, publish_to, subscribe_to


# TODO: implement a way to dynamically inject runtime variables into the tool input,
# which the agent should not be aware about.
# Think pythonic ways to do this, does it relate to Contexts?
class BaseToolNode(BaseNode, ABC):
    @classmethod
    @abstractmethod
    def tool_schema(cls) -> ToolDefinition: ...


def agent_tool(func: Callable[..., Any] | Callable[..., Awaitable[Any]]) -> BaseToolNode:
    """Agent tool decorator to turn a function into a deployable node"""
    tool = Tool(func)

    class ToolNode(BaseToolNode):
        @subscribe_to(f"tool_node.{func.__name__}.request")
        @publish_to(f"tool_node.{func.__name__}.result")
        async def on_enter(self, event_envelope: EventEnvelope) -> EventEnvelope:
            if not event_envelope.tool_call_request:
                raise RuntimeError("No tool call request found")
            tool_cal_req = event_envelope.tool_call_request
            kw_args = tool_cal_req.args_as_dict()
            result = func(**kw_args)
            if inspect.isawaitable(result):
                result = await result
            tool_result = ToolReturnPart(
                tool_name=tool_cal_req.tool_name,
                content=result,
                tool_call_id=tool_cal_req.tool_call_id,
            )
            event_envelope.add_to_uncommitted_messages(ModelRequest(parts=[tool_result]))
            return event_envelope

        @classmethod
        def tool_schema(cls) -> ToolDefinition:
            return cast(ToolDefinition, tool.tool_def)

    ToolNode.__name__ = func.__name__
    ToolNode.__qualname__ = func.__qualname__
    ToolNode.__doc__ = func.__doc__
    ToolNode.__module__ = func.__module__

    return ToolNode(name=ToolNode.__name__)
