import inspect
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable

from pydantic_ai import ModelRequest, Tool, ToolDefinition, ToolReturnPart

from calf.models.event_envelope import EventEnvelope
from calf.nodes.base_node import BaseNode, publish_to, subscribe_to


class BaseToolNode(BaseNode, ABC):
    @classmethod
    @abstractmethod
    def tool_schema(cls) -> ToolDefinition: ...


def agent_tool(func: Callable | Callable[..., Awaitable]) -> BaseToolNode:
    """tool decorator"""
    tool = Tool(func)

    class ToolNode(BaseToolNode):
        @subscribe_to("tool_node.{func.__name__}.request")
        @publish_to("tool_node.{func.__name__}.result")
        async def on_enter(self, event_envelope: EventEnvelope):
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
            event_envelope = event_envelope.model_copy(
                update={
                    "kind": "tool_result",
                    "incoming_node_messages": [ModelRequest(parts=[tool_result])],
                }
            )
            return event_envelope

        @classmethod
        def tool_schema(cls) -> ToolDefinition:
            return tool.tool_def

    ToolNode.__name__ = func.__name__
    ToolNode.__qualname__ = func.__qualname__
    ToolNode.__doc__ = func.__doc__
    ToolNode.__module__ = func.__module__

    return ToolNode()
