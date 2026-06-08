"""Payload carried on a tool ``Call`` so a tool node knows which tool call to service.

Replaces the former positional ``input_args=(tool_call_id,)`` channel. The agent
dispatches a tool with ``Call(tool_topic, state, body=ToolCallRef(tool_call_id=...))``
(no route); the tool node's ``@handler('*', schema=ToolCallRef)`` ``run`` validates
``current_frame.payload`` into a :class:`ToolCallRef` before it is entered.
"""

from pydantic import BaseModel, ConfigDict


class ToolCallRef(BaseModel):
    """The per-invocation reference handed to a tool node: which tool call it must service.

    A tool invocation cannot recover its own ``tool_call_id`` from ``ctx.state``
    alone — in parallel mode every fanned-out ``Call`` carries a deep copy of the
    same state holding *all* pending tool calls — so the id must be passed in.
    """

    model_config = ConfigDict(extra="forbid")
    tool_call_id: str
