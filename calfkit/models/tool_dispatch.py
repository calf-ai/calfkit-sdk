from typing import Any

from pydantic import BaseModel, ConfigDict
from typing_extensions import Self

from calfkit._vendor.pydantic_ai.messages import ToolCallPart


class ToolCallRef(BaseModel):
    """The per-invocation reference handed to a tool node: which tool call it must service.

    A tool invocation cannot recover its own ``tool_call_id`` from ``ctx.state``
    alone — in parallel mode every fanned-out ``Call`` carries a deep copy of the
    same state holding *all* pending tool calls — so the id must be passed in.

    ``extra="forbid"`` makes this a *closed* envelope: because the tool node's
    handler route is the universal ``'*'``, the schema is the only discriminator
    that stops a foreign routeless body from being mis-consumed by a tool node.

    Note: ``tool_call_id`` is intentionally **not** ``min_length``-constrained. An
    empty id is a defended-against edge case (the tool node falls back to a sentinel
    ``FailedToolCall`` marker, see ``ToolNodeDef.run``); constraining it here would
    make that defensive path unreachable via this channel.
    """

    model_config = ConfigDict(extra="forbid")
    tool_call_id: str
    args: dict[str, Any]
    name: str

    @classmethod
    def from_tool_call_part(cls, tool_call_part: ToolCallPart) -> Self:
        return cls(tool_call_id=tool_call_part.tool_call_id, args=tool_call_part.args_as_dict(), name=tool_call_part.tool_name)
