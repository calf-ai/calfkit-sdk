"""Offline test model + history helpers for the MCP roundtrip integration tests.

This is the *single* module that imports the vendored pydantic-ai model and
message types. The agent's model contract IS the vendored ``Model``
(``calfkit``'s ``PydanticModelClient`` is an empty marker over it, and the model
is handed verbatim into the vendored agent loop, which calls ``.request()`` and
expects a vendored ``ModelResponse`` of vendored parts). A programmatic test
model therefore unavoidably speaks the vendored message/part vocabulary;
confining those imports here keeps the rest of the suite off the
``calfkit._vendor`` path.
"""

from __future__ import annotations

from calfkit._vendor.pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    RetryPromptPart,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
)
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel

#: The text the scripted model emits once it has acted. Reaching it proves the
#: agent resumed past tool dispatch to a clean finalization.
FINAL_OUTPUT = "done"


def _has_acted(messages: list[ModelMessage]) -> bool:
    """True once the model's first turn has been answered.

    The answer is either a tool result (``ToolReturnPart``) or an unknown-tool
    retry (``RetryPromptPart``, the agent's response to a tool name not in its
    resolved registry). Either means "stop emitting calls, finalize".
    """
    last = messages[-1]
    if not isinstance(last, ModelRequest):
        return False
    return any(isinstance(p, (ToolReturnPart, RetryPromptPart)) for p in last.parts)


def scripted_model(tool_calls: list[ToolCallPart]) -> FunctionModel:
    """An offline model that emits ``tool_calls`` on the first turn, then
    finalizes with :data:`FINAL_OUTPUT` once it has acted.

    Passing one call drives the single-dispatch path; passing two or more drives
    the durable in-node fan-out path. The control flow mirrors
    ``tests/integration/test_durable_fanout_agent_kafka.py::_calls_two_then_done``.
    """

    def _fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        if _has_acted(messages):
            return ModelResponse(parts=[TextPart(FINAL_OUTPUT)])
        return ModelResponse(parts=list(tool_calls))

    return FunctionModel(_fn)


def tool_returns(history: list[ModelMessage]) -> dict[str, object]:
    """Map ``tool_name -> return content`` over every ToolReturnPart in history.

    For an MCP tool the content is whatever the toolbox materialized from the
    server's ``CallToolResult`` (wrapped in a ``ToolReturn``), so callers should
    assert against its serialized form rather than a bare scalar. When the same
    tool is called more than once, prefer :func:`returns_by_call_id`.
    """
    returns: dict[str, object] = {}
    for msg in history:
        if isinstance(msg, ModelRequest):
            for part in msg.parts:
                if isinstance(part, ToolReturnPart):
                    returns[part.tool_name] = part.content
    return returns


def returns_by_call_id(history: list[ModelMessage]) -> dict[str, object]:
    """Map ``tool_call_id -> return content`` — the slot-precise view.

    Use this when one model turn calls the same tool more than once: the names
    collide but the ids don't, so this proves each result landed in its own slot.
    """
    returns: dict[str, object] = {}
    for msg in history:
        if isinstance(msg, ModelRequest):
            for part in msg.parts:
                if isinstance(part, ToolReturnPart):
                    returns[part.tool_call_id] = part.content
    return returns


def retry_prompt_texts(history: list[ModelMessage]) -> list[str]:
    """Every RetryPromptPart's rendered text — e.g. the agent's "no tool named X"
    response when the model emits a tool outside its resolved registry."""
    texts: list[str] = []
    for msg in history:
        if isinstance(msg, ModelRequest):
            for part in msg.parts:
                if isinstance(part, RetryPromptPart):
                    content = part.content
                    texts.append(content if isinstance(content, str) else str(content))
    return texts
