"""Offline test model + history helpers for the real-broker roundtrip integration tests.

Shared by the MCP-toolbox and tool-node roundtrip suites; nothing here is
specific to either. This is the *single* module that imports the vendored
pydantic-ai model and
message types. The agent's model contract IS the vendored ``Model``
(``calfkit``'s ``PydanticModelClient`` is an empty marker over it, and the model
is handed verbatim into the vendored agent loop, which calls ``.request()`` and
expects a vendored ``ModelResponse`` of vendored parts). A programmatic test
model therefore unavoidably speaks the vendored message/part vocabulary;
confining those imports here keeps the rest of the suite off the
``calfkit._vendor`` path.
"""

from __future__ import annotations

from collections.abc import Callable

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
from calfkit._vendor.pydantic_ai.tools import ToolDefinition

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


def capturing_model(pov: dict[str, ToolDefinition], tool_calls: list[ToolCallPart]) -> FunctionModel:
    """Like :func:`scripted_model`, but first records the agent's POV of its tools.

    On the acting turn it captures ``info.function_tools`` — the exact
    :class:`ToolDefinition`\\ s the agent resolved from the Capability View and presented
    to the model — into ``pov`` (keyed by name), *then* emits ``tool_calls``. Lets a test
    assert that what the agent advertised to the model matches what the toolbox advertised
    on the view (names + schemas) and call one tool drawn from that POV, rather than
    hardcoding the available surface and assuming the agent saw it.
    """

    def _fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        if _has_acted(messages):
            return ModelResponse(parts=[TextPart(FINAL_OUTPUT)])
        pov.clear()
        pov.update({tool.name: tool for tool in info.function_tools})
        return ModelResponse(parts=list(tool_calls))

    return FunctionModel(_fn)


def final_model(text: str = FINAL_OUTPUT) -> FunctionModel:
    """A model that finalizes immediately with *text* — no tool calls, ever.

    For seam tests whose body must run and produce an output (e.g. an ``after_node``
    replacement, or a ``before_node`` recorder that declines) without dispatching any
    tools. Distinct from ``scripted_model([])`` (which would emit an empty tool-call
    turn, not a finalization).
    """

    def _fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[TextPart(text)])

    return FunctionModel(_fn)


def reactive_model(decide: Callable[[dict[str, object]], list[ToolCallPart] | None]) -> FunctionModel:
    """A multi-turn model driven by the tool returns seen so far.

    Each turn, ``decide`` receives ``{tool_name: return_content}`` accumulated
    across the conversation and returns the next turn's tool calls, or ``None``
    to finalize with :data:`FINAL_OUTPUT`. Lets a test chain calls where a later
    call's args depend on an earlier call's result — without the test importing
    any vendored message types (it only builds ``ToolCallPart``s).
    """

    def _fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        returns: dict[str, object] = {}
        for msg in messages:
            if isinstance(msg, ModelRequest):
                for part in msg.parts:
                    if isinstance(part, ToolReturnPart):
                        returns[part.tool_name] = part.content
        calls = decide(returns)
        if not calls:
            return ModelResponse(parts=[TextPart(FINAL_OUTPUT)])
        return ModelResponse(parts=list(calls))

    return FunctionModel(_fn)


def tool_returns(history: list[ModelMessage]) -> dict[str, object]:
    """Map ``tool_name -> return content`` over every ToolReturnPart in history.

    For a tool node the content is the function's raw return value (assert exact
    equality). For an MCP tool it is the server's ``CallToolResult`` (assert on
    its serialized form). When the same tool is called more than once, prefer
    :func:`returns_by_call_id`.
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
