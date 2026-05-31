"""Integration tests for the full Agent → McpBridge → FakeMcpServer flow.

These tests fill the gap exposed by the post-Phase-8 deep review: the
canonical ``Agent(tools=[mcp_server])`` pattern, advertised in the README,
mcp-guide.md, and the quickstart example, had no test coverage. The
implementation was patched (``calfkit/nodes/agent.py:_flatten_tools``) to
flatten ``McpServer`` instances at construction; this module ensures the
patch holds and that the resulting agent dispatches through ``McpBridge``
correctly.

We don't go through ``TestKafkaBroker`` here because the bridge dispatch
path is already covered end-to-end in
:mod:`tests.mcp.test_worker_integration`. What's exercised here is the
*Agent side*: that flattening produces dispatchable schemas, that the
agent's tools-registry resolves them, and that the emitted ``Call`` has
the right MCP topic.
"""

from __future__ import annotations

from typing import Any

from mcp.types import CallToolResult, TextContent

from calfkit._vendor.pydantic_ai.messages import (
    ModelMessage,
    ModelResponse,
    ToolCallPart,
    ToolReturn,
)
from calfkit._vendor.pydantic_ai.messages import TextPart as ModelTextPart
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit.mcp._testing import FakeMcpServer
from calfkit.mcp._tool_def import McpToolDef
from calfkit.models import SessionRunContext, State
from calfkit.models.actions import Call, ReturnCall
from calfkit.models.node_schema import BaseToolNodeSchema
from calfkit.models.session_context import Deps
from calfkit.nodes import Agent, agent_tool

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _td(name: str, description: str = "") -> McpToolDef:
    return McpToolDef(
        name=name,
        description=description or f"tool {name}",
        input_schema={"type": "object", "properties": {"q": {"type": "string"}}, "required": []},
    )


def _ok_result(text: str) -> CallToolResult:
    return CallToolResult(content=[TextContent(type="text", text=text)], isError=False)


def _make_ctx(state: State, *, frame_id: str = "frame-test") -> SessionRunContext:
    ctx = SessionRunContext(state=state, deps=Deps(correlation_id="cid-int", provided_deps={}))
    ctx._frame_id = frame_id
    return ctx


def _model_that_calls(tool_name: str, args: dict[str, Any], tool_call_id: str) -> FunctionModel:
    """Deterministic LLM that emits exactly one tool call on the first turn,
    then returns a final text response on the second turn.

    Used to drive ``Agent.run()`` through the dispatch path without an LLM.
    """
    call_count = {"n": 0}

    def _fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        call_count["n"] += 1
        if call_count["n"] == 1:
            return ModelResponse(parts=[ToolCallPart(tool_name=tool_name, args=args, tool_call_id=tool_call_id)])
        return ModelResponse(parts=[ModelTextPart("done")])

    return FunctionModel(_fn)


# ---------------------------------------------------------------------------
# Construction: Agent flattens McpServer into tools list
# ---------------------------------------------------------------------------


def test_agent_flattens_mcp_server_into_tools() -> None:
    """``Agent(tools=[McpServer])`` expands ``McpServer.__iter__`` at
    construction so the tools-registry build at run() works.

    This is the regression test for the P0 bug found in the post-Phase-8
    deep review where ``Agent.__init__`` stored ``McpServer`` raw and the
    registry dict-comp crashed with ``AttributeError``.
    """
    fake = FakeMcpServer(
        name="gmail",
        tools=[_td("search"), _td("send")],
        invoker=lambda n, a, m: _ok_result("ok"),
    )

    agent = Agent(
        "scribe",
        system_prompt="x",
        subscribe_topics="scribe.input",
        publish_topic="scribe.output",
        model_client=FunctionModel(lambda m, i: ModelResponse(parts=[ModelTextPart("done")])),
        tools=[fake],
    )

    # Two tools, both BaseToolNodeSchema, neither still wrapped in an McpServer
    assert len(agent.tools) == 2
    for tool in agent.tools:
        assert isinstance(tool, BaseToolNodeSchema)
    assert {t.tool_schema.name for t in agent.tools} == {"search", "send"}


def test_agent_mixed_mcp_and_native_tools() -> None:
    """Native ``@agent_tool`` entries pass through; ``McpServer`` entries flatten.

    The two flavours coexist in a single ``tools=`` list.
    """

    @agent_tool
    def my_native_tool() -> str:
        return "native"

    fake = FakeMcpServer(name="gmail", tools=[_td("search")], invoker=lambda n, a, m: _ok_result("ok"))

    agent = Agent(
        "mixed",
        system_prompt="x",
        subscribe_topics="mixed.input",
        publish_topic="mixed.output",
        model_client=FunctionModel(lambda m, i: ModelResponse(parts=[ModelTextPart("done")])),
        tools=[my_native_tool, fake],
    )

    # Three tools: the native one, plus one for each fake tool
    assert len(agent.tools) == 2
    names = {t.tool_schema.name for t in agent.tools}
    assert names == {"my_native_tool", "search"}


def test_agent_empty_tools_list_is_safe() -> None:
    agent = Agent(
        "no-tools",
        system_prompt="x",
        subscribe_topics="x.input",
        publish_topic="x.output",
        model_client=FunctionModel(lambda m, i: ModelResponse(parts=[ModelTextPart("done")])),
        tools=[],
    )
    assert agent.tools == []


def test_agent_none_tools_is_safe() -> None:
    agent = Agent(
        "no-tools",
        system_prompt="x",
        subscribe_topics="x.input",
        publish_topic="x.output",
        model_client=FunctionModel(lambda m, i: ModelResponse(parts=[ModelTextPart("done")])),
        tools=None,
    )
    assert agent.tools == []


def test_add_tools_flattens_mcp_server() -> None:
    """``add_tools(McpServer)`` flattens the same way constructor does."""
    fake = FakeMcpServer(name="gmail", tools=[_td("search"), _td("send")], invoker=lambda n, a, m: _ok_result("ok"))

    agent = Agent(
        "scribe",
        system_prompt="x",
        subscribe_topics="scribe.input",
        publish_topic="scribe.output",
        model_client=FunctionModel(lambda m, i: ModelResponse(parts=[ModelTextPart("done")])),
        tools=[],
    )
    agent.add_tools(fake)

    assert len(agent.tools) == 2
    assert {t.tool_schema.name for t in agent.tools} == {"search", "send"}


# ---------------------------------------------------------------------------
# Dispatch: Agent.run() routes MCP tool calls to the bridge's topic
# ---------------------------------------------------------------------------


async def test_agent_dispatches_mcp_tool_call_to_bridge_topic() -> None:
    """End-to-end: agent loops, model emits a tool call for the MCP tool,
    the agent emits ``Call`` addressed to the MCP bridge's input topic.

    This is the first turn — before any tool result comes back. We assert
    the dispatch target matches ``mcp.<server>.<tool>.input`` so the
    bridge worker would pick it up correctly.
    """
    fake = FakeMcpServer(
        name="gmail",
        tools=[_td("search", "search the inbox")],
        invoker=lambda n, a, m: _ok_result("results"),
    )

    agent = Agent(
        "scribe",
        system_prompt="x",
        subscribe_topics="scribe.input",
        publish_topic="scribe.output",
        model_client=_model_that_calls("search", {"q": "calf"}, "tc-1"),
        tools=[fake],
        sequential_only_mode=True,
    )

    state = State()
    ctx = _make_ctx(state)

    result = await agent.run(ctx)

    # Expect a Call addressed to the MCP bridge's input topic, with the
    # original tool_call_id passed positionally to the bridge handler.
    assert isinstance(result, Call)
    assert result.target_topic == "mcp.gmail.search.input"
    assert result.input_args == ("tc-1",)


async def test_agent_returns_after_mcp_tool_result_arrives() -> None:
    """Second turn: tool result is already in state, agent should call the
    model again and then return the final output.
    """
    fake = FakeMcpServer(
        name="gmail",
        tools=[_td("search")],
        invoker=lambda n, a, m: _ok_result("results"),
    )

    agent = Agent(
        "scribe",
        system_prompt="x",
        subscribe_topics="scribe.input",
        publish_topic="scribe.output",
        model_client=_model_that_calls("search", {"q": "calf"}, "tc-2"),
        tools=[fake],
        sequential_only_mode=True,
    )

    state = State()
    ctx = _make_ctx(state)

    # First turn — agent emits the tool call dispatch
    first = await agent.run(ctx)
    assert isinstance(first, Call)

    # Simulate the bridge having returned a result
    state.add_tool_result(
        "tc-2",
        ToolReturn(return_value="results", metadata={"tool_call_id": "tc-2"}),
    )

    # Second turn — agent finishes
    second = await agent.run(ctx)
    assert isinstance(second, ReturnCall)


# ---------------------------------------------------------------------------
# Renames and filters propagate through flattening
# ---------------------------------------------------------------------------


def test_renamed_mcp_server_yields_renamed_tool_schemas() -> None:
    """``McpServer.rename`` updates the LLM-facing name; topic still uses original."""
    fake = FakeMcpServer(
        name="gmail",
        tools=[_td("search")],
        invoker=lambda n, a, m: _ok_result("ok"),
    )
    renamed = fake.rename({"search": "find_email"})

    agent = Agent(
        "scribe",
        system_prompt="x",
        subscribe_topics="scribe.input",
        publish_topic="scribe.output",
        model_client=FunctionModel(lambda m, i: ModelResponse(parts=[ModelTextPart("done")])),
        tools=[renamed],
    )

    assert len(agent.tools) == 1
    schema = agent.tools[0]
    assert schema.tool_schema.name == "find_email"
    # Topic still uses the original MCP tool name
    assert schema.subscribe_topics == ["mcp.gmail.search.input"]
    assert schema.publish_topic == "mcp.gmail.search.output"


def test_filtered_mcp_server_only_yields_allowed_tools() -> None:
    """``McpServer.only`` narrows the iteration before flattening."""
    fake = FakeMcpServer(
        name="gmail",
        tools=[_td("search"), _td("send"), _td("draft")],
        invoker=lambda n, a, m: _ok_result("ok"),
    )

    agent = Agent(
        "scribe",
        system_prompt="x",
        subscribe_topics="scribe.input",
        publish_topic="scribe.output",
        model_client=FunctionModel(lambda m, i: ModelResponse(parts=[ModelTextPart("done")])),
        tools=[fake.only("search", "send")],
    )

    assert {t.tool_schema.name for t in agent.tools} == {"search", "send"}


# ---------------------------------------------------------------------------
# Schema metadata round-trips
# ---------------------------------------------------------------------------


def test_flattened_schema_carries_mcp_routing_metadata() -> None:
    """Each yielded ``BaseToolNodeSchema`` carries the MCP routing metadata
    the McpBridge derives from at dispatch time.
    """
    fake = FakeMcpServer(name="gmail", tools=[_td("search")], invoker=lambda n, a, m: _ok_result("ok"))

    agent = Agent(
        "scribe",
        system_prompt="x",
        subscribe_topics="scribe.input",
        publish_topic="scribe.output",
        model_client=FunctionModel(lambda m, i: ModelResponse(parts=[ModelTextPart("done")])),
        tools=[fake],
    )

    schema = agent.tools[0]
    md = schema.tool_schema.metadata
    assert md is not None
    assert md["source"] == "mcp"
    assert md["mcp_server"] == "gmail"
    assert md["mcp_tool_name"] == "search"


# ---------------------------------------------------------------------------
# Type-system sanity (these are static-check probes that run as tests)
# ---------------------------------------------------------------------------


def test_tool_like_alias_is_exported() -> None:
    """``ToolLike`` is the public alias for the heterogeneous tool entries.

    Importing the alias is part of the contract — downstream type stubs and
    user code that wants to annotate a ``list[ToolLike]`` parameter relies
    on it being importable from ``calfkit.nodes.agent``.
    """
    from calfkit.nodes.agent import ToolLike  # noqa: F401  — import-as-test
