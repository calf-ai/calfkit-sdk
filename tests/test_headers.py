"""End-to-end tests for the x-calf-emitter Kafka header propagation.

Follows the project pattern: TestKafkaBroker for in-memory broker, FunctionModel
for offline deterministic agent runs, before_node closures to capture per-hop ctx
state, and client.execute / start for real message dispatch.
"""

from typing import Annotated, Any

from faststream import Context
from faststream.kafka import KafkaBroker, TestKafkaBroker

from calfkit._protocol import HDR_EMITTER, HDR_EMITTER_KIND
from calfkit._vendor.pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
)
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit.client import Client
from calfkit.models.seam_context import SeamContext
from calfkit.models.tool_context import ToolContext
from calfkit.nodes import BaseToolNodeDef, StatelessAgent, agent_tool
from calfkit.worker import Worker
from tests.providers import prepare_worker

# ---------------------------------------------------------------------------
# Smoke test: TestKafkaBroker must preserve custom Kafka headers in-memory.
# Without this, the rest of these tests would be meaningless.
# ---------------------------------------------------------------------------


def _decode_header(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode()
    return str(value)


async def test_testkafkabroker_preserves_custom_header():
    broker = KafkaBroker("localhost")
    captured: dict[str, Any] = {}

    @broker.subscriber("hdr_smoke", group_id="hdr_smoke_grp")
    async def _h(body: str, headers: Annotated[dict[str, Any], Context("message.headers")]):
        captured.update(headers)

    async with TestKafkaBroker(broker):
        await broker.publish("hi", "hdr_smoke", headers={HDR_EMITTER: "probe", HDR_EMITTER_KIND: "client"})

    assert _decode_header(captured.get(HDR_EMITTER)) == "probe"
    assert _decode_header(captured.get(HDR_EMITTER_KIND)) == "client"


# ---------------------------------------------------------------------------
# A client invoking an agent should leave the agent with ctx.emitter_node_id
# starting with "client." — sourced from the x-calf-emitter header that the
# Client stamps onto every outbound publish.
# ---------------------------------------------------------------------------


def _function_model_calls_then_summarizes_named(tool_name: str | None = None):
    """Build a FunctionModel that calls *tool_name* on round 1, then summarizes.

    When *tool_name* is ``None``, the model always returns a plain text response.
    """

    def _fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        last = messages[-1]
        if tool_name is None or (isinstance(last, ModelRequest) and any(isinstance(p, ToolReturnPart) for p in last.parts)):
            return ModelResponse(parts=[TextPart("done")])
        return ModelResponse(parts=[ToolCallPart(tool_name)])

    return _fn


async def test_agent_receives_client_emitter(container):
    seen: list[str | None] = []

    def capture_emitter(ctx: SeamContext) -> None:
        seen.append(ctx.emitter_node_id)
        return None

    worker = container.get(Worker)
    agent = StatelessAgent(
        "test_emitter_client_hop",
        system_prompt="x",
        subscribe_topics="test_emitter_client_hop.input",
        model_client=FunctionModel(_function_model_calls_then_summarizes_named()),
        before_node=[capture_emitter],
    )
    worker.add_nodes(agent)
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        await client.agent(topic="test_emitter_client_hop.input").execute("hi", timeout=5)

    assert len(seen) == 1
    assert seen[0] is not None
    assert seen[0].startswith("client.")


# ---------------------------------------------------------------------------
# When an agent calls a tool, the tool's ctx.agent_name (sourced from the
# inbound x-calf-emitter header) must equal the calling agent's node_id.
# ---------------------------------------------------------------------------

_tool_capture: dict[str, Any] = {}


@agent_tool
def _emitter_probe_tool(ctx: ToolContext) -> str:
    """Captures who called this tool."""
    _tool_capture["agent_name"] = ctx.agent_name
    return "ok"


async def test_tool_receives_agent_id_as_emitter(container):
    _tool_capture.clear()

    worker = container.get(Worker)
    agent = StatelessAgent(
        "test_emitter_agent_hop",
        system_prompt="x",
        subscribe_topics="test_emitter_agent_hop.input",
        model_client=FunctionModel(_function_model_calls_then_summarizes_named("_emitter_probe_tool")),
        tools=[_emitter_probe_tool],
    )
    worker.add_nodes(agent, _emitter_probe_tool)
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        await client.agent(topic="test_emitter_agent_hop.input").execute("call the tool", timeout=5)

    assert _tool_capture["agent_name"] == "test_emitter_agent_hop"


# ---------------------------------------------------------------------------
# On a ReturnCall back to the calling agent, the agent's ctx.emitter_node_id
# must equal the returning tool's node_id.
# ---------------------------------------------------------------------------


@agent_tool
def _silent_tool() -> str:
    """Just returns."""
    return "ok"


async def test_agent_receives_tool_emitter_on_return(container):
    seen: list[str | None] = []

    def capture_emitter(ctx: SeamContext) -> None:
        seen.append(ctx.emitter_node_id)
        return None

    worker = container.get(Worker)
    agent = StatelessAgent(
        "test_emitter_return_hop",
        system_prompt="x",
        subscribe_topics="test_emitter_return_hop.input",
        model_client=FunctionModel(_function_model_calls_then_summarizes_named("_silent_tool")),
        tools=[_silent_tool],
        before_node=[capture_emitter],
    )
    worker.add_nodes(agent, _silent_tool)
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        await client.agent(topic="test_emitter_return_hop.input").execute("call the tool", timeout=5)

    # Two agent invocations: client→agent, then tool→agent (return).
    assert len(seen) == 2
    assert seen[0] is not None and seen[0].startswith("client.")
    assert seen[1] == _silent_tool.node_id


# ---------------------------------------------------------------------------
# A normal run on a node with ``publish_topic`` must stamp the emitter header on
# the auto-publish. Catches regressions that drop ``headers=`` from the Response.
# (The gate-reject counterpart was removed with gates in PR-6; a fire-and-forget
# no-output delivery's mirror still carries the emitter via the _Declined path.)
# ---------------------------------------------------------------------------


async def test_success_path_publish_topic_carries_emitter_header(container, deploy_gated_function_agent):
    agent = deploy_gated_function_agent(before_node=None)
    broker = container.get(KafkaBroker)
    client = container.get(Client)

    received_headers: list[dict[str, Any]] = []

    @broker.subscriber(agent.publish_topic, group_id="success_path_observer")
    async def _observer(body: Any, headers: Annotated[dict[str, Any], Context("message.headers")]):
        received_headers.append(dict(headers))

    prepare_worker(container)

    async with TestKafkaBroker(broker):
        await client.agent(topic=agent.subscribe_topics[0]).execute("hi", timeout=5)

    agent_published = [h for h in received_headers if _decode_header(h.get(HDR_EMITTER_KIND)) == "agent"]
    assert agent_published, f"no agent-emitted message reached {agent.publish_topic}; received={received_headers}"
    assert _decode_header(agent_published[0].get(HDR_EMITTER)) == agent.node_id


# ---------------------------------------------------------------------------
# Parallel fan-out: every cloned Call in the loop must carry the agent's
# emitter. Catches regressions that hoist ``headers=`` outside the loop or
# drop it on one branch of the fan-out.
# ---------------------------------------------------------------------------

_parallel_capture: dict[str, str | None] = {}


@agent_tool
def _probe_tool_a(ctx: ToolContext) -> str:
    _parallel_capture["a"] = ctx.agent_name
    return "a_ok"


@agent_tool
def _probe_tool_b(ctx: ToolContext) -> str:
    _parallel_capture["b"] = ctx.agent_name
    return "b_ok"


@agent_tool
def _probe_tool_c(ctx: ToolContext) -> str:
    _parallel_capture["c"] = ctx.agent_name
    return "c_ok"


def _function_model_calls_all(tool_names: list[str]):
    """FunctionModel that calls every name in *tool_names* in one round (parallel fan-out)."""

    def _fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        last = messages[-1]
        if isinstance(last, ModelRequest) and any(isinstance(p, ToolReturnPart) for p in last.parts):
            return ModelResponse(parts=[TextPart("done")])
        return ModelResponse(parts=[ToolCallPart(name) for name in tool_names])

    return _fn


async def test_parallel_fan_out_carries_emitter_per_call(container):
    _parallel_capture.clear()

    worker = container.get(Worker)
    agent = StatelessAgent(
        "test_parallel_emitter",
        system_prompt="x",
        subscribe_topics="test_parallel_emitter.input",
        model_client=FunctionModel(_function_model_calls_all(["_probe_tool_a", "_probe_tool_b", "_probe_tool_c"])),
        tools=[_probe_tool_a, _probe_tool_b, _probe_tool_c],
    )
    worker.add_nodes(agent, _probe_tool_a, _probe_tool_b, _probe_tool_c)
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        await client.agent(topic="test_parallel_emitter.input").execute("call all tools", timeout=10)

    assert _parallel_capture == {"a": agent.node_id, "b": agent.node_id, "c": agent.node_id}


# ---------------------------------------------------------------------------
# Client receives the emitter of the callback reply on InvocationResult.
# ---------------------------------------------------------------------------


async def test_client_node_result_carries_reply_emitter(container, deploy_gated_function_agent):
    agent = deploy_gated_function_agent(before_node=None)
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        result = await client.agent(topic=agent.subscribe_topics[0]).execute("hi", timeout=5)

    assert result.emitter_node_id == agent.node_id
    assert result.emitter_node_kind == "agent"


# ---------------------------------------------------------------------------
# Regression guard: subclasses of BaseToolNodeDef must inherit kind="tool",
# not the BaseNodeDef default "node". Without the ClassVar on BaseToolNodeDef
# itself, user-defined tool subclasses silently mis-stamp the emitter kind.
# ---------------------------------------------------------------------------


def test_base_tool_node_kind_is_tool():
    assert BaseToolNodeDef._node_kind == "tool"
