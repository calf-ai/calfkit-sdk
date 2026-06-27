"""PR-6 4.4 — end-to-end carriage switch through ``TestKafkaBroker``.

The carriage switch (fault-rail §4.5/§6.9): a tool returns its result on the reply slot
(``ReturnCall.value`` → ``reply.parts``), and the calling agent materializes it at the callee slot
(``_resolve_slot``) keyed by the echoed ``tag`` — no more ``state.tool_results`` blob-write. These
drive a REAL single tool call through ``TestKafkaBroker`` to prove the full round trip:

- a single (sequential) tool call's result materializes and the agent resumes to a final answer
  incorporating it — THE regression guard for the single-call ``tag`` (decision 9): without the tag
  on the ``Call``, the reply's ``tag`` is ``None`` and ``_resolve_slot`` no-ops, so the agent never
  sees the result and loops;
- a tool's ``ModelRetry`` rides as a ``calf.retry``-marked ``TextPart`` and the agent hydrates a
  ``RetryPromptPart`` the model reacts to (§4.5 option 1, end to end).
"""

from __future__ import annotations

from faststream.kafka import KafkaBroker, TestKafkaBroker

from calfkit._vendor.pydantic_ai.exceptions import ModelRetry
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
from calfkit.client import Client
from calfkit.models.tool_context import ToolContext
from calfkit.nodes import Agent, agent_tool
from calfkit.worker import Worker
from tests.providers import prepare_worker


@agent_tool
def lookup_year(ctx: ToolContext) -> str:
    return "1967"


def _call_then_echo_the_tool_result(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
    """Call the tool on the first turn; once its return is in history, echo the value back."""
    last = messages[-1]
    if isinstance(last, ModelRequest):
        returns = [p for p in last.parts if isinstance(p, ToolReturnPart)]
        if returns:
            return ModelResponse(parts=[TextPart(f"the year was {returns[0].content}")])
    return ModelResponse(parts=[ToolCallPart("lookup_year")])


async def test_single_tool_call_result_materializes_and_agent_resumes(container) -> None:
    # THE single-call tag regression guard (decision 9): a sequential agent calls one tool; the tool's
    # result rides the reply slot and must materialize at the callee slot (keyed by the echoed tag) so
    # the agent's next turn sees it. Without the tag on the Call, the reply tag is None, _resolve_slot
    # no-ops, the tool result never materializes, and the sequential agent loops until the TTL.
    worker = container.get(Worker)
    agent = Agent(
        "year_agent",
        system_prompt="x",
        subscribe_topics="year_agent.input",
        model_client=FunctionModel(_call_then_echo_the_tool_result),
        tools=[lookup_year],
        sequential_only_mode=True,
    )
    worker.add_nodes(agent, lookup_year)
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        result = await client.agent(topic="year_agent.input").execute("what year", timeout=10)

    # The agent resumed past the tool call to a final answer that INCLUDES the materialized result.
    assert result.output is not None and "1967" in result.output


@agent_tool
def flaky_tool(ctx: ToolContext) -> str:
    raise ModelRetry("please narrow your query")


def _call_then_react_to_the_retry(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
    """Call the flaky tool; once its retry prompt is in history, acknowledge it (the model adapts)."""
    last = messages[-1]
    if isinstance(last, ModelRequest):
        retries = [p for p in last.parts if isinstance(p, RetryPromptPart)]
        if retries:
            return ModelResponse(parts=[TextPart(f"got retry: {retries[0].content}")])
    return ModelResponse(parts=[ToolCallPart("flaky_tool")])


async def test_tool_model_retry_round_trips_as_retry_prompt(container) -> None:
    # The calf.retry round trip (§4.5 option 1): the tool renders ModelRetry to a calf.retry-marked
    # TextPart on the reply slot (raw message); the agent's _resolve_slot hydrates a RetryPromptPart the
    # model reacts to. End to end, the model sees the retry message it can act on.
    worker = container.get(Worker)
    agent = Agent(
        "retry_agent",
        system_prompt="x",
        subscribe_topics="retry_agent.input",
        model_client=FunctionModel(_call_then_react_to_the_retry),
        tools=[flaky_tool],
        sequential_only_mode=True,
    )
    worker.add_nodes(agent, flaky_tool)
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        result = await client.agent(topic="retry_agent.input").execute("do the thing", timeout=10)

    # The model received (and echoed) the raw retry message — the marker was honored, no doubled suffix.
    assert result.output is not None and "please narrow your query" in result.output
