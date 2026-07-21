"""Tests for runtime model_settings: two-tier merge (model client < StatelessAgent constructor).

The StatelessAgent-ctor tier is the overrides-removal KEEP list's runtime guard: the ctor baseline
bakes into the internal loop and must keep reaching the model."""

from typing import Any

from faststream.kafka import KafkaBroker, TestKafkaBroker

from calfkit._vendor.pydantic_ai.messages import ModelMessage, ModelResponse, TextPart
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit._vendor.pydantic_ai.settings import ModelSettings
from calfkit.client import Client
from calfkit.nodes import StatelessAgent, ToolNodeDef
from calfkit.worker import Worker
from tests.providers import prepare_worker


class _Capture:
    """Records FunctionModel observations across multiple invocations within one test."""

    settings: list[dict[str, Any] | None]
    tool_names: list[list[str]]

    def __init__(self) -> None:
        self.settings = []
        self.tool_names = []


def _make_capture_model(
    settings: ModelSettings | dict[str, Any] | None = None,
) -> tuple[FunctionModel, _Capture]:
    captured = _Capture()

    def fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        captured.settings.append(dict(info.model_settings) if info.model_settings is not None else None)
        captured.tool_names.append([t.name for t in info.function_tools])
        return ModelResponse(parts=[TextPart(content="ok")])

    model = FunctionModel(fn, settings=settings)
    return model, captured


def _deploy_agent(
    container,
    *,
    tier1: ModelSettings | dict[str, Any] | None = None,
    tier2: ModelSettings | dict[str, Any] | None = None,
    tools: list[ToolNodeDef] | None = None,
) -> tuple[StatelessAgent, _Capture]:
    worker = container.get(Worker)
    model, captured = _make_capture_model(tier1)
    agent = StatelessAgent(
        "test_model_settings_agent",
        system_prompt="You are a helpful AI assistant.",
        subscribe_topics="test_model_settings_agent.input",
        publish_topic="test_model_settings_agent.output",
        model_client=model,
        model_settings=tier2,
        tools=tools,
    )
    worker.add_nodes(agent)
    return agent, captured


async def test_no_settings_anywhere(container):
    """Baseline: when both tiers are unset, the model observes None for model_settings."""
    agent, captured = _deploy_agent(container)
    prepare_worker(container)
    client = container.get(Client)
    async with TestKafkaBroker(container.get(KafkaBroker)):
        await client.agent(topic=agent.subscribe_topics[0]).execute("hello")
    assert captured.settings[0] is None


async def test_tier2_only_overrides_baseline(container):
    """StatelessAgent-constructor settings reach the model when Tier 1 is unset."""
    agent, captured = _deploy_agent(container, tier2={"temperature": 0.3})
    prepare_worker(container)
    client = container.get(Client)
    async with TestKafkaBroker(container.get(KafkaBroker)):
        await client.agent(topic=agent.subscribe_topics[0]).execute("hello")
    assert captured.settings[0] == {"temperature": 0.3}


async def test_tier1_only_overrides_baseline(container):
    """Model-client settings reach the model when Tier 2 is unset."""
    agent, captured = _deploy_agent(container, tier1={"temperature": 0.1})
    prepare_worker(container)
    client = container.get(Client)
    async with TestKafkaBroker(container.get(KafkaBroker)):
        await client.agent(topic=agent.subscribe_topics[0]).execute("hello")
    assert captured.settings[0] == {"temperature": 0.1}


async def test_tier2_beats_tier1(container):
    """Tier 2 (StatelessAgent constructor) wins over Tier 1 (model client)."""
    agent, captured = _deploy_agent(container, tier1={"temperature": 0.1}, tier2={"temperature": 0.5})
    prepare_worker(container)
    client = container.get(Client)
    async with TestKafkaBroker(container.get(KafkaBroker)):
        await client.agent(topic=agent.subscribe_topics[0]).execute("hello")
    assert captured.settings[0] is not None
    assert captured.settings[0]["temperature"] == 0.5


async def test_disjoint_keys_merge(container):
    """When each tier sets a disjoint key, both keys appear in the final merged settings."""
    agent, captured = _deploy_agent(container, tier1={"max_tokens": 100}, tier2={"top_p": 0.95})
    prepare_worker(container)
    client = container.get(Client)
    async with TestKafkaBroker(container.get(KafkaBroker)):
        await client.agent(topic=agent.subscribe_topics[0]).execute("hello")
    assert captured.settings[0] is not None
    assert captured.settings[0]["max_tokens"] == 100
    assert captured.settings[0]["top_p"] == 0.95
