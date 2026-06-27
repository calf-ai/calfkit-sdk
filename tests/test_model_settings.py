"""Tests for runtime model_settings: three-tier merge (model client < Agent constructor < per-call wire)."""

from typing import Any

import pytest
from faststream.kafka import KafkaBroker, TestKafkaBroker

from calfkit._vendor.pydantic_ai.messages import ModelMessage, ModelResponse, TextPart
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit._vendor.pydantic_ai.settings import ModelSettings
from calfkit.client import Client
from calfkit.nodes import Agent, ToolNodeDef
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
) -> tuple[Agent, _Capture]:
    worker = container.get(Worker)
    model, captured = _make_capture_model(tier1)
    agent = Agent(
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
    """Baseline: when all three tiers are unset, the model observes None for model_settings."""
    agent, captured = _deploy_agent(container)
    prepare_worker(container)
    client = container.get(Client)
    async with TestKafkaBroker(container.get(KafkaBroker)):
        await client.agent(topic=agent.subscribe_topics[0]).execute("hello")
    assert captured.settings[0] is None


async def test_tier3_only_overrides_baseline(container):
    """Per-call wire settings reach the model when Tier 1/2 are unset."""
    agent, captured = _deploy_agent(container)
    prepare_worker(container)
    client = container.get(Client)
    async with TestKafkaBroker(container.get(KafkaBroker)):
        await client.agent(topic=agent.subscribe_topics[0]).execute("hello", model_settings={"temperature": 0.7})
    assert captured.settings[0] == {"temperature": 0.7}


async def test_tier2_only_overrides_baseline(container):
    """Agent-constructor settings reach the model when Tier 1/3 are unset."""
    agent, captured = _deploy_agent(container, tier2={"temperature": 0.3})
    prepare_worker(container)
    client = container.get(Client)
    async with TestKafkaBroker(container.get(KafkaBroker)):
        await client.agent(topic=agent.subscribe_topics[0]).execute("hello")
    assert captured.settings[0] == {"temperature": 0.3}


async def test_tier1_only_overrides_baseline(container):
    """Model-client settings reach the model when Tier 2/3 are unset."""
    agent, captured = _deploy_agent(container, tier1={"temperature": 0.1})
    prepare_worker(container)
    client = container.get(Client)
    async with TestKafkaBroker(container.get(KafkaBroker)):
        await client.agent(topic=agent.subscribe_topics[0]).execute("hello")
    assert captured.settings[0] == {"temperature": 0.1}


async def test_tier2_beats_tier1_when_tier3_unset(container):
    """When Tier 3 is unset, Tier 2 (Agent constructor) wins over Tier 1 (model client)."""
    agent, captured = _deploy_agent(container, tier1={"temperature": 0.1}, tier2={"temperature": 0.5})
    prepare_worker(container)
    client = container.get(Client)
    async with TestKafkaBroker(container.get(KafkaBroker)):
        await client.agent(topic=agent.subscribe_topics[0]).execute("hello")
    assert captured.settings[0] is not None
    assert captured.settings[0]["temperature"] == 0.5


async def test_three_tier_precedence(container):
    """When all three tiers set the same key, Tier 3 wins."""
    agent, captured = _deploy_agent(container, tier1={"temperature": 0.1}, tier2={"temperature": 0.5})
    prepare_worker(container)
    client = container.get(Client)
    async with TestKafkaBroker(container.get(KafkaBroker)):
        await client.agent(topic=agent.subscribe_topics[0]).execute("hello", model_settings={"temperature": 0.9})
    assert captured.settings[0] is not None
    assert captured.settings[0]["temperature"] == 0.9


async def test_disjoint_keys_merge(container):
    """When each tier sets a disjoint key, all keys appear in the final merged settings."""
    agent, captured = _deploy_agent(container, tier1={"max_tokens": 100}, tier2={"top_p": 0.95})
    prepare_worker(container)
    client = container.get(Client)
    async with TestKafkaBroker(container.get(KafkaBroker)):
        await client.agent(topic=agent.subscribe_topics[0]).execute("hello", model_settings={"temperature": 0.7})
    assert captured.settings[0] is not None
    assert captured.settings[0]["max_tokens"] == 100
    assert captured.settings[0]["top_p"] == 0.95
    assert captured.settings[0]["temperature"] == 0.7


async def test_non_serializable_model_settings_bubbles_at_publish_no_preflight(container):
    """No call-site pre-flight (spec §2.5): a non-serializable model_settings is NOT rejected at the
    keyboard — the serialization error bubbles from publish when the call is dispatched."""
    agent, _ = _deploy_agent(container)
    prepare_worker(container)
    client = container.get(Client)
    bad_settings: Any = {"bad": object()}
    with pytest.raises(Exception):  # noqa: B017 - bubbles raw from publish (serialization), not a calfkit type (§2.5)
        async with TestKafkaBroker(container.get(KafkaBroker)):
            await client.agent(topic=agent.subscribe_topics[0]).start("hello", model_settings=bad_settings)


async def test_nested_non_serializable_model_settings_bubbles_at_publish(container):
    """A non-serializable value nested under extra_body (the most likely real-world site) also bubbles
    at publish — there is no call-site pre-flight to catch it early (spec §2.5)."""
    agent, _ = _deploy_agent(container)
    prepare_worker(container)
    client = container.get(Client)
    bad_settings: Any = {"extra_body": {"nested": object()}}
    with pytest.raises(Exception):  # noqa: B017 - bubbles raw from publish, no localized call-site pre-flight (§2.5)
        async with TestKafkaBroker(container.get(KafkaBroker)):
            await client.agent(topic=agent.subscribe_topics[0]).start("hello", model_settings=bad_settings)


async def test_unchanged_baseline_when_no_settings_passed(container):
    """An invocation with no model_settings kwarg behaves identically to passing None."""
    agent, captured = _deploy_agent(container)
    prepare_worker(container)
    client = container.get(Client)
    async with TestKafkaBroker(container.get(KafkaBroker)):
        await client.agent(topic=agent.subscribe_topics[0]).execute("hello")
    async with TestKafkaBroker(container.get(KafkaBroker)):
        await client.agent(topic=agent.subscribe_topics[0]).execute("hello", model_settings=None)
    assert captured.settings[0] == captured.settings[1]
    assert captured.settings[0] is None


async def test_empty_dict_behaves_like_none(container):
    """An empty model_settings dict behaves like None (pydantic_ai's merge short-circuits on falsy overrides)."""
    agent, captured = _deploy_agent(container, tier1={"temperature": 0.1})
    prepare_worker(container)
    client = container.get(Client)
    async with TestKafkaBroker(container.get(KafkaBroker)):
        await client.agent(topic=agent.subscribe_topics[0]).execute("hello", model_settings={})
    async with TestKafkaBroker(container.get(KafkaBroker)):
        await client.agent(topic=agent.subscribe_topics[0]).execute("hello", model_settings=None)
    assert captured.settings[0] == captured.settings[1] == {"temperature": 0.1}


async def test_tools_available_with_model_settings_only(container, deploy_no_arg_tools):
    """Setting only model_settings (no tool_overrides) must not strip constructor tools from the agent."""
    agent, captured = _deploy_agent(container, tools=list(deploy_no_arg_tools))
    prepare_worker(container)
    client = container.get(Client)
    async with TestKafkaBroker(container.get(KafkaBroker)):
        await client.agent(topic=agent.subscribe_topics[0]).execute("hello", model_settings={"temperature": 0.5})
    assert captured.settings[0] == {"temperature": 0.5}
    expected = {tool.tool_schema.name for tool in deploy_no_arg_tools}
    assert set(captured.tool_names[0]) == expected


async def test_tool_overrides_and_model_settings_combined(container, deploy_no_arg_tools):
    """Setting both tool_overrides and model_settings on a single call: both take effect."""
    agent, captured = _deploy_agent(container)
    prepare_worker(container)
    client = container.get(Client)
    async with TestKafkaBroker(container.get(KafkaBroker)):
        await client.agent(topic=agent.subscribe_topics[0]).execute(
            "hello",
            tool_overrides=deploy_no_arg_tools,
            model_settings={"temperature": 0.7},
        )
    assert captured.settings[0] == {"temperature": 0.7}
    expected = {tool.tool_schema.name for tool in deploy_no_arg_tools}
    assert set(captured.tool_names[0]) == expected
