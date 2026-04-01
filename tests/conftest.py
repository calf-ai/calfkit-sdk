from collections.abc import Callable

import pytest
from dishka import make_container

from calfkit._types import OutputT
from calfkit._vendor.pydantic_ai.models.function import FunctionModel
from calfkit.nodes import Agent, ToolNodeDef
from calfkit.providers.pydantic_ai.model_client import PydanticModelClient
from calfkit.worker import Worker
from tests.providers import (
    INSTRUCTIONS_TEST_SYSTEM_PROMPT,
    AgentProvider,
    ClientProvider,
    ContextualTool,
    SimpleAgent,
    StructuredAgent,
    WorkerProvider,
    agent_name,
    echo_instructions,
)


@pytest.fixture
def container():
    c = make_container(WorkerProvider(), ClientProvider(), AgentProvider())
    yield c
    c.close()


@pytest.fixture(params=["parallel", "sequential"])
def deploy_agent(request, container) -> SimpleAgent:
    mode: str = request.param
    worker = container.get(Worker)
    model_client = container.get(PydanticModelClient)
    agent = SimpleAgent(
        "test_simple_agent",
        system_prompt=f"You are a helpful AI assistant. Your name is {agent_name}. Help the user with their questions as much as possible.",
        subscribe_topics="test_agent.input",
        publish_topic="test_agent.output",
        model_client=model_client,
        sequential_only_mode=mode == "sequential",
    )
    worker.add_nodes(agent)
    return agent


@pytest.fixture(params=["parallel", "sequential"])
def deploy_function_agent(request, container) -> Agent:
    mode: str = request.param
    worker = container.get(Worker)
    model = container.get(FunctionModel)
    agent = Agent(
        "test_function_agent",
        system_prompt="You are a helpful AI assistant.",
        subscribe_topics="test_function_agent.input",
        publish_topic="test_function_agent.output",
        model_client=model,
        sequential_only_mode=mode == "sequential",
    )
    print(f"\nDeployed agent with sequential_only_mode={mode == 'sequential'}")
    worker.add_nodes(agent)
    return agent


@pytest.fixture
def deploy_structured_agent(container) -> StructuredAgent:
    worker = container.get(Worker)
    agent = container.get(StructuredAgent)
    worker.add_nodes(agent)
    return agent


@pytest.fixture
def deploy_structured_agent_factory(container) -> Callable[..., Agent[OutputT]]:
    factory = container.get(Callable)
    return factory


@pytest.fixture
def deploy_multiple_agent_tools(container) -> list[ToolNodeDef]:
    tools = container.get(list[ToolNodeDef])
    worker = container.get(Worker)
    worker.add_nodes(*tools)
    return tools


@pytest.fixture
def deploy_multiple_contextual_tools(container) -> list[ToolNodeDef]:
    tools = container.get(list[ContextualTool])
    worker = container.get(Worker)
    worker.add_nodes(*tools)
    return tools


@pytest.fixture
def deploy_caller_id_agent_tool(container) -> ToolNodeDef:
    tool = container.get(ToolNodeDef)
    worker = container.get(Worker)
    worker.add_nodes(tool)
    return tool


@pytest.fixture
def deploy_instructions_agent(container) -> Agent:
    worker = container.get(Worker)
    model = FunctionModel(echo_instructions)
    agent = Agent(
        "test_instructions_agent",
        system_prompt=INSTRUCTIONS_TEST_SYSTEM_PROMPT,
        subscribe_topics="test_instructions_agent.input",
        publish_topic="test_instructions_agent.output",
        model_client=model,  # pyright: ignore[reportArgumentType]
    )
    worker.add_nodes(agent)
    return agent
