import os
from collections.abc import Callable
from typing import Any

import pytest
from dishka import make_container
from dotenv import load_dotenv

from calfkit._types import OutputT
from calfkit._vendor.pydantic_ai.models.function import FunctionModel
from calfkit.nodes import Agent, ToolNodeDef
from calfkit.providers.pydantic_ai.openai import OpenAIModelClient, OpenAIResponsesModelClient
from calfkit.worker import Worker
from tests.providers import (
    INSTRUCTIONS_TEST_SYSTEM_PROMPT,
    AgentProvider,
    ClientProvider,
    ContextualTool,
    Response,
    SimpleAgent,
    StructuredAgent,
    WorkerProvider,
    agent_name,
    echo_instructions,
)

load_dotenv()


@pytest.fixture
def container():
    c = make_container(WorkerProvider(), ClientProvider(), AgentProvider())
    yield c
    c.close()


@pytest.fixture(params=["parallel", "sequential"])
def agent_constructor_args_sequential_modes(request) -> dict[str, bool]:
    mode: str = request.param
    return {"sequential_only_mode": mode == "sequential"}


@pytest.fixture(params=["openai_responses", "openai_chat"])
def agent_constructor_args_model_client(request) -> dict[str, Any]:
    model_type: str = request.param
    if model_type == "openai_chat":
        model_client = OpenAIModelClient(os.environ["TEST_LLM_MODEL_NAME"], reasoning_effort=os.getenv("TEST_REASONING_EFFORT"))
        return {"model_client": model_client}
    elif model_type == "openai_responses":
        model_client = OpenAIResponsesModelClient(os.environ["TEST_LLM_MODEL_NAME"], reasoning_effort=os.getenv("TEST_REASONING_EFFORT"))
        return {"model_client": model_client}
    else:
        raise RuntimeError(f"Invalid model client: {model_type}")


@pytest.fixture
def deploy_agent(agent_constructor_args_sequential_modes, agent_constructor_args_model_client, container) -> SimpleAgent:
    worker = container.get(Worker)
    agent = SimpleAgent(
        "test_simple_agent",
        system_prompt=f"You are a helpful AI assistant. Your name is {agent_name}. Help the user with their questions as much as possible.",
        subscribe_topics="test_agent.input",
        publish_topic="test_agent.output",
        **agent_constructor_args_model_client,
        **agent_constructor_args_sequential_modes,
    )
    worker.add_nodes(agent)
    return agent


@pytest.fixture
def deploy_function_agent(agent_constructor_args_sequential_modes, container) -> Agent:
    worker = container.get(Worker)
    model = container.get(FunctionModel)
    agent = Agent(
        "test_function_agent",
        system_prompt="You are a helpful AI assistant.",
        subscribe_topics="test_function_agent.input",
        publish_topic="test_function_agent.output",
        model_client=model,
        **agent_constructor_args_sequential_modes,
    )
    print(f"\nDeployed agent with sequential_only_mode={agent_constructor_args_sequential_modes}")
    worker.add_nodes(agent)
    return agent


@pytest.fixture
def deploy_structured_agent(agent_constructor_args_sequential_modes, agent_constructor_args_model_client, container) -> StructuredAgent:
    worker = container.get(Worker)
    agent = StructuredAgent(
        "test_structured_agent",
        system_prompt=f"You are a helpful AI assistant. Your name is {agent_name}. Help the user with their questions as much as possible.",
        subscribe_topics="test_agent.input",
        publish_topic="test_agent.output",
        final_output_type=Response,
        **agent_constructor_args_model_client,
        **agent_constructor_args_sequential_modes,
    )
    worker.add_nodes(agent)
    return agent


@pytest.fixture
def deploy_structured_agent_factory(
    agent_constructor_args_sequential_modes, agent_constructor_args_model_client, container
) -> Callable[..., Agent[OutputT]]:
    worker = container.get(Worker)

    def agent_factory(output_type: type[OutputT]) -> Agent[OutputT]:
        agent = Agent[output_type](
            "test_custom_structured_agent",
            system_prompt=f"You are a helpful AI assistant. Your name is {agent_name}. Help the user with their questions as much as possible.",
            subscribe_topics="test_agent.input",
            publish_topic="test_agent.output",
            final_output_type=output_type,
            **agent_constructor_args_model_client,
            **agent_constructor_args_sequential_modes,
        )
        worker.add_nodes(agent)
        return agent

    return agent_factory


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
