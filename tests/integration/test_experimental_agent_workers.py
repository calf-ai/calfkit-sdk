import os
from dataclasses import dataclass

import pytest
from dishka import AnyOf, Provider, Scope, WithParents, make_container, provide
from dotenv import load_dotenv
from faststream.kafka import KafkaBroker, TestKafkaBroker

from calfkit._vendor.pydantic_ai import models
from calfkit._vendor.pydantic_ai.messages import ModelResponse
from calfkit.experimental.client import Client
from calfkit.experimental.nodes.agent_def import Agent, BaseAgentNodeDef
from calfkit.experimental.nodes.tool_def import BaseToolNodeDef, ToolNodeDef, agent_tool
from calfkit.experimental.worker.worker import Worker
from calfkit.models.tool_context import ToolContext
from calfkit.providers.pydantic_ai.model_client import PydanticModelClient
from calfkit.providers.pydantic_ai.openai import OpenAIModelClient
from tests.utils import print_message_history, skip_if_no_openai_key

load_dotenv()

# Ensure model requests are allowed for integration tests
models.ALLOW_MODEL_REQUESTS = True


@dataclass
class Response:
    response: str
    recipient_name: str


SimpleAgent = Agent[str]

StructuredAgent = Agent[Response]

user_name: str = "Conan"
agent_name: str = "LeBron James III"
birthday = "January 1, 1967"


def get_users_birthday():
    """Use this tool to get the user's birthday."""
    return birthday


def get_users_name():
    """Use this tool to get the user's name. If you do not know the user's name, use this tool to get their name."""
    return user_name


def weather(location: str):
    """Use this tool to get the current weather at a provided location.

    Args:
        location (str): The name of the location (e.g. Miami, Fl)

    Returns:
        The description of the weather at the location.
    """

    return f"The weather at {location} is currently heavy snow and possibly hail later."


caller_id_lookup = {"id1": "9496310387", "id2": "9287792710", "id3": "2136179907"}


def get_caller_id(ctx: ToolContext):
    """Use this tool to identify the phone number the user is messaging from."""
    ephemeral_id = ctx.deps.provided_deps.get("ephemeral_id")
    if ephemeral_id is None:
        return "invalid id"
    return caller_id_lookup.get(ephemeral_id, "no number found")


class AgentProvider(Provider):
    scope = Scope.APP

    @provide
    def get_model_client(self) -> WithParents[OpenAIModelClient]:
        return OpenAIModelClient(os.environ["TEST_LLM_MODEL_NAME"], reasoning_effort=os.getenv("TEST_REASONING_EFFORT"))

    @provide
    def get_simple_agent(self, model_client: PydanticModelClient) -> AnyOf[SimpleAgent, BaseAgentNodeDef]:
        return SimpleAgent(
            "test_simple_agent",
            system_prompt=f"You are a helpful AI assistant. Your name is {agent_name}. Help the user with their questions as much as possible.",
            subscribe_topics="test_agent.input",
            publish_topic="test_agent.output",
            model_client=model_client,
        )

    @provide
    def get_structured_agent(self, model_client: PydanticModelClient) -> StructuredAgent:
        return StructuredAgent(
            "test_structured_agent",
            system_prompt=f"You are a helpful AI assistant. Your name is {agent_name}. Help the user with their questions as much as possible.",
            subscribe_topics="test_agent.input",
            publish_topic="test_agent.output",
            model_client=model_client,
            final_output_type=Response,
        )

    @provide
    def get_multiple_tools(self) -> AnyOf[list[BaseToolNodeDef], list[ToolNodeDef]]:
        return [agent_tool(get_users_name), agent_tool(weather), agent_tool(get_users_birthday)]

    @provide
    def get_caller_id_tool(self) -> AnyOf[BaseToolNodeDef, ToolNodeDef]:
        return agent_tool(get_caller_id)


class ClientProvider(Provider):
    scope = Scope.APP

    @provide
    def get_client_connection(self) -> Client:
        return Client.connect()

    @provide
    def get_broker(self, client: Client) -> KafkaBroker:
        return client.broker


class WorkerProvider(Provider):
    scope = Scope.APP

    @provide
    def get_worker(self, client: Client) -> Worker:
        return Worker(client, max_workers=1)


@pytest.fixture
def container():
    c = make_container(WorkerProvider(), ClientProvider(), AgentProvider())
    yield c
    c.close()


@pytest.fixture
def deploy_agent(container):
    worker = container.get(Worker)
    agent = container.get(SimpleAgent)
    worker.add_nodes(agent)


@pytest.fixture
def deploy_structured_agent(container):
    worker = container.get(Worker)
    agent = container.get(StructuredAgent)
    worker.add_nodes(agent)


@pytest.fixture
def deploy_multiple_agent_tools(container):
    tools = container.get(list[ToolNodeDef])
    worker = container.get(Worker)
    worker.add_nodes(*tools)


@pytest.fixture
def deploy_caller_id_agent_tool(container):
    tool = container.get(ToolNodeDef)
    worker = container.get(Worker)
    worker.add_nodes(tool)


def prepare_worker(container):
    worker = container.get(Worker)
    worker.prepare()


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_simple_agent_q_and_a(container, deploy_agent):
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker) as _:
        result = await client.execute_node("Hi, what's your name?", "test_agent.input")

        assert result.output is not None
        assert isinstance(result.output, str)
        assert agent_name.lower() in result.output.lower()
        print()
        print(f"Response message: {result.output}")


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_simple_agent_with_tool(container, deploy_agent, deploy_multiple_agent_tools):
    agent = container.get(SimpleAgent)
    tools = container.get(list[ToolNodeDef])
    agent.add_tools(tools[2])
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker) as _:
        result = await client.execute_node("Hi, what's my birthday?", "test_agent.input")

        assert result.output is not None

        print_message_history(result.message_history)

        assert any(len(msg.tool_calls) > 0 for msg in result.message_history if isinstance(msg, ModelResponse))
        assert isinstance(result.output, str)
        assert "1967" in result.output


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_simple_agent_with_multiple_tools(container, deploy_agent, deploy_multiple_agent_tools):
    agent = container.get(SimpleAgent)
    tools = container.get(list[ToolNodeDef])
    agent.add_tools(*tools)
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker) as _:
        result = await client.execute_node(
            "Hi, do you know my name, the weather in Singapore rn, and what my birthday is?", "test_agent.input", output_type=str
        )

        print_message_history(result.message_history)

        assert sum(len(msg.tool_calls) for msg in result.message_history if isinstance(msg, ModelResponse)) > 1
        assert result.output is not None
        assert user_name.lower() in result.output.lower()
        assert isinstance(result.output, str)
        assert "1967" in result.output.lower()
        assert "snow" in result.output.lower()


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_simple_agent_with_multiturn_convo(container, deploy_agent, deploy_multiple_agent_tools):
    agent = container.get(SimpleAgent)
    tools = container.get(list[ToolNodeDef])
    agent.add_tools(*tools)
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker) as _:
        result = await client.execute_node("Do you know my name?", "test_agent.input")

        assert result.output is not None
        assert user_name.lower() in result.output.lower()

        result = await client.execute_node(
            "And what's your name?",
            "test_agent.input",
            message_history=result.message_history,
        )

        assert result.output is not None
        assert agent_name.lower() in result.output.lower()

        result = await client.execute_node(
            "What's the weather in vegas rn and what's my birthday?",
            "test_agent.input",
            message_history=result.message_history,
        )

        assert result.output is not None
        assert "1967" in result.output.lower()
        assert "snow" in result.output.lower()

        print_message_history(result.message_history)


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_simple_agent_with_injected_deps(container, deploy_agent, deploy_caller_id_agent_tool):
    agent = container.get(SimpleAgent)
    tool = container.get(ToolNodeDef)
    agent.add_tools(tool)
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker) as _:
        result = await client.execute_node(
            "I am messaging you from my iphone, do you know my phone number? Give my phone # with no spaces or special characters in between.",
            "test_agent.input",
            deps={"ephemeral_id": "id1"},
        )

        assert result.output is not None
        assert caller_id_lookup["id1"] in result.output.lower()

        result = await client.execute_node(
            "I am messaging you from my iphone, do you know my phone number? Give my phone # with no spaces or special characters in between.",
            "test_agent.input",
            deps={"ephemeral_id": "id2"},
        )

        assert result.output is not None
        assert caller_id_lookup["id2"] in result.output.lower()

        result = await client.execute_node(
            "I am messaging you from my iphone, do you know my phone number? Give my phone # with no spaces or special characters in between.",
            "test_agent.input",
            deps={"ephemeral_id": "id3"},
        )

        assert result.output is not None
        assert caller_id_lookup["id3"] in result.output.lower()

        print_message_history(result.message_history)


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_structured_output_agent(container, deploy_structured_agent):
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker) as _:
        result = await client.execute_node(
            f"What's your name? My name is {user_name}",
            "test_agent.input",
            output_type=Response,
            temp_instructions="When responding, always direct responses to the recipient's name you would like to target.",
        )

        assert result.output is not None
        assert isinstance(result.output, Response)
        assert user_name.lower() in result.output.recipient_name.lower()
        assert agent_name.lower() in result.output.response.lower()
        print(f"structured_output: {result.output}")

        result = await client.execute_node(
            "Do you remember my name?",
            "test_agent.input",
            output_type=Response,
            temp_instructions="when responding, always direct responses to specific recipients you would like to target.",
            message_history=result.message_history,
        )

        assert result.output is not None
        assert isinstance(result.output, Response)
        assert user_name.lower() in result.output.recipient_name.lower()
        assert user_name.lower() in result.output.response.lower()
        print(f"structured_output: {result.output}")

        result = await client.execute_node(
            "Please tell my friend Amy to remember to exercise today",
            "test_agent.input",
            output_type=Response,
            temp_instructions="when responding, always direct responses to specific recipients you would like to target.",
            message_history=result.message_history,
        )
        assert result.output is not None
        assert isinstance(result.output, Response)
        assert "amy" in result.output.recipient_name.lower()
        print(f"structured_output: {result.output}")

        print_message_history(result.message_history)
