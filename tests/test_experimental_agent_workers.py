import os
from dataclasses import dataclass
from typing import Annotated, Any

import pytest
import uuid_utils
from dishka import AnyOf, Provider, Scope, WithParents, make_container, provide
from dotenv import load_dotenv
from faststream import Context
from faststream.kafka import KafkaBroker, TestKafkaBroker

from calfkit._vendor.pydantic_ai import models
from calfkit._vendor.pydantic_ai.messages import ModelMessage, ModelRequest, ModelResponse
from calfkit.experimental.base_models.envelope import Envelope
from calfkit.experimental.client import Client
from calfkit.experimental.data_model.payload import ContentPart
from calfkit.experimental.nodes.agent_def import BaseAgentNodeDef
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


@dataclass
class StructuredOutput:
    text: str | None
    data: Response | None


SimpleAgent = BaseAgentNodeDef[str]

StructuredAgent = BaseAgentNodeDef[Response]

user_name: str = "Conan"
agent_name: str = "LeBron James III"


def read_mind():
    """Use this tool and you'll have the ability to read minds."""
    return "Gigantic Apple Pencils, and the Cars movie"


def get_users_name():
    """Use this tool to get the user's name."""
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
            system_prompt=f"You are a helpful AI assistant. Your name is {agent_name}.",
            subscribe_topics="test_agent.input",
            publish_topic="test_agent.output",
            model_client=model_client,
        )

    @provide
    def get_structured_agent(self, model_client: PydanticModelClient) -> StructuredAgent:
        return StructuredAgent(
            "test_structured_agent",
            system_prompt=f"You are a helpful AI assistant. Your name is {agent_name}.",
            subscribe_topics="test_agent.input",
            publish_topic="test_agent.output",
            model_client=model_client,
            final_output_type=Response,
        )

    @provide
    def get_multiple_tools(self) -> AnyOf[list[BaseToolNodeDef], list[ToolNodeDef]]:
        return [agent_tool(read_mind), agent_tool(get_users_name), agent_tool(weather)]

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


class TestUtilsProvider(Provider):
    scope = Scope.APP

    @provide
    def get_response_store(self) -> dict:
        return dict()


di_container = make_container(WorkerProvider(), ClientProvider(), AgentProvider(), TestUtilsProvider())


@pytest.fixture(scope="session")
def deploy_agent():
    worker = di_container.get(Worker)
    agent = di_container.get(SimpleAgent)
    worker.add_nodes(agent)


@pytest.fixture(scope="session")
def deploy_structured_agent():
    worker = di_container.get(Worker)
    agent = di_container.get(StructuredAgent)
    worker.add_nodes(agent)


@pytest.fixture(scope="session")
def deploy_multiple_agent_tools():
    tools = di_container.get(list[ToolNodeDef])
    worker = di_container.get(Worker)
    worker.add_nodes(*tools)


@pytest.fixture(scope="session")
def deploy_caller_id_agent_tool():
    tool = di_container.get(ToolNodeDef)
    worker = di_container.get(Worker)
    worker.add_nodes(tool)


def gather_factory():
    async def gather_results(
        envelope: Envelope,
        correlation_id: Annotated[str, Context()],
    ):
        # Access dishka container directly instead of FastStream's Depends,
        # which passes the return value through Pydantic validation and
        # shallow-copies bare types like `dict`.
        resp_store = di_container.get(dict)
        resp_store[correlation_id] = envelope

    return gather_results


def prepare_worker():
    worker = di_container.get(Worker)
    worker.prepare()


async def send_message(
    prompt: str,
    callback_topic: str,
    temp_instructions: str | None = None,
    msg_history: list[ModelMessage] | None = None,
    deps: dict[str, Any] | None = None,
) -> Envelope:
    corr_id = uuid_utils.uuid7().hex
    client = di_container.get(Client)
    await client.invoke_node(
        prompt, "test_agent.input", callback_topic, corr_id, temp_instructions=temp_instructions, message_history=msg_history, deps=deps
    )
    resp_store = di_container.get(dict)
    assert corr_id in resp_store, f"Expected response for corr_id={corr_id[:8]}... but resp_store is empty"

    response = resp_store[corr_id]

    assert isinstance(response, Envelope)
    assert isinstance(response.context.state.message_history[-1], (ModelResponse, ModelRequest))

    return response


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_simple_agent_q_and_a(deploy_agent):
    prepare_worker()

    broker = di_container.get(KafkaBroker)
    client = di_container.get(Client)
    resp_store = di_container.get(dict)

    gather_results = gather_factory()
    gather_results = broker.subscriber("test_agent.q_and_a.output")(gather_results)

    async with TestKafkaBroker(broker) as _:
        corr_id = uuid_utils.uuid7().hex
        await client.invoke_node("Hi, what's your name?", "test_agent.input", "test_agent.q_and_a.output", corr_id)

        # TestKafkaBroker is synchronous — the entire handler chain completes
        # inline during invoke_node(), so resp_store is already populated.
        assert corr_id in resp_store, f"Expected response for corr_id={corr_id[:8]}... but resp_store is empty"

        response = resp_store[corr_id]
        assert isinstance(response, Envelope)
        assert isinstance(response.context.state.message_history[-1], ModelResponse)
        print()
        print(f"Response message: {response.context.state.message_history[-1].text}")
        if response.context.state.message_history[-1].thinking:
            print(f"    thinking: {response.context.state.message_history[-1].thinking}")


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_simple_agent_with_tool(deploy_agent, deploy_multiple_agent_tools):
    agent = di_container.get(SimpleAgent)
    tools = di_container.get(list[ToolNodeDef])
    agent.add_tools(tools[0])
    prepare_worker()

    broker = di_container.get(KafkaBroker)
    resp_store = di_container.get(dict)
    client = di_container.get(Client)
    gather_results = gather_factory()
    gather_results = broker.subscriber("test_agent.tool_test.output")(gather_results)
    async with TestKafkaBroker(broker) as _:
        corr_id = uuid_utils.uuid7().hex
        await client.invoke_node("Hi, what am i thinking?", "test_agent.input", "test_agent.tool_test.output", corr_id)

        # TestKafkaBroker is synchronous — the entire handler chain completes
        # inline during invoke_node(), so resp_store is already populated.
        assert corr_id in resp_store, f"Expected response for corr_id={corr_id[:8]}... but resp_store is empty"

        response = resp_store[corr_id]

        assert isinstance(response, Envelope)
        assert isinstance(response.context.state.message_history[-1], ModelResponse)

        print_message_history(response.context.state.message_history)

        assert any(len(msg.tool_calls) > 0 for msg in response.context.state.message_history if isinstance(msg, ModelResponse))


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_simple_agent_with_multiple_tools(deploy_agent, deploy_multiple_agent_tools):
    agent = di_container.get(SimpleAgent)
    tools = di_container.get(list[ToolNodeDef])
    agent.add_tools(*tools)
    prepare_worker()

    broker = di_container.get(KafkaBroker)
    resp_store = di_container.get(dict)
    client = di_container.get(Client)
    gather_results = gather_factory()
    gather_results = broker.subscriber("test_agent.tools_test.output")(gather_results)
    async with TestKafkaBroker(broker) as _:
        corr_id = uuid_utils.uuid7().hex
        await client.invoke_node(
            "Hi, do you know my name and the weather in Singapore rn? And what am I thinking?",
            "test_agent.input",
            "test_agent.tools_test.output",
            corr_id,
        )

        # TestKafkaBroker is synchronous — the entire handler chain completes
        # inline during invoke_node(), so resp_store is already populated.
        assert corr_id in resp_store, f"Expected response for corr_id={corr_id[:8]}... but resp_store is empty"

        response = resp_store[corr_id]

        assert isinstance(response, Envelope)
        assert isinstance(response.context.state.message_history[-1], ModelResponse)

        print_message_history(response.context.state.message_history)

        assert sum(len(msg.tool_calls) for msg in response.context.state.message_history if isinstance(msg, ModelResponse)) > 1
        assert response.context.state.message_history[-1].text is not None
        assert user_name.lower() in response.context.state.message_history[-1].text.lower()
        assert "cars" in response.context.state.message_history[-1].text.lower()
        assert "snow" in response.context.state.message_history[-1].text.lower()


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_simple_agent_with_multiturn_convo(deploy_agent, deploy_multiple_agent_tools):
    agent = di_container.get(SimpleAgent)
    tools = di_container.get(list[ToolNodeDef])
    agent.add_tools(*tools)
    prepare_worker()

    broker = di_container.get(KafkaBroker)
    gather_results = gather_factory()
    gather_results = broker.subscriber("test_agent.tools_test.output")(gather_results)

    async with TestKafkaBroker(broker) as _:
        result = await send_message("do you know my name? DO NOT tell me your name for now.", "test_agent.tools_test.output")
        resp_msg = result.context.state.message_history[-1]

        assert isinstance(resp_msg, ModelResponse)
        assert resp_msg.text is not None
        assert user_name.lower() in resp_msg.text.lower()

        result = await send_message("And what's your name?", "test_agent.tools_test.output", msg_history=result.context.state.message_history)

        resp_msg = result.context.state.message_history[-1]

        assert isinstance(resp_msg, ModelResponse)
        assert resp_msg.text is not None
        assert agent_name.lower() in resp_msg.text.lower()

        result = await send_message(
            "What's the weather in vegas rn and read my mind pls.", "test_agent.tools_test.output", msg_history=result.context.state.message_history
        )

        resp_msg = result.context.state.message_history[-1]

        assert isinstance(resp_msg, ModelResponse)
        assert resp_msg.text is not None
        assert "cars" in resp_msg.text.lower()
        assert "snow" in resp_msg.text.lower()

        print_message_history(result.context.state.message_history)


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_simple_agent_with_injected_deps(deploy_agent, deploy_caller_id_agent_tool):
    agent = di_container.get(SimpleAgent)
    tool = di_container.get(ToolNodeDef)
    agent.add_tools(tool)
    prepare_worker()

    broker = di_container.get(KafkaBroker)
    gather_results = gather_factory()
    gather_results = broker.subscriber("test_agent.tools_test.output")(gather_results)

    async with TestKafkaBroker(broker) as _:
        result = await send_message(
            "I am messaging you from my iphone, do you know my phone number? Give my phone # with no spaces or special characters in between.",
            "test_agent.tools_test.output",
            deps={"ephemeral_id": "id1"},
        )
        resp_msg = result.context.state.message_history[-1]

        assert isinstance(resp_msg, ModelResponse)
        assert resp_msg.text is not None
        assert caller_id_lookup["id1"] in resp_msg.text.lower()

        result = await send_message(
            "I'm on my android phone now. What's this new number?",
            "test_agent.tools_test.output",
            msg_history=result.context.state.message_history,
            deps={"ephemeral_id": "id2"},
        )

        resp_msg = result.context.state.message_history[-1]

        assert isinstance(resp_msg, ModelResponse)
        assert resp_msg.text is not None
        assert caller_id_lookup["id2"] in resp_msg.text.lower()

        result = await send_message(
            "This is my google phone, please check this number.",
            "test_agent.tools_test.output",
            msg_history=result.context.state.message_history,
            deps={"ephemeral_id": "id3"},
        )

        resp_msg = result.context.state.message_history[-1]

        assert isinstance(resp_msg, ModelResponse)
        assert resp_msg.text is not None
        assert caller_id_lookup["id3"] in resp_msg.text.lower()

        print_message_history(result.context.state.message_history)


def deserialize_output(resp_parts: list[ContentPart]) -> StructuredOutput:
    data = None
    text = None
    for part in resp_parts:
        if part.kind == "data" and isinstance(part.data, dict):
            data = Response(**part.data)
        if part.kind == "text":
            text = part.text

    return StructuredOutput(text=text, data=data)


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_structured_output_agent(deploy_structured_agent):
    prepare_worker()

    broker = di_container.get(KafkaBroker)
    gather_results = gather_factory()
    gather_results = broker.subscriber("test_agent.structured_output_test.output")(gather_results)

    async with TestKafkaBroker(broker) as _:
        result = await send_message(
            f"What's your name? My name is {user_name}",
            "test_agent.structured_output_test.output",
            "When responding, always direct responses to the recipient's name you would like to target.",
        )
        structured_output = deserialize_output(result.context.state.final_output_parts)

        assert structured_output.data is not None
        assert user_name.lower() in structured_output.data.recipient_name.lower()
        assert agent_name.lower() in structured_output.data.response.lower()
        print(f"structured_output: {structured_output}")

        result = await send_message(
            "Do you remember my name?",
            "test_agent.structured_output_test.output",
            "when responding, always direct responses to specific recipients you would like to target.",
            msg_history=result.context.state.message_history,
        )
        structured_output = deserialize_output(result.context.state.final_output_parts)

        assert structured_output.data is not None
        assert user_name.lower() in structured_output.data.recipient_name.lower()
        assert user_name.lower() in structured_output.data.response.lower()
        print(f"structured_output: {structured_output}")

        result = await send_message(
            "Please tell my friend Amy to remember to exercise today",
            "test_agent.structured_output_test.output",
            "when responding, always direct responses to specific recipients you would like to target.",
            msg_history=result.context.state.message_history,
        )
        structured_output = deserialize_output(result.context.state.final_output_parts)
        assert structured_output.data is not None
        assert "amy" in structured_output.data.recipient_name.lower()
        print(f"structured_output: {structured_output}")

        print_message_history(result.context.state.message_history)
