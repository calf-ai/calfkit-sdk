import os
from collections.abc import Callable
from dataclasses import dataclass

from dishka import AnyOf, Provider, Scope, WithParents, provide
from dotenv import load_dotenv
from faststream.kafka import KafkaBroker

from calfkit.experimental._types import OutputT
from calfkit.experimental.client import Client
from calfkit.experimental.nodes.agent_def import Agent, BaseAgentNodeDef
from calfkit.experimental.nodes.tool_def import BaseToolNodeDef, ToolNodeDef, agent_tool
from calfkit.experimental.worker.worker import Worker
from calfkit.models.tool_context import ToolContext
from calfkit.providers.pydantic_ai.model_client import PydanticModelClient
from calfkit.providers.pydantic_ai.openai import OpenAIModelClient

load_dotenv()


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
    def get_structured_agent_factory(self, model_client: PydanticModelClient, worker: Worker) -> Callable:
        def factory(output_type: type[OutputT]) -> Agent[OutputT]:
            agent = Agent[output_type](
                "test_custom_structured_agent",
                system_prompt=f"You are a helpful AI assistant. Your name is {agent_name}. Help the user with their questions as much as possible.",
                subscribe_topics="test_agent.input",
                publish_topic="test_agent.output",
                model_client=model_client,
                final_output_type=output_type,
            )
            worker.add_nodes(agent)

            return agent

        return factory

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


def prepare_worker(container):
    worker = container.get(Worker)
    worker.prepare()
