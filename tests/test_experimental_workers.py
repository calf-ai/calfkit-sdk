import os
from typing import Annotated, Any

import pytest
import uuid_utils
from dishka import AnyOf, Provider, Scope, WithParents, make_container, provide
from dotenv import load_dotenv
from faststream import Context
from faststream.kafka import KafkaBroker, TestKafkaBroker

from calfkit._vendor.pydantic_ai import models
from calfkit.experimental._types import AgentDepsT
from calfkit.experimental.base_models.envelope import Envelope
from calfkit.experimental.base_models.session_context import Deps
from calfkit.experimental.client import Client
from calfkit.experimental.nodes.agent_def import BaseAgentNodeDef
from calfkit.experimental.worker.worker import Worker
from calfkit.providers.pydantic_ai.model_client import PydanticModelClient
from calfkit.providers.pydantic_ai.openai import OpenAIModelClient

load_dotenv()

# Ensure model requests are allowed for integration tests
models.ALLOW_MODEL_REQUESTS = True

# Skip integration tests if OpenAI API key is not available
skip_if_no_openai_key = pytest.mark.skipif(
    not os.getenv("OPENAI_API_KEY"),
    reason="Skipping integration test: OPENAI_API_KEY not set in environment",
)

SimpleAgent = BaseAgentNodeDef[AgentDepsT, str]


class AgentProvider(Provider):
    scope = Scope.APP

    @provide
    def get_model_client(self) -> WithParents[OpenAIModelClient]:
        return OpenAIModelClient(os.environ["TEST_LLM_MODEL_NAME"], reasoning_effort=os.getenv("TEST_REASONING_EFFORT"))

    @provide
    def get_simple_agent(self, model_client: PydanticModelClient) -> AnyOf[SimpleAgent, BaseAgentNodeDef]:
        return SimpleAgent(
            "test_simple_agent",
            system_prompt="You are a helpful AI assistant. You're name is LeBron James III.",
            subscribe_topics="test_agent.input",
            publish_topic="test_agent.output",
            model_client=model_client,
        )


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
    def get_worker(self, client: Client, agent_node: SimpleAgent) -> Worker:
        return Worker(client, nodes=[agent_node], max_workers=1)


class TestUtilsProvider(Provider):
    scope = Scope.APP

    @provide
    def get_response_store(self) -> dict:
        return dict()


di_container = make_container(WorkerProvider(), ClientProvider(), AgentProvider(), TestUtilsProvider())


@pytest.fixture(scope="session")
def deploy_worker():
    worker = di_container.get(Worker)
    worker.prepare()


def gather_factory():
    async def gather_results(
        envelope: Envelope[Deps[Any]],
        correlation_id: Annotated[str, Context()],
    ):
        # Access dishka container directly instead of FastStream's Depends,
        # which passes the return value through Pydantic validation and
        # shallow-copies bare types like `dict`.
        resp_store = di_container.get(dict)
        resp_store[correlation_id] = envelope

    return gather_results


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_agent(deploy_worker):
    broker = di_container.get(KafkaBroker)
    client = di_container.get(Client)
    resp_store = di_container.get(dict)

    gather_results = gather_factory()
    gather_results = broker.subscriber("test_agent.output")(gather_results)

    async with TestKafkaBroker(broker) as _:
        corr_id = uuid_utils.uuid7().hex
        await client.invoke_node("Hi, what's your name?", "test_agent.input", "test_agent.output", corr_id)

        # TestKafkaBroker is synchronous — the entire handler chain completes
        # inline during invoke_node(), so resp_store is already populated.
        assert corr_id in resp_store, f"Expected response for corr_id={corr_id[:8]}... but resp_store is empty"

        response = resp_store[corr_id]
        assert isinstance(response, Envelope)
