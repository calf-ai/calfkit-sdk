import os

import pytest
from dotenv import load_dotenv

from calfkit._vendor.pydantic_ai import models

load_dotenv()

# Ensure model requests are allowed for integration tests
models.ALLOW_MODEL_REQUESTS = True

skip_if_no_openai_key = pytest.mark.skipif(
    not os.getenv("OPENAI_API_KEY"),
    reason="Skipping integration test: OPENAI_API_KEY not set in environment",
)

@pytest.fixture(scope="session")
def deploy_broker() -> BrokerClient:
    # simulate the deployment pre-testing
    broker = BrokerClient()
    service = NodesService(broker)

    # 1. Deploy llm model node worker
    model_client = OpenAIModelClient(
        os.environ["TEST_LLM_MODEL_NAME"], reasoning_effort=os.getenv("TEST_REASONING_EFFORT")
    )
    chat_node = ChatNode(model_client)
    service.register_node(chat_node)

    # 2a. Deploy tool node worker for get_weather tool
    service.register_node(get_weather)

    # 2b. Deploy 2 tool node worker for get_temperature tool
    service.register_node(get_temperature, max_workers=2)

    # 3. Deploy router node worker
    router_node = AgentRouterNode(
        chat_node=ChatNode(),
        tool_nodes=[get_weather, get_temperature],
        message_history_store=InMemoryMessageHistoryStore(),
        system_prompt="Please always greet the user as Conan before every message",
        name="main_agent",
    )
    service.register_node(router_node)

    return broker
