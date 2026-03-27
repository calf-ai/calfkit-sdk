import pytest
from faststream.kafka import KafkaBroker, TestKafkaBroker

from calfkit._vendor.pydantic_ai import models
from calfkit.experimental.client import Client
from tests.providers import agent_name, prepare_worker, user_name
from tests.utils import skip_if_no_openai_key

# Ensure model requests are allowed for integration tests
models.ALLOW_MODEL_REQUESTS = True


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_structured_output_agent_without_client_output_type(container, deploy_structured_agent):
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker) as _:
        result = await client.execute_node(
            f"What's your name? My name is {user_name}",
            "test_agent.input",
            temp_instructions="When responding, always direct responses to the recipient's name you would like to target.",
        )

        assert result.output is not None
        assert isinstance(result.output, dict)
        assert user_name.lower() in result.output["recipient_name"].lower()
        assert agent_name.lower() in result.output["response"].lower()
        print(f"structured_output: {result.output}")


@pytest.mark.asyncio
@skip_if_no_openai_key
async def test_structured_output_agent_with_dict(container, deploy_structured_agent):
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker) as _:
        result = await client.execute_node(
            f"What's your name? My name is {user_name}",
            "test_agent.input",
            temp_instructions="When responding, always direct responses to the recipient's name you would like to target.",
        )

        assert result.output is not None
        assert isinstance(result.output, dict)
        assert user_name.lower() in result.output["recipient_name"].lower()
        assert agent_name.lower() in result.output["response"].lower()
        print(f"structured_output: {result.output}")
