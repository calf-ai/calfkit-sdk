from faststream.kafka import KafkaBroker, TestKafkaBroker
from pydantic import BaseModel

from calfkit._vendor.pydantic_ai import models
from calfkit.client import Client
from tests.providers import Response, agent_name, prepare_worker, user_name
from tests.utils import skip_if_no_openai_key

# Ensure model requests are allowed for integration tests
models.ALLOW_MODEL_REQUESTS = True


@skip_if_no_openai_key
async def test_structured_output_agent_with_dataclass(container, deploy_structured_agent):
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker) as _:
        result = await client.execute_node(
            f"What's your name? My name is {user_name}",
            "test_agent.input",
            temp_instructions="When responding, always direct responses to the recipient's name you would like to target.",
            output_type=Response,
        )

        assert result.output is not None
        assert isinstance(result.output, Response)
        assert user_name.lower() in result.output.recipient_name.lower()
        assert agent_name.lower() in result.output.response.lower()
        print(f"structured_output: {result.output}")

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


@skip_if_no_openai_key
async def test_structured_output_agent_with_list(container, deploy_structured_agent_factory):
    deploy_structured_agent_factory(list[str])
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker) as _:
        result = await client.execute_node(
            "Provide a 3 item-long list of random colors",
            "test_agent.input",
            output_type=list[str],
        )

        assert result.output is not None
        assert isinstance(result.output, list)
        assert all(isinstance(item, str) for item in result.output)
        assert len(result.output) == 3
        print(f"structured_output: {result.output}")

        result = await client.execute_node(
            "Provide a 3 item-long list of random colors",
            "test_agent.input",
            temp_instructions="Extract all colors and put them into a list",
        )

        assert result.output is not None
        assert isinstance(result.output, list)
        assert all(isinstance(item, str) for item in result.output)
        assert len(result.output) == 3
        print(f"structured_output: {result.output}")


class Box(BaseModel):
    length: int
    width: int
    depth: int
    unit: str


@skip_if_no_openai_key
async def test_structured_output_agent_with_basemodel(container, deploy_structured_agent_factory):
    deploy_structured_agent_factory(Box)
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker) as _:
        result = await client.execute_node(
            "Here are the measurements (l x w x d): 3x2x5 cm",
            "test_agent.input",
            temp_instructions="Extract the box measurements.",
            output_type=Box,
        )

        assert result.output is not None
        assert isinstance(result.output, Box)
        assert Box(length=3, width=2, depth=5, unit="cm") == result.output
        print(f"structured_output: {result.output}")

        result = await client.execute_node(
            "Here are the measurements (l x w x d): 3x2x5 cm",
            "test_agent.input",
            temp_instructions="Extract the box measurements.",
        )

        assert result.output is not None
        assert isinstance(result.output, dict)
        assert Box(length=3, width=2, depth=5, unit="cm") == Box(**result.output)
        print(f"structured_output: {result.output}")
