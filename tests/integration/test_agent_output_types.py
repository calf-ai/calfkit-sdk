import pytest
from faststream.kafka import KafkaBroker, TestKafkaBroker
from pydantic import BaseModel

from calfkit._vendor.pydantic_ai import models
from calfkit.client import Client
from tests.providers import Response, agent_name, prepare_worker, user_name
from tests.utils import skip_if_no_live_llm

# Every test here drives a real model API. Opt the whole module into the `live`
# lane (deselected by default — see ADR 0007); `skip_if_no_live_llm` then skips
# cleanly when credentials are absent.
pytestmark = pytest.mark.live

# Ensure model requests are allowed for integration tests
models.ALLOW_MODEL_REQUESTS = True


@skip_if_no_live_llm
async def test_structured_output_agent_with_dataclass(container, deploy_structured_agent):
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker) as _:
        # output_type=Response: the DataPart is validated into the typed object (spec §2.2).
        result = await client.agent("test_structured_agent", output_type=Response).execute(
            f"What's your name? My name is {user_name}",
            temp_instructions="When responding, always direct responses to the recipient's name you would like to target.",
        )

        assert result.output is not None
        assert isinstance(result.output, Response)
        assert user_name.lower() in result.output.recipient_name.lower()
        assert agent_name.lower() in result.output.response.lower()
        print(f"structured_output: {result.output}")

        # Default str: the SAME structured DataPart is COERCED to its JSON string (spec §2.2) — never a
        # DeserializationError. The JSON carries the field names + values.
        result = await client.agent("test_structured_agent").execute(
            f"What's your name? My name is {user_name}",
            temp_instructions="When responding, always direct responses to the recipient's name you would like to target.",
        )

        assert isinstance(result.output, str)
        assert "recipient_name" in result.output  # the DataPart was JSON-stringified
        assert user_name.lower() in result.output.lower()
        assert agent_name.lower() in result.output.lower()
        print(f"str-coerced output: {result.output}")


@skip_if_no_live_llm
async def test_structured_output_agent_with_list(container, deploy_structured_agent_factory):
    deploy_structured_agent_factory(list[str])
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker) as _:
        # output_type=list[str]: the DataPart is validated into the typed list (spec §2.2).
        result = await client.agent("test_custom_structured_agent", output_type=list[str]).execute(
            "Provide a 3 item-long list of random colors",
        )

        assert result.output is not None
        assert isinstance(result.output, list)
        assert all(isinstance(item, str) for item in result.output)
        assert len(result.output) == 3
        print(f"structured_output: {result.output}")

        # Default str: the list DataPart is COERCED to its JSON-array string (spec §2.2).
        result = await client.agent("test_custom_structured_agent").execute(
            "Provide a 3 item-long list of random colors",
            temp_instructions="Extract all colors and put them into a list",
        )

        assert isinstance(result.output, str)
        assert "[" in result.output and "]" in result.output  # the list DataPart was JSON-stringified
        print(f"str-coerced output: {result.output}")


class Box(BaseModel):
    length: int
    width: int
    depth: int
    unit: str


@skip_if_no_live_llm
async def test_structured_output_agent_with_basemodel(container, deploy_structured_agent_factory):
    deploy_structured_agent_factory(Box)
    prepare_worker(container)

    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker) as _:
        # output_type=Box: the DataPart is validated into the typed BaseModel (spec §2.2).
        result = await client.agent("test_custom_structured_agent", output_type=Box).execute(
            "Here are the measurements (l x w x d): 3x2x5 cm",
            temp_instructions="Extract the box measurements.",
        )

        assert result.output is not None
        assert isinstance(result.output, Box)
        assert Box(length=3, width=2, depth=5, unit="cm") == result.output
        print(f"structured_output: {result.output}")

        # Default str: the Box DataPart is COERCED to its JSON string (spec §2.2).
        result = await client.agent("test_custom_structured_agent").execute(
            "Here are the measurements (l x w x d): 3x2x5 cm",
            temp_instructions="Extract the box measurements.",
        )

        assert isinstance(result.output, str)
        assert "length" in result.output  # the Box DataPart was JSON-stringified
        assert "cm" in result.output
        print(f"str-coerced output: {result.output}")
