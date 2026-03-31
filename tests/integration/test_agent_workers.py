from faststream.kafka import KafkaBroker, TestKafkaBroker

from calfkit._vendor.pydantic_ai import models
from calfkit._vendor.pydantic_ai.messages import ModelResponse
from calfkit.client import Client
from calfkit.nodes import ToolNodeDef
from tests.integration.providers import (
    Response,
    agent_name,
    caller_id_lookup,
    prepare_worker,
    user_name,
)
from tests.utils import print_message_history, skip_if_no_openai_key

# Ensure model requests are allowed for integration tests
models.ALLOW_MODEL_REQUESTS = True


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


@skip_if_no_openai_key
async def test_simple_agent_with_tool(container, deploy_agent, deploy_multiple_agent_tools):
    deploy_agent.add_tools(deploy_multiple_agent_tools[2])
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


@skip_if_no_openai_key
async def test_simple_agent_with_multiple_tools(container, deploy_agent, deploy_multiple_agent_tools):
    deploy_agent.add_tools(*deploy_multiple_agent_tools)
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


@skip_if_no_openai_key
async def test_simple_agent_with_multiturn_convo(container, deploy_agent, deploy_multiple_agent_tools):
    deploy_agent.add_tools(*deploy_multiple_agent_tools)
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


@skip_if_no_openai_key
async def test_simple_agent_with_injected_deps(container, deploy_agent, deploy_caller_id_agent_tool):
    deploy_agent.add_tools(deploy_caller_id_agent_tool)
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
            "Please tell my friend Amy that it's snowing in Calgary.",
            "test_agent.input",
            output_type=Response,
            temp_instructions="when responding, always direct responses to specific recipients you would like to target.",
            message_history=result.message_history,
        )
        assert result.output is not None
        assert isinstance(result.output, Response)
        assert "amy" in result.output.recipient_name.lower()
        assert "snow" in result.output.response.lower()
        print(f"structured_output: {result.output}")

        print_message_history(result.message_history)
