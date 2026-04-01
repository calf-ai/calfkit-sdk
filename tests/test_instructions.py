"""Unit tests for the three levels of instructions: __init__, runtime temp_instructions, and @agent.instructions decorator."""

from faker import Faker
from faststream.kafka import KafkaBroker, TestKafkaBroker

from calfkit.client import Client
from tests.providers import INSTRUCTIONS_TEST_SYSTEM_PROMPT, prepare_worker


async def test_init_instructions_only(container, deploy_instructions_agent):
    """When no temp_instructions or dynamic instructions are provided, the model should receive only the system_prompt."""
    prepare_worker(container)
    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        result = await client.execute_node("hello", "test_instructions_agent.input")

    assert result.output is not None
    assert isinstance(result.output, str)
    assert INSTRUCTIONS_TEST_SYSTEM_PROMPT.strip().lower() == result.output.strip().lower()
    assert result.output.count(INSTRUCTIONS_TEST_SYSTEM_PROMPT) == 1


async def test_runtime_instructions_appended(container, deploy_instructions_agent):
    """temp_instructions passed at the client level should be appended after the system_prompt."""
    prepare_worker(container)
    broker = container.get(KafkaBroker)
    client = container.get(Client)
    random_runtime_instruction = Faker().sentence(nb_words=50)

    async with TestKafkaBroker(broker):
        result = await client.execute_node(
            "hello",
            "test_instructions_agent.input",
            temp_instructions=random_runtime_instruction,
        )

    assert result.output is not None
    assert isinstance(result.output, str)
    assert INSTRUCTIONS_TEST_SYSTEM_PROMPT in result.output
    assert random_runtime_instruction in result.output
    assert result.output.index(INSTRUCTIONS_TEST_SYSTEM_PROMPT) < result.output.index(random_runtime_instruction)
    assert result.output.count(random_runtime_instruction) == 1


async def test_dynamic_instructions_appended(container, deploy_instructions_agent):
    """@agent.instructions decorated functions should contribute to the instructions sent to the model."""
    prepare_worker(container)
    broker = container.get(KafkaBroker)
    client = container.get(Client)
    random_dynamic_instruction = []

    @deploy_instructions_agent.instructions
    def dynamic_instructions() -> str:
        random_dynamic_instruction.append(Faker().sentence(nb_words=50))
        return random_dynamic_instruction[-1]

    async with TestKafkaBroker(broker):
        result = await client.execute_node("hello", "test_instructions_agent.input")

    assert result.output is not None
    assert isinstance(result.output, str)
    assert INSTRUCTIONS_TEST_SYSTEM_PROMPT in result.output
    assert random_dynamic_instruction[0] in result.output
    assert result.output.index(INSTRUCTIONS_TEST_SYSTEM_PROMPT) < result.output.index(random_dynamic_instruction[0])
    assert result.output.count(random_dynamic_instruction[0]) == 1


async def test_all_three_instruction_levels(container, deploy_instructions_agent):
    """All three instruction levels (__init__, dynamic, temp) should be present in the model's instructions."""
    prepare_worker(container)
    broker = container.get(KafkaBroker)
    client = container.get(Client)
    random_dynamic_instruction = []
    random_runtime_instruction = Faker().sentence(nb_words=50)

    @deploy_instructions_agent.instructions
    def dynamic_instructions() -> str:
        random_dynamic_instruction.append(Faker().sentence(nb_words=50))
        return random_dynamic_instruction[-1]

    async with TestKafkaBroker(broker):
        result = await client.execute_node(
            "hello",
            "test_instructions_agent.input",
            temp_instructions=random_runtime_instruction,
        )

    assert result.output is not None
    assert isinstance(result.output, str)
    assert INSTRUCTIONS_TEST_SYSTEM_PROMPT in result.output
    assert random_runtime_instruction in result.output
    assert random_dynamic_instruction[0] in result.output
    assert result.output.index(INSTRUCTIONS_TEST_SYSTEM_PROMPT) < result.output.index(random_dynamic_instruction[0]) and result.output.index(
        random_runtime_instruction
    ) < result.output.index(random_dynamic_instruction[0])
    assert result.output.count(INSTRUCTIONS_TEST_SYSTEM_PROMPT) == 1
    assert result.output.count(random_runtime_instruction) == 1
    assert result.output.count(random_dynamic_instruction[0]) == 1


async def test_instructions_are_temporary(container, deploy_instructions_agent):
    prepare_worker(container)
    broker = container.get(KafkaBroker)
    client = container.get(Client)
    random_dynamic_instruction = []
    random_runtime_instruction = Faker().sentence(nb_words=75)

    @deploy_instructions_agent.instructions
    def dynamic_instructions() -> str:
        random_dynamic_instruction.append(Faker().sentence(nb_words=75))
        return random_dynamic_instruction[-1]

    async with TestKafkaBroker(broker):
        result = await client.execute_node(
            "hello",
            "test_instructions_agent.input",
            temp_instructions=random_runtime_instruction,
        )
    assert result.output is not None and isinstance(result.output, str)
    assert INSTRUCTIONS_TEST_SYSTEM_PROMPT in result.output
    assert random_runtime_instruction in result.output
    assert random_dynamic_instruction[-1] in result.output

    async with TestKafkaBroker(broker):
        result = await client.execute_node(
            "hello again.",
            "test_instructions_agent.input",
        )
    assert result.output is not None and isinstance(result.output, str)
    assert INSTRUCTIONS_TEST_SYSTEM_PROMPT in result.output
    assert random_runtime_instruction not in result.output
    assert random_dynamic_instruction[0] not in result.output
    assert random_dynamic_instruction[-1] in result.output
