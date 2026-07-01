"""Unit tests for the three levels of instructions: __init__, runtime temp_instructions, and @agent.instructions decorator."""

from faker import Faker
from faststream.kafka import KafkaBroker, TestKafkaBroker

from calfkit.client import Client
from tests.providers import INSTRUCTIONS_TEST_SYSTEM_PROMPT, prepare_worker


async def test_init_instructions_only(container, deploy_instructions_agent):
    """With no temp_instructions or dynamic instructions, the model receives exactly the injected
    identity line (``You are {name}.``) leading the system_prompt — and nothing else."""
    prepare_worker(container)
    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        result = await client.agent(topic="test_instructions_agent.input").execute("hello")

    assert result.output is not None
    assert isinstance(result.output, str)
    identity = f"You are {deploy_instructions_agent.name}."
    assert result.output.strip() == f"{identity}\n\n{INSTRUCTIONS_TEST_SYSTEM_PROMPT}"
    assert result.output.count(INSTRUCTIONS_TEST_SYSTEM_PROMPT) == 1


async def test_name_injected_as_leading_line(container, deploy_instructions_agent):
    """The constructor name is baked into the agent's instructions as the leading
    ``You are {name}.`` line, ahead of the system_prompt, on every invocation."""
    prepare_worker(container)
    broker = container.get(KafkaBroker)
    client = container.get(Client)

    async with TestKafkaBroker(broker):
        result = await client.agent(topic="test_instructions_agent.input").execute("hello")

    assert result.output is not None and isinstance(result.output, str)
    identity = f"You are {deploy_instructions_agent.name}."
    assert result.output.startswith(identity)
    assert result.output.index(identity) < result.output.index(INSTRUCTIONS_TEST_SYSTEM_PROMPT)
    assert result.output.count(identity) == 1


async def test_runtime_instructions_appended(container, deploy_instructions_agent):
    """temp_instructions passed at the client level should be appended after the system_prompt."""
    prepare_worker(container)
    broker = container.get(KafkaBroker)
    client = container.get(Client)
    random_runtime_instruction = Faker().sentence(nb_words=50)

    async with TestKafkaBroker(broker):
        result = await client.agent(topic="test_instructions_agent.input").execute(
            "hello",
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
        result = await client.agent(topic="test_instructions_agent.input").execute("hello")

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
        result = await client.agent(topic="test_instructions_agent.input").execute(
            "hello",
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
        result = await client.agent(topic="test_instructions_agent.input").execute(
            "hello",
            temp_instructions=random_runtime_instruction,
        )
    assert result.output is not None and isinstance(result.output, str)
    assert INSTRUCTIONS_TEST_SYSTEM_PROMPT in result.output
    assert random_runtime_instruction in result.output
    assert random_dynamic_instruction[-1] in result.output

    async with TestKafkaBroker(broker):
        result = await client.agent(topic="test_instructions_agent.input").execute(
            "hello again.",
        )
    assert result.output is not None and isinstance(result.output, str)
    assert INSTRUCTIONS_TEST_SYSTEM_PROMPT in result.output
    assert random_runtime_instruction not in result.output
    assert random_dynamic_instruction[0] not in result.output
    assert random_dynamic_instruction[-1] in result.output
