import random
import string

from faker import Faker
from faststream.kafka import KafkaBroker, TestKafkaBroker

from calfkit.client import Client
from tests.providers import prepare_worker
from tests.utils import find_last_tool_call_message, print_message_history


async def test_concurrent_tool_calling(container, deploy_function_agent, deploy_multiple_contextual_tools):
    deploy_function_agent.add_tools(*deploy_multiple_contextual_tools)
    prepare_worker(container)

    client = container.get(Client)

    async with TestKafkaBroker(container.get(KafkaBroker)) as _:
        random_num = random.randint(1, 100)
        random_str = "".join(random.choices(string.ascii_letters, k=20))  # 20 char random string
        random_city = Faker().city()
        result = await client.execute_node(
            "Hey! Call all your tools in concurrently right now",
            deploy_function_agent.subscribe_topics[0],
            deps={"random_number": random_num, "random_string": random_str, "random_city": random_city},
        )

        assert result.output is not None
        assert str(random_num) in result.output
        assert random_str in result.output
        assert random_city in random_city
        last_tool_msg = find_last_tool_call_message(result.message_history)
        assert last_tool_msg
        assert len(last_tool_msg.tool_calls) == len(deploy_multiple_contextual_tools)

        print_message_history(result.message_history)
