from faststream.kafka import KafkaBroker, TestKafkaBroker

from calfkit.client import Client
from tests.providers import NO_TOOLS_RESPONSE_TEXT, prepare_worker
from tests.utils import find_last_tool_call_message


async def test_agent_tools_overrides_none(container, deploy_function_agent, deploy_multiple_contextual_tools):
    deploy_function_agent.add_tools(*deploy_multiple_contextual_tools)
    prepare_worker(container)

    client = container.get(Client)

    async with TestKafkaBroker(container.get(KafkaBroker)) as _:
        result = await client.execute_node(
            "Hey! Call all your tools concurrently right now", deploy_function_agent.subscribe_topics[0], tool_overrides=None
        )

        assert result.output is not None
        last_tool_msg = find_last_tool_call_message(result.message_history)
        assert last_tool_msg
        assert len(last_tool_msg.tool_calls) == len(deploy_multiple_contextual_tools)
        assert set(call.tool_name for call in last_tool_msg.tool_calls) == set(tool.tool_schema.name for tool in deploy_multiple_contextual_tools)


async def test_agent_tools_overrides_empty(container, deploy_function_agent, deploy_multiple_contextual_tools):
    deploy_function_agent.add_tools(*deploy_multiple_contextual_tools)
    prepare_worker(container)

    client = container.get(Client)

    async with TestKafkaBroker(container.get(KafkaBroker)) as _:
        result = await client.execute_node(
            "Hey! Call all your tools concurrently right now again.", deploy_function_agent.subscribe_topics[0], tool_overrides=[]
        )

        assert result.output is not None and result.output == NO_TOOLS_RESPONSE_TEXT


async def test_agent_tools_overrides_new_tools(container, deploy_function_agent, deploy_multiple_contextual_tools, deploy_no_arg_tools):
    deploy_function_agent.add_tools(*deploy_multiple_contextual_tools)
    prepare_worker(container)

    client = container.get(Client)

    async with TestKafkaBroker(container.get(KafkaBroker)) as _:
        result = await client.execute_node(
            "Hey! Call all your tools concurrently right now", deploy_function_agent.subscribe_topics[0], tool_overrides=deploy_no_arg_tools
        )

        assert result.output is not None
        last_tool_msg = find_last_tool_call_message(result.message_history)
        assert last_tool_msg
        assert len(last_tool_msg.tool_calls) == len(deploy_no_arg_tools)
        assert set(call.tool_name for call in last_tool_msg.tool_calls) == set(tool.tool_schema.name for tool in deploy_no_arg_tools)
