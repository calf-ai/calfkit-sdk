import asyncio
import itertools
from typing import Annotated

import pytest
from dotenv import load_dotenv
from faststream import Context
from faststream.kafka import TestKafkaBroker

from calf.agents.chat import Chat
from calf.context.schema import EventContext
from calf.providers.openai.client import OpenAIClient
from calf.runtime import CalfRuntime

load_dotenv()

CalfRuntime.initialize()

client = OpenAIClient("gpt-5-nano", create_kwargs={'reasoning_effort': 'minimal'})
chat = Chat(client)

# async test syncing purposes
counter = itertools.count(start=1)
test_results: dict[str, EventContext] = {}
condition = asyncio.Condition()


@chat.calf.subscriber(chat.DEFAULT_OUTPUT_TOPIC)
async def gather_result(
    ctx: EventContext,
    correlation_id: Annotated[str, Context()],
):
    assert correlation_id
    test_results[correlation_id] = ctx


@pytest.mark.asyncio
async def test_simple_chat():
    print(f"\n{'=' * 10}Start test_simple_chat{'=' * 10}")
    async with TestKafkaBroker(chat.calf) as _:
        trace_id = str(next(counter))
        await chat.invoke("Hi my name is Conan. can you repeat my name back to me?", trace_id)
        await asyncio.wait_for(condition.wait_for(lambda: trace_id in test_results), timeout=10.0)
        result_ctx = test_results[trace_id]
        assert result_ctx.text.strip()
        assert "Conan" in result_ctx.text
        print(f"correlation id = {trace_id}")
        print(f"Response: {result_ctx.text}")
    print(f"{'=' * 10}Finished test_simple_chat{'=' * 10}\n\n")
