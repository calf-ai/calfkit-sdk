import asyncio
import os
import textwrap
import time
from collections.abc import Callable

import pytest
from dotenv import load_dotenv

from calfkit._vendor.pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
)

load_dotenv()

# Skip integration tests if OpenAI API key is not available
skip_if_no_openai_key = pytest.mark.skipif(
    not os.getenv("OPENAI_API_KEY"),
    reason="Skipping integration test: OPENAI_API_KEY not set in environment",
)


async def wait_for_condition(
    predicate: Callable[[], bool],
    timeout: float = 20.0,
    poll_interval: float = 0.1,
) -> None:
    """Wait for a predicate to become True.

    Args:
        predicate: A callable that returns True when the condition is met.
        timeout: Maximum time to wait in seconds.
        poll_interval: Time between checks in seconds.

    Raises:
        asyncio.TimeoutError: If the condition is not met within the timeout.

    Example:
        await wait_for_condition(lambda: trace_id in store, timeout=10.0)
    """
    start = time.monotonic()
    while not predicate():
        elapsed = time.monotonic() - start
        if elapsed > timeout:
            raise asyncio.TimeoutError(f"Condition not met within {timeout}s timeout")
        await asyncio.sleep(poll_interval)


def print_message_history(message_history: list[ModelMessage]) -> None:
    separator = "-" * 60

    for i, message in enumerate(message_history):
        print(separator)

        if isinstance(message, ModelRequest):
            print(f"[{i}] REQUEST")
            for part in message.parts:
                print(textwrap.indent(f"[{part.part_kind}] {part!r}", "  "))

        elif isinstance(message, ModelResponse):
            model = message.model_name or "unknown"
            print(f"[{i}] RESPONSE (model={model})")
            if message.text:
                print(textwrap.indent(f"[text] \n{message.text}", "  "))
            if message.thinking:
                print(textwrap.indent(f"[thinking] \n{message.thinking[:200]}", "  "))
            for tc in message.tool_calls:
                print(textwrap.indent(f"[tool-call] \n{tc.tool_name}(id={tc.tool_call_id})", "  "))
                print(textwrap.indent(f"args: {tc.args_as_json_str()[:300]}", "    "))

    print(separator)


def find_last_tool_call_message(messages: list[ModelMessage]) -> ModelResponse | None:
    return next(
        (m for m in reversed(messages) if isinstance(m, ModelResponse) and m.tool_calls),
        None,
    )
