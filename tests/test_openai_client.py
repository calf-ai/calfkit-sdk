"""Integration tests for OpenAIClient provider."""

import os
from typing import cast

import pytest
from dotenv import load_dotenv
from openai.types.chat import (
    ChatCompletionAssistantMessageParam,
    ChatCompletionMessageParam,
    ChatCompletionToolParam,
)

from calf.providers.openai import OpenAIClient

load_dotenv()


@pytest.fixture(scope="session")
def openai_client() -> OpenAIClient:
    api_key = os.environ.get("OPENAI_API_KEY")
    model = os.environ.get("OPENAI_MODEL")
    base_url = os.environ.get("BASE_URL")
    if not api_key or not model:
        pytest.skip("Set OPENAI_API_KEY and OPENAI_MODEL to run this integration test.")
    return OpenAIClient(
        model=model,
        api_key=api_key,
        base_url=base_url,
        create_kwargs={"reasoning_effort": "low"},
    )


@pytest.mark.asyncio
async def test_openai_generate_simple(openai_client: OpenAIClient) -> None:
    print("\n\nRunning simple sanity check...")
    msgs = [
        openai_client.create_user_message(
            "Respond with what LLM model you are and a short intro about yourself."
        )
    ]

    result = await openai_client.generate(messages=msgs)

    assert isinstance(result.text, str)
    assert result.text
    print(f"Response: {result.text}")


@pytest.mark.asyncio
async def test_openai_generate_tool_call(openai_client: OpenAIClient) -> None:
    """Test that the LLM correctly generates a tool call when prompted."""

    def get_weather(location: str) -> str:
        """Get the current weather in a given location.

        Args:
            location: The city and state, e.g. San Francisco, CA
        """
        return f"Weather in {location} is insanely hot"

    msgs = [openai_client.create_user_message("What's the weather like in Tokyo?")]
    tool_schemas = [openai_client.create_tool_schema(get_weather)]

    result = await openai_client.generate(messages=msgs, tools=tool_schemas)

    # The LLM should respond with a tool call
    assert result.tool_calls is not None, "Expected tool_calls but got None"
    assert len(result.tool_calls) > 0, "Expected at least one tool call"

    tool_call = result.tool_calls[0]
    assert tool_call.name == "get_weather", f"Expected 'get_weather' but got '{tool_call.name}'"
    assert tool_call.id, "Tool call should have an id"

    # Arguments are now parsed as kwargs dict
    assert isinstance(tool_call.kwargs, dict), f"Expected dict but got {type(tool_call.kwargs)}"
    assert "location" in tool_call.kwargs, f"Expected 'location' key in kwargs: {tool_call.kwargs}"
    assert "tokyo" in tool_call.kwargs["location"].lower(), (
        f"Expected 'Tokyo' in location but got: {tool_call.kwargs['location']}"
    )
    print(f"\nTool call: {tool_call.name}({tool_call.kwargs})")

    msgs.append(cast(ChatCompletionAssistantMessageParam, result.message.model_dump()))
    msgs.append(
        openai_client.create_tool_message(
            tool_call_id=tool_call.id, message=get_weather(tool_call.kwargs["location"])
        )
    )

    result = await openai_client.generate(messages=msgs, tools=tool_schemas)

    assert result.text
    assert "hot" in result.text
    print(f"Response: '{result.text}'")


@pytest.mark.asyncio
async def test_openai_generate_multi_turn(openai_client: OpenAIClient) -> None:
    """Test that the LLM maintains context across a multi-turn conversation."""
    # First turn: user introduces themselves
    messages = [openai_client.create_user_message("Hi, my name is Conan O'Brien.")]

    first_response = await openai_client.generate(messages=messages)
    assert first_response.text is not None, "Expected a response message"
    print(f"\nFirst response: {first_response.text}")

    # Build up chat history with the assistant's response
    messages.append(openai_client.create_assistant_message(first_response.text))

    # Second turn: ask if they remember the name
    messages.append(
        openai_client.create_user_message(
            "What is my name? Reply with just my name and nothing else."
        )
    )

    second_response = await openai_client.generate(messages=messages)
    assert second_response.text is not None, "Expected a response message"
    print(f"\n\nSecond response: {second_response.text}")

    # The LLM should recall the name from the conversation history
    assert "conan" in second_response.text.lower(), (
        f"Expected 'Conan' in response but got: {second_response.text}"
    )
