"""Integration tests for OpenAIClient provider."""

import os

import pytest
from dotenv import load_dotenv
from openai.types.chat import ChatCompletionMessageParam

from calf.providers.openai import OpenAIClient

load_dotenv()


@pytest.fixture(scope="session")
def openai_client() -> OpenAIClient:
    api_key = os.environ.get("OPENAI_API_KEY")
    model = os.environ.get("OPENAI_MODEL")
    base_url = os.environ.get("BASE_URL")
    if not api_key or not model:
        pytest.skip("Set OPENAI_API_KEY and OPENAI_MODEL to run this integration test.")
    return OpenAIClient(model=model, api_key=api_key, base_url=base_url)


@pytest.mark.asyncio
async def test_openai_generate_simple(openai_client: OpenAIClient) -> None:
    print("\n\nRunning simple sanity check...")
    messages: list[ChatCompletionMessageParam] = [
        {
            "role": "user",
            "content": "Respond with what LLM model you are and a short intro about yourself.",
        }
    ]

    result = await openai_client.generate(messages=messages)

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

    messages: list[ChatCompletionMessageParam] = [
        {
            "role": "user",
            "content": "What's the weather like in Tokyo?",
        }
    ]

    result = await openai_client.generate(messages=messages, tools=[get_weather])

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


@pytest.mark.asyncio
async def test_openai_generate_multi_turn(openai_client: OpenAIClient) -> None:
    """Test that the LLM maintains context across a multi-turn conversation."""
    # First turn: user introduces themselves
    messages: list[ChatCompletionMessageParam] = [
        {"role": "user", "content": "Hi, my name is Conan O'Brien."}
    ]

    first_response = await openai_client.generate(messages=messages)
    assert first_response.text is not None, "Expected a response message"
    print(f"\nFirst response: {first_response.text}")

    # Build up chat history with the assistant's response
    messages.append({"role": "assistant", "content": first_response.text})

    # Second turn: ask if they remember the name
    messages.append(
        {
            "role": "user",
            "content": "What is my name? Reply with just my name and nothing else.",
        }
    )

    second_response = await openai_client.generate(messages=messages)
    assert second_response.text is not None, "Expected a response message"
    print(f"\n\nSecond response: {second_response.text}")

    # The LLM should recall the name from the conversation history
    assert "conan" in second_response.text.lower(), (
        f"Expected 'Bartholomew' in response but got: {second_response.text}"
    )
