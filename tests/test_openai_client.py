"""Integration tests for OpenAIClient provider."""

import json
import os

from dotenv import load_dotenv
import pytest

from calf.providers.base import Message
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
    messages = [Message(data = {'role': "user", 'content': "Respond with what LLM model you are and nothing else."})]

    result = await openai_client.generate(messages=messages)

    assert isinstance(result.message, str)
    assert result.message
    print(f"Response: {result.message}")

@pytest.mark.asyncio
async def test_openai_generate_tool_call(openai_client: OpenAIClient) -> None:
    """Test that the LLM correctly generates a tool call when prompted."""
    from calf.providers.base import Tool

    # Define a simple weather tool in OpenAI format
    weather_tool = Tool(
        data={
            "type": "function",
            "function": {
                "name": "get_weather",
                "description": "Get the current weather in a given location",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "location": {
                            "type": "string",
                            "description": "The city and state, e.g. San Francisco, CA",
                        }
                    },
                    "required": ["location"],
                },
            },
        }
    )

    messages = [
        Message(
            data={
                "role": "user",
                "content": "What's the weather like in Tokyo?",
            }
        )
    ]

    result = await openai_client.generate(messages=messages, tools=[weather_tool])

    # The LLM should respond with a tool call
    assert result.tool_calls is not None, "Expected tool_calls but got None"
    assert len(result.tool_calls) > 0, "Expected at least one tool call"

    tool_call = result.tool_calls[0]
    assert tool_call.name == "get_weather", f"Expected 'get_weather' but got '{tool_call.name}'"
    assert tool_call.id, "Tool call should have an id"

    # Parse arguments JSON string into a dict
    arguments = json.loads(tool_call.arguments)
    assert isinstance(arguments, dict), f"Expected dict but got {type(arguments)}"
    assert "location" in arguments, f"Expected 'location' key in arguments: {arguments}"
    assert "tokyo" in arguments["location"].lower(), (
        f"Expected 'Tokyo' in location but got: {arguments['location']}"
    )
    print(f"\nTool call: {tool_call.name}({arguments})")


@pytest.mark.asyncio
async def test_openai_generate_multi_turn(openai_client: OpenAIClient) -> None:
    """Test that the LLM maintains context across a multi-turn conversation."""
    # First turn: user introduces themselves
    messages = [
        Message(data={"role": "user", "content": "Hi, my name is Bartholomew."})
    ]

    first_response = await openai_client.generate(messages=messages)
    assert first_response.message is not None, "Expected a response message"
    print(f"\nFirst response: {first_response.message}")

    # Build up chat history with the assistant's response
    messages.append(
        Message(data={"role": "assistant", "content": first_response.message})
    )

    # Second turn: ask if they remember the name
    messages.append(
        Message(data={"role": "user", "content": "What is my name? Reply with just my name and nothing else."})
    )

    second_response = await openai_client.generate(messages=messages)
    assert second_response.message is not None, "Expected a response message"
    print(f"\n\nSecond response: {second_response.message}")

    # The LLM should recall the name from the conversation history
    assert "bartholomew" in second_response.message.lower(), (
        f"Expected 'Bartholomew' in response but got: {second_response.message}"
    )
