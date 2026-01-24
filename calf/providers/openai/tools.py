"""Utilities for converting Python functions to OpenAI tool schemas."""

from collections.abc import Callable
from typing import Any

from agents.function_schema import function_schema
from openai.types.chat import ChatCompletionToolParam


def func_to_tool(func: Callable[..., Any]) -> ChatCompletionToolParam:
    """Convert a Python function to an OpenAI ChatCompletionToolParam.

    Uses openai-agents' function_schema to extract the schema from the function's
    type hints and docstring.

    Args:
        func: A Python function with type hints and optionally a docstring.

    Returns:
        A ChatCompletionToolParam dict suitable for passing to OpenAI's API.

    Example:
        def get_weather(location: str, unit: str = "celsius") -> str:
            '''Get the current weather in a location.

            Args:
                location: The city and state, e.g. San Francisco, CA
                unit: Temperature unit (celsius or fahrenheit)
            '''
            return call_weather_api(location, unit)

        tool = func_to_tool(get_weather)
        # tool is now a ChatCompletionToolParam dict
    """
    schema = function_schema(func)

    return {
        "type": "function",
        "function": {
            "name": schema.name,
            "description": schema.description or "",
            "parameters": schema.params_json_schema,
        },
    }
