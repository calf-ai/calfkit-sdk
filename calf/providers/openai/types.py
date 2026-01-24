import json

from openai.types.chat import ChatCompletion
from openai.types.chat.chat_completion_message_tool_call import ChatCompletionMessageToolCall

from calf.providers.base import GenerateResponse, ToolCall


class OpenAIToolCall(ToolCall):
    """OpenAI-specific tool call."""

    def __init__(self, tool_call: ChatCompletionMessageToolCall):
        super().__init__(
            id=tool_call.id,
            name=tool_call.function.name,
            args=(),  # OpenAI uses kwargs only
            kwargs=json.loads(tool_call.function.arguments),
        )


class OpenAIClientResponse(GenerateResponse):
    """OpenAI-specific response wrapping ChatCompletion."""

    def __init__(self, response: ChatCompletion):
        message = response.choices[0].message
        tool_calls = (
            [
                OpenAIToolCall(tc)
                for tc in message.tool_calls
                if isinstance(tc, ChatCompletionMessageToolCall)
            ]
            if message.tool_calls
            else None
        )
        super().__init__(
            text=message.content,
            tool_calls=tool_calls,
        )
