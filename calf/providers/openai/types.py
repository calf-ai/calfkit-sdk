import json
from typing import Literal, NotRequired, TypedDict

from openai.types.chat import (
    ChatCompletion,
)
from openai.types.chat.chat_completion_message_tool_call import ChatCompletionMessageToolCall

from calf.providers.base import GenerateResult, ToolCall


class CreateKeywordArgs(TypedDict):
    reasoning_effort: NotRequired[Literal["none", "minimal", "low", "medium", "high", "xhigh"]]


class OpenAIToolCall(ToolCall):
    """OpenAI-specific tool call."""

    def __init__(self, tool_call: ChatCompletionMessageToolCall):
        super().__init__(
            id=tool_call.id,
            name=tool_call.function.name,
            args=(),  # OpenAI uses kwargs only
            kwargs=json.loads(tool_call.function.arguments),
        )


class OpenAIClientResponse(GenerateResult):
    """OpenAI-specific response wrapping ChatCompletion."""

    # TODO: define an interface for the client response for users to extract the client-compliant assistant message-typed objects
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
        self.message = message
        super().__init__(
            text=message.content,
            tool_calls=tool_calls,
        )
