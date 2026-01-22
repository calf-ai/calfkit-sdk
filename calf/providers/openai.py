"""OpenAI-compatible model client."""

from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, cast

from openai import AsyncOpenAI
from openai.types.chat import ChatCompletion, ChatCompletionMessageFunctionToolCall
from openai._types import Omit, omit
from calf.providers.base import ToolCall, GenerateResponse, Message, ModelClient, Tool
from openai.types.chat import ChatCompletionMessageParam, ChatCompletionToolUnionParam
from openai.types.chat.chat_completion_message import ChatCompletionMessage

class OpenAIClient(ModelClient):
    """OpenAI-compatible model client.

    Supports OpenAI API and compatible providers (Azure, Ollama, vLLM, etc.).
    """

    def __init__(
        self,
        *,
        model: str,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        create_kwargs: Dict[str, Any] = {},
        **client_kwargs
    ) -> None:
        """Initialize the OpenAI client.

        Args:
            api_key: OpenAI API key. If None, reads from OPENAI_API_KEY env var.
            base_url: Base URL for the API. Use for compatible providers.
            model: Model name to use for completions.
        """
        self.model = model
        self.api_key = api_key
        self.base_url = base_url
        self.create_kwargs = create_kwargs
        self.client = AsyncOpenAI(api_key=api_key, base_url=base_url, **client_kwargs)

    async def generate(
        self,
        messages: Sequence[Message],
        *,
        tools:  Optional[Sequence[Tool]] = None,
        **extra_create_kwargs
    ) -> GenerateResponse:
        """Execute a chat completion request against the LLM.

        Args:
            messages: List of chat messages in OpenAI format.
            tools: Optional list of tool definitions in OpenAI format.

        Returns:
            GenerateResponse containing the generated content, tool calls, and usage stats.
        """
        typed_messages = _to_chat_completion_messages(messages)

        typed_tools: Iterable[ChatCompletionToolUnionParam] | Omit
        if tools:
            openai_tools = [tool.data for tool in tools]
            typed_tools = cast(list[ChatCompletionToolUnionParam], openai_tools)
        else:
            typed_tools = omit
        
        create_kwargs = self.create_kwargs.copy()
        create_kwargs.update(extra_create_kwargs)
        create_kwargs.pop('stream') if 'stream' in create_kwargs else None
        
        response: ChatCompletion = await self.client.chat.completions.create(
            model=self.model,
            messages=typed_messages,
            tools=typed_tools,
            stream=False,
            **create_kwargs,
        )

        if not response.choices:
            raise ValueError("OpenAI response contained no choices.")

        choice = response.choices[0]
        message = choice.message

        tool_calls: list[ToolCall] | None = None
        if message.tool_calls:
            tool_calls = []
            for tool_call in message.tool_calls:
                if not isinstance(tool_call, ChatCompletionMessageFunctionToolCall):
                    continue
                tool_calls.append(
                    ToolCall(
                        id=tool_call.id,
                        name=tool_call.function.name,
                        arguments=tool_call.function.arguments,
                    )
                )

        usage_stats = None
        if response.usage:
            usage_stats = response.usage.model_dump()

        return GenerateResponse(
            message=message.content,
            tool_calls=tool_calls,
            usage=usage_stats,
        )


def _to_chat_completion_messages(
    messages: Sequence[Message],
) -> list[ChatCompletionMessageParam]:
    # for message in messages:
    #     if message.role == 'assistant':
    #         oai_message = {'content': message.content, 'role': message.role, 'name': message.name}
    #     elif message.role == 'system':
    #         oai_message = {'content': message.content, 'role': message.role, 'name': message.name}
    #     elif message.role == 'user':
            
    #     elif message.role == 'tool':
                        
    openai_messages = [message.data for message in messages]
    return cast(list[ChatCompletionMessageParam], openai_messages)
