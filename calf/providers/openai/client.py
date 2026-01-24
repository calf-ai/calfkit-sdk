"""OpenAI-compatible model client."""

from collections.abc import Callable
from typing import Any

from openai import AsyncOpenAI, omit
from openai.types.chat import ChatCompletion, ChatCompletionMessageParam, ChatCompletionToolParam

from calf.providers.base import ModelClient
from calf.providers.openai.tools import func_to_tool
from calf.providers.openai.types import OpenAIClientResponse

# Type alias for tools that can be either a schema dict or a callable
ToolInput = ChatCompletionToolParam | Callable[..., Any]


class OpenAIClient(ModelClient[ChatCompletionMessageParam, ToolInput, OpenAIClientResponse]):
    """OpenAI-compatible model client.

    Supports OpenAI API and compatible providers (Azure, Ollama, vLLM, etc.).
    """

    def __init__(
        self,
        model: str,
        *,
        api_key: str | None = None,
        base_url: str | None = None,
        create_kwargs: dict[str, Any] = {},
        **client_kwargs,
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
        messages: list[ChatCompletionMessageParam],
        *,
        tools: list[ToolInput] | None = None,
        **extra_create_kwargs,
    ) -> OpenAIClientResponse:
        """Execute a chat completion request against the LLM.

        Args:
            messages: List of chat messages in OpenAI format.
            tools: Optional list of tool definitions (ChatCompletionToolParam dicts)
                or Python callables. Callables will be converted to tool schemas
                using their type hints and docstrings.

        Returns:
            OpenAIClientResponse wrapping the ChatCompletion.
        """
        create_kwargs = self.create_kwargs.copy()
        create_kwargs.update(extra_create_kwargs)
        create_kwargs.pop("stream", None)

        # Convert any callable tools to ChatCompletionToolParam schemas
        tool_params: list[ChatCompletionToolParam] | None = None
        if tools:
            tool_params = [func_to_tool(t) if callable(t) else t for t in tools]

        response: ChatCompletion = await self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            tools=tool_params or omit,
            stream=False,
            **create_kwargs,
        )

        if not response.choices:
            raise ValueError("OpenAI response contained no choices.")

        return OpenAIClientResponse(response)
