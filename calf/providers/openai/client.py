"""OpenAI-compatible model client."""

from collections.abc import Awaitable, Callable
from typing import Any, cast

from openai import AsyncOpenAI, omit
from openai.types.chat import (
    ChatCompletion,
    ChatCompletionMessageParam,
    ChatCompletionToolMessageParam,
    ChatCompletionToolParam,
    ChatCompletionUserMessageParam,
)

from calf.providers.base import ProviderClient
from calf.providers.openai.adaptor import OpenAIClientMessage, OpenAIClientTool
from calf.providers.openai.types import CreateKeywordArgs, OpenAIClientResponse

# Type alias for tools that can be either a schema dict or a callable


class OpenAIClient(
    ProviderClient[ChatCompletionMessageParam, ChatCompletionToolParam, OpenAIClientResponse],
    OpenAIClientMessage,
    OpenAIClientTool,
):
    """OpenAI-compatible model client.

    Supports OpenAI API and compatible providers (Azure, Ollama, vLLM, etc.).
    """

    def __init__(
        self,
        model: str,
        *,
        api_key: str | Callable[[], Awaitable[str]] | None = None,
        base_url: str | None = None,
        create_kwargs: CreateKeywordArgs = {},
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
        super().__init__()

    async def generate(
        self,
        messages: list[ChatCompletionMessageParam],
        *,
        tools: list[ChatCompletionToolParam] | None = None,
        **extra_create_kwargs: CreateKeywordArgs,
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
        create_kwargs.update(cast(CreateKeywordArgs, extra_create_kwargs))
        create_kwargs.pop("stream", None)

        # Convert any callable tools to ChatCompletionToolParam schemas

        response: ChatCompletion = await self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            tools=tools or omit,
            stream=False,
            **create_kwargs,
        )

        if not response.choices:
            raise ValueError("OpenAI response contained no choices.")

        return OpenAIClientResponse(response)
