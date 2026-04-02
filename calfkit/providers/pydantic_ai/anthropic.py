from typing import Any, Literal

from httpx import Timeout

from calfkit._vendor.pydantic_ai.models.anthropic import AnthropicModel, AnthropicModelSettings
from calfkit._vendor.pydantic_ai.providers.anthropic import AnthropicProvider
from calfkit.providers.pydantic_ai.model_client import PydanticModelClient


class AnthropicModelClient(AnthropicModel, PydanticModelClient):
    def __init__(
        self,
        model_name: str,
        *,
        base_url: str | None = None,
        api_key: str | None = None,
        max_tokens: int | None = None,
        temperature: float | None = None,
        top_p: float | None = None,
        timeout: float | Timeout | None = None,
        parallel_tool_calls: bool | None = None,
        anthropic_thinking: dict[str, Any] | None = None,
        anthropic_cache_tool_definitions: bool | Literal["5m", "1h"] | None = None,
        anthropic_cache_instructions: bool | Literal["5m", "1h"] | None = None,
        anthropic_cache_messages: bool | Literal["5m", "1h"] | None = None,
        **kwargs: Any,
    ):
        settings_kwargs: dict[str, object] = {}
        if max_tokens is not None:
            settings_kwargs["max_tokens"] = max_tokens
        if temperature is not None:
            settings_kwargs["temperature"] = temperature
        if top_p is not None:
            settings_kwargs["top_p"] = top_p
        if timeout is not None:
            settings_kwargs["timeout"] = timeout
        if parallel_tool_calls is not None:
            settings_kwargs["parallel_tool_calls"] = parallel_tool_calls
        if anthropic_thinking is not None:
            settings_kwargs["anthropic_thinking"] = anthropic_thinking
        if anthropic_cache_tool_definitions is not None:
            settings_kwargs["anthropic_cache_tool_definitions"] = anthropic_cache_tool_definitions
        if anthropic_cache_instructions is not None:
            settings_kwargs["anthropic_cache_instructions"] = anthropic_cache_instructions
        if anthropic_cache_messages is not None:
            settings_kwargs["anthropic_cache_messages"] = anthropic_cache_messages
        model_settings: AnthropicModelSettings = AnthropicModelSettings(**settings_kwargs, **kwargs)  # type: ignore[typeddict-item]

        anthropic_client = AnthropicProvider(api_key=api_key, base_url=base_url)
        self.model_settings = model_settings
        super().__init__(model_name, provider=anthropic_client, settings=model_settings)
