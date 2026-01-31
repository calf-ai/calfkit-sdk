from typing import Any

from httpx import Timeout
from pydantic_ai.models.openai import OpenAIChatModel, OpenAIChatModelSettings
from pydantic_ai.providers.openai import OpenAIProvider


class OpenAIModelClient(OpenAIChatModel):
    def __init__(
        self,
        model_name: str,
        *,
        base_url: str | None = None,
        api_key: str | None = None,
        reasoning_effort: str | None = None,
        max_tokens: int | None = None,
        temperature: float | None = None,
        top_p: float | None = None,
        timeout: float | Timeout | None = None,
        parallel_tool_calls: bool | None = None,
        seed: int | None = None,
        presence_penalty: float | None = None,
        frequency_penalty: float | None = None,
        logit_bias: dict[str, int] | None = None,
        stop_sequences: list[str] | None = None,
        extra_headers: dict[str, str] | None = None,
        extra_body: object | None = None,
        **kwargs: Any,
    ):
        settings_kwargs: dict[str, object] = {}
        if reasoning_effort is not None:
            settings_kwargs["openai_reasoning_effort"] = reasoning_effort
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
        if seed is not None:
            settings_kwargs["seed"] = seed
        if presence_penalty is not None:
            settings_kwargs["presence_penalty"] = presence_penalty
        if frequency_penalty is not None:
            settings_kwargs["frequency_penalty"] = frequency_penalty
        if logit_bias is not None:
            settings_kwargs["logit_bias"] = logit_bias
        if stop_sequences is not None:
            settings_kwargs["stop_sequences"] = stop_sequences
        if extra_headers is not None:
            settings_kwargs["extra_headers"] = extra_headers
        if extra_body is not None:
            settings_kwargs["extra_body"] = extra_body
        model_settings: OpenAIChatModelSettings = OpenAIChatModelSettings(**settings_kwargs)  # type: ignore[typeddict-item]

        openai_client = OpenAIProvider(base_url=base_url, api_key=api_key)
        self.model_settings = model_settings
        super().__init__(model_name, provider=openai_client, settings=model_settings)
