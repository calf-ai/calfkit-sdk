from typing import Any, Literal

from httpx import Timeout

from calfkit._vendor.pydantic_ai.models.openai import (
    OpenAIChatModel,
    OpenAIChatModelSettings,
    OpenAIResponsesModel,
    OpenAIResponsesModelSettings,
)
from calfkit._vendor.pydantic_ai.providers.openai import OpenAIProvider
from calfkit.providers.pydantic_ai.model_client import PydanticModelClient


class OpenAIModelClient(OpenAIChatModel, PydanticModelClient):
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
        model_settings: OpenAIChatModelSettings = OpenAIChatModelSettings(**settings_kwargs, **kwargs)  # type: ignore[typeddict-item]

        openai_client = OpenAIProvider(base_url=base_url, api_key=api_key)
        self.model_settings = model_settings
        super().__init__(model_name, provider=openai_client, settings=model_settings)


class OpenAIResponsesModelClient(OpenAIResponsesModel, PydanticModelClient):
    def __init__(
        self,
        model_name: str,
        *,
        base_url: str | None = None,
        api_key: str | None = None,
        reasoning_effort: str | None = None,
        reasoning_summary: Literal["detailed", "concise", "auto"] | None = None,
        max_tokens: int | None = None,
        temperature: float | None = None,
        top_p: float | None = None,
        timeout: float | Timeout | None = None,
        parallel_tool_calls: bool | None = None,
        truncation: Literal["disabled", "auto"] | None = None,
        text_verbosity: Literal["low", "medium", "high"] | None = None,
        previous_response_id: Literal["auto"] | str | None = None,
        send_reasoning_ids: bool | None = None,
        user: str | None = None,
        service_tier: Literal["auto", "default", "flex", "priority"] | None = None,
        logprobs: bool | None = None,
        top_logprobs: int | None = None,
        prompt_cache_key: str | None = None,
        prompt_cache_retention: Literal["in-memory", "24h"] | None = None,
        extra_headers: dict[str, str] | None = None,
        extra_body: object | None = None,
        **kwargs: Any,
    ):
        settings_kwargs: dict[str, object] = {}
        if reasoning_effort is not None:
            settings_kwargs["openai_reasoning_effort"] = reasoning_effort
        if reasoning_summary is not None:
            settings_kwargs["openai_reasoning_summary"] = reasoning_summary
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
        if truncation is not None:
            settings_kwargs["openai_truncation"] = truncation
        if text_verbosity is not None:
            settings_kwargs["openai_text_verbosity"] = text_verbosity
        if previous_response_id is not None:
            settings_kwargs["openai_previous_response_id"] = previous_response_id
        if send_reasoning_ids is not None:
            settings_kwargs["openai_send_reasoning_ids"] = send_reasoning_ids
        if user is not None:
            settings_kwargs["openai_user"] = user
        if service_tier is not None:
            settings_kwargs["openai_service_tier"] = service_tier
        if logprobs is not None:
            settings_kwargs["openai_logprobs"] = logprobs
        if top_logprobs is not None:
            settings_kwargs["openai_top_logprobs"] = top_logprobs
        if prompt_cache_key is not None:
            settings_kwargs["openai_prompt_cache_key"] = prompt_cache_key
        if prompt_cache_retention is not None:
            settings_kwargs["openai_prompt_cache_retention"] = prompt_cache_retention
        if extra_headers is not None:
            settings_kwargs["extra_headers"] = extra_headers
        if extra_body is not None:
            settings_kwargs["extra_body"] = extra_body
        model_settings: OpenAIResponsesModelSettings = OpenAIResponsesModelSettings(**settings_kwargs, **kwargs)  # type: ignore[typeddict-item]

        openai_client = OpenAIProvider(base_url=base_url, api_key=api_key)
        self.model_settings = model_settings
        super().__init__(model_name, provider=openai_client, settings=model_settings)
