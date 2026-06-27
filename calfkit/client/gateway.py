"""The caller-surface agent gateway (spec §2.2): the ``Dispatch`` fire token, the typed
``AgentGateway``, and the verb triad ``send`` / ``start`` / ``execute``.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic

from calfkit._types import OutputT
from calfkit._vendor.pydantic_ai.messages import ModelMessage
from calfkit._vendor.pydantic_ai.settings import ModelSettings
from calfkit.client.hub import InvocationHandle, _RunChannel
from calfkit.models.node_result import InvocationResult
from calfkit.models.tool_dispatch import ToolBinding, ToolProvider

if TYPE_CHECKING:
    from calfkit.client.caller import Client


@dataclass(frozen=True)
class Dispatch:
    """What ``send()`` returns: a fire token carrying only the ``correlation_id``.

    Deliberately **not** an :class:`~calfkit.client.hub.InvocationHandle` — the type itself says
    the result is not retrievable by id. Observe a ``send()`` result on the firehose
    (``client.events()``), or use ``start()`` / ``execute()`` to get a per-run result.
    """

    correlation_id: str


@dataclass(frozen=True)
class AgentGateway(Generic[OutputT]):
    """A typed gateway to **one** destination (spec §2.2), minted by ``client.agent(name|topic=)``.

    Speaks the verb triad ``send`` / ``start`` / ``execute`` — all sharing the mint-bound ``_topic``
    (the agent's Private input topic, or the ``topic=`` escape hatch) and ``_output_type`` (the
    per-deployment type, default ``str``). Per-call conversational knobs ride each verb.
    """

    _client: Client
    _topic: str
    _output_type: type[OutputT]

    async def send(
        self,
        prompt: str,
        *,
        correlation_id: str | None = None,
        message_history: list[ModelMessage] | None = None,
        deps: dict[str, Any] | None = None,
        model_settings: ModelSettings | None = None,
        temp_instructions: str | None = None,
        tool_overrides: Sequence[ToolBinding | ToolProvider] | None = None,
        author: str | None = None,
    ) -> Dispatch:
        """Dispatch without awaiting (spec §2.2) — resolves when durably accepted; returns a
        :class:`Dispatch` (a fire token, **not** a handle: no ``result()``/``stream()``). Its terminal
        routes to the client's inbox (observe via ``events()``), but ``send()`` registers no per-run
        handle."""
        client = self._client
        cid, state, overrides = client._build_state_and_overrides(
            prompt,
            correlation_id=correlation_id,
            temp_instructions=temp_instructions,
            message_history=message_history,
            tool_overrides=tool_overrides,
            model_settings=model_settings,
            author=author,
        )
        await client._ensure_started()
        await client._publish_call(topic=self._topic, correlation_id=cid, state=state, overrides=overrides, deps=client._merge_deps(deps))
        return Dispatch(correlation_id=cid)

    async def start(
        self,
        prompt: str,
        *,
        correlation_id: str | None = None,
        message_history: list[ModelMessage] | None = None,
        deps: dict[str, Any] | None = None,
        model_settings: ModelSettings | None = None,
        temp_instructions: str | None = None,
        tool_overrides: Sequence[ToolBinding | ToolProvider] | None = None,
        author: str | None = None,
    ) -> InvocationHandle[OutputT]:
        """Dispatch and return a per-run handle (spec §2.2/§5.2). The handle is the **only** way to get
        this run's ``result()``/``stream()`` — hold it for the run's lifetime; there is no
        reattach-by-correlation-id."""
        client = self._client
        cid, state, overrides = client._build_state_and_overrides(
            prompt,
            correlation_id=correlation_id,
            temp_instructions=temp_instructions,
            message_history=message_history,
            tool_overrides=tool_overrides,
            model_settings=model_settings,
            author=author,
        )
        # Race-free ordering (§5.2): create the handle + channel and register the weak cid→handle in a
        # single synchronous step BEFORE any await, so the reply can never race an unregistered handle.
        handle: InvocationHandle[OutputT] = InvocationHandle(correlation_id=cid, _channel=_RunChannel(), _output_type=self._output_type)
        client._hub.track(handle)
        await client._ensure_started()
        await client._publish_call(topic=self._topic, correlation_id=cid, state=state, overrides=overrides, deps=client._merge_deps(deps))
        return handle

    async def execute(
        self,
        prompt: str,
        *,
        timeout: float | None = None,
        correlation_id: str | None = None,
        message_history: list[ModelMessage] | None = None,
        deps: dict[str, Any] | None = None,
        model_settings: ModelSettings | None = None,
        temp_instructions: str | None = None,
        tool_overrides: Sequence[ToolBinding | ToolProvider] | None = None,
        author: str | None = None,
    ) -> InvocationResult[OutputT]:
        """``start`` + ``result`` — the request/response convenience (spec §2.2). ``timeout`` is
        client-side patience with no default (a durable run may legitimately pause; §4.3)."""
        handle = await self.start(
            prompt,
            correlation_id=correlation_id,
            message_history=message_history,
            deps=deps,
            model_settings=model_settings,
            temp_instructions=temp_instructions,
            tool_overrides=tool_overrides,
            author=author,
        )
        return await handle.result(timeout=timeout)
