"""Unit tests for the optional human ``author`` kwarg on the client (design §8).

These tests are pure: they intercept the network boundary (``_invoke`` /
``invoke_node``) so no Kafka broker or model call is required. They assert that
``author=`` is mapped onto the staged ``UserPromptPart.name`` by ``invoke_node``
and that ``execute_node`` forwards it (it must not be silently dropped).
"""

from __future__ import annotations

from typing import Any

import pytest

from calfkit._vendor.pydantic_ai.messages import ModelRequest, UserPromptPart
from calfkit.client.client import Client
from calfkit.client.invocation_handle import InvocationHandle
from calfkit.models import State
from calfkit.models.node_result import _UNSET


class _CaptureClient(Client):
    """Client subclass that captures the constructed ``State`` instead of
    publishing it, so ``invoke_node`` can be exercised without a broker."""

    def __init__(self) -> None:  # noqa: D107 - test double, skip BaseClient wiring
        self.captured_state: State | None = None
        self._reply_topic = "test-reply"

    async def _invoke(self, *, topic: str, reply_topic: str, correlation_id: str, state: State, **_: Any) -> InvocationHandle:  # type: ignore[override]
        self.captured_state = state
        return InvocationHandle(
            correlation_id=correlation_id,
            topic=topic,
            reply_topic=reply_topic,
            _future=None,  # type: ignore[arg-type]
            _output_type=_UNSET,
        )


def _staged_user_part(state: State) -> UserPromptPart:
    msg = state.uncommitted_message
    assert isinstance(msg, ModelRequest)
    part = msg.parts[0]
    assert isinstance(part, UserPromptPart)
    return part


@pytest.mark.asyncio
async def test_invoke_node_author_stamps_user_prompt_name() -> None:
    client = _CaptureClient()

    await client.invoke_node("hello", "topic", author="Alice")

    assert client.captured_state is not None
    assert _staged_user_part(client.captured_state).name == "Alice"


@pytest.mark.asyncio
async def test_invoke_node_no_author_leaves_name_none() -> None:
    client = _CaptureClient()

    await client.invoke_node("hello", "topic")

    assert client.captured_state is not None
    assert _staged_user_part(client.captured_state).name is None


@pytest.mark.asyncio
async def test_execute_node_forwards_author_to_invoke_node(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _CaptureClient()
    captured_kwargs: dict[str, Any] = {}

    real_invoke_node = client.invoke_node

    async def spy_invoke_node(user_prompt: str, topic: str, **kwargs: Any) -> InvocationHandle:
        captured_kwargs.update(kwargs)
        return await real_invoke_node(user_prompt, topic, **kwargs)

    monkeypatch.setattr(client, "invoke_node", spy_invoke_node)

    # InvocationHandle.result() would await a real future; monkeypatch it out (it is
    # restored automatically after the test) so we assert purely on what execute_node
    # forwarded down to invoke_node.
    async def _no_result(self: InvocationHandle, *_: Any, **__: Any) -> None:
        return None

    monkeypatch.setattr(InvocationHandle, "result", _no_result)

    await client.execute_node("hello", "topic", author="Bob")

    assert captured_kwargs.get("author") == "Bob", "execute_node dropped author= before forwarding to invoke_node"
    assert client.captured_state is not None
    assert _staged_user_part(client.captured_state).name == "Bob"
