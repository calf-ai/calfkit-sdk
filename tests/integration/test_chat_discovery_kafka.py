"""Real-broker (``kafka`` lane) discovery for ``ck chat``.

A live worker advertises an ``Agent``; the CLI's discovery primitive
(``client.mesh.get_agents()``) reads it back, and the CLI's own pure consumers —
``format_picker`` and ``_resolve_target`` — render/resolve the *real* mesh output,
proving the ``ck chat`` discovery path end to end against a live mesh.

Opt-in (``-m kafka`` / ``make test-kafka``); skips cleanly without Docker.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable

import pytest

from calfkit import MeshUnavailableError
from calfkit._vendor.pydantic_ai import models
from calfkit.cli._chat import _resolve_target
from calfkit.cli._chat_render import format_picker
from calfkit.client import Client, MeshViewConfig
from calfkit.nodes import Agent
from calfkit.providers.pydantic_ai.model_client import PydanticModelClient
from calfkit.tuning import KTableReaderTuning
from calfkit.worker import Worker
from tests.integration._kafka_helpers import fast_control_plane

pytestmark = pytest.mark.kafka
models.ALLOW_MODEL_REQUESTS = True

_EARLIEST = {"auto_offset_reset": "earliest"}
_FAST_MESH = MeshViewConfig(reader_tuning=KTableReaderTuning(poll_timeout_ms=20, fetch_max_wait_ms=10))


class FakeModel(PydanticModelClient):
    """Advertise-only model: the agent is hosted to publish its AgentCard, never to run a turn."""

    @property
    def model_name(self) -> str:
        return "fake"

    @property
    def system(self) -> str:
        return "fake"

    async def request(self, *args: object, **kwargs: object) -> object:
        raise NotImplementedError


def _eof_reader() -> Callable[[str], Awaitable[str]]:
    async def read_line(_prompt: str) -> str:
        raise EOFError  # a name-given resolution never reads a line

    return read_line


async def test_chat_discovers_and_resolves_a_live_agent(kafka_bootstrap: str, topic_namespace: str) -> None:
    agent_name = f"{topic_namespace}-helpbot"
    agent = Agent(agent_name, subscribe_topics=f"{agent_name}.in", model_client=FakeModel(), description="Helps with things")
    worker = Worker(
        Client.connect(kafka_bootstrap),
        nodes=[agent],
        control_plane=fast_control_plane(kafka_bootstrap),
        extra_subscribe_kwargs=_EARLIEST,
    )
    client = Client.connect(kafka_bootstrap, mesh_config=_FAST_MESH)
    try:
        async with worker:
            deadline = asyncio.get_event_loop().time() + 60
            agents: dict = {}
            while asyncio.get_event_loop().time() < deadline:
                with contextlib.suppress(MeshUnavailableError):  # tolerate cold-start catch-up
                    agents = dict(await client.mesh.get_agents())
                    if agent_name in agents:
                        break
                await asyncio.sleep(0.2)
            assert agent_name in agents, "the agent did not advertise within 60s"

            # the CLI's discovery consumers render/resolve the real mesh output
            assert agents[agent_name].description == "Helps with things"
            picker = format_picker(sorted(agents), agents)
            assert any(agent_name in line and "Helps with things" in line for line in picker)
            assert await _resolve_target(agent_name, agents, _eof_reader()) == agent_name
    finally:
        await client.aclose()
        await worker._client.aclose()
