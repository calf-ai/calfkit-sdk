"""Integration (kafka lane): the AgentCard plane end-to-end on a real broker.

Every agent advertises an :class:`AgentCard` on the compacted ``calf.agents`` topic; a
``ControlPlaneView[AgentCard]`` reads it back collapsed. Mirrors
``test_controlplane_substrate_kafka.py``, but with a REAL ``Agent`` + ``AgentCard`` +
the real ``calf.agents`` topic — isolated per test by a unique agent name keyed on
``topic_namespace`` (the same shared-topic isolation ``test_tool_discovery_kafka.py``
uses, since the agent's name is the wire key on the shared topic).

Covers: the advertise→read round-trip + clean-shutdown tombstone, the 2-replica collapse
(one card per name; a survivor keeps the node live), and the worker's own auto-wired
publisher + writer creating a **compacted** ``calf.agents`` and round-tripping a card.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from types import SimpleNamespace

import pytest
from ktables import GroupedKafkaTableWriter

from calfkit.client.client import Client
from calfkit.controlplane import ControlPlaneView
from calfkit.controlplane.advert import AdvertInfo
from calfkit.controlplane.publisher import ControlPlanePublisher, control_plane_writer_key
from calfkit.models.agents import AGENTS_TOPIC, AgentCard
from calfkit.nodes.agent import Agent
from calfkit.providers.pydantic_ai.model_client import PydanticModelClient
from calfkit.provisioning import ProvisioningConfig
from calfkit.worker.worker import Worker
from tests.integration._kafka_helpers import fast_control_plane

pytestmark = pytest.mark.kafka


class _FakeModel(PydanticModelClient):
    @property
    def model_name(self) -> str:
        return "fake"

    @property
    def system(self) -> str:
        return "fake"

    async def request(self, *args: object, **kwargs: object) -> object:
        raise NotImplementedError


def _agent(name: str, *, description: str | None) -> Agent:
    return Agent(name, subscribe_topics=f"{name}.in", model_client=_FakeModel(), description=description)


def _only_advert(node: Agent) -> AdvertInfo:
    return next(iter(type(node)._adverts.values()))


async def _poll(refresh: Callable[[], Awaitable[object]], predicate: Callable[[], bool], *, tries: int = 300, delay: float = 0.02) -> bool:
    for _ in range(tries):
        await refresh()
        if predicate():
            return True
        await asyncio.sleep(delay)
    return predicate()


async def _start_publisher(
    bootstrap: str, node: Agent, worker_id: str, *, ensure_topic: bool
) -> tuple[GroupedKafkaTableWriter[AgentCard], ControlPlanePublisher, SimpleNamespace]:
    writer: GroupedKafkaTableWriter[AgentCard] = GroupedKafkaTableWriter.json(
        bootstrap_servers=bootstrap, topic=AGENTS_TOPIC, ensure_topic=ensure_topic
    )
    await writer.start()
    pub = ControlPlanePublisher(worker_id=worker_id, adverts=[(node, _only_advert(node))], config=fast_control_plane(bootstrap))
    ctx = SimpleNamespace(resources={control_plane_writer_key(AGENTS_TOPIC): writer})
    await pub.start(ctx)
    return writer, pub, ctx


def _open_view(bootstrap: str) -> ControlPlaneView[AgentCard]:
    return ControlPlaneView.open(bootstrap_servers=bootstrap, topic=AGENTS_TOPIC, record_type=AgentCard, ensure_topic=False)


async def test_agent_advertises_and_view_roundtrip_and_clean_tombstone(kafka_bootstrap: str, topic_namespace: str) -> None:
    name = f"{topic_namespace}-planner"
    agent = _agent(name, description="Plans the work")
    writer, pub, ctx = await _start_publisher(kafka_bootstrap, agent, "w1", ensure_topic=True)
    view = _open_view(kafka_bootstrap)
    await view.start()
    try:
        # round-trip: the advertised agent shows up, collapsed, with its card content
        assert await _poll(view.barrier, lambda: view.get(name) is not None)
        card = view.get(name)
        assert card is not None
        assert card.description == "Plans the work"
        assert card.node_kind == "agent"
        assert name in view.online_nodes()

        # clean shutdown tombstones the instance; the view drops the agent
        await pub.stop(ctx)
        assert await _poll(view.barrier, lambda: view.get(name) is None)
        assert name not in view.online_nodes()
    finally:
        if pub._task is not None:
            await pub.stop(ctx)
        await view.stop()
        await writer.stop()


async def test_two_replicas_collapse_to_one_card(kafka_bootstrap: str, topic_namespace: str) -> None:
    name = f"{topic_namespace}-replicated"
    writer_a, pub_a, ctx_a = await _start_publisher(kafka_bootstrap, _agent(name, description="r"), "w-a", ensure_topic=True)
    writer_b, pub_b, ctx_b = await _start_publisher(kafka_bootstrap, _agent(name, description="r"), "w-b", ensure_topic=False)
    view = _open_view(kafka_bootstrap)
    await view.start()
    try:
        # both replicas present under the one name, collapsed to exactly one card
        assert await _poll(view.barrier, lambda: view.get(name) is not None)
        assert view.get(name) is not None
        assert sorted(view.online_nodes()).count(name) == 1

        # one replica leaves cleanly: the agent is STILL online via the survivor
        await pub_a.stop(ctx_a)
        await view.barrier()
        assert view.get(name) is not None

        # when the second replica leaves too, the agent finally goes offline
        await pub_b.stop(ctx_b)
        assert await _poll(view.barrier, lambda: view.get(name) is None)
    finally:
        for pub, ctx in ((pub_a, ctx_a), (pub_b, ctx_b)):
            if pub._task is not None:
                await pub.stop(ctx)
        await view.stop()
        await writer_a.stop()
        await writer_b.stop()


async def test_worker_wiring_creates_topic_and_roundtrips(kafka_bootstrap: str, topic_namespace: str) -> None:
    """The worker's OWN auto-wired publisher + writer create ``calf.agents`` and round-trip a
    card — no hand-built publisher (mirrors the substrate wiring test).

    Topic config (``cleanup.policy=compact``) is **entirely ktables' create-path concern**
    (``DEFAULT_TOPIC_CONFIGS``), deferred to ktables and provisioned like ``calf.capabilities``;
    calfkit owns none of it, so this test asserts the wiring + round-trip, not the policy (which
    can't be asserted robustly on the shared topic — ktables won't reconfigure an existing one)."""
    name = f"{topic_namespace}-wired"
    agent = _agent(name, description="wired")
    client = Client.connect(kafka_bootstrap, provisioning=ProvisioningConfig(enabled=True))
    worker = Worker(client, nodes=[agent], control_plane=fast_control_plane(kafka_bootstrap))
    worker._maybe_register_control_plane()

    key = control_plane_writer_key(AGENTS_TOPIC)
    [(_, genfn)] = [(n, g) for n, g in worker._resource_cms() if n == key]  # the worker-registered writer resource
    writer_gen = genfn(None)  # type: ignore[arg-type]
    writer = await anext(writer_gen)  # ensure_topic=True via provisioning creates the compacted topic
    pub = worker._control_plane_publisher
    assert pub is not None
    ctx = SimpleNamespace(resources={key: writer})
    view = _open_view(kafka_bootstrap)
    await view.start()
    try:
        await pub.start(ctx)  # looks up the writer under the key the worker registered
        assert await _poll(view.barrier, lambda: view.get(name) is not None)
        card = view.get(name)
        assert card is not None and card.description == "wired"

        await pub.stop(ctx)
        assert await _poll(view.barrier, lambda: view.get(name) is None)
    finally:
        if pub._task is not None:
            await pub.stop(ctx)
        await view.stop()
        with pytest.raises(StopAsyncIteration):
            await anext(writer_gen)
