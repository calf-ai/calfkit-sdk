"""Every agent advertises an :class:`AgentCard` on the ``calf.agents`` control plane.

Like the function tool node (``test_tool_node_advert.py``), an agent is a content
contributor: it declares one ``@advertises`` factory that the worker-owned
``ControlPlanePublisher`` pulls each heartbeat tick. Its content is static — the
optional ``Agent(description=…)`` blurb — so the factory reads ``self._description``
directly (no session, no cache, nothing that can fail at publish time). Advertising is
**always-on, no opt-out** (spec §7 / L7). The heartbeat loop + tombstone live in the
substrate's publisher (tested in ``test_controlplane_publisher.py``), not here.
"""

from __future__ import annotations

from datetime import datetime, timezone

from calfkit.controlplane import ControlPlaneStamp
from calfkit.models.agents import AGENTS_TOPIC, AgentCard
from calfkit.nodes.agent import Agent
from calfkit.providers.pydantic_ai.model_client import PydanticModelClient


class _FakeModel(PydanticModelClient):
    @property
    def model_name(self) -> str:
        return "fake"

    @property
    def system(self) -> str:
        return "fake"

    async def request(self, *args: object, **kwargs: object) -> object:
        raise NotImplementedError


def make_agent(name: str = "planner", description: str | None = "A helpful planner") -> Agent:
    return Agent(name, subscribe_topics=f"{name}.in", model_client=_FakeModel(), description=description)


def make_stamp(*, node_kind: str = "agent") -> ControlPlaneStamp:
    now = datetime.now(tz=timezone.utc)
    return ControlPlaneStamp(started_at=now, last_heartbeat_at=now, heartbeat_interval=30.0, node_kind=node_kind)


class TestAdvertDeclaration:
    def test_declares_one_agent_card_advert(self) -> None:
        adverts = type(make_agent())._adverts
        assert AGENTS_TOPIC in adverts
        assert adverts[AGENTS_TOPIC].record is AgentCard

    def test_advert_factory_is_a_bound_method(self) -> None:
        factories = make_agent().control_plane_adverts()
        assert AGENTS_TOPIC in factories
        assert callable(factories[AGENTS_TOPIC])


class TestAgentCardFactory:
    def test_factory_builds_a_card_from_description_and_stamp(self) -> None:
        agent = make_agent("planner", description="Plans things")
        stamp = make_stamp()
        card = agent._agent_card_advert(stamp)
        assert isinstance(card, AgentCard)
        # the worker-stamped fields ride through verbatim
        assert card.started_at == stamp.started_at
        assert card.last_heartbeat_at == stamp.last_heartbeat_at
        assert card.heartbeat_interval == 30.0
        assert card.node_kind == "agent"
        # content: the directory blurb
        assert card.description == "Plans things"

    def test_factory_defaults_description_to_none(self) -> None:
        agent = make_agent("bare", description=None)
        assert agent._agent_card_advert(make_stamp()).description is None

    def test_node_kind_rides_the_stamp(self) -> None:
        # The factory never sets node_kind; it rides on the worker stamp ("agent").
        assert make_agent()._agent_card_advert(make_stamp(node_kind="agent")).node_kind == "agent"


class TestDescriptionCtorParam:
    def test_description_is_stored(self) -> None:
        assert make_agent("planner", description="Plans things")._description == "Plans things"

    def test_description_defaults_to_none(self) -> None:
        agent = Agent("noblurb", subscribe_topics="noblurb.in", model_client=_FakeModel())
        assert agent._description is None


class _FakeWriter:
    def __init__(self) -> None:
        self.sets: list[tuple[str, str, AgentCard]] = []

    async def set(self, group: str, member: str, value: AgentCard) -> None:
        self.sets.append((group, member, value))

    async def delete(self, group: str, member: str) -> None:
        return None


class _FakeCtx:
    def __init__(self, resources: dict[str, _FakeWriter]) -> None:
        self.resources = resources


class TestPublisherDrivenCard:
    """The full production path: the worker-owned ControlPlanePublisher builds a bare
    ControlPlaneStamp (node_kind from _node_kind="agent") and splats it into
    AgentCard(**stamp.model_dump(), description=...). Exercised offline here (the kafka
    lane covers the real wire) so a future stamp field that collides with the description
    splat — the duplicate-kwarg TypeError the type checker can't see (advert.py) — fails
    a unit test, not only a broker test."""

    async def test_publisher_publishes_a_well_formed_agent_card(self) -> None:
        from calfkit.controlplane import ControlPlaneConfig
        from calfkit.controlplane.publisher import ControlPlanePublisher, control_plane_writer_key

        agent = make_agent("planner", description="Plans things")
        writer = _FakeWriter()
        pub = ControlPlanePublisher(
            worker_id="wkr",
            adverts=[(agent, type(agent)._adverts[AGENTS_TOPIC])],
            config=ControlPlaneConfig(heartbeat_interval=3600.0),
        )
        ctx = _FakeCtx({control_plane_writer_key(AGENTS_TOPIC): writer})
        await pub.start(ctx)  # fail-loud first publish
        try:
            assert len(writer.sets) == 1
            group, member, record = writer.sets[0]
            assert (group, member) == ("planner", "wkr")  # name × worker is the wire key
            assert isinstance(record, AgentCard)
            assert record.description == "Plans things"
            assert record.node_kind == "agent"
        finally:
            await pub.stop(ctx)
