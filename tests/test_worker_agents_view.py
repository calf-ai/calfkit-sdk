"""The worker auto-wires the AgentCard write side (every agent) and the read-side
Agents View (gated, dormant until ``peers=`` lands in PR-B).

Write side: hosting any agent trips the generic ``@advertises`` auto-wiring — the
``calf.agents`` writer + the ``ControlPlanePublisher`` — with zero user config (mirrors
``test_worker_capability_view.py::TestControlPlaneWriterRegistration``). Read side: a
distinct ``ControlPlaneView[AgentCard]`` resource under ``AGENTS_VIEW_RESOURCE_KEY``,
registered iff a hosted node carries a ``_peers`` handle — absent in PR-A, so the gate is
a no-op here and exercised via a stub (mirrors ``TestCapabilityViewRegistration``). The
collapse/staleness/schema filtering is the substrate's (``test_controlplane_view.py``);
the live path is the kafka lane.
"""

from __future__ import annotations

import pytest

from calfkit.client import Client
from calfkit.controlplane import ControlPlaneConfig
from calfkit.controlplane.publisher import control_plane_writer_key
from calfkit.mcp.mcp_toolbox import MCPToolboxNode
from calfkit.mcp.mcp_transport import StreamableHttpParameters
from calfkit.models.agents import AGENTS_TOPIC, AGENTS_VIEW_RESOURCE_KEY, AgentCard
from calfkit.models.capability import CAPABILITY_TOPIC
from calfkit.providers.pydantic_ai.model_client import PydanticModelClient
from calfkit.worker.worker import Worker


class _FakeModel(PydanticModelClient):
    @property
    def model_name(self) -> str:
        return "fake"

    @property
    def system(self) -> str:
        return "fake"

    async def request(self, *args: object, **kwargs: object) -> object:
        raise NotImplementedError


def make_agent(name: str = "planner", *tools: object):
    from calfkit.nodes.agent import Agent

    return Agent(name, subscribe_topics=f"{name}.in", model_client=_FakeModel(), tools=list(tools))


def make_toolbox() -> MCPToolboxNode:
    return MCPToolboxNode("docs_server", connection_params=StreamableHttpParameters(url="http://unused.local/mcp"))


def worker_resource_names(worker: Worker) -> list[str]:
    return [name for name, _ in worker._resource_cms()]


# ---------------------------------------------------------------------------
# Write side: every agent advertises on calf.agents (R8)
# ---------------------------------------------------------------------------


class TestAgentCardWriterRegistration:
    def test_hosting_an_agent_registers_the_agents_writer_and_publisher(self) -> None:
        worker = Worker(Client.connect("kafka:9092"), nodes=[make_agent()])
        worker._maybe_register_control_plane()
        assert control_plane_writer_key(AGENTS_TOPIC) in worker_resource_names(worker)
        publisher = worker._control_plane_publisher
        assert publisher is not None
        assert AGENTS_TOPIC in [info.topic for _, info in publisher._adverts]

    def test_agent_only_worker_registers_no_capability_writer(self) -> None:
        # A plain agent advertises on calf.agents, NOT calf.capabilities (only
        # tool/toolbox nodes advertise capabilities).
        worker = Worker(Client.connect("kafka:9092"), nodes=[make_agent()])
        worker._maybe_register_control_plane()
        names = worker_resource_names(worker)
        assert control_plane_writer_key(AGENTS_TOPIC) in names
        assert control_plane_writer_key(CAPABILITY_TOPIC) not in names

    def test_mixed_worker_registers_both_writers(self) -> None:
        # Agent (calf.agents) + toolbox (calf.capabilities) co-reside: one writer per topic.
        worker = Worker(Client.connect("kafka:9092"), nodes=[make_agent(), make_toolbox()])
        worker._maybe_register_control_plane()
        names = worker_resource_names(worker)
        assert control_plane_writer_key(AGENTS_TOPIC) in names
        assert control_plane_writer_key(CAPABILITY_TOPIC) in names

    def test_toolbox_only_worker_registers_no_agents_writer(self) -> None:
        worker = Worker(Client.connect("kafka:9092"), nodes=[make_toolbox()])
        worker._maybe_register_control_plane()
        names = worker_resource_names(worker)
        assert control_plane_writer_key(CAPABILITY_TOPIC) in names
        assert control_plane_writer_key(AGENTS_TOPIC) not in names


# ---------------------------------------------------------------------------
# Read side: the Agents View resource, gated on a `_peers` handle (R9)
# ---------------------------------------------------------------------------


class TestAgentsViewRegistration:
    def test_not_registered_for_a_plain_agent(self) -> None:
        # The PR-A reality: no node sets `_peers`, so the gate is a true no-op.
        worker = Worker(Client.connect("kafka:9092"), nodes=[make_agent()])
        worker._maybe_register_agents_view()
        assert AGENTS_VIEW_RESOURCE_KEY not in worker_resource_names(worker)

    def test_registered_when_a_node_has_peers(self) -> None:
        # PR-B: a real `Messaging` handle sets `_peers`, activating the dormant gate.
        from calfkit.nodes.agent import Agent
        from calfkit.peers import Messaging

        agent = Agent("planner", subscribe_topics="planner.in", model_client=_FakeModel(), peers=[Messaging("billing")])
        worker = Worker(Client.connect("kafka:9092"), nodes=[agent])
        worker._maybe_register_agents_view()
        assert AGENTS_VIEW_RESOURCE_KEY in worker_resource_names(worker)

    def test_registered_for_a_handoff_only_agent(self) -> None:
        # PR-C: a `Handoff` handle ALSO sets `_peers`, so a Handoff-only agent (no Messaging) still trips
        # the gate — handoff needs the live agents view to render its tool-description directory. (The store
        # @resource is narrowed to messaging, but the agents-view gate stays on any `_peers` handle.)
        from calfkit.nodes.agent import Agent
        from calfkit.peers import Handoff

        agent = Agent("planner", subscribe_topics="planner.in", model_client=_FakeModel(), peers=[Handoff("billing")])
        worker = Worker(Client.connect("kafka:9092"), nodes=[agent])
        worker._maybe_register_agents_view()
        assert AGENTS_VIEW_RESOURCE_KEY in worker_resource_names(worker)

    def test_idempotent_on_repeat_calls(self) -> None:
        agent = make_agent()
        agent._peers = [object()]  # type: ignore[attr-defined]
        worker = Worker(Client.connect("kafka:9092"), nodes=[agent])
        worker._maybe_register_agents_view()
        worker._maybe_register_agents_view()
        assert worker_resource_names(worker).count(AGENTS_VIEW_RESOURCE_KEY) == 1


# ---------------------------------------------------------------------------
# Read side: the Agents View resource open mechanics (R10)
# ---------------------------------------------------------------------------


class FakeGroupedTable:
    """Stands in for ktables.GroupedKafkaTable; records construction and lifecycle."""

    instances: list[FakeGroupedTable] = []

    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs
        self.started = False
        self.stopped = False
        FakeGroupedTable.instances.append(self)

    @classmethod
    def json(cls, *, model: object, **kwargs: object) -> FakeGroupedTable:
        return cls(model=model, **kwargs)

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True


@pytest.fixture
def fake_table(monkeypatch: pytest.MonkeyPatch) -> type[FakeGroupedTable]:
    FakeGroupedTable.instances = []
    monkeypatch.setattr("ktables.GroupedKafkaTable", FakeGroupedTable)
    return FakeGroupedTable


async def drive(worker: Worker):
    gen = worker._agents_view_resource(None)  # type: ignore[arg-type]
    view = await anext(gen)
    return gen, view


async def close(gen) -> None:
    with pytest.raises(StopAsyncIteration):
        await anext(gen)


class TestAgentsViewResource:
    async def test_opens_view_over_calf_agents_with_lifecycle(self, fake_table: type[FakeGroupedTable]) -> None:
        worker = Worker(Client.connect("kafka:9092"), control_plane=ControlPlaneConfig(catchup_timeout=7.0))
        gen, _ = await drive(worker)
        table = fake_table.instances[0]
        assert table.started and not table.stopped
        assert table.kwargs["topic"] == AGENTS_TOPIC == "calf.agents"
        assert table.kwargs["model"] is AgentCard
        assert table.kwargs["catchup_timeout"] == 7.0
        assert table.kwargs["ensure_topic"] is False  # provisioning disabled by default
        await close(gen)
        assert table.stopped

    async def test_ensure_topic_follows_provisioning(self, fake_table: type[FakeGroupedTable]) -> None:
        from calfkit.provisioning import ProvisioningConfig

        worker = Worker(Client.connect("kafka:9092", provisioning=ProvisioningConfig(enabled=True)))
        gen, _ = await drive(worker)
        assert fake_table.instances[0].kwargs["ensure_topic"] is True
        await close(gen)

    async def test_bootstrap_derives_from_client(self, fake_table: type[FakeGroupedTable]) -> None:
        worker = Worker(Client.connect(["kafka-a:9092", "kafka-b:9092"]))
        gen, _ = await drive(worker)
        assert fake_table.instances[0].kwargs["connection"].bootstrap_servers == "kafka-a:9092,kafka-b:9092"
        await close(gen)

    async def test_underivable_bootstrap_raises_actionable_error(self, fake_table: type[FakeGroupedTable]) -> None:
        client = Client.connect("kafka:9092")
        client._connection_profile = None  # the direct-built posture: no profile to derive from
        client._server_urls = None
        client.broker._connection_kwargs = {}
        worker = Worker(client)
        with pytest.raises(RuntimeError, match="ControlPlaneConfig"):
            await drive(worker)
        assert fake_table.instances == []  # failed before construction

    async def test_stale_after_and_reader_tuning_are_forwarded(self, fake_table: type[FakeGroupedTable]) -> None:
        # Both are behavior-affecting: reader_tuning lowers convergence latency (reaches
        # the table), stale_after sets when a card expires (held on the view). A regression
        # dropping either would otherwise pass every other test.
        from calfkit.tuning import KTableReaderTuning

        worker = Worker(
            Client.connect("kafka:9092"),
            control_plane=ControlPlaneConfig(
                stale_after=12.0,
                reader_tuning=KTableReaderTuning(poll_timeout_ms=20, fetch_max_wait_ms=10),
            ),
        )
        gen, view = await drive(worker)
        kwargs = fake_table.instances[0].kwargs
        assert kwargs["poll_timeout_ms"] == 20  # reader_tuning -> table
        assert kwargs["fetch_max_wait_ms"] == 10
        assert view._stale_after == 12.0  # stale_after -> view
        await close(gen)
