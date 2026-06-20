"""The worker auto-registers the Capability View when agents need it.

One ``ControlPlaneView[CapabilityRecord]`` per worker, registered as a
worker-level resource iff any hosted node declares MCP tool selectors; zero user
wiring. These tests cover registration + open mechanics only — the view's
collapse/staleness/schema filtering is the substrate's responsibility
(``test_controlplane_view.py``), and the live path is the integration test.
"""

from __future__ import annotations

import pytest

from calfkit.client.client import Client
from calfkit.controlplane import ControlPlaneConfig
from calfkit.mcp.mcp_toolbox import MCPToolboxNode
from calfkit.mcp.mcp_transport import StreamableHttpParameters
from calfkit.models.capability import CAPABILITY_TOPIC, CAPABILITY_VIEW_RESOURCE_KEY, CapabilityRecord
from calfkit.providers.pydantic_ai.model_client import PydanticModelClient
from calfkit.worker.worker import Worker


class FakeModel(PydanticModelClient):
    @property
    def model_name(self) -> str:
        return "fake"

    @property
    def system(self) -> str:
        return "fake"

    async def request(self, *args: object, **kwargs: object) -> object:
        raise NotImplementedError


def make_toolbox() -> MCPToolboxNode:
    return MCPToolboxNode("docs_server", connection_params=StreamableHttpParameters(url="http://unused.local/mcp"))


def make_agent(*tools: object):
    from calfkit.nodes.agent import Agent

    return Agent("a", subscribe_topics="a.in", model_client=FakeModel(), tools=list(tools))


def worker_resource_names(worker: Worker) -> list[str]:
    return [name for name, _ in worker._resource_cms()]


class TestCapabilityViewRegistration:
    def test_registered_when_an_agent_declares_selectors(self) -> None:
        worker = Worker(Client.connect("kafka:9092"), nodes=[make_agent(make_toolbox())])
        worker._maybe_register_capability_view()
        assert CAPABILITY_VIEW_RESOURCE_KEY in worker_resource_names(worker)

    def test_not_registered_without_selectors(self) -> None:
        worker = Worker(Client.connect("kafka:9092"), nodes=[make_agent()])
        worker._maybe_register_capability_view()
        assert CAPABILITY_VIEW_RESOURCE_KEY not in worker_resource_names(worker)

    def test_idempotent_on_repeat_calls(self) -> None:
        worker = Worker(Client.connect("kafka:9092"), nodes=[make_agent(make_toolbox())])
        worker._maybe_register_capability_view()
        worker._maybe_register_capability_view()  # must not raise duplicate-name
        assert worker_resource_names(worker).count(CAPABILITY_VIEW_RESOURCE_KEY) == 1

    def test_scoped_selectors_also_trigger_registration(self) -> None:
        worker = Worker(Client.connect("kafka:9092"), nodes=[make_agent(make_toolbox().select(include=["search"]))])
        worker._maybe_register_capability_view()
        assert CAPABILITY_VIEW_RESOURCE_KEY in worker_resource_names(worker)

    def test_control_plane_config_defaults_when_omitted(self) -> None:
        worker = Worker(Client.connect("kafka:9092"))
        assert worker._control_plane == ControlPlaneConfig()


class TestControlPlaneWriterRegistration:
    """The writer side: hosting an advertising toolbox auto-wires the publisher + writer."""

    def test_hosting_a_toolbox_registers_the_capability_writer_and_publisher(self) -> None:
        from calfkit.controlplane.publisher import control_plane_writer_key
        from calfkit.models.capability import CAPABILITY_TOPIC

        worker = Worker(Client.connect("kafka:9092"), nodes=[make_toolbox()])
        worker._maybe_register_control_plane()
        assert control_plane_writer_key(CAPABILITY_TOPIC) in worker_resource_names(worker)
        publisher = worker._control_plane_publisher
        assert publisher is not None
        assert CAPABILITY_TOPIC in [info.topic for _, info in publisher._adverts]

    def test_agent_only_worker_registers_no_capability_writer(self) -> None:
        from calfkit.controlplane.publisher import control_plane_writer_key
        from calfkit.models.capability import CAPABILITY_TOPIC

        # An agent declares a tool selector but advertises nothing — only the toolbox
        # (host) advertises. A worker hosting just the agent wires no control-plane writer.
        worker = Worker(Client.connect("kafka:9092"), nodes=[make_agent(make_toolbox())])
        worker._maybe_register_control_plane()
        assert control_plane_writer_key(CAPABILITY_TOPIC) not in worker_resource_names(worker)
        assert worker._control_plane_publisher is None


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
    gen = worker._capability_view_resource(None)  # type: ignore[arg-type]
    view = await anext(gen)
    return gen, view


async def close(gen) -> None:
    with pytest.raises(StopAsyncIteration):
        await anext(gen)


class TestCapabilityViewResource:
    async def test_opens_view_over_calf_capabilities_with_lifecycle(self, fake_table: type[FakeGroupedTable]) -> None:
        worker = Worker(Client.connect("kafka:9092"), control_plane=ControlPlaneConfig(catchup_timeout=7.0))
        gen, view = await drive(worker)
        table = fake_table.instances[0]
        assert table.started and not table.stopped
        assert table.kwargs["topic"] == CAPABILITY_TOPIC == "calf.capabilities"  # de-MCP rename
        assert table.kwargs["model"] is CapabilityRecord
        assert table.kwargs["catchup_timeout"] == 7.0
        assert table.kwargs["ensure_topic"] is False  # provisioning disabled by default
        await close(gen)
        assert table.stopped

    async def test_bootstrap_explicit_override_wins(self, fake_table: type[FakeGroupedTable]) -> None:
        worker = Worker(Client.connect("kafka:9092"), control_plane=ControlPlaneConfig(bootstrap_servers="cp-kafka:9092"))
        gen, _ = await drive(worker)
        assert fake_table.instances[0].kwargs["bootstrap_servers"] == "cp-kafka:9092"
        await close(gen)

    async def test_bootstrap_derives_from_client(self, fake_table: type[FakeGroupedTable]) -> None:
        worker = Worker(Client.connect(["kafka-a:9092", "kafka-b:9092"]))
        gen, _ = await drive(worker)
        assert fake_table.instances[0].kwargs["bootstrap_servers"] == "kafka-a:9092,kafka-b:9092"
        await close(gen)

    async def test_underivable_bootstrap_raises_actionable_error(self, fake_table: type[FakeGroupedTable]) -> None:
        client = Client.connect("kafka:9092")
        client._server_urls = None
        client.broker._connection_kwargs = {}
        worker = Worker(client)
        with pytest.raises(RuntimeError, match="ControlPlaneConfig"):
            await drive(worker)
        assert fake_table.instances == []  # failed before construction

    async def test_ensure_topic_follows_provisioning(self, fake_table: type[FakeGroupedTable]) -> None:
        from calfkit.provisioning import ProvisioningConfig

        worker = Worker(Client.connect("kafka:9092", provisioning=ProvisioningConfig(enabled=True)))
        gen, _ = await drive(worker)
        assert fake_table.instances[0].kwargs["ensure_topic"] is True
        await close(gen)


class TestRegisterHandlersIdempotency:
    def test_second_call_is_a_guarded_no_op(self) -> None:
        worker = Worker(Client.connect("kafka:9092"))  # zero nodes: registration is broker-free
        worker.register_handlers()
        assert worker._prepared
        worker.register_handlers()  # exercises the already-prepared early return
        assert worker._prepared
