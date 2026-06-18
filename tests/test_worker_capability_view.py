"""PR C: the worker auto-registers the Capability View when agents need it.

Spec §8.3: one KafkaTable per worker, registered as a worker-level resource
iff any hosted node declares MCP tool selectors; zero user wiring. These
tests cover registration mechanics only — the table itself is ktables'
responsibility, and the live path is the integration test.
"""

from __future__ import annotations

import pytest

from calfkit.client.client import Client
from calfkit.mcp.mcp_toolbox import MCPToolboxNode
from calfkit.mcp.mcp_transport import StreamableHttpParameters
from calfkit.models.capability import CAPABILITY_VIEW_RESOURCE_KEY
from calfkit.providers.pydantic_ai.model_client import PydanticModelClient
from calfkit.worker.worker import Worker
from calfkit.worker.worker_config import MCPDiscoveryConfig


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
        client = Client.connect("kafka:9092")
        worker = Worker(client, nodes=[make_agent(make_toolbox())])
        worker._maybe_register_capability_view()
        assert CAPABILITY_VIEW_RESOURCE_KEY in worker_resource_names(worker)

    def test_not_registered_without_selectors(self) -> None:
        client = Client.connect("kafka:9092")
        worker = Worker(client, nodes=[make_agent()])
        worker._maybe_register_capability_view()
        assert CAPABILITY_VIEW_RESOURCE_KEY not in worker_resource_names(worker)

    def test_idempotent_on_repeat_calls(self) -> None:
        client = Client.connect("kafka:9092")
        worker = Worker(client, nodes=[make_agent(make_toolbox())])
        worker._maybe_register_capability_view()
        worker._maybe_register_capability_view()  # must not raise duplicate-name
        assert worker_resource_names(worker).count(CAPABILITY_VIEW_RESOURCE_KEY) == 1

    def test_scoped_selectors_also_trigger_registration(self) -> None:
        client = Client.connect("kafka:9092")
        worker = Worker(client, nodes=[make_agent(make_toolbox().select(include=["search"]))])
        worker._maybe_register_capability_view()
        assert CAPABILITY_VIEW_RESOURCE_KEY in worker_resource_names(worker)

    def test_discovery_config_accepted_on_worker(self) -> None:
        client = Client.connect("kafka:9092")
        worker = Worker(client, mcp_discovery=MCPDiscoveryConfig(topic="custom.capabilities"))
        assert worker._mcp_discovery.topic == "custom.capabilities"

    def test_defaults_when_config_omitted(self) -> None:
        client = Client.connect("kafka:9092")
        worker = Worker(client)
        assert worker._mcp_discovery == MCPDiscoveryConfig()


class FakeKafkaTable:
    """Stands in for ktables.KafkaTable; records construction and lifecycle."""

    instances: list[FakeKafkaTable] = []

    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs
        self.started = False
        self.stopped = False
        FakeKafkaTable.instances.append(self)

    @classmethod
    def json(cls, *, model: object, **kwargs: object) -> FakeKafkaTable:
        return cls(model=model, **kwargs)

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True


@pytest.fixture
def fake_table(monkeypatch: pytest.MonkeyPatch) -> type[FakeKafkaTable]:
    FakeKafkaTable.instances = []
    monkeypatch.setattr("ktables.KafkaTable", FakeKafkaTable)
    return FakeKafkaTable


async def drive(worker: Worker):
    gen = worker._capability_view_resource(None)  # type: ignore[arg-type]
    table = await anext(gen)
    return gen, table


async def close(gen) -> None:
    with pytest.raises(StopAsyncIteration):
        await anext(gen)


class TestCapabilityViewResource:
    async def test_opens_table_with_config_and_lifecycle(self, fake_table: type[FakeKafkaTable]) -> None:
        client = Client.connect("kafka:9092")
        worker = Worker(client, mcp_discovery=MCPDiscoveryConfig(topic="custom.caps", catchup_timeout=7.0))
        gen, table = await drive(worker)
        assert table.started and not table.stopped
        assert table.kwargs["topic"] == "custom.caps"
        assert table.kwargs["catchup_timeout"] == 7.0
        assert table.kwargs["ensure_topic"] is False  # provisioning disabled by default
        await close(gen)
        assert table.stopped

    async def test_bootstrap_explicit_override_wins(self, fake_table: type[FakeKafkaTable]) -> None:
        client = Client.connect("kafka:9092")
        worker = Worker(client, mcp_discovery=MCPDiscoveryConfig(bootstrap_servers="cp-kafka:9092"))
        gen, table = await drive(worker)
        assert table.kwargs["bootstrap_servers"] == "cp-kafka:9092"
        await close(gen)

    async def test_bootstrap_derives_from_client(self, fake_table: type[FakeKafkaTable]) -> None:
        client = Client.connect(["kafka-a:9092", "kafka-b:9092"])
        worker = Worker(client)
        gen, table = await drive(worker)
        assert table.kwargs["bootstrap_servers"] == "kafka-a:9092,kafka-b:9092"
        await close(gen)

    async def test_bootstrap_falls_back_to_broker_kwargs_list(self, fake_table: type[FakeKafkaTable]) -> None:
        client = Client.connect("kafka:9092")
        client._server_urls = None  # hand-built-client scenario
        worker = Worker(client)
        gen, table = await drive(worker)
        assert table.kwargs["bootstrap_servers"] == "kafka:9092"  # joined from broker kwargs
        await close(gen)

    async def test_bootstrap_falls_back_to_broker_kwargs_str(self, fake_table: type[FakeKafkaTable]) -> None:
        client = Client.connect("kafka:9092")
        client._server_urls = None
        client.broker._connection_kwargs = {"bootstrap_servers": "raw-str:9092"}
        worker = Worker(client)
        gen, table = await drive(worker)
        assert table.kwargs["bootstrap_servers"] == "raw-str:9092"
        await close(gen)

    async def test_underivable_bootstrap_raises_actionable_error(self, fake_table: type[FakeKafkaTable]) -> None:
        client = Client.connect("kafka:9092")
        client._server_urls = None
        client.broker._connection_kwargs = {}
        worker = Worker(client)
        with pytest.raises(RuntimeError, match="bootstrap_servers"):
            await drive(worker)
        assert fake_table.instances == []  # failed before construction

    async def test_ensure_topic_follows_provisioning(self, fake_table: type[FakeKafkaTable]) -> None:
        from calfkit.provisioning import ProvisioningConfig

        client = Client.connect("kafka:9092", provisioning=ProvisioningConfig(enabled=True))
        worker = Worker(client)
        gen, table = await drive(worker)
        assert table.kwargs["ensure_topic"] is True
        await close(gen)


class TestRegisterHandlersIdempotency:
    def test_second_call_is_a_guarded_no_op(self) -> None:
        client = Client.connect("kafka:9092")
        worker = Worker(client)  # zero nodes: registration is broker-free
        worker.register_handlers()
        assert worker._prepared
        worker.register_handlers()  # exercises the already-prepared early return
        assert worker._prepared
