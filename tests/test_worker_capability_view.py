"""PR C: the worker auto-registers the Capability View when agents need it.

Spec §8.3: one KafkaTable per worker, registered as a worker-level resource
iff any hosted node declares MCP tool selectors; zero user wiring. These
tests cover registration mechanics only — the table itself is ktables'
responsibility, and the live path is the integration test.
"""

from __future__ import annotations

from calfkit.client.client import Client
from calfkit.mcp.mcp_toolbox import MCPToolbox
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


def make_toolbox() -> MCPToolbox:
    return MCPToolbox("docs_server", connection_params=StreamableHttpParameters(url="http://unused.local/mcp"))


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
