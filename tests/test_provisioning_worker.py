"""Tests for worker-side topic declaration into the startup ensurer.

The worker no longer provisions its node topics itself; it *declares* them into
the client's :class:`~calfkit.provisioning.StartupTopicEnsurer` (during
``_on_startup``), which provisions them — alongside the client's reply topic —
at broker start, reusing FastStream's admin client. These tests drive
``_on_startup`` (no live Kafka) and then run the ensurer against a fake admin
broker to assert the declared topic set:

* each registered node's ``subscribe_topics``, framework ``_return_topic``,
  ``publish_topic``, and (for agents) each tool's input ``subscribe_topics``,
* user ``topic_configs`` on data topics only, never on framework return inboxes,
* default-off is a pure no-op (the ensurer never provisions).
"""

import asyncio
from typing import Any

from calfkit.client import Client
from calfkit.nodes.agent import StatelessAgent
from calfkit.nodes.tool import ToolNodeDef
from calfkit.providers.pydantic_ai.model_client import PydanticModelClient
from calfkit.provisioning import ProvisioningConfig
from calfkit.worker.worker import Worker
from tests._provisioning_fakes import EnsurerBroker, FakeResponse

# ---------------------------------------------------------------------------
# Fakes: a create-all admin reached through the shared ``EnsurerBroker`` seam
# (``broker.config.broker_config.admin_client``, what the ensurer reads).
# ---------------------------------------------------------------------------


class _FakeModel(PydanticModelClient):
    """Minimal pydantic-ai ``Model`` so a real ``StatelessAgent`` node can be built
    without any network / API key. Never actually invoked by these tests."""

    @property
    def model_name(self) -> str:
        return "fake"

    @property
    def system(self) -> str:
        return "fake"

    async def request(self, *args: object, **kwargs: object) -> object:
        raise NotImplementedError


class _FakeAdmin:
    """Create-all admin: returns code-0 for every requested topic."""

    def __init__(self) -> None:
        self.create_calls: list[list] = []

    async def create_topics(self, new_topics, *args: Any, **kwargs: Any):  # noqa: ANN001
        self.create_calls.append(list(new_topics))
        return FakeResponse([(nt.name, 0) for nt in new_topics])

    async def close(self) -> None:
        return None


def _run_ensurer(client: Client) -> _FakeAdmin:
    """Run the client's startup ensurer against a fresh create-all admin and
    return it (its ``create_calls`` capture every provisioned NewTopic)."""
    admin = _FakeAdmin()
    asyncio.run(client._startup_ensurer.run(EnsurerBroker(admin)))
    return admin


def _names(admin: _FakeAdmin) -> list[str]:
    return [nt.name for call in admin.create_calls for nt in call]


def _by_name(admin: _FakeAdmin) -> dict[str, Any]:
    return {nt.name: nt for call in admin.create_calls for nt in call}


def _tool_node(name: str, sub: str, pub: str) -> ToolNodeDef:
    def _fn(x: int) -> int:
        return x

    _fn.__name__ = name
    return ToolNodeDef.create_tool_node(func=_fn, subscribe_topics=sub, publish_topic=pub)


def _make_client(provisioning: ProvisioningConfig | None = None) -> Client:
    return Client.connect("localhost:9092", provisioning=provisioning)


# ---------------------------------------------------------------------------
# Default-off: the ensurer never provisions
# ---------------------------------------------------------------------------


def test_default_off_ensurer_does_not_provision() -> None:
    client = _make_client()  # provisioning disabled (default)
    worker = Worker(client, nodes=[_tool_node("echo", "echo.in", "echo.out")])

    asyncio.run(worker._on_startup())  # declares node topics into the ensurer
    admin = _run_ensurer(client)

    assert admin.create_calls == []  # disabled -> nothing is created


# ---------------------------------------------------------------------------
# Enabled: the declared topic set is correct
# ---------------------------------------------------------------------------


def test_enabled_declares_plain_node_subscribe_return_publish() -> None:
    client = _make_client(ProvisioningConfig(enabled=True))
    node = _tool_node("echo", "echo.in", "echo.out")
    worker = Worker(client, nodes=[node])

    asyncio.run(worker._on_startup())
    names = _names(_run_ensurer(client))

    assert "echo.in" in names
    assert "echo.out" in names
    assert node._return_topic in names  # framework return inbox


def test_enabled_declares_agent_tool_input_topics() -> None:
    """An agent publishes tool ``Call`` envelopes onto each tool's input topic,
    so that topic must be provisioned even though the agent does not subscribe
    to it (regression guard for the missing-tool-topics blocker)."""
    client = _make_client(ProvisioningConfig(enabled=True))
    tool = _tool_node("weather_lookup", "weather_lookup.in", "weather_lookup.out")
    agent = StatelessAgent(
        "weather",
        subscribe_topics="weather.in",
        publish_topic="weather.out",
        tools=[tool],
        model_client=_FakeModel(),
    )
    worker = Worker(client, nodes=[agent])

    asyncio.run(worker._on_startup())
    names = _names(_run_ensurer(client))

    assert "weather.in" in names
    assert "weather.out" in names
    assert agent._return_topic in names
    assert "weather_lookup.in" in names  # tool input topic (not subscribed by the agent)


def test_return_inbox_does_not_receive_user_topic_configs() -> None:
    cfg = ProvisioningConfig(enabled=True, topic_configs={"retention.ms": "604800000"})
    client = _make_client(cfg)
    node = _tool_node("echo", "echo.in", "echo.out")
    worker = Worker(client, nodes=[node])

    asyncio.run(worker._on_startup())
    by_name = _by_name(_run_ensurer(client))

    assert by_name["echo.in"].topic_configs == {"retention.ms": "604800000"}
    assert by_name["echo.out"].topic_configs == {"retention.ms": "604800000"}
    # Framework return inbox: never gets user configs (None -> {} in aiokafka).
    assert by_name[node._return_topic].topic_configs == {}


def test_declares_only_registered_nodes_not_later_additions() -> None:
    client = _make_client(ProvisioningConfig(enabled=True))
    worker = Worker(client, nodes=[_tool_node("echo", "echo.in", "echo.out")])

    asyncio.run(worker._on_startup())  # registers + declares echo
    worker.add_nodes(_tool_node("late", "late.in", "late.out"))  # never registered

    names = _names(_run_ensurer(client))

    assert "echo.in" in names
    assert "late.in" not in names
    assert "late.out" not in names


# ---------------------------------------------------------------------------
# ADR-0017: the name-scoped private input inbox is registered framework-owned
# ---------------------------------------------------------------------------


def test_private_input_topic_is_provisioned_framework_owned() -> None:
    # The inbound inbox is provisioned but framework-owned: never carries user
    # topic_configs (declared via framework_topics_for_nodes — the C1 fix).
    cfg = ProvisioningConfig(enabled=True, topic_configs={"retention.ms": "604800000"})
    client = _make_client(cfg)
    node = _tool_node("echo", "echo.in", "echo.out")
    worker = Worker(client, nodes=[node])

    asyncio.run(worker._on_startup())
    by_name = _by_name(_run_ensurer(client))

    assert node._private_input_topic in by_name  # provisioned
    assert by_name[node._private_input_topic].topic_configs == {}  # framework-owned
    assert by_name["echo.in"].topic_configs == {"retention.ms": "604800000"}  # data topic still configured


def test_node_subscribes_to_its_private_input_topic() -> None:
    # Every node CONSUMES its inbound inbox (contributed at registration like
    # _return_topic — not appended into user-facing subscribe_topics).
    client = _make_client()
    node = _tool_node("echo", "echo.in", "echo.out")
    worker = Worker(client, nodes=[node])

    worker.register_handlers()

    node_topics = next(s.topics for s in worker._client._connection._subscribers if node._return_topic in (s.topics or []))
    assert node._private_input_topic in node_topics
    assert node._private_input_topic not in node.subscribe_topics


def test_bare_agent_is_reachable_via_its_private_input_topic() -> None:
    # An agent with no public subscribe_topics is still reachable: registration
    # contributes its name-derived private input inbox (ADR-0017), the topic every
    # caller (client gateway, message_agent, handoff) addresses it by.
    client = _make_client()
    agent = StatelessAgent("bare", model_client=_FakeModel())
    assert agent.subscribe_topics == []  # no public inbox declared
    worker = Worker(client, nodes=[agent])

    worker.register_handlers()

    agent_topics = next(s.topics for s in worker._client._connection._subscribers if agent._return_topic in (s.topics or []))
    assert agent._private_input_topic in agent_topics
    assert agent._private_input_topic == "agent.bare.private.input"
