"""Integration tests for worker-side topic provisioning.

These exercise the REAL ``Worker`` provisioning wiring against the
``calfkit.provisioning.provisioner._make_admin_client`` seam (patched to a fake
admin), so no live Kafka broker is required. They verify:

* default-off is a pure no-op — neither ``_on_startup`` nor the public
  ``provision_topics`` reaches the admin-client seam,
* when enabled, the eagerly-provisioned topic set is correct: each registered
  node's ``subscribe_topics``, its framework-private ``_return_topic``, its
  ``publish_topic``, AND each agent tool's input ``subscribe_topics`` (the
  regression guard for the missing-tool-topics blocker),
* MCP bridge topics are included only after the bridges are constructed in
  ``_on_startup`` (so the eager pass covers them), and
* user ``topic_configs`` are applied to data topics but never to framework
  topics (the ``*.private.return`` inboxes).
"""

import asyncio
from typing import Any

from mcp.types import CallToolResult, TextContent

from calfkit.client import Client
from calfkit.mcp._testing import FakeMcpServer
from calfkit.mcp._tool_def import McpToolDef
from calfkit.nodes.agent import Agent
from calfkit.nodes.tool import ToolNodeDef
from calfkit.providers.pydantic_ai.model_client import PydanticModelClient
from calfkit.provisioning import ProvisioningConfig
from calfkit.provisioning import provisioner as provisioner_mod
from calfkit.worker.worker import Worker

# ---------------------------------------------------------------------------
# Fakes (mirroring tests/test_provisioning_client.py)
# ---------------------------------------------------------------------------


class _FakeModel(PydanticModelClient):
    """Minimal pydantic-ai ``Model`` so a real ``Agent`` node can be built
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
    """Minimal stand-in for ``AIOKafkaAdminClient`` returning code-0 (created)
    for every requested topic, so the real provisioning flow runs end-to-end."""

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.create_calls: list[list] = []
        self.start_calls = 0
        self.close_calls = 0

    async def start(self) -> None:
        self.start_calls += 1

    async def create_topics(self, new_topics, *args: Any, **kwargs: Any):  # noqa: ANN001
        self.create_calls.append(list(new_topics))
        return _FakeResponse([(nt.name, 0) for nt in new_topics])

    async def close(self) -> None:
        self.close_calls += 1


class _FakeResponse:
    def __init__(self, topic_errors: list[tuple]) -> None:
        self.topic_errors = topic_errors


class _CodeAdmin(_FakeAdmin):
    """Fake admin returning a configurable per-topic error code.

    ``code_for`` maps a topic name to the Kafka error code the broker should
    return, so a test can drive an ``unauthorized`` (29) outcome.
    """

    def __init__(self, *, code_for, **kwargs: Any) -> None:  # noqa: ANN001
        super().__init__(**kwargs)
        self._code_for = code_for

    async def create_topics(self, new_topics, *args: Any, **kwargs: Any):  # noqa: ANN001
        self.create_calls.append(list(new_topics))
        return _FakeResponse([(nt.name, self._code_for(nt.name)) for nt in new_topics])


def _install_fake_admin(monkeypatch) -> list[_FakeAdmin]:
    """Patch the provisioner seam; return a list capturing created fakes."""
    created: list[_FakeAdmin] = []

    def _factory(**kwargs: Any) -> _FakeAdmin:
        admin = _FakeAdmin(**kwargs)
        created.append(admin)
        return admin

    monkeypatch.setattr(provisioner_mod, "_make_admin_client", _factory)
    return created


def _tool_node(name: str, sub: str, pub: str) -> ToolNodeDef:
    def _fn(x: int) -> int:
        return x

    _fn.__name__ = name
    return ToolNodeDef.create_tool_node(func=_fn, subscribe_topics=sub, publish_topic=pub)


def _td(name: str = "t") -> McpToolDef:
    return McpToolDef(name=name, input_schema={"type": "object"})


def _ok_result(text: str = "ok") -> CallToolResult:
    return CallToolResult(content=[TextContent(type="text", text=text)], isError=False)


def _make_client(provisioning: ProvisioningConfig | None = None) -> Client:
    return Client.connect("localhost:9092", provisioning=provisioning)


# ---------------------------------------------------------------------------
# Default-off no-op
# ---------------------------------------------------------------------------


def test_default_off_on_startup_never_constructs_admin(monkeypatch) -> None:
    created = _install_fake_admin(monkeypatch)
    client = _make_client()  # provisioning defaults to disabled
    worker = Worker(client, nodes=[_tool_node("echo", "echo.in", "echo.out")])

    asyncio.run(worker._on_startup())

    assert created == []


def test_default_off_provision_topics_is_pure_noop(monkeypatch) -> None:
    created = _install_fake_admin(monkeypatch)
    client = _make_client()  # provisioning defaults to disabled
    worker = Worker(client, nodes=[_tool_node("echo", "echo.in", "echo.out")])
    worker.register_handlers()

    asyncio.run(worker.provision_topics())

    assert created == []


# ---------------------------------------------------------------------------
# Enabled: eager topic set is correct
# ---------------------------------------------------------------------------


def _provisioned_names(created: list[_FakeAdmin]) -> list[str]:
    """Flatten every NewTopic name across all create calls of all fakes."""
    return [nt.name for admin in created for call in admin.create_calls for nt in call]


def test_enabled_provision_topics_covers_plain_node_subscribe_return_publish(monkeypatch) -> None:
    created = _install_fake_admin(monkeypatch)
    client = _make_client(ProvisioningConfig(enabled=True))
    node = _tool_node("echo", "echo.in", "echo.out")
    worker = Worker(client, nodes=[node])
    worker.register_handlers()

    asyncio.run(worker.provision_topics())

    names = _provisioned_names(created)
    assert "echo.in" in names
    assert "echo.out" in names
    # The framework-private return inbox for a PLAIN node must be provisioned.
    assert node._return_topic in names


def test_enabled_provision_topics_includes_agent_tool_input_topics(monkeypatch) -> None:
    """Regression guard for the missing-tool-topics blocker: an agent publishes
    tool ``Call`` envelopes onto each tool's input topic, so that topic must be
    provisioned even though the agent does not *subscribe* to it."""
    created = _install_fake_admin(monkeypatch)
    client = _make_client(ProvisioningConfig(enabled=True))
    tool = _tool_node("weather_lookup", "weather_lookup.in", "weather_lookup.out")
    agent = Agent(
        "weather",
        subscribe_topics="weather.in",
        publish_topic="weather.out",
        tools=[tool],
        model_client=_FakeModel(),
    )
    worker = Worker(client, nodes=[agent])
    worker.register_handlers()

    asyncio.run(worker.provision_topics())

    names = _provisioned_names(created)
    assert "weather.in" in names
    assert "weather.out" in names
    assert agent._return_topic in names
    # The agent's tool input topic — NOT a subscribed topic of the agent — is
    # provisioned so tool dispatch lands.
    assert "weather_lookup.in" in names


# ---------------------------------------------------------------------------
# Enabled via _on_startup: MCP bridge topics included after bridge construction
# ---------------------------------------------------------------------------


def test_on_startup_provisions_mcp_bridge_topics(monkeypatch) -> None:
    """MCP bridges are constructed inside ``_on_startup`` and appended to the
    registered set just before ``register_handlers``. The eager provisioning
    pass — which runs after registration — must therefore cover the bridge's
    derived input/output topics."""
    created = _install_fake_admin(monkeypatch)
    client = _make_client(ProvisioningConfig(enabled=True))
    fake = FakeMcpServer(
        name="gmail",
        tools=[_td("search"), _td("send")],
        invoker=lambda n, a, m: _ok_result(),
    )
    worker = Worker(client, nodes=[fake])

    asyncio.run(worker._on_startup())

    names = _provisioned_names(created)
    # Bridge topics are mcp.<server>.<tool>.input / .output (see McpBridge).
    assert "mcp.gmail.search.input" in names
    assert "mcp.gmail.search.output" in names
    assert "mcp.gmail.send.input" in names
    assert "mcp.gmail.send.output" in names

    # Cleanup the opened MCP session.
    asyncio.run(worker._on_shutdown())


def test_on_startup_provisioning_failure_closes_opened_mcp_sessions(monkeypatch) -> None:
    """A provisioning failure must still honor the BaseException cleanup
    contract: any MCP session opened earlier in ``_on_startup`` is closed
    before the error propagates."""
    import pytest

    from calfkit.provisioning import TopicProvisioningError

    created = _install_fake_admin(monkeypatch)
    client = _make_client(ProvisioningConfig(enabled=True))
    fake = FakeMcpServer(name="gmail", tools=[_td("search")], invoker=lambda n, a, m: _ok_result())
    worker = Worker(client, nodes=[fake])

    async def _boom() -> None:
        raise TopicProvisioningError("kaboom", topic="mcp.gmail.search.input", code=None)

    monkeypatch.setattr(worker, "provision_topics", _boom)

    with pytest.raises(TopicProvisioningError, match="kaboom"):
        asyncio.run(worker._on_startup())

    # The MCP session opened before provisioning ran was closed during cleanup.
    assert fake.session is None
    # Sanity: the failure short-circuited before any real admin client work via
    # the patched provision_topics (no admin constructed here).
    assert created == []


# ---------------------------------------------------------------------------
# topic_configs applied to data topics, never to framework (_return) topics
# ---------------------------------------------------------------------------


def test_topic_configs_not_applied_to_node_return_inbox(monkeypatch) -> None:
    """User ``topic_configs`` (retention / compaction) belong on data topics
    only. A node's ``*.private.return`` inbox is a framework, correlation-keyed
    topic, so its ``NewTopic`` must carry NO ``topic_configs``."""
    created = _install_fake_admin(monkeypatch)
    cfg = ProvisioningConfig(enabled=True, topic_configs={"retention.ms": "604800000"})
    client = _make_client(cfg)
    node = _tool_node("echo", "echo.in", "echo.out")
    worker = Worker(client, nodes=[node])
    worker.register_handlers()

    asyncio.run(worker.provision_topics())

    # One admin client, one create call carrying every NewTopic.
    assert len(created) == 1
    by_name = {nt.name: nt for call in created[0].create_calls for nt in call}

    # Data topics get the user-supplied config.
    assert by_name["echo.in"].topic_configs == {"retention.ms": "604800000"}
    assert by_name["echo.out"].topic_configs == {"retention.ms": "604800000"}
    # The framework return inbox must NOT receive the config override
    # (None -> {} in aiokafka's NewTopic). Compaction / short retention on a
    # correlation-keyed return inbox is semantically wrong.
    assert by_name[node._return_topic].topic_configs == {}


# ---------------------------------------------------------------------------
# Registered-set is the single source of truth (survives idempotent 2nd call)
# ---------------------------------------------------------------------------


def test_provision_topics_uses_only_registered_nodes_not_later_additions(monkeypatch) -> None:
    """``provision_topics`` provisions exactly what ``register_handlers``
    recorded. A node added AFTER registration is NOT wired up (the second
    register_handlers call is a no-op), so it must not be provisioned either —
    otherwise we'd create topics no subscriber listens on."""
    created = _install_fake_admin(monkeypatch)
    client = _make_client(ProvisioningConfig(enabled=True))
    worker = Worker(client, nodes=[_tool_node("echo", "echo.in", "echo.out")])
    worker.register_handlers()

    # Added after registration: never wired (register_handlers is now a no-op).
    worker.add_nodes(_tool_node("late", "late.in", "late.out"))
    worker.register_handlers()  # idempotent no-op

    asyncio.run(worker.provision_topics())

    names = _provisioned_names(created)
    assert "echo.in" in names
    # The late node was not registered, so its topics are not provisioned.
    assert "late.in" not in names
    assert "late.out" not in names


# ---------------------------------------------------------------------------
# Provision report logging ([4c])
# ---------------------------------------------------------------------------


def test_provision_topics_logs_summary(monkeypatch, caplog) -> None:
    import logging

    _install_fake_admin(monkeypatch)  # all topics -> created (code 0)
    client = _make_client(ProvisioningConfig(enabled=True))
    worker = Worker(client, nodes=[_tool_node("echo", "echo.in", "echo.out")])
    worker.register_handlers()

    with caplog.at_level(logging.INFO, logger="calfkit.worker.worker"):
        asyncio.run(worker.provision_topics())

    summaries = [r.getMessage() for r in caplog.records if "provisioned topics:" in r.getMessage()]
    assert len(summaries) == 1
    # echo.in, echo.out, and the framework return inbox were all created.
    assert "3 created" in summaries[0]
    assert "0 existing" in summaries[0]
    assert "0 unauthorized" in summaries[0]


def test_provision_topics_warns_on_unauthorized(monkeypatch, caplog) -> None:
    import logging

    created: list[_FakeAdmin] = []

    def _factory(**kwargs: Any) -> _CodeAdmin:
        # "echo.in" is denied (code 29); everything else is created (code 0).
        admin = _CodeAdmin(code_for=lambda name: 29 if name == "echo.in" else 0, **kwargs)
        created.append(admin)
        return admin

    monkeypatch.setattr(provisioner_mod, "_make_admin_client", _factory)

    client = _make_client(ProvisioningConfig(enabled=True))
    worker = Worker(client, nodes=[_tool_node("echo", "echo.in", "echo.out")])
    worker.register_handlers()

    with caplog.at_level(logging.WARNING, logger="calfkit.worker.worker"):
        asyncio.run(worker.provision_topics())

    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("echo.in" in m and "unauthorized" in m.lower() for m in warnings)


# ---------------------------------------------------------------------------
# Security passthrough end-to-end ([7])
# ---------------------------------------------------------------------------


def test_worker_security_passes_through_to_admin(monkeypatch) -> None:
    """A FastStream SASLPlaintext security object given to ``Client.connect``
    must reach the provisioner's admin client as real SASL kwargs — not be
    silently dropped to PLAINTEXT."""
    from faststream.security import SASLPlaintext

    created = _install_fake_admin(monkeypatch)
    client = Client.connect(
        "localhost:9092",
        provisioning=ProvisioningConfig(enabled=True),
        security=SASLPlaintext(username="u", password="p"),
    )
    worker = Worker(client, nodes=[_tool_node("echo", "echo.in", "echo.out")])
    worker.register_handlers()

    asyncio.run(worker.provision_topics())

    assert len(created) == 1
    kw = created[0].kwargs
    assert kw["security_protocol"] == "SASL_PLAINTEXT"
    assert kw["sasl_plain_username"] == "u"
