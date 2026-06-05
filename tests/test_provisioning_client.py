"""Integration tests for client-side reply-topic provisioning.

These tests exercise the REAL ``BaseClient`` provisioning wiring against the
``calfkit.provisioning.provisioner._make_admin_client`` seam (patched to a fake
admin), so no live Kafka broker is required. They verify:

* the new read-only ``provisioning`` / ``server_urls`` / ``security_kwargs``
  properties exist and carry the right values,
* default-off is a pure no-op (the admin seam is never reached through
  ``_invoke``), and
* when enabled, the reply topic is provisioned exactly ONCE even across many
  ``_invoke`` calls.
"""

import asyncio

from calfkit.client import Client
from calfkit.models.state import State
from calfkit.provisioning import ProvisioningConfig
from calfkit.provisioning import provisioner as provisioner_mod


class _FakeAdmin:
    """Minimal stand-in for ``AIOKafkaAdminClient`` returning code-0 (created)
    for every requested topic, so the real provisioning flow runs end-to-end."""

    def __init__(self, **kwargs) -> None:  # noqa: ANN003
        self.kwargs = kwargs
        self.create_calls: list[list] = []
        self.start_calls = 0
        self.close_calls = 0

    async def start(self) -> None:
        self.start_calls += 1

    async def create_topics(self, new_topics, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        self.create_calls.append(list(new_topics))
        return _FakeResponse([(nt.name, 0) for nt in new_topics])

    async def close(self) -> None:
        self.close_calls += 1


class _FakeResponse:
    def __init__(self, topic_errors: list[tuple]) -> None:
        self.topic_errors = topic_errors


def _install_fake_admin(monkeypatch) -> list[_FakeAdmin]:
    """Patch the provisioner seam; return a list capturing created fakes."""
    created: list[_FakeAdmin] = []

    def _factory(**kwargs):  # noqa: ANN003
        admin = _FakeAdmin(**kwargs)
        created.append(admin)
        return admin

    monkeypatch.setattr(provisioner_mod, "_make_admin_client", _factory)
    return created


# ---------------------------------------------------------------------------
# Properties: provisioning / server_urls / security_kwargs
# ---------------------------------------------------------------------------


def test_default_provisioning_property_is_disabled_config() -> None:
    client = Client.connect("localhost:9092")

    assert isinstance(client.provisioning, ProvisioningConfig)
    # Never None; defaults to a disabled config so .enabled is always safe.
    assert client.provisioning.enabled is False


def test_provisioning_property_reflects_passed_config() -> None:
    cfg = ProvisioningConfig(enabled=True, num_partitions=3)
    client = Client.connect("localhost:9092", provisioning=cfg)

    assert client.provisioning is cfg


def test_server_urls_property_reflects_connect_argument() -> None:
    client = Client.connect("broker-a:9092")

    assert client.server_urls == "broker-a:9092"


def test_security_kwargs_captures_security_object_and_raw_kwargs() -> None:
    from faststream.security import SASLPlaintext

    sec = SASLPlaintext(username="u", password="p")
    client = Client.connect(
        "localhost:9092",
        security=sec,
        security_protocol="SASL_PLAINTEXT",
        sasl_kerberos_service_name="custom",
    )

    sk = client.security_kwargs
    assert sk["security"] is sec
    assert sk["security_protocol"] == "SASL_PLAINTEXT"
    assert sk["sasl_kerberos_service_name"] == "custom"


def test_security_kwargs_excludes_non_security_broker_kwargs() -> None:
    client = Client.connect("localhost:9092", client_id="my-client")

    assert "client_id" not in client.security_kwargs


# ---------------------------------------------------------------------------
# _invoke: reply-topic provisioning (default-off no-op + enabled one-shot)
# ---------------------------------------------------------------------------


def _stub_broker_io(client: Client, monkeypatch) -> None:
    """Neutralize the broker's network I/O so ``_invoke`` runs without Kafka.

    Forces the connect-guard to fire on every call (``_connection`` falsy) and
    replaces ``start`` / ``publish`` with async no-ops. Provisioning happens
    *before* ``start`` inside the guard, which is exactly what we want to assert
    on without a live broker.
    """
    broker = client.broker
    monkeypatch.setattr(broker, "_connection", None, raising=False)

    async def _noop_start() -> None:
        return None

    async def _noop_publish(*args, **kwargs):  # noqa: ANN002, ANN003
        return None

    monkeypatch.setattr(broker, "start", _noop_start, raising=False)
    monkeypatch.setattr(broker, "publish", _noop_publish, raising=False)


async def _invoke_once(client: Client, correlation_id: str) -> None:
    await client._invoke(
        topic="some.node.in",
        reply_topic=client.reply_topic,
        correlation_id=correlation_id,
        state=State(message_history=[], temp_instructions=None),
    )


def test_default_off_is_pure_noop_admin_never_constructed(monkeypatch) -> None:
    created = _install_fake_admin(monkeypatch)
    client = Client.connect("localhost:9092")  # provisioning defaults to disabled
    _stub_broker_io(client, monkeypatch)

    asyncio.run(_invoke_once(client, "c-default-off"))

    # Disabled provisioning must never reach the admin-client seam.
    assert created == []


def test_enabled_provisions_reply_topic_once_across_invokes(monkeypatch) -> None:
    created = _install_fake_admin(monkeypatch)
    cfg = ProvisioningConfig(enabled=True)
    client = Client.connect("localhost:9092", provisioning=cfg)
    _stub_broker_io(client, monkeypatch)

    asyncio.run(_invoke_once(client, "c-1"))
    asyncio.run(_invoke_once(client, "c-2"))
    asyncio.run(_invoke_once(client, "c-3"))

    # Provisioned exactly once: a single admin client built, one create call,
    # carrying exactly the client's reply topic.
    assert len(created) == 1
    assert created[0].start_calls == 1
    assert created[0].close_calls == 1
    assert len(created[0].create_calls) == 1
    names = [nt.name for nt in created[0].create_calls[0]]
    assert names == [client.reply_topic]


def test_enabled_provisions_before_broker_start(monkeypatch) -> None:
    created = _install_fake_admin(monkeypatch)
    cfg = ProvisioningConfig(enabled=True)
    client = Client.connect("localhost:9092", provisioning=cfg)

    order: list[str] = []
    broker = client.broker
    monkeypatch.setattr(broker, "_connection", None, raising=False)

    async def _start() -> None:
        order.append("broker_start")

    async def _publish(*args, **kwargs):  # noqa: ANN002, ANN003
        return None

    monkeypatch.setattr(broker, "start", _start, raising=False)
    monkeypatch.setattr(broker, "publish", _publish, raising=False)

    # Record when the admin start runs relative to the broker start.
    real_start = _FakeAdmin.start

    async def _tracking_start(self) -> None:  # noqa: ANN001
        order.append("provision_start")
        await real_start(self)

    monkeypatch.setattr(_FakeAdmin, "start", _tracking_start, raising=False)

    asyncio.run(_invoke_once(client, "c-order"))

    assert order == ["provision_start", "broker_start"]
    assert len(created) == 1
