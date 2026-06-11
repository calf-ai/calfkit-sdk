"""Tests for the client's provisioning config + security handling.

The reply-topic *provisioning behaviour* (declared into a ``StartupTopicEnsurer``
and created at broker start, reusing FastStream's admin client) lives in
``tests/test_startup_provisioning.py``. Since provisioning no longer builds a
second admin client, the client no longer captures ``server_urls`` /
``security_kwargs``: security is configured the FastStream way, via a
``security=`` object that flows to the broker (and its admin client).
"""

import pytest

from calfkit.client import Client
from calfkit.provisioning import ProvisioningConfig


def test_default_provisioning_property_is_disabled_config() -> None:
    client = Client.connect("localhost:9092")

    assert isinstance(client.provisioning, ProvisioningConfig)
    # Never None; defaults to a disabled config so .enabled is always safe.
    assert client.provisioning.enabled is False


def test_provisioning_property_reflects_passed_config() -> None:
    cfg = ProvisioningConfig(enabled=True, num_partitions=3)
    client = Client.connect("localhost:9092", provisioning=cfg)

    assert client.provisioning is cfg


def test_connect_accepts_a_faststream_security_object() -> None:
    # The supported way to configure security: a FastStream `security=` object,
    # which flows to the broker (and the admin client used for provisioning).
    from faststream.security import SASLPlaintext

    client = Client.connect("localhost:9092", security=SASLPlaintext(username="u", password="p"))

    assert client.provisioning.enabled is False  # constructed without error


def test_connect_rejects_raw_security_protocol_kwarg() -> None:
    # Raw security kwargs are no longer accepted; the client gives an actionable
    # migration error pointing at `security=`, not a cryptic KafkaBroker TypeError.
    with pytest.raises(ValueError, match="security="):
        Client.connect("localhost:9092", security_protocol="SASL_PLAINTEXT")


def test_connect_rejects_raw_sasl_plain_kwargs() -> None:
    with pytest.raises(ValueError, match="security="):
        Client.connect("localhost:9092", sasl_plain_username="u", sasl_plain_password="p")


def test_connect_rejects_illegal_reply_topic_name() -> None:
    # An explicit reply_topic is the wire callback on EVERY start()/execute()
    # frame and the client's own subscription — a Kafka-illegal name would
    # recreate exactly the cross-process metadata stall send(reply_to=...)
    # validation exists to prevent. Same rule, same loud client-side rejection.
    for bad in ("has space", "foo/bar", "a" * 250, ".", ".."):
        with pytest.raises(ValueError, match="not a valid Kafka topic name"):
            Client.connect("localhost:9092", reply_topic=bad)

    # Explicit legal names still work.
    client = Client.connect("localhost:9092", reply_topic="my-app.replies_1")
    assert client.reply_topic == "my-app.replies_1"


def test_client_never_captures_security_kwargs() -> None:
    """Credentials flow exclusively through FastStream's ``security=`` object
    (the #188 invariant): the client must never capture raw security kwargs.

    Note: this pin originally also covered ``server_urls``. That half was
    deliberately relaxed for MCP capability discovery — the client now retains
    the (non-credential) bootstrap URL it was connected with, so control-plane
    consumers are zero-config. Addressing data may be retained; security
    material may not.
    """
    client = Client.connect("localhost:9092")

    assert not hasattr(client, "security_kwargs")
    # The retained URL is addressing data only — never credentials.
    assert client.server_urls == "localhost:9092"
