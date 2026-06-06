"""Tests for the client's provisioning-related read-only properties.

The reply-topic *provisioning behaviour* itself (declared into a
``StartupTopicEnsurer`` and created at broker start) lives in
``tests/test_startup_provisioning.py``; this file only covers the public
``provisioning`` / ``server_urls`` / ``security_kwargs`` surface on the client.
"""

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
