"""The ``max_message_bytes`` knob on ``Client.connect`` (design §4.1, §5 Leg 1, §4.3).

Covers design §8.1 (validation + default/custom propagation to the profile), §8.2's
broker arm (the reserved ``max_request_size`` kwarg), §8.5 (security derivation into the
profile via ``parse_security``), and §8.6 (the broker's producer actually receives the
guard). ``Client.connect`` is sync/lazy/no-I/O, so none of this needs a broker.
"""

from __future__ import annotations

import pytest
from faststream.kafka.security import parse_security
from faststream.security import SASLPlaintext

from calfkit import DEFAULT_MAX_MESSAGE_BYTES, Client

_FIVE_MIB = 5 * 1024 * 1024


# ── §8.1 validation + propagation ────────────────────────────────────────────────────────


def test_default_knob_lands_on_the_profile() -> None:
    client = Client.connect("localhost:9092")
    assert client._connection_profile is not None
    assert client._connection_profile.max_message_bytes == DEFAULT_MAX_MESSAGE_BYTES


def test_custom_knob_propagates_to_the_profile() -> None:
    client = Client.connect("localhost:9092", max_message_bytes=2 * 1024 * 1024)
    assert client._connection_profile is not None
    assert client._connection_profile.max_message_bytes == 2 * 1024 * 1024


@pytest.mark.parametrize("bad", [0, -1, True, False])
def test_invalid_knob_is_rejected(bad: object) -> None:
    with pytest.raises(ValueError, match="max_message_bytes"):
        Client.connect("localhost:9092", max_message_bytes=bad)  # type: ignore[arg-type]


def test_profile_carries_the_resolved_bootstrap() -> None:
    client = Client.connect(["a:9092", "b:9092"])
    assert client._connection_profile is not None
    assert client._connection_profile.bootstrap_servers == "a:9092,b:9092"


def test_directly_built_client_has_no_profile() -> None:
    # The __init__ path (no connect()) must default the profile to None, not AttributeError.
    from unittest.mock import Mock

    from calfkit.provisioning import ProvisioningConfig
    from calfkit.provisioning.ensurer import StartupTopicEnsurer

    client = Client(
        Mock(),
        Mock(),
        "inbox",
        emitter_id="e",
        firehose_buffer_size=1,
        deps_factory=None,
        provisioning=ProvisioningConfig(),
        startup_ensurer=StartupTopicEnsurer(config=ProvisioningConfig()),
        server_urls=None,
    )
    assert client._connection_profile is None


# ── §8.2 (broker arm): the knob is authoritative ─────────────────────────────────────────


def test_max_request_size_in_broker_kwargs_is_rejected() -> None:
    with pytest.raises(ValueError, match="max_message_bytes"):
        Client.connect("localhost:9092", max_request_size=123456)


# ── §8.5 security derivation into the profile ────────────────────────────────────────────


def test_security_object_lands_on_the_profile_as_aiokafka_kwargs() -> None:
    security = SASLPlaintext(username="svc", password="hunter2")
    client = Client.connect("localhost:9092", security=security)
    assert client._connection_profile is not None
    assert dict(client._connection_profile.security_opts) == parse_security(security)


def test_no_security_means_empty_security_opts() -> None:
    client = Client.connect("localhost:9092")
    assert client._connection_profile is not None
    assert dict(client._connection_profile.security_opts) == {}


# ── §8.8 Leg 3: the client inbox reader receives the fetch floor ─────────────────────────


def _inbox_subscriber(client: Client):  # noqa: ANN202 — FastStream subscriber type is internal
    subs = [s for s in client.broker.subscribers if client.inbox_topic in getattr(s, "topics", [])]
    assert subs, "the hub's inbox subscriber must be registered at connect()"
    return subs[0]


def test_inbox_reader_receives_the_fetch_floor() -> None:
    client = Client.connect("localhost:9092", max_message_bytes=7 * 1024 * 1024)
    args = _inbox_subscriber(client)._connection_args
    assert args["max_partition_fetch_bytes"] == 7 * 1024 * 1024
    assert args["fetch_max_bytes"] == 52_428_800  # knob < 50 MiB → aiokafka default preserved


def test_inbox_reader_fetch_max_bytes_raised_for_a_large_knob() -> None:
    knob = 64 * 1024 * 1024
    client = Client.connect("localhost:9092", max_message_bytes=knob)
    assert _inbox_subscriber(client)._connection_args["fetch_max_bytes"] == knob


# ── §8.2/§8.7 Leg 2: worker node subscribers receive the floor; escape hatches rejected ──


def test_worker_rejects_reserved_fetch_kwargs() -> None:
    from calfkit import Worker
    from calfkit.nodes import NodeDef

    client = Client.connect("localhost:9092")
    for reserved in ("max_partition_fetch_bytes", "fetch_max_bytes"):
        with pytest.raises(ValueError, match="max_message_bytes"):
            Worker(client, nodes=[NodeDef(node_id="n", subscribe_topics=["t"])], extra_subscribe_kwargs={reserved: 1})


def test_node_subscribers_receive_the_fetch_floor() -> None:
    from calfkit import Worker
    from calfkit.nodes import NodeDef

    client = Client.connect("localhost:9092", max_message_bytes=3 * 1024 * 1024)
    worker = Worker(client, nodes=[NodeDef(node_id="floored", subscribe_topics=["floor.in"])])
    worker.register_handlers()
    node_subs = [s for s in client.broker.subscribers if "floor.in" in getattr(s, "topics", [])]
    assert node_subs, "the node subscriber must be registered"
    args = node_subs[0]._connection_args
    assert args["max_partition_fetch_bytes"] == 3 * 1024 * 1024
    assert args["fetch_max_bytes"] == 52_428_800


def test_no_profile_means_no_fetch_floor_on_node_subscribers() -> None:
    from calfkit import Worker
    from calfkit.nodes import NodeDef

    client = Client.connect("localhost:9092")
    client._connection_profile = None  # the direct-built posture (design §5 Leg 2 None arm)
    worker = Worker(client, nodes=[NodeDef(node_id="bare", subscribe_topics=["bare.in"])])
    worker.register_handlers()
    node_subs = [s for s in client.broker.subscribers if "bare.in" in getattr(s, "topics", [])]
    args = node_subs[0]._connection_args
    # No floor injected: the keys are absent, so aiokafka's own defaults apply — today's behavior.
    assert "max_partition_fetch_bytes" not in args
    assert "fetch_max_bytes" not in args


# ── §8.6 the broker producer actually receives the guard ─────────────────────────────────


def test_broker_connection_kwargs_carry_the_guard() -> None:
    client = Client.connect("localhost:9092", max_message_bytes=_FIVE_MIB)
    assert client.broker._connection_kwargs["max_request_size"] == _FIVE_MIB


def test_broker_guard_follows_a_custom_knob() -> None:
    client = Client.connect("localhost:9092", max_message_bytes=7 * 1024 * 1024)
    assert client.broker._connection_kwargs["max_request_size"] == 7 * 1024 * 1024
