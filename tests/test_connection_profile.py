"""ConnectionProfile — the client's single carrier of Kafka connection facts (design §4.2).

Covers design §8.3: the two kwargs-derivation methods (guard vs floor asymmetry), the
``fetch_max_bytes = max(50 MiB, knob)`` boundary on both sides, immutability (frozen +
read-only, defensively-copied ``security_opts``), and the secrets-redacting ``__repr__``.
"""

from __future__ import annotations

import dataclasses

import pytest

from calfkit.client._connection import DEFAULT_MAX_MESSAGE_BYTES, ConnectionProfile

_FIVE_MIB = 5 * 1024 * 1024
_FIFTY_MIB = 52_428_800  # aiokafka's fetch_max_bytes default


def _profile(max_message_bytes: int = _FIVE_MIB, **kwargs: object) -> ConnectionProfile:
    defaults: dict[str, object] = {"bootstrap_servers": "localhost:9092", "security_opts": {}}
    defaults.update(kwargs)
    return ConnectionProfile(max_message_bytes=max_message_bytes, **defaults)  # type: ignore[arg-type]


def test_default_max_message_bytes_is_5_mib() -> None:
    assert DEFAULT_MAX_MESSAGE_BYTES == _FIVE_MIB


def test_default_is_publicly_exported_from_calfkit_root() -> None:
    import calfkit

    assert calfkit.DEFAULT_MAX_MESSAGE_BYTES == _FIVE_MIB
    assert "DEFAULT_MAX_MESSAGE_BYTES" in calfkit.__all__


def test_producer_size_kwargs_carries_the_guard() -> None:
    assert _profile().producer_size_kwargs() == {"max_request_size": _FIVE_MIB}


def test_consumer_fetch_kwargs_floors_partition_cap_at_the_knob() -> None:
    assert _profile().consumer_fetch_kwargs()["max_partition_fetch_bytes"] == _FIVE_MIB


def test_fetch_max_bytes_never_lowered_below_aiokafka_default() -> None:
    # 5 MiB knob: fetch_max_bytes stays at aiokafka's 50 MiB (never throttle multi-partition fetch).
    assert _profile().consumer_fetch_kwargs()["fetch_max_bytes"] == _FIFTY_MIB


def test_fetch_max_bytes_raised_when_knob_exceeds_aiokafka_default() -> None:
    knob = 64 * 1024 * 1024
    assert _profile(max_message_bytes=knob).consumer_fetch_kwargs()["fetch_max_bytes"] == knob


def test_type_backstop_rejects_invalid_values_at_construction() -> None:
    # connect() validates first with a richer error, but the TYPE must also refuse —
    # dataclasses.replace() re-enters __post_init__ while bypassing connect() entirely.
    import dataclasses as dc

    for bad in (0, -5, True, False):
        with pytest.raises(ValueError, match="max_message_bytes"):
            ConnectionProfile(bootstrap_servers="a:9092", security_opts={}, max_message_bytes=bad)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="bootstrap_servers"):
        ConnectionProfile(bootstrap_servers="", security_opts={}, max_message_bytes=1)
    with pytest.raises(ValueError, match="bootstrap_servers"):
        dc.replace(_profile(), bootstrap_servers="")  # the replace() door is guarded too


def test_unhashable_at_the_protocol_level() -> None:
    # __hash__ = None: an isinstance(…, Hashable) guard must not be misled into a
    # call-time TypeError (mapping field). Equality stays value-based.
    from collections.abc import Hashable

    assert not isinstance(_profile(), Hashable)
    with pytest.raises(TypeError):
        hash(_profile())
    assert _profile() == _profile()


def test_profile_is_frozen() -> None:
    with pytest.raises(dataclasses.FrozenInstanceError):
        _profile().max_message_bytes = 1  # type: ignore[misc]


def test_security_opts_is_read_only() -> None:
    profile = _profile(security_opts={"sasl_plain_username": "svc"})
    with pytest.raises(TypeError):
        profile.security_opts["sasl_plain_username"] = "evil"  # type: ignore[index]


def test_security_opts_is_defensively_copied() -> None:
    passed: dict[str, object] = {"sasl_plain_username": "svc"}
    profile = _profile(security_opts=passed)
    passed["sasl_plain_username"] = "mutated"
    assert profile.security_opts["sasl_plain_username"] == "svc"


def test_ktables_connection_maps_the_profile_onto_the_config() -> None:
    # §8.4: security → common_opts (all three client types), the guard → producer_opts,
    # the floor → consumer_opts; bootstrap carried verbatim.
    security = {"security_protocol": "SASL_PLAINTEXT", "sasl_mechanism": "PLAIN", "sasl_plain_username": "svc", "sasl_plain_password": "pw"}
    profile = _profile(security_opts=security, bootstrap_servers="a:9092,b:9092")
    conn = profile.ktables_connection()

    from ktables import KafkaConnectionConfig

    assert isinstance(conn, KafkaConnectionConfig)
    assert conn.bootstrap_servers == "a:9092,b:9092"
    assert dict(conn.common_opts) == security
    assert dict(conn.producer_opts) == profile.producer_size_kwargs()
    assert dict(conn.consumer_opts) == profile.consumer_fetch_kwargs()
    assert dict(conn.admin_opts) == {}


def test_ktables_connection_import_is_lazy() -> None:
    # The profile module must not eagerly import ktables (design §5 Leg 4: the config
    # materializes only inside the lazy blocks). Subprocess avoids sys.modules pollution.
    import subprocess
    import sys

    code = (
        "import sys, calfkit.client._connection\n"
        "leaked = sorted(m for m in sys.modules if m == 'ktables' or m.startswith('ktables.'))\n"
        "assert not leaked, leaked\n"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_client_import_does_not_load_agent_worker_or_provider_stacks() -> None:
    # Importing any submodule executes calfkit's package initializer first. A caller that only
    # needs Client must not pay for the agent, worker, and model-provider stacks as a side effect.
    import subprocess
    import sys

    code = (
        "import sys\n"
        "from calfkit.client import Client\n"
        "assert Client is not None\n"
        "blocked = ('calfkit.nodes.agent', 'calfkit.providers.pydantic_ai', 'calfkit.worker.worker')\n"
        "loaded = [name for name in blocked if name in sys.modules]\n"
        "assert not loaded, loaded\n"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_every_root_export_resolves() -> None:
    # Each lazy name lives in three places (TYPE_CHECKING block, __all__, _LAZY_EXPORTS); a typo in
    # _LAZY_EXPORTS would stay silent until someone imports that name. Resolve every export eagerly.
    import calfkit

    for name in calfkit.__all__:
        getattr(calfkit, name)
    with pytest.raises(AttributeError):
        calfkit.not_a_real_export


def test_repr_redacts_security_values() -> None:
    profile = _profile(security_opts={"sasl_plain_password": "hunter2", "sasl_plain_username": "svc"})
    rendered = repr(profile)
    assert "hunter2" not in rendered
    assert "svc" not in rendered  # values redacted wholesale, not just passwords
    assert "sasl_plain_password" in rendered  # keys stay visible for debuggability
