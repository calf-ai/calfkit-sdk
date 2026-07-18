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


def test_repr_redacts_security_values() -> None:
    profile = _profile(security_opts={"sasl_plain_password": "hunter2", "sasl_plain_username": "svc"})
    rendered = repr(profile)
    assert "hunter2" not in rendered
    assert "svc" not in rendered  # values redacted wholesale, not just passwords
    assert "sasl_plain_password" in rendered  # keys stay visible for debuggability
