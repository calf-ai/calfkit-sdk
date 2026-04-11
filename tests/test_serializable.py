"""Roundtrip serialization tests for all network-serialized models.

These tests verify that every model used in the Kafka wire format survives
``model_dump_json()`` → ``model_validate_json()`` (or the TypeAdapter equivalent
for standalone dataclasses).  The existing test suite uses ``TestKafkaBroker``
which passes objects in-memory, bypassing actual JSON serde — so these tests
catch missing fields, broken discriminated unions, dataclass quirks, etc.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Roundtrip helpers
# ---------------------------------------------------------------------------


def assert_json_roundtrip(instance):
    """Assert a Pydantic BaseModel survives JSON roundtrip."""
    json_bytes = instance.model_dump_json()
    cls = type(instance)
    reconstructed = cls.model_validate_json(json_bytes)
    assert reconstructed == instance, f"Roundtrip mismatch for {cls.__name__}"


# ---------------------------------------------------------------------------
# Factory functions
# ---------------------------------------------------------------------------


def test_envelope_serialization_roundtrip(make_envelope):
    assert_json_roundtrip(make_envelope)
