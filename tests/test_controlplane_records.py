"""Unit tests for the control-plane record base + identity envelope (spec §5)."""

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from calfkit.controlplane import ControlPlaneIdentity, ControlPlaneRecord


class _Rec(ControlPlaneRecord):
    """A concrete record: sets the required schema_version default + adds content."""

    schema_version: int = 1
    content: str


def _identity(**overrides: object) -> ControlPlaneIdentity:
    base: dict[str, object] = dict(
        node_id="n1",
        worker_id="w1",
        started_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        last_heartbeat_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        heartbeat_interval=30.0,
    )
    base.update(overrides)
    return ControlPlaneIdentity(**base)  # type: ignore[arg-type]


def test_subclass_constructs_from_identity_plus_content() -> None:
    """A concrete record is built as `R(**identity.model_dump(), <content>)`."""
    ident = _identity()
    rec = _Rec(**ident.model_dump(), content="hello")
    assert rec.schema_version == 1
    assert rec.node_id == "n1"
    assert rec.worker_id == "w1"
    assert rec.started_at == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert rec.last_heartbeat_at == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert rec.heartbeat_interval == 30.0
    assert rec.content == "hello"


def test_tolerant_reader_ignores_unknown_fields() -> None:
    """extra='ignore' — a newer writer's added field decodes without error and is dropped."""
    ident = _identity()
    data = {**ident.model_dump(mode="json"), "content": "x", "future_field": 123}
    rec = _Rec.model_validate(data)
    assert rec.content == "x"
    assert not hasattr(rec, "future_field")


def test_base_has_no_schema_version_default() -> None:
    """The base intentionally has no schema_version default (abstract-by-omission, spec §5)."""
    ident = _identity()
    with pytest.raises(ValidationError):
        ControlPlaneRecord(**ident.model_dump())  # missing required schema_version


def test_identity_is_frozen() -> None:
    """ControlPlaneIdentity is a frozen envelope — built once per tick, never mutated."""
    ident = _identity()
    with pytest.raises(ValidationError):
        ident.node_id = "other"  # type: ignore[misc]


def test_identity_carries_the_five_envelope_fields() -> None:
    """The identity envelope is exactly the base's non-schema_version fields (spec §5)."""
    assert set(ControlPlaneIdentity.model_fields) == {
        "node_id",
        "worker_id",
        "started_at",
        "last_heartbeat_at",
        "heartbeat_interval",
    }
