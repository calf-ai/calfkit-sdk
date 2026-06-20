"""Unit tests for the control-plane record base + worker stamp (spec §5)."""

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from calfkit.controlplane import ControlPlaneRecord, ControlPlaneStamp


class _Rec(ControlPlaneRecord):
    """A concrete record: sets the required schema_version default + adds content."""

    schema_version: int = 1
    content: str


def _stamp(**overrides: object) -> ControlPlaneStamp:
    base: dict[str, object] = dict(
        started_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        last_heartbeat_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        heartbeat_interval=30.0,
        node_kind="node",
    )
    base.update(overrides)
    return ControlPlaneStamp(**base)  # type: ignore[arg-type]


def test_subclass_constructs_from_stamp_plus_content() -> None:
    """A concrete record is built as `R(**stamp.model_dump(), <content>)`."""
    stamp = _stamp()
    rec = _Rec(**stamp.model_dump(), content="hello")
    assert rec.schema_version == 1
    assert rec.started_at == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert rec.last_heartbeat_at == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert rec.heartbeat_interval == 30.0
    assert rec.content == "hello"


def test_identity_is_not_carried_in_the_value() -> None:
    """node_id / worker_id are the wire key, never duplicated in the record value (Option D)."""
    assert "node_id" not in ControlPlaneRecord.model_fields
    assert "worker_id" not in ControlPlaneRecord.model_fields
    assert "node_id" not in _Rec.model_fields


def test_record_is_a_stamp_plus_schema_version() -> None:
    """Tidy structural form: the record extends the stamp, so worker fields are declared once."""
    assert issubclass(ControlPlaneRecord, ControlPlaneStamp)
    assert set(ControlPlaneRecord.model_fields) == {"started_at", "last_heartbeat_at", "heartbeat_interval", "node_kind", "schema_version"}


def test_stamp_carries_only_worker_owned_fields() -> None:
    assert set(ControlPlaneStamp.model_fields) == {"started_at", "last_heartbeat_at", "heartbeat_interval", "node_kind"}


def test_stamp_carries_node_kind() -> None:
    """node_kind is worker-stamped onto every record (over-pull guard + mixed-kind observability)."""
    assert _stamp(node_kind="agent").node_kind == "agent"


def test_node_kind_is_required() -> None:
    """node_kind has no default (a breaking wire change) — a stamp without it does not decode."""
    with pytest.raises(ValidationError):
        ControlPlaneStamp(  # type: ignore[call-arg]
            started_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            last_heartbeat_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            heartbeat_interval=30.0,
        )


def test_tolerant_reader_ignores_unknown_fields() -> None:
    """extra='ignore' — a newer writer's added field decodes without error and is dropped."""
    stamp = _stamp()
    data = {**stamp.model_dump(mode="json"), "content": "x", "future_field": 123}
    rec = _Rec.model_validate(data)
    assert rec.content == "x"
    assert not hasattr(rec, "future_field")


def test_base_has_no_schema_version_default() -> None:
    """The base intentionally has no schema_version default (abstract-by-omission, spec §5)."""
    stamp = _stamp()
    with pytest.raises(ValidationError):
        ControlPlaneRecord(**stamp.model_dump())  # missing required schema_version


def test_stamp_is_frozen() -> None:
    stamp = _stamp()
    with pytest.raises(ValidationError):
        stamp.heartbeat_interval = 99.0  # type: ignore[misc]


def test_record_is_frozen() -> None:
    rec = _Rec(**_stamp().model_dump(), content="x")
    with pytest.raises(ValidationError):
        rec.content = "y"  # type: ignore[misc]
