"""Capability wire model (migrated onto the control-plane substrate).

The capability record is now a :class:`ControlPlaneRecord` subclass: identity
(``toolbox_id`` × ``worker_id``) is the wire key, never in the value; liveness
(``last_heartbeat_at``) is split from content currency (``content_updated_at``).
See ``docs/designs/mcp-capability-substrate-migration-plan.md``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest
from pydantic_core import PydanticUndefined

from calfkit.client import Client
from calfkit.controlplane import ControlPlaneRecord
from calfkit.models.capability import (
    CAPABILITY_SCHEMA_VERSION,
    CAPABILITY_TOPIC,
    CapabilityRecord,
    CapabilityToolDef,
    record_to_bindings,
)
from calfkit.models.tool_dispatch import ToolBinding


def make_record(**overrides: Any) -> CapabilityRecord:
    now = datetime.now(tz=timezone.utc)
    defaults: dict[str, Any] = dict(
        started_at=now,
        last_heartbeat_at=now,
        heartbeat_interval=30.0,
        node_kind="toolbox",
        dispatch_topic="mcp_server.docs_server",
        tools=[
            CapabilityToolDef(
                name="search",
                description="Search the docs",
                parameters_json_schema={"type": "object", "properties": {"q": {"type": "string"}}},
            ),
            CapabilityToolDef(name="fetch", parameters_json_schema={"type": "object", "properties": {}}),
        ],
        content_updated_at=now,
    )
    defaults.update(overrides)
    return CapabilityRecord(**defaults)


class TestCapabilityRecordWireFormat:
    def test_is_a_control_plane_record(self) -> None:
        # The migration's foundation: capability records carry the substrate's
        # identity/liveness base (started_at, last_heartbeat_at, heartbeat_interval).
        assert issubclass(CapabilityRecord, ControlPlaneRecord)
        record = make_record()
        assert record.heartbeat_interval == 30.0
        assert record.last_heartbeat_at is not None
        assert record.started_at is not None

    def test_schema_version_has_a_concrete_default(self) -> None:
        # Load-bearing: ControlPlaneView derives its reader version from this
        # default and rejects a record type that lacks one.
        default = CapabilityRecord.model_fields["schema_version"].default
        assert default is not PydanticUndefined
        assert default == CAPABILITY_SCHEMA_VERSION == 1

    def test_identity_is_not_carried_in_the_value(self) -> None:
        # toolbox_id is the wire key now, never a field on the record value.
        assert "toolbox_id" not in CapabilityRecord.model_fields

    def test_liveness_and_content_currency_are_separate_fields(self) -> None:
        # CRITICAL-3 fix: a heartbeat refreshes last_heartbeat_at without
        # touching content_updated_at, so the two facts are distinguishable.
        assert "last_heartbeat_at" in CapabilityRecord.model_fields
        assert "content_updated_at" in CapabilityRecord.model_fields
        assert "published_at" not in CapabilityRecord.model_fields

    def test_round_trips_through_json(self) -> None:
        record = make_record()
        restored = CapabilityRecord.model_validate_json(record.model_dump_json())
        assert restored == record
        assert restored.schema_version == CAPABILITY_SCHEMA_VERSION == 1

    def test_tolerant_reader_ignores_unknown_fields(self) -> None:
        # Records outlive deploys; a NEWER writer may add fields this reader
        # doesn't know. They must be ignored, not rejected.
        payload = make_record().model_dump_json()
        widened = payload[:-1] + ', "future_field": {"x": 1}}'
        restored = CapabilityRecord.model_validate_json(widened)
        assert restored.dispatch_topic == "mcp_server.docs_server"

    def test_description_is_optional(self) -> None:
        record = make_record()
        assert record.tools[1].description is None


class TestCapabilityConstants:
    def test_topic_is_the_renamed_control_plane_topic(self) -> None:
        assert CAPABILITY_TOPIC == "calf.capabilities"


class TestRecordToBindings:
    def test_builds_validatorless_bindings_with_record_topic(self) -> None:
        record = make_record()  # node_kind="toolbox"
        bindings = record_to_bindings(record, name="docs_server")
        # C1: a toolbox's LLM-facing tool names are namespaced <node_id>__<tool>.
        assert [b.name for b in bindings] == ["docs_server__search", "docs_server__fetch"]
        assert all(isinstance(b, ToolBinding) for b in bindings)
        assert all(b.dispatch_topic == "mcp_server.docs_server" for b in bindings)
        # Wire-crossing tools dispatch unvalidated (schema-only carve-out).
        assert all(b.validator is None for b in bindings)

    def test_tool_def_fields_survive(self) -> None:
        [search, _] = record_to_bindings(make_record(), name="docs_server")
        assert search.tool_def.description == "Search the docs"
        assert search.tool_def.parameters_json_schema == {"type": "object", "properties": {"q": {"type": "string"}}}

    def test_tool_node_record_stays_bare(self) -> None:
        # C2: namespacing is toolbox-scoped. A function tool node's record
        # (node_kind="tool") expands to BARE names, preserving the
        # node_id == tool name == capability key identity (ADR-0013).
        bindings = record_to_bindings(make_record(node_kind="tool"), name="docs_server")
        assert [b.name for b in bindings] == ["search", "fetch"]


class TestClientServerUrlsRetention:
    def test_connect_retains_explicit_url(self) -> None:
        client = Client.connect("kafka-a:9092")
        assert client.server_urls == "kafka-a:9092"

    def test_connect_normalizes_multiple_urls(self) -> None:
        client = Client.connect(["kafka-a:9092", "kafka-b:9092"])
        assert client.server_urls == "kafka-a:9092,kafka-b:9092"

    def test_connect_retains_env_fallback(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CALF_HOST_URL", "env-kafka:9092")
        client = Client.connect()
        assert client.server_urls == "env-kafka:9092"

    def test_one_shot_iterable_reaches_broker_intact(self) -> None:
        # Regression: normalization must not consume a generator before the
        # broker sees it — the broker must receive real bootstrap servers,
        # not just the property reporting the right string.
        client = Client.connect(url for url in ["kafka-a:9092", "kafka-b:9092"])
        assert client.server_urls == "kafka-a:9092,kafka-b:9092"
        # The broker must receive the LIST form: FastStream wraps a str into
        # [str] and aiokafka never comma-splits a list element, so anything
        # other than both distinct hosts here means multi-host is broken.
        brokers = client.broker._connection_kwargs["bootstrap_servers"]
        assert brokers == ["kafka-a:9092", "kafka-b:9092"]

    def test_server_urls_is_read_only(self) -> None:
        client = Client.connect("kafka-a:9092")
        with pytest.raises(AttributeError):
            client.server_urls = "other:9092"  # type: ignore[misc]
