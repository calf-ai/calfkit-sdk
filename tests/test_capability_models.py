"""PR A: capability wire model, discovery config, and server_urls retention.

Spec: docs/designs/mcp-capability-discovery-spec.md §3.2 (wire format),
§8.3 (zero-config bootstrap derivation, MCPDiscoveryConfig defaults).
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from calfkit.client.client import Client
from calfkit.models.capability import (
    CAPABILITY_SCHEMA_VERSION,
    CapabilityRecord,
    CapabilityToolDef,
    is_unsupported_schema,
    record_to_bindings,
)
from calfkit.models.tool_dispatch import ToolBinding
from calfkit.worker.worker_config import MCPDiscoveryConfig


def make_record(**overrides) -> CapabilityRecord:
    defaults = dict(
        toolbox_id="docs_server",
        dispatch_topic="mcp_server.docs_server",
        tools=[
            CapabilityToolDef(
                name="search",
                description="Search the docs",
                parameters_json_schema={"type": "object", "properties": {"q": {"type": "string"}}},
            ),
            CapabilityToolDef(name="fetch", parameters_json_schema={"type": "object", "properties": {}}),
        ],
        published_at=datetime.now(tz=timezone.utc),
    )
    defaults.update(overrides)
    return CapabilityRecord(**defaults)


class TestCapabilityRecordWireFormat:
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
        assert restored.toolbox_id == "docs_server"

    def test_description_is_optional(self) -> None:
        record = make_record()
        assert record.tools[1].description is None

    def test_newer_major_schema_is_detected_not_rejected(self) -> None:
        # Decoding succeeds (ktables applies any valid JSON); the POLICY of
        # skipping newer-major records lives with the caller via this helper.
        newer = make_record(schema_version=CAPABILITY_SCHEMA_VERSION + 1)
        assert is_unsupported_schema(newer)
        assert not is_unsupported_schema(make_record())


class TestRecordToBindings:
    def test_builds_validatorless_bindings_with_record_topic(self) -> None:
        record = make_record()
        bindings = record_to_bindings(record)
        assert [b.name for b in bindings] == ["search", "fetch"]
        assert all(isinstance(b, ToolBinding) for b in bindings)
        assert all(b.dispatch_topic == "mcp_server.docs_server" for b in bindings)
        # Wire-crossing tools dispatch unvalidated (schema-only carve-out).
        assert all(b.validator is None for b in bindings)

    def test_tool_def_fields_survive(self) -> None:
        [search, _] = record_to_bindings(make_record())
        assert search.tool_def.description == "Search the docs"
        assert search.tool_def.parameters_json_schema == {"type": "object", "properties": {"q": {"type": "string"}}}


class TestMCPDiscoveryConfig:
    def test_defaults_match_spec(self) -> None:
        cfg = MCPDiscoveryConfig()
        assert cfg.topic == "mcp.capabilities"
        assert cfg.catchup_timeout == 30.0
        assert cfg.heartbeat_interval == 30.0
        assert cfg.bootstrap_servers is None  # None = derive from the client

    def test_rejects_non_positive_intervals(self) -> None:
        with pytest.raises(ValueError):
            MCPDiscoveryConfig(catchup_timeout=0)
        with pytest.raises(ValueError):
            MCPDiscoveryConfig(heartbeat_interval=-1)


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
