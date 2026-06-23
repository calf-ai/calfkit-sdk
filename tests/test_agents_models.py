"""AgentCard wire model on the ``calf.agents`` control plane (spec §4.2).

An :class:`AgentCard` is one agent instance's advertisement: identity (the agent's
``name``) is the wire key (``node_id`` × ``worker_id``, held by the substrate, never in
the value); the only content is an optional, length-bounded ``description``. It mirrors
``CapabilityRecord`` but is **minimal** (L7) — no dispatch topic (derived, ADR-0017), no
system prompt, no tool list — and bounds ``description`` **loudly** (L12).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest
from pydantic import ValidationError
from pydantic_core import PydanticUndefined

from calfkit.controlplane import ControlPlaneRecord
from calfkit.models.agents import (
    AGENT_CARD_DESCRIPTION_MAX,
    AGENT_CARD_SCHEMA_VERSION,
    AGENTS_TOPIC,
    AGENTS_VIEW_RESOURCE_KEY,
    AgentCard,
)


def make_card(**overrides: Any) -> AgentCard:
    now = datetime.now(tz=timezone.utc)
    defaults: dict[str, Any] = dict(
        started_at=now,
        last_heartbeat_at=now,
        heartbeat_interval=30.0,
        node_kind="agent",
        description="A helpful planner agent",
    )
    defaults.update(overrides)
    return AgentCard(**defaults)


class TestAgentCardWireFormat:
    def test_is_a_control_plane_record(self) -> None:
        # Carries the substrate's identity/liveness base (started_at, last_heartbeat_at,
        # heartbeat_interval, node_kind) so the view's staleness filter stays generic.
        assert issubclass(AgentCard, ControlPlaneRecord)
        card = make_card()
        assert card.heartbeat_interval == 30.0
        assert card.last_heartbeat_at is not None
        assert card.started_at is not None
        assert card.node_kind == "agent"

    def test_schema_version_has_a_concrete_default(self) -> None:
        # Load-bearing: ControlPlaneView derives its reader version from this default.
        default = AgentCard.model_fields["schema_version"].default
        assert default is not PydanticUndefined
        assert default == AGENT_CARD_SCHEMA_VERSION == 1

    def test_identity_is_not_carried_in_the_value(self) -> None:
        # The agent's name is the wire key, never a field on the record value.
        assert "node_id" not in AgentCard.model_fields
        assert "name" not in AgentCard.model_fields

    def test_description_is_optional_default_none(self) -> None:
        assert make_card(description=None).description is None
        now = datetime.now(tz=timezone.utc)
        bare = AgentCard(started_at=now, last_heartbeat_at=now, heartbeat_interval=30.0, node_kind="agent")
        assert bare.description is None

    def test_round_trips_through_json(self) -> None:
        card = make_card()
        restored = AgentCard.model_validate_json(card.model_dump_json())
        assert restored == card
        assert restored.schema_version == 1

    def test_tolerant_reader_ignores_unknown_fields(self) -> None:
        # Records outlive deploys; a newer writer may add fields this reader ignores.
        payload = make_card().model_dump_json()
        widened = payload[:-1] + ', "future_field": {"x": 1}}'
        restored = AgentCard.model_validate_json(widened)
        assert restored.description == "A helpful planner agent"

    def test_minimal_card_has_no_extra_content_fields(self) -> None:
        # L7: the card is minimal — name (the key) + bounded description ONLY.
        # No content_updated_at (capability-only), no published_at, no topic/prompt/tools.
        assert "content_updated_at" not in AgentCard.model_fields
        assert "published_at" not in AgentCard.model_fields
        assert "dispatch_topic" not in AgentCard.model_fields
        assert "tools" not in AgentCard.model_fields
        stamp_and_version = {"started_at", "last_heartbeat_at", "heartbeat_interval", "node_kind", "schema_version"}
        assert set(AgentCard.model_fields) == stamp_and_version | {"description"}


class TestLoudDescriptionCap:
    def test_at_max_length_constructs(self) -> None:
        make_card(description="x" * AGENT_CARD_DESCRIPTION_MAX)  # must not raise

    def test_over_max_length_raises_at_construction(self) -> None:
        # L12: an over-long description is a CONSTRUCTION error (never silent
        # truncation, never an oversize record that decode-fails and drops the node).
        with pytest.raises(ValidationError):
            make_card(description="x" * (AGENT_CARD_DESCRIPTION_MAX + 1))


class TestAgentsConstants:
    def test_topic_is_calf_agents(self) -> None:
        assert AGENTS_TOPIC == "calf.agents"

    def test_view_resource_key_is_distinct_from_capability(self) -> None:
        # L11: a distinct resource key from the capability view.
        from calfkit.models.capability import CAPABILITY_VIEW_RESOURCE_KEY

        assert AGENTS_VIEW_RESOURCE_KEY != CAPABILITY_VIEW_RESOURCE_KEY
        assert AGENTS_VIEW_RESOURCE_KEY == "calfkit.controlplane.agents_view"

    def test_description_max_is_512(self) -> None:
        assert AGENT_CARD_DESCRIPTION_MAX == 512
