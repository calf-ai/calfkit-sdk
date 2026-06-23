"""Agent control-plane wire model (spec §4.2).

An :class:`AgentCard` is one agent instance's advertisement on the ``calf.agents``
control-plane topic — keyed on the wire by ``node_id`` × ``worker_id`` (the agent's
``name`` × its worker instance; held by the substrate, never in the value). Records are
calfkit-owned models: compacted records outlive deploys, so the wire shape must not move
when a vendored library does.

The card is **minimal** (spec §7 / L7): the agent's name is the wire key, and the only
content is an optional, length-bounded ``description`` — no input topic (the caller
derives ``agent.{name}.private.input``, ADR-0017), no system prompt, no tool list, so an
agent's internals never leak.

Reader policy: tolerant (unknown fields ignored — a newer writer may add fields).
Staleness and newer-``schema_version`` filtering are owned by the reader's
:class:`~calfkit.controlplane.ControlPlaneView`.
"""

from __future__ import annotations

from pydantic import Field

from calfkit.controlplane import ControlPlaneRecord

AGENT_CARD_SCHEMA_VERSION = 1
"""The record schema major this reader understands. Bump only on breaking wire
changes; additive fields ride on the tolerant reader instead."""

AGENTS_TOPIC = "calf.agents"
"""The cluster-wide compacted control-plane topic every agent advertises to.

A control-plane topic (one record schema per topic): the agent's ``@advertises``
factory and the reader's ``ControlPlaneView[AgentCard]`` both bind to it. Provisioned
``cleanup.policy=compact`` like ``calf.capabilities``."""

AGENT_CARD_DESCRIPTION_MAX = 512
"""Max chars for the directory ``description`` — a short blurb, not a system prompt.
Bounded loudly: an over-long value fails at construction (L12)."""

AGENTS_VIEW_RESOURCE_KEY = "calfkit.controlplane.agents_view"
"""Worker resource-bag key under which the Agents View (a
``ControlPlaneView[AgentCard]``) is published to hosted nodes. Distinct from the
capability view's key (L11): a different topic / record / trigger."""


class AgentCard(ControlPlaneRecord):
    """One agent instance's current advertisement on the ``calf.agents`` control plane.

    A :class:`~calfkit.controlplane.ControlPlaneRecord` (identity ``node_id`` ×
    ``worker_id`` is the wire key; the worker stamps ``started_at`` /
    ``last_heartbeat_at`` / ``heartbeat_interval`` / ``node_kind``) plus a single optional
    ``description``. **No input topic** (derived from the name, ADR-0017), no system
    prompt, no tool list — the minimal card (L7).

    ``description`` is bounded **loudly** (``Field(max_length=…)``, L12): an over-long
    value raises at construction, so the fail-loud first advert surfaces it rather than
    silently truncating or emitting an oversize record that decode-fails (which would drop
    the agent from every reader's view).

    **Precondition for the collapsed view:** all replicas of one agent (same ``name``)
    should advertise an *equivalent* ``description``. The view collapses replicas to one
    record by most-recent heartbeat — it does not merge — so divergent descriptions would
    flap the visible card tick-to-tick. Trivially satisfied: ``description`` is a static
    ``Agent(description=…)`` construction value, identical across an agent's replicas.

    Inherits the base's tolerant, frozen ``model_config`` (``extra="ignore"``); the value
    object is rebuilt per heartbeat tick, never mutated.
    """

    schema_version: int = AGENT_CARD_SCHEMA_VERSION
    description: str | None = Field(default=None, max_length=AGENT_CARD_DESCRIPTION_MAX)
