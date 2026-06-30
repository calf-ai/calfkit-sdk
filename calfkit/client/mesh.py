"""Caller-side mesh view — ``client.mesh`` (spec ``docs/designs/caller-side-mesh-view-spec.md``).

A supported, read-only window onto the live control plane for a non-agent process
(a gateway, a bridge, a CLI, a dashboard): ``client.mesh.get_agents()`` /
``get_tools()`` return a fresh, name-keyed ``Mapping`` of the agents and tools
currently online, projecting the internal control-plane records to public DTOs.

The surface, DTOs, cache, and projections are built up across this feature's commits.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from calfkit.tuning import KTableReaderTuning, PositiveFiniteFloat

# === Public DTOs (frozen dataclasses; the RunEvent precedent) ====================
#
# Immutable projections of the wire records (spec §5.3). They carry no schema_version /
# node_kind / worker-instance stamps, and use the caller-friendly `last_seen` (not the
# wire's `last_heartbeat_at`). Callers distinguish the ToolInfo union by TYPE, not a
# discriminator field.


@dataclass(frozen=True)
class AgentInfo:
    """One online agent, projected from its ``AgentCard`` (spec §5.3)."""

    name: str
    """The agent's addressable identity (the control-plane wire key)."""
    description: str | None
    """The advertised public blurb (may be absent)."""
    last_seen: datetime
    """Aware UTC — the agent's last heartbeat; the liveness basis."""


@dataclass(frozen=True)
class ToolSpec:
    """One tool advertised within a toolbox (spec §5.3)."""

    name: str
    """The BARE tool name (e.g. ``search``), not the LLM-facing ``<toolbox>__search`` form."""
    description: str | None
    parameters_schema: dict[str, Any]


@dataclass(frozen=True)
class ToolNodeInfo:
    """One online function tool node — its single tool inlined (spec §5.3).

    A function tool node advertises exactly one tool, so it maps to a flat record (only a
    toolbox carries a list). ``name`` is the node name == tool name == capability key.
    """

    name: str
    description: str | None
    parameters_schema: dict[str, Any]
    last_seen: datetime


@dataclass(frozen=True)
class ToolboxInfo:
    """One online MCP toolbox — its name and the tools it advertises (spec §5.3).

    No toolbox-level ``description``: the wire ``CapabilityRecord`` carries none (descriptions
    live per :class:`ToolSpec`; a documented asymmetry).
    """

    name: str
    tools: tuple[ToolSpec, ...]
    last_seen: datetime


ToolInfo = ToolNodeInfo | ToolboxInfo
"""The public projection of a tool advertiser — a closed, ``match``-friendly union over the two
real shapes (mirrors ``RunEvent``): a :class:`ToolNodeInfo` (one function tool node) or a
:class:`ToolboxInfo` (one MCP toolbox). Branch by **type**, not a discriminator field (spec §5.3)."""


# === Reader-scoped configuration =================================================


class MeshViewConfig(BaseModel):
    """Reader-scoped tuning for the mesh views — set once via ``Client.connect(mesh_config=…)``
    (spec §5.4).

    Deliberately *not* the worker's ``ControlPlaneConfig``: it omits the publisher-only
    ``heartbeat_interval`` and ``bootstrap_servers`` (the client supplies that), and there is no
    ``ensure_topic`` (the observer opens naively) and no ``probe_interval`` (no probe).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    stale_after: PositiveFiniteFloat | None = None
    """Reader staleness override (seconds). ``None`` (default) derives the threshold from the
    record's heartbeat cadence."""
    catchup_timeout: PositiveFiniteFloat = 30.0
    """Bound (seconds) on a view's open-time catch-up gate (spec §6.2)."""
    reader_tuning: KTableReaderTuning = Field(default_factory=KTableReaderTuning)
    """Cadence for the view reader (idle ``barrier()`` latency)."""
