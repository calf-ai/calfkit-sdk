"""Caller-side mesh view — ``client.mesh`` (spec ``docs/designs/caller-side-mesh-view-spec.md``).

A supported, read-only window onto the live control plane for a non-agent process
(a gateway, a bridge, a CLI, a dashboard): ``client.mesh.get_agents()`` /
``get_tools()`` return a fresh, name-keyed ``Mapping`` of the agents and tools
currently online, projecting the internal control-plane records to public DTOs.

The surface, DTOs, cache, and projections are built up across this feature's commits.
"""

from __future__ import annotations

import copy
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field

from calfkit.controlplane.records import ControlPlaneRecord
from calfkit.models.agents import AgentCard
from calfkit.models.capability import CapabilityRecord
from calfkit.tuning import KTableReaderTuning, PositiveFiniteFloat

logger = logging.getLogger(__name__)

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


# === Projections (wire record -> public DTO, spec §6.5) ==========================
#
# Pure over one record; a record that does not project is skipped (degrade, don't crash —
# mirroring resolve_capability). The reachable skips are an unknown future node_kind and a
# "tool" record whose tools list is not exactly one element. A malformed ToolSpec is not a
# projection concern: parameters_json_schema is a required wire field, so such a record fails
# wire-decode in ktables before projection.


def _project_agent(node_id: str, record: AgentCard) -> AgentInfo:
    """Project an ``AgentCard`` to a public :class:`AgentInfo`.

    ``name`` is the wire key (``node_id``); ``last_seen`` is the wire ``last_heartbeat_at``.
    """
    return AgentInfo(name=node_id, description=record.description, last_seen=record.last_heartbeat_at)


def _project_tool(node_id: str, record: CapabilityRecord) -> ToolInfo | None:
    """Project a ``CapabilityRecord`` to a :class:`ToolNodeInfo` / :class:`ToolboxInfo`, or
    ``None`` when it is not a projectable shape.

    Dispatch on ``node_kind``: ``"tool"`` -> one inlined tool (the record must carry exactly one;
    a foreign/malformed record with 0 or >1 is skipped, never blind-indexed); ``"toolbox"`` -> the
    full tool list with **bare** names; any other kind -> ``None``. ``parameters_schema`` is
    deep-copied so a caller mutating the DTO never reaches the live cached record.
    """
    if record.node_kind == "tool":
        if len(record.tools) != 1:
            return None
        tool = record.tools[0]
        return ToolNodeInfo(
            name=node_id,
            description=tool.description,
            parameters_schema=copy.deepcopy(tool.parameters_json_schema),
            last_seen=record.last_heartbeat_at,
        )
    if record.node_kind == "toolbox":
        return ToolboxInfo(
            name=node_id,
            tools=tuple(
                ToolSpec(
                    name=tool.name,  # bare, not the LLM-facing <toolbox>__tool form
                    description=tool.description,
                    parameters_schema=copy.deepcopy(tool.parameters_json_schema),
                )
                for tool in record.tools
            ),
            last_seen=record.last_heartbeat_at,
        )
    return None


_R = TypeVar("_R", bound=ControlPlaneRecord)
_D = TypeVar("_D")


class _Projector(Generic[_R, _D]):
    """Projects a control-plane view ``snapshot()`` to a name-keyed DTO map, skipping records that
    do not project and logging each skip **once** (deduped, mirroring the view's schema-skip log).

    Holds the per-kind skip-log dedup state, so it is 1:1 with the long-lived cached view and is
    *not* a per-call function (which would re-log every read).
    """

    def __init__(self, project_one: Callable[[str, _R], _D | None], *, label: str) -> None:
        self._project_one = project_one
        self._label = label
        self._logged_skips: set[tuple[str, str]] = set()  # (node_id, node_kind) already warned

    def __call__(self, snapshot: dict[str, _R]) -> dict[str, _D]:
        out: dict[str, _D] = {}
        for node_id, record in snapshot.items():
            projected = self._project_one(node_id, record)
            if projected is None:
                self._log_skip_once(node_id, record)
                continue
            out[node_id] = projected
        return out

    def _log_skip_once(self, node_id: str, record: _R) -> None:
        key = (node_id, record.node_kind)
        if key in self._logged_skips:
            return
        self._logged_skips.add(key)
        logger.warning(
            "mesh %s view: skipping node=%s (node_kind=%s) — not a projectable control-plane record",
            self._label,
            node_id,
            record.node_kind,
        )
