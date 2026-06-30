"""Caller-side mesh view — ``client.mesh`` (spec ``docs/designs/caller-side-mesh-view-spec.md``).

A supported, read-only window onto the live control plane for a non-agent process
(a gateway, a bridge, a CLI, a dashboard): ``client.mesh.get_agents()`` /
``get_tools()`` return a fresh, name-keyed ``Mapping`` of the agents and tools
currently online, projecting the internal control-plane records to public DTOs.
"""

from __future__ import annotations

import asyncio
import contextlib
import copy
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field

from calfkit.controlplane.records import ControlPlaneRecord
from calfkit.controlplane.view import ControlPlaneView
from calfkit.exceptions import ClientClosedError, MeshUnavailableError
from calfkit.models.agents import AGENTS_TOPIC, AgentCard
from calfkit.models.capability import CAPABILITY_TOPIC, CapabilityRecord
from calfkit.tuning import KTableReaderTuning, PositiveFiniteFloat

if TYPE_CHECKING:
    from calfkit.client.caller import Client

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

    A function tool node advertises exactly one tool, so it maps to a flat ``ToolNodeInfo`` (only
    ``ToolboxInfo`` carries a list). ``name`` is the node name == tool name == capability key.
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
    ``heartbeat_interval``, and ``bootstrap_servers`` (which the client supplies), and there is no
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


# === The mesh cache (per-kind single-flight, cancel-safe; spec §7) ===============


@dataclass(frozen=True)
class _Kind(Generic[_R, _D]):
    """Binds one control-plane kind: its ``name`` (cache key + message/log label), topic, wire record
    type, and projector."""

    name: str
    topic: str
    record_type: type[_R]
    project_one: Callable[[str, _R], _D | None]


_AGENTS: _Kind[AgentCard, AgentInfo] = _Kind(name="agents", topic=AGENTS_TOPIC, record_type=AgentCard, project_one=_project_agent)
_TOOLS: _Kind[CapabilityRecord, ToolInfo] = _Kind(name="tools", topic=CAPABILITY_TOPIC, record_type=CapabilityRecord, project_one=_project_tool)


@dataclass(frozen=True)
class _Cell(Generic[_R, _D]):
    """The per-kind cache cell — the open ``Task`` IS the single-flight slot; ``project`` carries the
    kind-bound projector + its deduped skip-log (1:1 with the cached view)."""

    task: asyncio.Task[ControlPlaneView[_R]]
    project: _Projector[_R, _D]


class Mesh:
    """Caller-side ``client.mesh`` — a cached singleton owning one Control Plane View per kind (spec §7).

    Zero-I/O until the first ``get_*``; that lazily opens and catches up the kind's view (per-kind
    single-flight, cancel-safe), and every later read is an O(1) snapshot of the live cached view.
    The views are torn down at ``Client.aclose()``.
    """

    def __init__(self, client: Client, config: MeshViewConfig | None = None) -> None:
        self._client = client
        self._config = config if config is not None else MeshViewConfig()
        self._cells: dict[str, _Cell[Any, Any]] = {}
        self._lock = asyncio.Lock()  # guards _cells + _closed only — NEVER held across start()
        self._closed = False

    async def get_agents(self) -> Mapping[str, AgentInfo]:
        return await self._read(_AGENTS)

    async def get_tools(self) -> Mapping[str, ToolInfo]:
        return await self._read(_TOOLS)

    async def _read(self, kind: _Kind[_R, _D]) -> Mapping[str, _D]:
        cell = await self._cell(kind)
        view = cell.task.result()  # the resolved view, read as a local (no _cells re-index → no aclose-race KeyError)
        # Load-bearing order: test `failure` BEFORE `is_caught_up` (a sticky one-time latch). A reader
        # that died AFTER catching up has is_caught_up=True AND failure set; checking is_caught_up first
        # would return a stale snapshot from a dead view instead of raising reader_dead (spec §6.4).
        if view.failure is not None:
            raise MeshUnavailableError(f"the {kind.name} reader died", reason="reader_dead") from view.failure
        if not view.is_caught_up:
            raise MeshUnavailableError(f"the {kind.name} directory is still establishing", reason="establishing")
        return MappingProxyType(cell.project(view.snapshot()))

    async def _cell(self, kind: _Kind[_R, _D]) -> _Cell[_R, _D]:
        bootstrap = self._client.server_urls
        if not bootstrap:  # a directly-built client (no connect()) — a programming error, not retriable
            raise ValueError("client.mesh requires a connected client — build it with Client.connect(...).")
        async with self._lock:
            if self._closed:
                raise ClientClosedError()
            cell: _Cell[_R, _D] | None = self._cells.get(kind.name)
            if cell is None:
                cell = self._make_cell(kind, bootstrap)
        try:
            await asyncio.shield(cell.task)  # a waiter's cancel is absorbed here, not propagated into the shared open
        except asyncio.CancelledError:
            if self._closed:
                raise ClientClosedError() from None  # aclose cancelled us → the closed contract
            raise  # the waiter's OWN cancellation propagates
        except Exception as exc:  # the open task raised (start() failed); its done-callback evicted the cell
            raise MeshUnavailableError(
                f"could not open the {kind.name} directory — the topic isn't present yet "
                "(it is created when an agent or tool comes online, or provision it via ops)",
                reason="open_failed",
            ) from exc
        return cell

    def _make_cell(self, kind: _Kind[_R, _D], bootstrap: str) -> _Cell[_R, _D]:  # caller holds self._lock
        cell: _Cell[_R, _D] = _Cell(
            task=asyncio.create_task(self._open(kind, bootstrap)),
            project=_Projector(kind.project_one, label=kind.name),
        )
        name = kind.name

        def _on_done(task: asyncio.Future[Any]) -> None:  # the open's terminal state — evict iff it failed
            self._evict(name, cell, task)

        cell.task.add_done_callback(_on_done)
        self._cells[name] = cell
        return cell

    def _evict(self, name: str, cell: _Cell[Any, Any], task: asyncio.Future[Any]) -> None:
        # done-callback: drop ONLY on the open's own failure — not an aclose-cancel, not success. The
        # identity guard never drops a retry's newer cell. No await: runs as one event-loop step (do not add one).
        if task.cancelled() or task.exception() is None:
            return
        if self._cells.get(name) is cell:
            self._cells.pop(name, None)

    async def _open(self, kind: _Kind[_R, _D], bootstrap_servers: str) -> ControlPlaneView[_R]:
        # Naive open: ensure_topic=False, always — the observer never creates a control-plane topic and
        # never consults client._provisioning (spec §6.3 / §7.1).
        view = ControlPlaneView.open(
            bootstrap_servers=bootstrap_servers,
            topic=kind.topic,
            record_type=kind.record_type,
            ensure_topic=False,
            catchup_timeout=self._config.catchup_timeout,
            stale_after=self._config.stale_after,
            reader_tuning=self._config.reader_tuning,
        )
        try:
            await view.start()  # start() IS the catch-up gate
        except BaseException:  # incl. CancelledError → stop the half-built view (no leaked consumer / reader task)
            with contextlib.suppress(Exception):
                await view.stop()
            raise
        return view

    async def aclose(self) -> None:
        async with self._lock:
            self._closed = True
            cells = list(self._cells.values())
            self._cells.clear()
        for cell in cells:  # cancel in-flight; stop completed; best-effort (log per-view, don't aggregate-raise)
            cell.task.cancel()
            try:
                view = await cell.task
            except (asyncio.CancelledError, Exception):
                continue  # in-flight (the §7.1 guard tore it down) / failed open — nothing to stop
            try:
                await view.stop()
            except Exception:
                logger.exception("mesh %s view stop failed during aclose", cell.project._label)
