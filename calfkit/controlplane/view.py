"""Read side of the control-plane substrate (spec §8).

A thin, typed wrapper over a ktables ``GroupedKafkaTable`` that collapses the
instance-keyed (``node_id × worker_id``) storage to a live ``node_id -> record``
view, applying advisory staleness and a schema-version filter. Generic over the
record type ``R``; it knows only the :class:`ControlPlaneRecord` base (for
liveness), so it stays use-case-blind.

ktables is imported lazily inside :meth:`ControlPlaneView.open` (the existing
in-function pattern in ``worker.py``), so importing this module pulls no ktables
— which keeps the node layer ktables-free even though ``BaseNodeDef`` transitively
imports the ``calfkit.controlplane`` package.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar

from pydantic_core import PydanticUndefined

from calfkit.controlplane.config import STALE_MULTIPLIER
from calfkit.controlplane.records import ControlPlaneRecord
from calfkit.exceptions import RegistryConfigError

if TYPE_CHECKING:
    from ktables import TableStatus

    from calfkit.tuning import KTableReaderTuning

logger = logging.getLogger(__name__)

R = TypeVar("R", bound=ControlPlaneRecord)


class GroupedTableReader(Protocol):
    """The subset of ktables' ``GroupedKafkaTable`` read API the view uses.

    A plain dict-backed fake satisfies it, which is the entire unit-test strategy
    (no broker, no ktables).
    """

    def groups(self) -> set[str]: ...

    def members(self, group: str) -> dict[str, Any]: ...

    @property
    def status(self) -> TableStatus: ...

    @property
    def failure(self) -> BaseException | None: ...

    @property
    def is_caught_up(self) -> bool: ...

    async def barrier(self, timeout: float | None = None) -> bool: ...

    async def wait_until_caught_up(self, timeout: float | None = None) -> bool: ...

    async def start(self) -> None: ...

    async def stop(self) -> None: ...


class ControlPlaneView(Generic[R]):
    """A live, collapsed ``node_id -> record`` view of one control-plane topic.

    Reads are liveness-aware: a node's instances are filtered to the supported,
    non-stale set, then collapsed to one (the most-recently-heartbeated). This
    gives the invariant ``get(g) is not None  <=>  g in online_nodes()``.
    """

    def __init__(self, table: GroupedTableReader, record_type: type[R], *, stale_after: float | None = None) -> None:
        field = record_type.model_fields["schema_version"]
        if field.default is PydanticUndefined:
            raise RegistryConfigError(
                f"{record_type.__name__} must set `schema_version: int = N`; a control-plane record "
                "needs a concrete schema_version default for the view's version filter"
            )
        self._table = table
        self._record_type = record_type
        self._reader_version: int = field.default
        self._stale_after = stale_after
        self._logged_skips: set[tuple[str, str, int]] = set()  # (node, worker, version) already warned about
        self._logged_mixed_kinds: set[tuple[str, frozenset[str]]] = set()  # (node, kind-set) already warned about

    # -- collapsed, liveness-aware reads -------------------------------------

    def get(self, node_id: str) -> R | None:
        """The one live record for ``node_id`` (most-recent heartbeat), or ``None``."""
        live = self._live_members(node_id, datetime.now(tz=timezone.utc))
        return max(live.values(), key=lambda record: record.last_heartbeat_at) if live else None

    def online_nodes(self) -> set[str]:
        """The node ids with at least one supported, non-stale instance."""
        now = datetime.now(tz=timezone.utc)
        return {group for group in self._table.groups() if self._live_members(group, now)}

    def snapshot(self) -> dict[str, R]:
        """A ``node_id -> live record`` map for every online node.

        Uses one ``now`` for the whole snapshot (consistent across groups) and
        computes each group's live set exactly once — it never recomputes via
        ``online_nodes()``/``get()`` nor samples the clock twice for one group.
        """
        now = datetime.now(tz=timezone.utc)
        out: dict[str, R] = {}
        for group in self._table.groups():
            live = self._live_members(group, now)
            if live:
                out[group] = max(live.values(), key=lambda record: record.last_heartbeat_at)
        return out

    def _live_members(self, node_id: str, now: datetime) -> dict[str, R]:
        """Members that are supported (schema) and live (staleness) — filter, no tie-break."""
        live: dict[str, R] = {}
        for worker_id, record in self._table.members(node_id).items():
            if record.schema_version > self._reader_version:
                self._log_schema_skip(node_id, worker_id, record.schema_version)
                continue
            threshold = self._stale_after if self._stale_after is not None else STALE_MULTIPLIER * record.heartbeat_interval
            if (now - record.last_heartbeat_at).total_seconds() > threshold:
                continue  # stale (spec §9)
            live[worker_id] = record
        self._warn_if_mixed_kind(node_id, live)
        return live

    def _log_schema_skip(self, node_id: str, worker_id: str, schema_version: int) -> None:
        """Warn once per ``(node, worker, version)`` that a newer-schema record is hidden (spec §8).

        A record from a newer schema major decodes fine but is unsupported here, so it
        is filtered out — which would otherwise make a live node silently vanish from the
        view. Dedup keeps this off the read hot-path; a further major re-warns, and it
        self-heals once the reader upgrades.
        """
        key = (node_id, worker_id, schema_version)
        if key in self._logged_skips:
            return
        self._logged_skips.add(key)
        logger.warning(
            "control-plane view hiding node=%s worker=%s: record schema_version=%d > reader version=%d "
            "(node invisible to this reader until it upgrades)",
            node_id,
            worker_id,
            schema_version,
            self._reader_version,
        )

    def _warn_if_mixed_kind(self, node_id: str, live: dict[str, R]) -> None:
        """Warn once per ``(node, kind-set)`` when one node_id holds live members of differing kinds (spec C1/L14).

        A heterogeneous-owner collision — two advertisers of *different* ``node_kind`` sharing a
        ``node_id`` — would flap the collapsed view between owners tick to tick. It is a facet of
        the global ``node_id``-uniqueness contract (documented, not policed), surfaced here for
        observability. **Cross-kind only**: same-kind/different-content collisions are invisible to
        the view (it does not compare content) and rely on that contract alone. Dedup keeps this off
        the read hot-path; a changed kind-set re-warns and it self-clears once the collision resolves.
        """
        if len(live) < 2:
            return  # the common case (single instance) — nothing to compare
        kinds = frozenset(record.node_kind for record in live.values())
        if len(kinds) < 2:
            return  # same-kind (benign replicas)
        key = (node_id, kinds)
        if key in self._logged_mixed_kinds:
            return
        self._logged_mixed_kinds.add(key)
        logger.warning(
            "control-plane view: node=%s has live members of differing node_kind=%s "
            "(duplicate node_id across owner kinds; resolve the identity collision)",
            node_id,
            sorted(kinds),
        )

    # -- health passthrough (closes frozen-view blindness, spec §9) ----------

    @property
    def status(self) -> TableStatus:
        return self._table.status

    @property
    def failure(self) -> BaseException | None:
        return self._table.failure

    @property
    def is_caught_up(self) -> bool:
        return self._table.is_caught_up

    async def barrier(self, timeout: float | None = None) -> bool:
        return await self._table.barrier(timeout)

    async def wait_until_caught_up(self, timeout: float | None = None) -> bool:
        return await self._table.wait_until_caught_up(timeout)

    async def start(self) -> None:
        await self._table.start()

    async def stop(self) -> None:
        await self._table.stop()

    # -- construction over a real grouped table ------------------------------

    @classmethod
    def open(
        cls,
        *,
        bootstrap_servers: str,
        topic: str,
        record_type: type[R],
        catchup_timeout: float = 30.0,
        ensure_topic: bool = False,
        stale_after: float | None = None,
        reader_tuning: KTableReaderTuning | None = None,
    ) -> ControlPlaneView[R]:  # pragma: no cover - exercised in the kafka lane
        """Open a view over a real ``GroupedKafkaTable``. The caller still awaits ``start()``."""
        from ktables import GroupedKafkaTable

        table: GroupedTableReader = GroupedKafkaTable.json(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            model=record_type,
            catchup_timeout=catchup_timeout,
            ensure_topic=ensure_topic,
            **(reader_tuning.as_kwargs() if reader_tuning else {}),
        )
        return cls(table, record_type, stale_after=stale_after)
