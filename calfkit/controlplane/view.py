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

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar

from pydantic_core import PydanticUndefined

from calfkit.controlplane.config import STALE_MULTIPLIER
from calfkit.controlplane.records import ControlPlaneRecord
from calfkit.exceptions import RegistryConfigError

if TYPE_CHECKING:
    from ktables import TableStatus

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

    # -- collapsed, liveness-aware reads -------------------------------------

    def get(self, node_id: str) -> R | None:
        """The one live record for ``node_id`` (most-recent heartbeat), or ``None``."""
        live = self._live_members(node_id)
        if not live:
            return None
        return max(live.values(), key=lambda record: record.last_heartbeat_at)

    def online_nodes(self) -> set[str]:
        """The node ids with at least one supported, non-stale instance."""
        return {group for group in self._table.groups() if self._live_members(group)}

    def snapshot(self) -> dict[str, R]:
        """A ``node_id -> live record`` map for every online node."""
        return {group: record for group in self.online_nodes() if (record := self.get(group)) is not None}

    def _live_members(self, node_id: str) -> dict[str, R]:
        """Members that are supported (schema) and live (staleness) — filter, no tie-break."""
        now = datetime.now(tz=timezone.utc)
        live: dict[str, R] = {}
        for worker_id, record in self._table.members(node_id).items():
            if record.schema_version > self._reader_version:
                continue  # skip records written by a newer schema major (spec §8)
            threshold = self._stale_after if self._stale_after is not None else STALE_MULTIPLIER * record.heartbeat_interval
            if (now - record.last_heartbeat_at).total_seconds() > threshold:
                continue  # stale (spec §9)
            live[worker_id] = record
        return live

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
    ) -> ControlPlaneView[R]:  # pragma: no cover - exercised in the kafka lane
        """Open a view over a real ``GroupedKafkaTable``. The caller still awaits ``start()``."""
        from ktables import GroupedKafkaTable

        table: GroupedTableReader = GroupedKafkaTable.json(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            model=record_type,
            catchup_timeout=catchup_timeout,
            ensure_topic=ensure_topic,
        )
        return cls(table, record_type, stale_after=stale_after)
