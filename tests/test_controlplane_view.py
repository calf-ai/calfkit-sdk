"""Unit tests for ControlPlaneView (spec §8, plan §3.4) — dict-backed fake, no broker."""

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from calfkit.controlplane import ControlPlaneRecord, ControlPlaneView
from calfkit.exceptions import RegistryConfigError


class _Rec(ControlPlaneRecord):
    schema_version: int = 1
    content: str


def _rec(worker_id: str, *, age_s: float = 0.0, hb: float = 30.0, schema_version: int = 1, content: str = "x") -> _Rec:
    now = datetime.now(tz=timezone.utc)
    return _Rec(
        schema_version=schema_version,
        node_id="n",
        worker_id=worker_id,
        started_at=now,
        last_heartbeat_at=now - timedelta(seconds=age_s),
        heartbeat_interval=hb,
        content=content,
    )


class _FakeTable:
    """A minimal GroupedTableReader: ``group -> {member -> record}``."""

    def __init__(
        self,
        data: dict[str, dict[str, _Rec]],
        *,
        status: str = "caught_up",
        failure: BaseException | None = None,
        is_caught_up: bool = True,
    ) -> None:
        self._data = data
        self._status = status
        self._failure = failure
        self._is_caught_up = is_caught_up

    def groups(self) -> set[str]:
        return set(self._data)

    def members(self, group: str) -> dict[str, Any]:
        return dict(self._data.get(group, {}))

    @property
    def status(self) -> Any:
        return self._status

    @property
    def failure(self) -> BaseException | None:
        return self._failure

    @property
    def is_caught_up(self) -> bool:
        return self._is_caught_up

    async def barrier(self, timeout: float | None = None) -> bool:
        return self._is_caught_up

    async def wait_until_caught_up(self, timeout: float | None = None) -> bool:
        return self._is_caught_up

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None


def _view(data: dict[str, dict[str, _Rec]], **kwargs: Any) -> ControlPlaneView[_Rec]:
    return ControlPlaneView(_FakeTable(data, **kwargs), _Rec)


# -- collapsed reads ---------------------------------------------------------


def test_get_returns_live_record() -> None:
    v = _view({"a": {"w1": _rec("w1")}})
    rec = v.get("a")
    assert rec is not None and rec.worker_id == "w1"
    assert v.online_nodes() == {"a"}
    assert set(v.snapshot()) == {"a"}


def test_get_none_for_unknown_node() -> None:
    v = _view({"a": {"w1": _rec("w1")}})
    assert v.get("missing") is None


def test_collapse_picks_most_recent_live_instance() -> None:
    v = _view({"a": {"w1": _rec("w1", age_s=20.0), "w2": _rec("w2", age_s=1.0)}})
    rec = v.get("a")
    assert rec is not None and rec.worker_id == "w2"  # freshest heartbeat wins


def test_stale_member_excluded() -> None:
    # age 100s > 3*30 = 90 threshold -> stale; sole member -> node offline
    v = _view({"a": {"w1": _rec("w1", age_s=100.0, hb=30.0)}})
    assert v.get("a") is None
    assert v.online_nodes() == set()


def test_staleness_uses_record_heartbeat_interval() -> None:
    # same age (100s), different cadence: hb=60 -> threshold 180 (live); hb=10 -> threshold 30 (stale)
    v = _view(
        {
            "live": {"w1": _rec("w1", age_s=100.0, hb=60.0)},
            "dead": {"w1": _rec("w1", age_s=100.0, hb=10.0)},
        }
    )
    assert v.online_nodes() == {"live"}


def test_stale_after_override_takes_precedence() -> None:
    # member age 100s, hb=30 (would be live at 90); explicit stale_after=50 -> stale
    v = ControlPlaneView(_FakeTable({"a": {"w1": _rec("w1", age_s=100.0, hb=30.0)}}), _Rec, stale_after=50.0)
    assert v.get("a") is None


def test_get_iff_online_invariant() -> None:
    v = _view(
        {
            "online": {"w1": _rec("w1", age_s=1.0), "w2": _rec("w2", age_s=100.0)},  # one live, one stale
            "offline": {"w1": _rec("w1", age_s=100.0)},  # all stale
        }
    )
    online = v.online_nodes()
    for node in ("online", "offline"):
        assert (v.get(node) is not None) == (node in online)
    assert online == {"online"}


def test_schema_skip_excludes_newer_records() -> None:
    # reader version = _Rec default (1); a v2 record is skipped
    v = _view({"a": {"w1": _rec("w1", schema_version=2)}})
    assert v.get("a") is None
    assert v.online_nodes() == set()


def test_schema_skip_is_logged_once_per_member(caplog: pytest.LogCaptureFixture) -> None:
    # spec §8: a newer-schema member is skipped AND logged (so a vanishing node is observable).
    v = _view({"a": {"w1": _rec("w1", schema_version=2)}})
    with caplog.at_level("WARNING"):
        v.get("a")
        v.get("a")  # second read must NOT re-log the same (node, worker, version)
    skip_logs = [r for r in caplog.records if "schema" in r.getMessage().lower() and r.levelname == "WARNING"]
    assert len(skip_logs) == 1
    msg = skip_logs[0].getMessage()
    assert "a" in msg and "w1" in msg


def test_collapse_filters_stale_before_tiebreak() -> None:
    # The member with the NEWEST heartbeat is stale (tiny cadence); the live member is
    # older. filter-then-tie-break must return the older LIVE one — a tie-break-then-filter
    # bug would pick the freshest, find it stale, and wrongly return None.
    v = _view(
        {
            "a": {
                "w_fresh_stale": _rec("w_fresh_stale", age_s=2.0, hb=0.1),  # newest hb, but stale (0.3s threshold)
                "w_old_live": _rec("w_old_live", age_s=5.0, hb=100.0),  # older hb, but live (300s threshold)
            }
        }
    )
    rec = v.get("a")
    assert rec is not None and rec.worker_id == "w_old_live"
    assert v.online_nodes() == {"a"}


def test_collapse_filters_newer_schema_before_tiebreak() -> None:
    # The newest-heartbeat member is a newer (unsupported) schema; the live one is older+supported.
    v = _view(
        {
            "a": {
                "w_fresh_newschema": _rec("w_fresh_newschema", age_s=1.0, schema_version=2),
                "w_old_live": _rec("w_old_live", age_s=5.0),
            }
        }
    )
    rec = v.get("a")
    assert rec is not None and rec.worker_id == "w_old_live"
    assert v.online_nodes() == {"a"}


def test_snapshot_maps_online_nodes_to_collapsed_records() -> None:
    v = _view({"a": {"w1": _rec("w1", content="A")}, "b": {"w1": _rec("w1", content="B")}})
    snap = v.snapshot()
    assert {k: r.content for k, r in snap.items()} == {"a": "A", "b": "B"}


# -- defaultless record type -------------------------------------------------


def test_rejects_record_type_without_schema_version_default() -> None:
    with pytest.raises(RegistryConfigError, match="schema_version"):
        ControlPlaneView(_FakeTable({}), ControlPlaneRecord)  # base has no default


# -- health passthrough ------------------------------------------------------


def test_health_properties_passthrough() -> None:
    err = RuntimeError("boom")
    v = _view({}, status="degraded", failure=err, is_caught_up=False)
    assert v.status == "degraded"
    assert v.failure is err
    assert v.is_caught_up is False


async def test_async_health_delegates() -> None:
    v = _view({}, is_caught_up=True)
    assert await v.barrier() is True
    assert await v.wait_until_caught_up() is True
    await v.start()
    await v.stop()
