"""Unit tests for ControlPlaneConfig (spec §11, plan §3.3)."""

import pytest
from pydantic import ValidationError

from calfkit.controlplane import ControlPlaneConfig
from calfkit.controlplane.config import STALE_MULTIPLIER
from calfkit.tuning import KTableReaderTuning


def test_defaults() -> None:
    cfg = ControlPlaneConfig()
    assert cfg.heartbeat_interval == 30.0
    assert cfg.stale_after is None
    assert cfg.catchup_timeout == 30.0
    assert cfg.bootstrap_servers is None


def test_stale_multiplier_is_three() -> None:
    assert STALE_MULTIPLIER == 3


def test_rejects_nonpositive_heartbeat_interval() -> None:
    with pytest.raises(ValueError, match="heartbeat_interval"):
        ControlPlaneConfig(heartbeat_interval=0)


def test_rejects_nonpositive_catchup_timeout() -> None:
    with pytest.raises(ValueError, match="catchup_timeout"):
        ControlPlaneConfig(catchup_timeout=-1.0)


def test_stale_after_none_is_allowed() -> None:
    assert ControlPlaneConfig(stale_after=None).stale_after is None


def test_rejects_nonpositive_stale_after() -> None:
    with pytest.raises(ValueError, match="stale_after"):
        ControlPlaneConfig(stale_after=0)


def test_positive_stale_after_allowed() -> None:
    assert ControlPlaneConfig(stale_after=45.0).stale_after == 45.0


def test_is_frozen() -> None:
    cfg = ControlPlaneConfig()
    with pytest.raises(ValidationError):
        cfg.heartbeat_interval = 10.0  # type: ignore[misc]


def test_reader_tuning_defaults_to_empty() -> None:
    assert ControlPlaneConfig().reader_tuning == KTableReaderTuning()


def test_accepts_reader_tuning() -> None:
    cfg = ControlPlaneConfig(reader_tuning=KTableReaderTuning(poll_timeout_ms=20, fetch_max_wait_ms=10))
    assert cfg.reader_tuning.as_kwargs() == {"poll_timeout_ms": 20, "fetch_max_wait_ms": 10}


def test_reader_tuning_is_validated() -> None:
    # Nested validation: a bad cadence value is rejected when built from a dict.
    with pytest.raises(ValidationError):
        ControlPlaneConfig(reader_tuning={"poll_timeout_ms": 0})


def test_forbids_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        ControlPlaneConfig(heatbeat_interval=10.0)  # typo must raise, not silently no-op


def test_rejects_nonfinite_fields() -> None:
    # NaN slips past every `> 0` / `<= 0` comparison, so guard finiteness explicitly:
    # nan heartbeat => hot loop + poisoned staleness; nan stale_after => nothing ever stale.
    for bad in (float("nan"), float("inf")):
        with pytest.raises(ValueError, match="heartbeat_interval"):
            ControlPlaneConfig(heartbeat_interval=bad)
        with pytest.raises(ValueError, match="catchup_timeout"):
            ControlPlaneConfig(catchup_timeout=bad)
        with pytest.raises(ValueError, match="stale_after"):
            ControlPlaneConfig(stale_after=bad)
