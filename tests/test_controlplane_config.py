"""Unit tests for ControlPlaneConfig (spec §11, plan §3.3)."""

import dataclasses

import pytest

from calfkit.controlplane import ControlPlaneConfig
from calfkit.controlplane.config import STALE_MULTIPLIER


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
    with pytest.raises(dataclasses.FrozenInstanceError):
        cfg.heartbeat_interval = 10.0  # type: ignore[misc]
