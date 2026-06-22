"""Unit tests for the ktables tuning configs (spec §9/§11, plan §3.2).

`KTableReaderTuning` (the shared reader-cadence pair) and `FanoutConfig` (the worker-level
fan-out store config), both Pydantic models with declarative constrained types.
"""

import pytest
from pydantic import ValidationError

from calfkit.tuning import FanoutConfig, KTableReaderTuning


def test_top_level_exports() -> None:
    import calfkit

    assert calfkit.KTableReaderTuning is KTableReaderTuning
    assert calfkit.FanoutConfig is FanoutConfig


# ── KTableReaderTuning ────────────────────────────────────────────────────────


def test_reader_tuning_defaults_are_none() -> None:
    t = KTableReaderTuning()
    assert t.poll_timeout_ms is None
    assert t.fetch_max_wait_ms is None


def test_as_kwargs_empty_at_defaults() -> None:
    # None => omit => ktables owns the default (behavior-preserving).
    assert KTableReaderTuning().as_kwargs() == {}


def test_as_kwargs_returns_only_the_set_subset() -> None:
    assert KTableReaderTuning(poll_timeout_ms=20).as_kwargs() == {"poll_timeout_ms": 20}
    assert KTableReaderTuning(poll_timeout_ms=20, fetch_max_wait_ms=10).as_kwargs() == {
        "poll_timeout_ms": 20,
        "fetch_max_wait_ms": 10,
    }


def test_reader_tuning_accepts_positive_int() -> None:
    assert KTableReaderTuning(poll_timeout_ms=1, fetch_max_wait_ms=500).poll_timeout_ms == 1


@pytest.mark.parametrize("bad", [0, -1])
def test_reader_tuning_rejects_nonpositive(bad: int) -> None:
    with pytest.raises(ValidationError):
        KTableReaderTuning(poll_timeout_ms=bad)
    with pytest.raises(ValidationError):
        KTableReaderTuning(fetch_max_wait_ms=bad)


@pytest.mark.parametrize("bad", [True, 5.0, "5"])
def test_reader_tuning_is_strict_int(bad: object) -> None:
    # strict=True: a config knob must be a real int, not a coerced bool/float/str.
    with pytest.raises(ValidationError):
        KTableReaderTuning(poll_timeout_ms=bad)


def test_reader_tuning_is_frozen() -> None:
    t = KTableReaderTuning()
    with pytest.raises(ValidationError):
        t.poll_timeout_ms = 10  # type: ignore[misc]


def test_reader_tuning_forbids_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        KTableReaderTuning(pol_timeout_ms=20)  # typo must raise, not silently no-op


# ── FanoutConfig ──────────────────────────────────────────────────────────────


def test_fanout_config_defaults() -> None:
    c = FanoutConfig()
    assert c.reader_tuning == KTableReaderTuning()
    assert c.reader_tuning.as_kwargs() == {}
    assert c.catchup_timeout is None
    assert c.barrier_timeout == 30.0


def test_fanout_config_accepts_tuning_and_timeouts() -> None:
    c = FanoutConfig(
        reader_tuning=KTableReaderTuning(fetch_max_wait_ms=50),
        catchup_timeout=10.0,
        barrier_timeout=5.0,
    )
    assert c.reader_tuning.fetch_max_wait_ms == 50
    assert c.catchup_timeout == 10.0
    assert c.barrier_timeout == 5.0


@pytest.mark.parametrize("bad", [0, -1.0, float("inf"), float("nan")])
def test_fanout_config_rejects_bad_catchup_timeout(bad: float) -> None:
    with pytest.raises(ValidationError):
        FanoutConfig(catchup_timeout=bad)


@pytest.mark.parametrize("bad", [0, -1.0, float("inf"), float("nan")])
def test_fanout_config_rejects_bad_barrier_timeout(bad: float) -> None:
    with pytest.raises(ValidationError):
        FanoutConfig(barrier_timeout=bad)


def test_fanout_config_catchup_timeout_none_allowed() -> None:
    assert FanoutConfig(catchup_timeout=None).catchup_timeout is None


def test_fanout_config_is_frozen() -> None:
    c = FanoutConfig()
    with pytest.raises(ValidationError):
        c.barrier_timeout = 10.0  # type: ignore[misc]
