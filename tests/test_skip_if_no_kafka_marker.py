"""Unit test for the ``skip_if_no_kafka`` gate in ``tests.utils``.

This is NOT an integration test: it verifies the marker exists and is driven
SYNCHRONOUSLY by the ``CALF_TEST_KAFKA`` env var (an instant ``shutil.which``-
style check), with NO live TCP probe at collection time. It's a normal unit
test that always runs.
"""

import importlib

import pytest


def _reload_utils():
    import tests.utils as utils_mod

    return importlib.reload(utils_mod)


def test_skip_if_no_kafka_is_a_mark_decorator() -> None:
    utils = _reload_utils()

    assert hasattr(utils, "skip_if_no_kafka")
    assert isinstance(utils.skip_if_no_kafka, type(pytest.mark.skipif(True, reason="x")))


def test_marker_active_when_env_unset(monkeypatch) -> None:
    monkeypatch.delenv("CALF_TEST_KAFKA", raising=False)
    utils = _reload_utils()

    mark = utils.skip_if_no_kafka.mark
    # skipif(True, ...) -> first positional arg is the (truthy) condition.
    assert mark.args[0] is True


def test_marker_inactive_when_env_set(monkeypatch) -> None:
    monkeypatch.setenv("CALF_TEST_KAFKA", "1")
    utils = _reload_utils()

    mark = utils.skip_if_no_kafka.mark
    assert mark.args[0] is False
