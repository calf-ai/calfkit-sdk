"""Unit test for the ``skip_if_no_live_llm`` gate in ``tests.utils``.

This is NOT a live-LLM test: it verifies the clean-skip gate exists and is
driven SYNCHRONOUSLY by env vars (an instant ``os.getenv`` check), with NO live
API call at collection time. It's a normal offline unit test that always runs.

The gate guards the ``live`` lane the same way ``skip_if_no_kafka`` guards the
broker lane: even when a test is selected (``-m live``), it skips cleanly if the
credentials are absent rather than erroring on a real request. Both
``OPENAI_API_KEY`` (the provider key) and ``TEST_LLM_MODEL_NAME`` (which the
model fixture hard-requires) must be set — see ADR 0007.

The gate's logic is exercised through the ``_live_llm_enabled`` helper, which
reads ``os.getenv`` on each call. We test it directly rather than reloading the
module: ``tests.utils`` calls ``load_dotenv()`` at import, and ``.env`` carries
both vars, so a reload would silently repopulate anything we cleared.
"""

import pytest

from tests.utils import _live_llm_enabled, skip_if_no_live_llm


def test_skip_if_no_live_llm_is_a_mark_decorator() -> None:
    assert isinstance(skip_if_no_live_llm, type(pytest.mark.skipif(True, reason="x")))


def test_gate_open_only_when_both_vars_set(monkeypatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setenv("TEST_LLM_MODEL_NAME", "gpt-test")
    assert _live_llm_enabled() is True


def test_gate_closed_when_key_unset(monkeypatch) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setenv("TEST_LLM_MODEL_NAME", "gpt-test")
    assert _live_llm_enabled() is False


def test_gate_closed_when_model_name_unset(monkeypatch) -> None:
    """A present key alone must NOT open the gate: the model fixture reads
    ``TEST_LLM_MODEL_NAME`` directly, so without it the test would error on a
    KeyError instead of skipping cleanly (the latent gap ADR 0007 calls out)."""
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.delenv("TEST_LLM_MODEL_NAME", raising=False)
    assert _live_llm_enabled() is False


def test_gate_closed_when_both_unset(monkeypatch) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("TEST_LLM_MODEL_NAME", raising=False)
    assert _live_llm_enabled() is False
