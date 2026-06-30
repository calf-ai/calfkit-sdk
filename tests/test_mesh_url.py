"""Unit tests for mesh-URL resolution (``calfkit.client._mesh_url``).

``resolve_mesh_url`` is the single source of truth for the
arg > ``$CALFKIT_MESH_URL`` > localhost precedence and the normalization to the
list form aiokafka expects. Both ``Client.connect`` and the ``ck run`` banner
resolve through it, so the banner can never drift from the real target.
"""

from __future__ import annotations

import pytest

from calfkit.client._mesh_url import DEFAULT_MESH_URL, MESH_URL_ENV_VAR, resolve_mesh_url


def test_env_var_name_is_calfkit_mesh_url() -> None:
    assert MESH_URL_ENV_VAR == "CALFKIT_MESH_URL"


def test_default_is_localhost() -> None:
    assert DEFAULT_MESH_URL == "localhost"


def test_explicit_str_is_normalized_to_a_single_element_list() -> None:
    # aiokafka never comma-splits inside a list element, so a single string must
    # reach the broker as exactly one bootstrap entry.
    assert resolve_mesh_url("kafka-a:9092") == ["kafka-a:9092"]


def test_explicit_str_ignores_the_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(MESH_URL_ENV_VAR, "env-kafka:9092")
    assert resolve_mesh_url("explicit:9092") == ["explicit:9092"]


def test_explicit_iterable_is_materialized_to_a_list() -> None:
    assert resolve_mesh_url(["a:9092", "b:9092"]) == ["a:9092", "b:9092"]


def test_one_shot_generator_is_materialized_once() -> None:
    # A generator must be materialized here so the broker receives real
    # bootstrap servers, not an already-consumed iterator.
    gen = (u for u in ["a:9092", "b:9092"])
    assert resolve_mesh_url(gen) == ["a:9092", "b:9092"]


def test_none_falls_back_to_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(MESH_URL_ENV_VAR, "env-kafka:9092")
    assert resolve_mesh_url(None) == ["env-kafka:9092"]


def test_none_falls_back_to_localhost_when_env_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(MESH_URL_ENV_VAR, raising=False)
    assert resolve_mesh_url(None) == [DEFAULT_MESH_URL]


def test_empty_env_var_falls_through_to_localhost(monkeypatch: pytest.MonkeyPatch) -> None:
    # A set-but-empty env var is falsy and must not become a bogus "" server.
    monkeypatch.setenv(MESH_URL_ENV_VAR, "")
    assert resolve_mesh_url(None) == [DEFAULT_MESH_URL]
