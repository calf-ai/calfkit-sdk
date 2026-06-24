"""PR-B Item 4: the name->topic derivation + the live, scoped, self-excluded peer directory render.

``derive_input_topic(name)`` maps a peer's name to its private input topic (``agent.{name}.private.input``,
ADR-0017) — the caller derives it from the name alone (the AgentCard carries no topic). The render
resolves the Messaging-scoped, live, self-excluded ``(name, description)`` set from a
``ControlPlaneView[AgentCard]`` (``view.snapshot()``), sorted by name; a curated name absent from the
live view warns + is omitted; an empty set renders a "none reachable" body; a degraded/missing view
warns-and-degrades (never raises).
"""

import logging
from types import SimpleNamespace

import pytest

from calfkit.models.agents import derive_input_topic
from calfkit.peers import Messaging
from calfkit.peers.directory import render_peer_directory, resolve_live_peers


def _view(cards: dict[str, str | None]) -> object:
    # A duck-typed ControlPlaneView: snapshot() -> {name: card-with-.description}.
    snap = {name: SimpleNamespace(description=desc) for name, desc in cards.items()}
    return SimpleNamespace(snapshot=lambda: snap)


def _raising_view() -> object:
    def _boom() -> dict[str, object]:
        raise RuntimeError("view degraded")

    return SimpleNamespace(snapshot=_boom)


def test_derive_input_topic() -> None:
    assert derive_input_topic("planner") == "agent.planner.private.input"
    assert derive_input_topic("billing-v2") == "agent.billing-v2.private.input"


def test_resolve_curated_intersects_live() -> None:
    view = _view({"billing": "Billing.", "support": None, "other": "Other."})
    entries = resolve_live_peers(view, [Messaging("billing", "support")], self_name="triage")
    assert entries == [("billing", "Billing."), ("support", None)]  # 'other' out of scope; sorted by name


def test_resolve_discover_all_live_minus_self() -> None:
    view = _view({"a": "A.", "b": "B.", "triage": "me"})
    entries = resolve_live_peers(view, [Messaging(discover=True)], self_name="triage")
    assert entries == [("a", "A."), ("b", "B.")]  # self excluded (load-bearing in discover mode); sorted


def test_resolve_unions_multiple_curated_handles() -> None:
    view = _view({"billing": None, "support": None, "refunds": None})
    entries = resolve_live_peers(view, [Messaging("billing"), Messaging("support")], self_name="triage")
    assert [n for n, _ in entries] == ["billing", "support"]  # union of independent handles; refunds out of scope


def test_resolve_curated_absent_warns_and_omits(caplog: pytest.LogCaptureFixture) -> None:
    view = _view({"billing": "B."})  # 'ghost' is curated but not live
    with caplog.at_level(logging.WARNING):
        entries = resolve_live_peers(view, [Messaging("billing", "ghost")], self_name="triage")
    assert entries == [("billing", "B.")]
    assert any("ghost" in r.message for r in caplog.records)


def test_resolve_missing_view_degrades_to_empty() -> None:
    assert resolve_live_peers(None, [Messaging(discover=True)], self_name="x") == []


def test_resolve_degraded_view_warns_and_degrades(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING):
        assert resolve_live_peers(_raising_view(), [Messaging(discover=True)], self_name="x") == []
    assert caplog.records  # warned, did not raise


def test_render_directory_lines() -> None:
    out = render_peer_directory([("billing", "Billing questions."), ("support", None)])
    assert "billing — Billing questions." in out
    assert "support" in out


def test_render_empty_is_none_reachable() -> None:
    assert "reachable" in render_peer_directory([]).lower()
