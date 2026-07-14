"""Caller-capable nodes register via the key-ordered subscriber; observers are untouched.

This replaces the old ``max_workers=1`` pin suite: per-key lanes now provide, per key, the
serialization the pin provided across all keys within a subscriber. Caller-capable nodes honor the worker's
``max_workers`` as cross-correlation parallelism — messages sharing a ``correlation_id``
(the partition key) stay strictly serial, in order. The framework still chooses the
subscriber *kind* per node type; it no longer discards the user's concurrency value.
"""

import pytest
from faststream.exceptions import SetupError

from calfkit.client import Client
from calfkit.nodes.consumer import ConsumerNode
from calfkit.nodes.node import NodeDef
from calfkit.worker import Worker


def _spy_registration(monkeypatch, client) -> list:  # noqa: ANN001
    """Capture ``(method, topics, kwargs)`` for every subscriber the worker registers.

    Both registration methods are spied into ONE list: post-flip the worker uses
    ``key_ordered_subscriber`` for caller-capable nodes and the stock ``subscriber``
    for observers.
    """
    calls: list[tuple[str, tuple, dict]] = []

    def fake_subscriber(*topics, **kwargs):  # noqa: ANN002, ANN003
        calls.append(("subscriber", topics, kwargs))
        return lambda fn, **_call_kwargs: fn

    def fake_key_ordered_subscriber(*topics, **kwargs):  # noqa: ANN002, ANN003
        calls.append(("key_ordered_subscriber", topics, kwargs))
        return lambda fn, **_call_kwargs: fn

    def fake_publisher(*args, **kwargs):  # noqa: ANN002, ANN003
        return lambda fn: fn

    monkeypatch.setattr(client.broker, "subscriber", fake_subscriber)
    monkeypatch.setattr(client.broker, "key_ordered_subscriber", fake_key_ordered_subscriber)
    monkeypatch.setattr(client.broker, "publisher", fake_publisher)
    return calls


def _registration_for(calls: list, return_topic: str) -> tuple[str, dict]:
    matches = [(method, kw) for method, topics, kw in calls if return_topic in topics]
    assert len(matches) == 1, f"expected one subscriber for {return_topic!r}, got {len(matches)}"
    return matches[0]


def test_caller_capable_node_registers_key_ordered_with_worker_max_workers(monkeypatch) -> None:  # noqa: ANN001
    client = Client.connect()
    worker = Worker(client, nodes=[NodeDef(node_id="caller", subscribe_topics=["work"])], max_workers=4)
    calls = _spy_registration(monkeypatch, client)

    worker.register_handlers()

    method, kwargs = _registration_for(calls, "caller.private.return")
    assert method == "key_ordered_subscriber"
    assert kwargs["max_workers"] == 4


def test_caller_capable_default_stays_serial(monkeypatch) -> None:  # noqa: ANN001
    # Regression guard for the default: worker max_workers=1 ⇒ one lane ⇒ serial, as the
    # old pin behaved.
    client = Client.connect()
    worker = Worker(client, nodes=[NodeDef(node_id="caller", subscribe_topics=["work"])], max_workers=1)
    calls = _spy_registration(monkeypatch, client)

    worker.register_handlers()

    method, kwargs = _registration_for(calls, "caller.private.return")
    assert method == "key_ordered_subscriber"
    assert kwargs["max_workers"] == 1


def test_caller_capable_extra_subscribe_kwargs_max_workers_wins_once(monkeypatch) -> None:  # noqa: ANN001
    # Single-source resolution: the explicit extra_subscribe_kwargs value wins over the
    # worker default and is passed exactly once (no duplicate-kwarg error).
    client = Client.connect()
    worker = Worker(
        client,
        nodes=[NodeDef(node_id="caller", subscribe_topics=["work"])],
        max_workers=8,
        extra_subscribe_kwargs={"max_workers": 3},
    )
    calls = _spy_registration(monkeypatch, client)

    worker.register_handlers()

    method, kwargs = _registration_for(calls, "caller.private.return")
    assert method == "key_ordered_subscriber"
    assert kwargs["max_workers"] == 3


def test_no_pin_log_remains(monkeypatch, caplog) -> None:  # noqa: ANN001
    # The pin is gone; so is its breadcrumb. Nothing about pinning may be logged.
    import logging

    client = Client.connect()
    worker = Worker(client, nodes=[NodeDef(node_id="caller", subscribe_topics=["work"])], max_workers=4)
    _spy_registration(monkeypatch, client)

    with caplog.at_level(logging.INFO, logger="calfkit.worker.worker"):
        worker.register_handlers()

    assert not any("pinning max_workers" in r.getMessage() for r in caplog.records)


def test_caller_capable_connection_kwargs_flow_through(monkeypatch) -> None:  # noqa: ANN001
    # The kafka lane's standard extra kwarg must keep working on the flipped path.
    client = Client.connect()
    worker = Worker(
        client,
        nodes=[NodeDef(node_id="caller", subscribe_topics=["work"])],
        max_workers=2,
        extra_subscribe_kwargs={"auto_offset_reset": "earliest"},
    )
    calls = _spy_registration(monkeypatch, client)

    worker.register_handlers()

    _method, kwargs = _registration_for(calls, "caller.private.return")
    assert kwargs["auto_offset_reset"] == "earliest"


def test_caller_capable_structural_extra_kwarg_raises_named_error() -> None:
    # No spy: the REAL key_ordered_subscriber enforces the allow-list, so a structural
    # kwarg fails loudly at registration, naming the key — never a bare TypeError, never
    # a silent drop (worker-layer face of the extension's D15 contract).
    client = Client.connect()
    worker = Worker(
        client,
        nodes=[NodeDef(node_id="caller", subscribe_topics=["work"])],
        max_workers=2,
        extra_subscribe_kwargs={"batch": True},
    )

    with pytest.raises(SetupError, match="batch"):
        worker.register_handlers()


def test_observer_uses_worker_max_workers(monkeypatch) -> None:  # noqa: ANN001
    client = Client.connect()
    consumer = ConsumerNode(name="obs", consume_fn=lambda ctx: None, subscribe_topics=["events"])
    worker = Worker(client, nodes=[consumer], max_workers=4)
    calls = _spy_registration(monkeypatch, client)

    worker.register_handlers()

    method, kwargs = _registration_for(calls, "obs.private.return")
    assert method == "subscriber", "observers keep the stock subscriber path"
    assert kwargs["max_workers"] == 4


def test_observer_honors_extra_subscribe_kwargs_max_workers(monkeypatch) -> None:  # noqa: ANN001
    # The observer branch uses setdefault, so an explicit extra value wins over the worker
    # default (which only fills in when extra didn't set one).
    client = Client.connect()
    consumer = ConsumerNode(name="obs", consume_fn=lambda ctx: None, subscribe_topics=["events"])
    worker = Worker(client, nodes=[consumer], max_workers=2, extra_subscribe_kwargs={"max_workers": 5})
    calls = _spy_registration(monkeypatch, client)

    worker.register_handlers()

    method, kwargs = _registration_for(calls, "obs.private.return")
    assert method == "subscriber"
    assert kwargs["max_workers"] == 5


def test_real_registration_yields_key_ordered_subscriber_object() -> None:
    # End-to-end registration wiring without spies: the caller-capable node's subscriber
    # on the broker IS a KeyOrderedSubscriber with the worker's lane count.
    from calfkit._faststream_ext import KeyOrderedSubscriber

    client = Client.connect()
    worker = Worker(client, nodes=[NodeDef(node_id="caller", subscribe_topics=["work"])], max_workers=3)
    worker.register_handlers()

    key_ordered = [s for s in client.broker.subscribers if isinstance(s, KeyOrderedSubscriber)]
    assert len(key_ordered) == 1
    assert key_ordered[0].max_workers == 3
    assert "caller.private.return" in key_ordered[0].topics


def test_is_caller_capable_matches_node_kind_taxonomy() -> None:
    # Per-class expectations (NOT a formula binding the wire taxonomy to dispatch
    # semantics — a future second observer kind is legitimate): ConsumerNode is the one
    # observer today; every other current kind handles Calls/continuations and must be
    # caller-capable, so a new caller-capable kind can't silently forget the flag.
    from calfkit.nodes import Agent, ToolNodeDef
    from calfkit.nodes.base import BaseNodeDef

    assert ConsumerNode.is_caller_capable is False
    for cls in (BaseNodeDef, NodeDef, Agent, ToolNodeDef):
        assert cls.is_caller_capable is True, cls.__name__
