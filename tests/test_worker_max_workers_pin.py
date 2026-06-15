"""PR-3b: caller-capable nodes are pinned to ``max_workers=1`` at registration.

FastStream's ``max_workers>1`` is a no-affinity in-process coroutine pool that would race
the await-spanning read-modify-write of workflow state that handling a continuation
performs. The worker's ``max_workers`` knob applies to observers (consumers); caller-capable
nodes are always serial. The framework chooses the value per node type (not policing a
knob), so a caller-capable node is pinned to 1 even when the worker / ``extra_subscribe_kwargs``
ask for more.
"""

import logging

from calfkit.client import Client
from calfkit.nodes.consumer import ConsumerNode
from calfkit.nodes.node import NodeDef
from calfkit.worker import Worker


def _spy_registration(monkeypatch, client) -> list:  # noqa: ANN001
    """Capture ``(topics, kwargs)`` for every subscriber the worker registers."""
    calls: list[tuple[tuple, dict]] = []

    def fake_subscriber(*topics, **kwargs):  # noqa: ANN002, ANN003
        calls.append((topics, kwargs))
        return lambda fn: fn

    def fake_publisher(*args, **kwargs):  # noqa: ANN002, ANN003
        return lambda fn: fn

    monkeypatch.setattr(client.broker, "subscriber", fake_subscriber)
    monkeypatch.setattr(client.broker, "publisher", fake_publisher)
    return calls


def _kwargs_for(calls: list, return_topic: str) -> dict:
    matches = [kw for topics, kw in calls if return_topic in topics]
    assert len(matches) == 1, f"expected one subscriber for {return_topic!r}, got {len(matches)}"
    return matches[0]


def test_caller_capable_node_pinned_to_max_workers_1(monkeypatch) -> None:  # noqa: ANN001
    client = Client.connect()
    worker = Worker(client, nodes=[NodeDef(node_id="caller", subscribe_topics=["work"])], max_workers=4)
    calls = _spy_registration(monkeypatch, client)

    worker.register_handlers()

    assert _kwargs_for(calls, "caller.private.return")["max_workers"] == 1


def test_observer_uses_worker_max_workers(monkeypatch) -> None:  # noqa: ANN001
    client = Client.connect()
    consumer = ConsumerNode(node_id="obs", consume_fn=lambda ctx: None, subscribe_topics=["events"])
    worker = Worker(client, nodes=[consumer], max_workers=4)
    calls = _spy_registration(monkeypatch, client)

    worker.register_handlers()

    assert _kwargs_for(calls, "obs.private.return")["max_workers"] == 4


def test_caller_capable_pin_overrides_extra_subscribe_kwargs(monkeypatch) -> None:  # noqa: ANN001
    client = Client.connect()
    worker = Worker(
        client,
        nodes=[NodeDef(node_id="caller", subscribe_topics=["work"])],
        max_workers=8,
        extra_subscribe_kwargs={"max_workers": 8},
    )
    calls = _spy_registration(monkeypatch, client)

    worker.register_handlers()

    assert _kwargs_for(calls, "caller.private.return")["max_workers"] == 1


def test_pin_logs_when_overriding_a_requested_max_workers(monkeypatch, caplog) -> None:  # noqa: ANN001
    # Silently discarding a value the user passed to the framework's own constructor is
    # poor DX; the override must leave a breadcrumb.
    client = Client.connect()
    worker = Worker(client, nodes=[NodeDef(node_id="caller", subscribe_topics=["work"])], max_workers=4)
    _spy_registration(monkeypatch, client)

    with caplog.at_level(logging.INFO, logger="calfkit.worker.worker"):
        worker.register_handlers()

    assert any("max_workers" in r.getMessage() and "caller" in r.getMessage() for r in caplog.records), (
        "expected an INFO log noting the max_workers pin overrode the requested value"
    )


def test_no_pin_log_when_worker_default_is_already_one(monkeypatch, caplog) -> None:  # noqa: ANN001
    # No breadcrumb when nothing was overridden (the default is already 1) — avoid log noise.
    client = Client.connect()
    worker = Worker(client, nodes=[NodeDef(node_id="caller", subscribe_topics=["work"])], max_workers=1)
    _spy_registration(monkeypatch, client)

    with caplog.at_level(logging.INFO, logger="calfkit.worker.worker"):
        worker.register_handlers()

    assert not any("pinning max_workers" in r.getMessage() for r in caplog.records)


def test_observer_honors_extra_subscribe_kwargs_max_workers(monkeypatch) -> None:  # noqa: ANN001
    # The observer branch uses setdefault, so an explicit extra value wins over the worker
    # default (which only fills in when extra didn't set one).
    client = Client.connect()
    consumer = ConsumerNode(node_id="obs", consume_fn=lambda ctx: None, subscribe_topics=["events"])
    worker = Worker(client, nodes=[consumer], max_workers=2, extra_subscribe_kwargs={"max_workers": 5})
    calls = _spy_registration(monkeypatch, client)

    worker.register_handlers()

    assert _kwargs_for(calls, "obs.private.return")["max_workers"] == 5


def test_is_caller_capable_matches_node_kind_taxonomy() -> None:
    # Pin the invariant: only the observer kind ("consumer") is not caller-capable. Two
    # independently-set ClassVars (is_caller_capable, _node_kind) must agree, so a new node
    # kind can't silently drift (and get wrongly pinned to max_workers=1, or left concurrent).
    from calfkit.nodes import Agent, ToolNodeDef
    from calfkit.nodes.base import BaseNodeDef

    for cls in (BaseNodeDef, NodeDef, Agent, ToolNodeDef, ConsumerNode):
        assert cls.is_caller_capable == (cls._node_kind != "consumer"), cls.__name__
