"""PR-3b: caller-capable nodes are pinned to ``max_workers=1`` at registration.

FastStream's ``max_workers>1`` is a no-affinity in-process coroutine pool that would
race the await-spanning seam pipeline / fan-out fold (``concurrency-model.md``). The
worker's ``max_workers`` knob applies to observers (consumers); caller-capable nodes are
always serial. The framework chooses the value per node type (not policing a knob), so a
caller-capable node is pinned to 1 even when the worker / ``extra_subscribe_kwargs`` ask
for more.
"""

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
