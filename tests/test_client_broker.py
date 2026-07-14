"""Unit tests for `_PreStartHookBroker` (issue #180).

The broker is the generic start-time seam: it runs a one-shot async hook after
``connect()`` and before subscribers consume. We stub ``connect`` (per instance)
and the parent ``KafkaBroker.start``/``stop`` (so ``super()`` calls hit recorders)
to exercise the override without a live Kafka broker.
"""

import asyncio

import pytest
from faststream.kafka import KafkaBroker

from calfkit.client._broker import _PreStartHookBroker


def _stub_connect(monkeypatch, broker, calls: list[str]) -> None:
    async def fake_connect() -> None:
        calls.append("connect")

    monkeypatch.setattr(broker, "connect", fake_connect)


def _stub_parent_start(monkeypatch, calls: list[str]) -> None:
    async def fake_parent_start(self) -> None:  # noqa: ANN001
        calls.append("parent_start")

    monkeypatch.setattr(KafkaBroker, "start", fake_parent_start)


def _stub_parent_stop(monkeypatch, calls: list[str]) -> None:
    async def fake_parent_stop(self, *exc) -> None:  # noqa: ANN001, ANN002
        calls.append("parent_stop")

    monkeypatch.setattr(KafkaBroker, "stop", fake_parent_stop)


def test_pre_start_runs_once_after_connect_and_before_super_start(monkeypatch) -> None:
    calls: list[str] = []

    async def hook(broker) -> None:  # noqa: ANN001
        calls.append("hook")

    broker = _PreStartHookBroker("localhost:9092", pre_start=hook)
    _stub_connect(monkeypatch, broker, calls)
    _stub_parent_start(monkeypatch, calls)

    asyncio.run(broker.start())
    assert calls == ["connect", "hook", "parent_start"]

    # A second start() must NOT re-run the one-shot hook.
    calls.clear()
    asyncio.run(broker.start())
    assert calls == ["connect", "parent_start"]


def test_pre_start_none_behaves_like_plain_start(monkeypatch) -> None:
    calls: list[str] = []
    broker = _PreStartHookBroker("localhost:9092", pre_start=None)
    _stub_connect(monkeypatch, broker, calls)
    _stub_parent_start(monkeypatch, calls)

    asyncio.run(broker.start())

    assert calls == ["connect", "parent_start"]


def test_pre_start_raise_propagates_and_super_start_not_reached(monkeypatch) -> None:
    calls: list[str] = []

    async def hook(broker) -> None:  # noqa: ANN001
        raise RuntimeError("boom")

    broker = _PreStartHookBroker("localhost:9092", pre_start=hook)
    _stub_connect(monkeypatch, broker, calls)
    _stub_parent_start(monkeypatch, calls)

    with pytest.raises(RuntimeError, match="boom"):
        asyncio.run(broker.start())

    assert "parent_start" not in calls  # subscribers never started


def test_pre_start_re_runs_after_a_failed_hook(monkeypatch) -> None:
    # A hook that RAISES must not be marked done — a retry start() must re-run
    # it (otherwise a transient provisioning failure would silently skip
    # provisioning on the next start and re-introduce the #180 hang).
    calls: list[str] = []
    fail = {"on": True}

    async def hook(broker) -> None:  # noqa: ANN001
        calls.append("hook")
        if fail["on"]:
            raise RuntimeError("boom")

    broker = _PreStartHookBroker("localhost:9092", pre_start=hook)
    _stub_connect(monkeypatch, broker, calls)
    _stub_parent_start(monkeypatch, calls)

    with pytest.raises(RuntimeError, match="boom"):
        asyncio.run(broker.start())
    assert "parent_start" not in calls  # first start aborted before subscribers

    fail["on"] = False
    asyncio.run(broker.start())  # retry without stop(): the hook must run again

    assert calls.count("hook") == 2
    assert "parent_start" in calls


def test_stop_before_start_is_safe(monkeypatch) -> None:
    calls: list[str] = []
    broker = _PreStartHookBroker("localhost:9092", pre_start=None)
    _stub_parent_stop(monkeypatch, calls)

    asyncio.run(broker.stop())  # stop() before any start() must not raise

    assert calls == ["parent_stop"]


def test_stop_resets_one_shot_so_a_restart_reprovisions(monkeypatch) -> None:
    calls: list[str] = []

    async def hook(broker) -> None:  # noqa: ANN001
        calls.append("hook")

    broker = _PreStartHookBroker("localhost:9092", pre_start=hook)
    _stub_connect(monkeypatch, broker, calls)
    _stub_parent_start(monkeypatch, calls)
    _stub_parent_stop(monkeypatch, calls)

    asyncio.run(broker.start())
    asyncio.run(broker.stop())
    asyncio.run(broker.start())

    assert calls.count("hook") == 2
    assert "parent_stop" in calls


def test_broker_gains_key_ordered_registration_via_the_extension_mixin() -> None:
    """The calfkit broker composes the standalone extension's registration mixin (first
    base), gaining `key_ordered_subscriber()` while the stock `subscriber()` builder and
    the pre-start hook machinery stay untouched."""
    from calfkit._faststream_ext import KeyOrderedRegistratorMixin, KeyOrderedSubscriber

    broker = _PreStartHookBroker("localhost:9092")
    assert isinstance(broker, KeyOrderedRegistratorMixin)
    # The mixin sits ABOVE KafkaBroker so its additive method resolves, and nothing else.
    mro = type(broker).__mro__
    assert mro.index(KeyOrderedRegistratorMixin) < mro.index(KafkaBroker)

    sub = broker.key_ordered_subscriber("t-in", "t-return", group_id="g", max_workers=3)
    assert isinstance(sub, KeyOrderedSubscriber)
    assert sub in broker.subscribers
    stock = broker.subscriber("t-other")
    assert not isinstance(stock, KeyOrderedSubscriber)


def test_pre_start_hook_fires_with_key_ordered_subscribers_registered(monkeypatch) -> None:
    calls: list[str] = []

    async def hook(broker) -> None:  # noqa: ANN001
        calls.append("hook")

    broker = _PreStartHookBroker("localhost:9092", pre_start=hook)
    broker.key_ordered_subscriber("t-in", group_id="g", max_workers=2)
    _stub_connect(monkeypatch, broker, calls)
    _stub_parent_start(monkeypatch, calls)

    asyncio.run(broker.start())
    assert calls == ["connect", "hook", "parent_start"]
