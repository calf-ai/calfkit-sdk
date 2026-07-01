"""Tests for ``calfkit.dev.probe`` — the topic-less Kafka reachability probe (spec §5.1).

The probe must return True on a successful ``bootstrap()``, False on a connection error or timeout, and
**always** close the client (no leaked metadata-sync task). aiokafka is faked so the tests stay offline.
"""

from __future__ import annotations

import asyncio

import pytest

from calfkit.cli import _dev_probe as probe

# Per-test behavior for the fake client: "ok" | "refused" | "hang".
_BEHAVIOR = "ok"


class _FakeAIOKafkaClient:
    instances: list[_FakeAIOKafkaClient] = []

    def __init__(self, *, bootstrap_servers: object, **_: object) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.closed = False
        _FakeAIOKafkaClient.instances.append(self)

    async def bootstrap(self) -> None:
        if _BEHAVIOR == "refused":
            from aiokafka.errors import KafkaConnectionError

            raise KafkaConnectionError("no broker there")
        if _BEHAVIOR == "hang":
            await asyncio.sleep(10)
        # "ok": return immediately

    async def close(self) -> None:
        self.closed = True


@pytest.fixture(autouse=True)
def _fake_aiokafka(monkeypatch: pytest.MonkeyPatch) -> None:
    import aiokafka

    _FakeAIOKafkaClient.instances = []
    monkeypatch.setattr(aiokafka, "AIOKafkaClient", _FakeAIOKafkaClient)


def _set(behavior: str) -> None:
    global _BEHAVIOR
    _BEHAVIOR = behavior


async def test_reachable_true_on_successful_bootstrap() -> None:
    _set("ok")
    assert await probe.broker_reachable("127.0.0.1:9092", timeout=1.0) is True
    assert _FakeAIOKafkaClient.instances[-1].closed


async def test_reachable_false_on_connection_error() -> None:
    _set("refused")
    assert await probe.broker_reachable(["127.0.0.1:9092"], timeout=1.0) is False
    assert _FakeAIOKafkaClient.instances[-1].closed


async def test_reachable_false_on_timeout() -> None:
    _set("hang")
    assert await probe.broker_reachable("127.0.0.1:9092", timeout=0.05) is False
    assert _FakeAIOKafkaClient.instances[-1].closed  # closed even after a timeout


def test_is_reachable_sync_wrapper() -> None:
    _set("ok")
    assert probe.is_reachable("127.0.0.1:9092", timeout=1.0) is True
