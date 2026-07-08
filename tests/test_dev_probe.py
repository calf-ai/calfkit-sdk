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
        if _BEHAVIOR == "oserror":
            raise OSError("socket-level failure surfaced unwrapped (e.g. DNS)")
        if _BEHAVIOR == "log_and_refuse":
            import logging

            logging.getLogger("aiokafka.client").error('Unable connect to "%s:%s": refused', "127.0.0.1", 9092)
            raise OSError("connection refused")
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


async def test_reachable_false_on_raw_os_error() -> None:
    # Not every socket failure arrives wrapped in a KafkaError; a raw OSError (DNS, address
    # family) must read as "unreachable", never crash ensure_broker.
    _set("oserror")
    assert await probe.broker_reachable("nosuch.invalid:9092", timeout=1.0) is False
    assert _FakeAIOKafkaClient.instances[-1].closed


async def test_reachable_false_on_timeout() -> None:
    _set("hang")
    assert await probe.broker_reachable("127.0.0.1:9092", timeout=0.05) is False
    assert _FakeAIOKafkaClient.instances[-1].closed  # closed even after a timeout


def test_is_reachable_sync_wrapper() -> None:
    _set("ok")
    assert probe.is_reachable("127.0.0.1:9092", timeout=1.0) is True


async def test_broker_reachable_suppresses_the_aiokafka_unable_connect_error(caplog: pytest.LogCaptureFixture) -> None:
    """The probe raises the aiokafka.client log threshold so the expected per-attempt 'Unable
    connect' ERROR does not leak while a broker is coming up (spec §4.5)."""
    import logging

    _set("log_and_refuse")
    caplog.set_level(logging.DEBUG, logger="aiokafka.client")
    assert await probe.broker_reachable("127.0.0.1:9092", timeout=0.5) is False
    assert not any("Unable connect" in r.getMessage() for r in caplog.records)


async def test_broker_reachable_restores_the_aiokafka_log_level() -> None:
    """The quieting is scoped: the aiokafka.client level is restored after the probe, never left
    permanently suppressed."""
    import logging

    logger = logging.getLogger("aiokafka.client")
    logger.setLevel(logging.INFO)
    try:
        _set("refused")
        await probe.broker_reachable("127.0.0.1:9092", timeout=0.5)
        assert logger.level == logging.INFO
    finally:
        logger.setLevel(logging.NOTSET)
