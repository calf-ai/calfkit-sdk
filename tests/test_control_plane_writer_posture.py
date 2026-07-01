"""The worker's control-plane advert writer (``calf.agents`` / ``calf.capabilities``) inherits the
client's producer idempotence posture — the same single knob that drives the shared producer and the
fan-out writers. Default (unset) => calfkit passes nothing to ktables (its writer default applies).

The writer resource opens a real ``GroupedKafkaTableWriter`` under a live broker; here the ktables
factory is patched so the wiring is exercised offline.
"""

from __future__ import annotations

import pytest

from calfkit.client import Client
from calfkit.worker.worker import Worker


class FakeWriter:
    """Records the kwargs the control-plane writer resource passes to ``GroupedKafkaTableWriter.json``."""

    last_kwargs: dict = {}

    @classmethod
    def json(cls, **kwargs: object) -> FakeWriter:
        FakeWriter.last_kwargs = dict(kwargs)
        return cls()

    async def start(self) -> None: ...

    async def stop(self) -> None: ...


@pytest.fixture
def fake_grouped_writer(monkeypatch: pytest.MonkeyPatch) -> None:
    FakeWriter.last_kwargs = {}
    monkeypatch.setattr("ktables.GroupedKafkaTableWriter", FakeWriter)


async def _drive_writer_resource(worker: Worker) -> dict:
    """Enter the control-plane writer resource once and return the captured writer kwargs."""
    gen = worker._make_control_plane_writer_resource("calf.agents")(None)  # type: ignore[arg-type]
    await anext(gen)
    return FakeWriter.last_kwargs


async def test_control_plane_writer_opts_in_from_client(fake_grouped_writer: None) -> None:
    worker = Worker(Client.connect("kafka:9092", enable_idempotence=True))
    kwargs = await _drive_writer_resource(worker)
    assert kwargs["enable_idempotence"] is True


async def test_control_plane_writer_explicit_false_from_client(fake_grouped_writer: None) -> None:
    worker = Worker(Client.connect("kafka:9092", enable_idempotence=False))
    kwargs = await _drive_writer_resource(worker)
    assert kwargs["enable_idempotence"] is False


async def test_control_plane_writer_default_sets_nothing(fake_grouped_writer: None) -> None:
    worker = Worker(Client.connect("kafka:9092"))
    kwargs = await _drive_writer_resource(worker)
    assert "enable_idempotence" not in kwargs
