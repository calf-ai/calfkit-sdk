"""Worker auto-wiring of the control-plane publisher + per-topic writers (plan §3.6).

Registration mechanics only (no broker); the live path is the kafka-lane integration
test. Mirrors tests/test_worker_capability_view.py.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest

from calfkit.client import Client
from calfkit.controlplane import ControlPlaneConfig, ControlPlaneRecord, ControlPlaneStamp, advertises
from calfkit.controlplane.publisher import control_plane_writer_key
from calfkit.nodes import BaseNodeDef
from calfkit.worker.worker import Worker


class _Rec(ControlPlaneRecord):
    schema_version: int = 1
    content: str


class _OneTopic(BaseNodeDef):
    @advertises(topic="calf.a", record=_Rec)
    def _ra(self, identity: ControlPlaneStamp) -> _Rec:
        return _Rec(**identity.model_dump(), content="a")


class _AlsoTopicA(BaseNodeDef):
    @advertises(topic="calf.a", record=_Rec)
    def _r(self, identity: ControlPlaneStamp) -> _Rec:
        return _Rec(**identity.model_dump(), content="other")


class _TwoTopics(BaseNodeDef):
    @advertises(topic="calf.a", record=_Rec)
    def _ra(self, identity: ControlPlaneStamp) -> _Rec:
        return _Rec(**identity.model_dump(), content="a")

    @advertises(topic="calf.b", record=_Rec)
    def _rb(self, identity: ControlPlaneStamp) -> _Rec:
        return _Rec(**identity.model_dump(), content="b")


class _PlainNode(BaseNodeDef):
    pass


def _node(cls: type[BaseNodeDef], node_id: str) -> BaseNodeDef:
    return cls(node_id=node_id, subscribe_topics=[f"{node_id}.in"])


def resource_names(worker: Worker) -> list[str]:
    return [name for name, _ in worker._resource_cms()]


def _wkey(topic: str) -> str:
    return control_plane_writer_key(topic)


# -- registration mechanics --------------------------------------------------


def test_registered_when_a_node_advertises() -> None:
    client = Client.connect("kafka:9092")
    worker = Worker(client, nodes=[_node(_OneTopic, "n1")])
    worker._maybe_register_control_plane()
    assert _wkey("calf.a") in resource_names(worker)
    pub = worker._control_plane_publisher
    assert pub is not None
    assert pub.start in worker._hooks_for("after_startup")
    assert pub.stop in worker._hooks_for("on_shutdown")


def test_not_registered_without_adverts() -> None:
    client = Client.connect("kafka:9092")
    worker = Worker(client, nodes=[_node(_PlainNode, "p")])
    worker._maybe_register_control_plane()
    assert worker._control_plane_publisher is None
    assert not any(name.startswith("calfkit.controlplane.writer.") for name in resource_names(worker))


def test_idempotent_on_repeat_calls() -> None:
    client = Client.connect("kafka:9092")
    worker = Worker(client, nodes=[_node(_OneTopic, "n1")])
    worker._maybe_register_control_plane()
    worker._maybe_register_control_plane()  # must not raise duplicate-name or double-wire
    assert resource_names(worker).count(_wkey("calf.a")) == 1
    assert worker._hooks_for("after_startup").count(worker._control_plane_publisher.start) == 1  # type: ignore[union-attr]


def test_two_node_types_same_topic_share_one_writer() -> None:
    client = Client.connect("kafka:9092")
    worker = Worker(client, nodes=[_node(_OneTopic, "n1"), _node(_AlsoTopicA, "n2")])
    worker._maybe_register_control_plane()
    assert resource_names(worker).count(_wkey("calf.a")) == 1  # one writer for the shared topic
    assert len(worker._control_plane_publisher._adverts) == 2  # type: ignore[union-attr]  # both nodes advertise


def test_one_node_two_topics_two_writers() -> None:
    client = Client.connect("kafka:9092")
    worker = Worker(client, nodes=[_node(_TwoTopics, "n1")])
    worker._maybe_register_control_plane()
    names = resource_names(worker)
    assert _wkey("calf.a") in names
    assert _wkey("calf.b") in names


def test_control_plane_config_accepted_and_defaulted() -> None:
    client = Client.connect("kafka:9092")
    assert Worker(client, control_plane=ControlPlaneConfig(heartbeat_interval=10.0))._control_plane.heartbeat_interval == 10.0
    assert Worker(client)._control_plane == ControlPlaneConfig()


# -- the writer resource (fake GroupedKafkaTableWriter) ----------------------


class _FakeWriter:
    instances: list[_FakeWriter] = []

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.started = False
        self.stopped = False
        _FakeWriter.instances.append(self)

    @classmethod
    def json(cls, **kwargs: Any) -> _FakeWriter:
        return cls(**kwargs)

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True


@pytest.fixture
def fake_writer(monkeypatch: pytest.MonkeyPatch) -> type[_FakeWriter]:
    _FakeWriter.instances = []
    monkeypatch.setattr("ktables.GroupedKafkaTableWriter", _FakeWriter)
    return _FakeWriter


async def _drive(worker: Worker, topic: str) -> tuple[AsyncIterator[Any], _FakeWriter]:
    gen = worker._make_control_plane_writer_resource(topic)(None)  # type: ignore[arg-type]
    writer = await anext(gen)
    return gen, writer


async def _close(gen: AsyncIterator[Any]) -> None:
    with pytest.raises(StopAsyncIteration):
        await anext(gen)


async def test_writer_opens_with_config_and_lifecycle(fake_writer: type[_FakeWriter]) -> None:
    client = Client.connect("kafka:9092")
    worker = Worker(client)
    gen, writer = await _drive(worker, "calf.a")
    assert writer.started and not writer.stopped
    assert writer.kwargs["topic"] == "calf.a"
    assert writer.kwargs["bootstrap_servers"] == "kafka:9092"
    assert writer.kwargs["ensure_topic"] is False  # provisioning disabled by default
    await _close(gen)
    assert writer.stopped


async def test_writer_bootstrap_override_wins(fake_writer: type[_FakeWriter]) -> None:
    client = Client.connect("kafka:9092")
    worker = Worker(client, control_plane=ControlPlaneConfig(bootstrap_servers="cp-kafka:9092"))
    gen, writer = await _drive(worker, "calf.a")
    assert writer.kwargs["bootstrap_servers"] == "cp-kafka:9092"
    await _close(gen)


async def test_writer_underivable_bootstrap_raises(fake_writer: type[_FakeWriter]) -> None:
    client = Client.connect("kafka:9092")
    client._server_urls = None
    client.broker._connection_kwargs = {}
    worker = Worker(client)
    with pytest.raises(RuntimeError, match="bootstrap_servers"):
        await _drive(worker, "calf.a")
    assert fake_writer.instances == []  # failed before construction


async def test_writer_ensure_topic_follows_provisioning(fake_writer: type[_FakeWriter]) -> None:
    from calfkit.provisioning import ProvisioningConfig

    client = Client.connect("kafka:9092", provisioning=ProvisioningConfig(enabled=True))
    worker = Worker(client)
    gen, writer = await _drive(worker, "calf.a")
    assert writer.kwargs["ensure_topic"] is True
    await _close(gen)


# -- call-site + lifecycle boundary ------------------------------------------


def test_register_handlers_wires_the_control_plane() -> None:
    # the real call site: register_handlers() must invoke the control-plane wiring
    client = Client.connect("kafka:9092")
    worker = Worker(client, nodes=[_node(_OneTopic, "n1")])
    worker.register_handlers()
    assert _wkey("calf.a") in resource_names(worker)
    assert worker._control_plane_publisher is not None


def test_node_added_after_prepare_does_not_join_a_plane() -> None:
    # _prepared latches at register_handlers; a node added afterward is not wired (documented boundary)
    client = Client.connect("kafka:9092")
    worker = Worker(client, nodes=[_node(_OneTopic, "n1")])
    worker.register_handlers()  # latches _prepared
    worker.add_nodes(_node(_TwoTopics, "n2"))  # advertises calf.a + calf.b
    worker.register_handlers()  # guarded no-op
    assert _wkey("calf.b") not in resource_names(worker)  # the late node's new topic never wired
