"""Tests for Worker orchestration and ``_prepare_aggregators`` preconditions.

These tests pin the public contract of :class:`~calfkit.worker.Worker`:

* ``_prepare_aggregators`` threads the exact kwargs (``broker``, ``node_id``,
  ``main_topic``, ``kafka_config``) into each agent's ``FanOutAggregator.setup``
  and raises loudly when ``kafka_config`` is missing for an agent.
* ``register_handlers`` forwards subscription metadata (``listener``,
  ``ack_policy``, ``max_workers``) into ``broker.subscriber`` — required so
  the durable aggregator's rebalance + NACK_ON_ERROR semantics survive.
* ``run`` calls ``_prepare_aggregators`` BEFORE ``register_handlers`` so the
  rebalance listener is populated before the handler subscribes (otherwise
  durability silently degrades).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from faststream import AckPolicy

from calfkit.client.base import BaseClient
from calfkit.client.kafka_config import KafkaConfig
from calfkit.client.reply_dispatcher import _ReplyDispatcher
from calfkit.nodes.aggregator.errors import AggregatorStateStoreError
from calfkit.nodes.base import _KafkaSubscription
from calfkit.worker import Worker


def _client_without_kafka_config() -> BaseClient:
    """Build a BaseClient via the __init__ path (no Client.connect
    snapshot) so kafka_config is None."""
    broker = MagicMock()
    broker.connect = AsyncMock()
    dispatcher = _ReplyDispatcher()
    return BaseClient(
        connection=broker,
        reply_topic="reply-topic",
        dispatcher=dispatcher,
        emitter_id="client.test",
    )


def _client_with_kafka_config(
    kafka_config: KafkaConfig | None = None,
    broker: MagicMock | None = None,
) -> BaseClient:
    """Build a BaseClient with a KafkaConfig snapshot — the production
    shape after ``Client.connect``. Tests that exercise the agent branch
    of ``_prepare_aggregators`` need this; the missing-config guard
    raises otherwise."""
    if broker is None:
        broker = MagicMock()
        broker.connect = AsyncMock()
    if kafka_config is None:
        kafka_config = KafkaConfig(bootstrap_servers="localhost:9092", client_kwargs={})
    dispatcher = _ReplyDispatcher()
    return BaseClient(
        connection=broker,
        reply_topic="reply-topic",
        dispatcher=dispatcher,
        emitter_id="client.test",
        kafka_config=kafka_config,
    )


async def test_prepare_aggregators_raises_when_kafka_config_missing_with_agent_node() -> None:
    """Critical guard: an agent node requires kafka_config to wire up the
    durable aggregator. Missing config must raise loudly rather than
    silently regressing to the old localhost-fallback behaviour."""
    from calfkit._vendor.pydantic_ai.models.function import FunctionModel
    from calfkit.nodes.agent import BaseAgentNodeDef

    client = _client_without_kafka_config()
    assert client.kafka_config is None

    agent = BaseAgentNodeDef(
        node_id="planner",
        subscribe_topics="planner.input",
        model_client=FunctionModel(lambda messages, info: None),  # type: ignore[arg-type]
    )
    worker = Worker(client, nodes=[agent])

    with pytest.raises(AggregatorStateStoreError, match="kafka_config"):
        await worker._prepare_aggregators()


async def test_prepare_aggregators_succeeds_without_agent_nodes_even_if_kafka_config_missing() -> None:
    """A Worker with only non-agent nodes doesn't need kafka_config — the
    guard is scoped to BaseAgentNodeDef instances. This pins the
    intentional narrowness of the check; if it were broadened to all
    nodes, this test would regress."""
    from calfkit.models import NodeResult, ReturnCall, State
    from calfkit.models.session_context import SessionRunContext
    from calfkit.nodes import BaseNodeDef

    class _StubNode(BaseNodeDef):
        async def run(self, ctx: SessionRunContext) -> NodeResult[State]:
            return ReturnCall(state=ctx.state)

    client = _client_without_kafka_config()
    assert client.kafka_config is None

    worker = Worker(client, nodes=[_StubNode(node_id="n", subscribe_topics=["t"])])

    # Should NOT raise even though kafka_config is None — no agent nodes.
    await worker._prepare_aggregators()


async def test_prepare_aggregators_threads_setup_kwargs_to_each_agent() -> None:
    from calfkit._vendor.pydantic_ai.models.function import FunctionModel
    from calfkit.nodes.agent import BaseAgentNodeDef

    kafka_config = KafkaConfig(bootstrap_servers="broker:9092", client_kwargs={"client_id": "x"})
    client = _client_with_kafka_config(kafka_config=kafka_config)

    agent = BaseAgentNodeDef(
        node_id="planner",
        subscribe_topics=["planner.input", "planner.alt"],
        model_client=FunctionModel(lambda messages, info: None),  # type: ignore[arg-type]
    )
    agent.aggregator.setup = AsyncMock()  # type: ignore[method-assign]

    worker = Worker(client, nodes=[agent])
    await worker._prepare_aggregators()

    agent.aggregator.setup.assert_awaited_once_with(
        client._connection,
        node_id="planner",
        main_topic="planner.input",
        kafka_config=kafka_config,
    )


async def test_prepare_aggregators_skips_non_agent_nodes() -> None:
    from calfkit.models import NodeResult, ReturnCall, State
    from calfkit.models.session_context import SessionRunContext
    from calfkit.nodes import BaseNodeDef

    class _StubNode(BaseNodeDef):
        async def run(self, ctx: SessionRunContext) -> NodeResult[State]:
            return ReturnCall(state=ctx.state)

    client = _client_with_kafka_config()
    worker = Worker(client, nodes=[_StubNode(node_id="stub", subscribe_topics=["t"])])

    await worker._prepare_aggregators()


def _build_stub_node_returning(*subs: _KafkaSubscription) -> object:
    """Return a BaseNodeDef-like object whose ``kafka_subscriptions``
    returns the supplied specs. Worker doesn't introspect anything else
    during ``register_handlers``."""
    from calfkit.models import NodeResult, ReturnCall, State
    from calfkit.models.session_context import SessionRunContext
    from calfkit.nodes import BaseNodeDef

    class _StubNode(BaseNodeDef):
        async def run(self, ctx: SessionRunContext) -> NodeResult[State]:
            return ReturnCall(state=ctx.state)

        def kafka_subscriptions(self) -> list[_KafkaSubscription]:
            return list(subs)

    return _StubNode(node_id="stub", subscribe_topics=["stub.in"])


def test_register_handlers_threads_listener_from_subscription() -> None:
    listener_sentinel = object()
    sub = _KafkaSubscription(
        topics=["t"],
        handler=AsyncMock(),
        listener=listener_sentinel,
    )
    node = _build_stub_node_returning(sub)

    client = _client_without_kafka_config()
    worker = Worker(client, nodes=[node])  # type: ignore[list-item]

    worker.register_handlers()

    _, kwargs = client._connection.subscriber.call_args
    assert kwargs["listener"] is listener_sentinel


def test_register_handlers_threads_ack_policy_from_subscription() -> None:
    sub = _KafkaSubscription(
        topics=["t"],
        handler=AsyncMock(),
        ack_policy=AckPolicy.NACK_ON_ERROR,
    )
    node = _build_stub_node_returning(sub)

    client = _client_without_kafka_config()
    worker = Worker(client, nodes=[node])  # type: ignore[list-item]

    worker.register_handlers()

    _, kwargs = client._connection.subscriber.call_args
    assert kwargs["ack_policy"] is AckPolicy.NACK_ON_ERROR


def test_register_handlers_passes_max_workers_from_subscription() -> None:
    sub = _KafkaSubscription(
        topics=["t"],
        handler=AsyncMock(),
        max_workers=1,
    )
    node = _build_stub_node_returning(sub)

    client = _client_without_kafka_config()
    worker = Worker(client, nodes=[node], max_workers=8)  # type: ignore[list-item]

    worker.register_handlers()

    _, kwargs = client._connection.subscriber.call_args
    assert kwargs["max_workers"] == 1


def test_register_handlers_falls_back_to_worker_max_workers_when_subscription_none() -> None:
    sub = _KafkaSubscription(
        topics=["t"],
        handler=AsyncMock(),
        max_workers=None,
    )
    node = _build_stub_node_returning(sub)

    client = _client_without_kafka_config()
    worker = Worker(client, nodes=[node], max_workers=4)  # type: ignore[list-item]

    worker.register_handlers()

    _, kwargs = client._connection.subscriber.call_args
    assert kwargs["max_workers"] == 4


def test_register_handlers_wraps_publisher_when_publish_topic_set() -> None:
    sub = _KafkaSubscription(
        topics=["t"],
        handler=AsyncMock(),
        publish_topic="downstream",
    )
    node = _build_stub_node_returning(sub)

    client = _client_without_kafka_config()
    worker = Worker(client, nodes=[node])  # type: ignore[list-item]

    worker.register_handlers()

    client._connection.publisher.assert_called_once_with("downstream")


def test_register_handlers_raises_if_called_twice() -> None:
    sub = _KafkaSubscription(topics=["t"], handler=AsyncMock())
    node = _build_stub_node_returning(sub)

    client = _client_without_kafka_config()
    worker = Worker(client, nodes=[node])  # type: ignore[list-item]

    worker.register_handlers()
    with pytest.raises(RuntimeError, match="already called"):
        worker.register_handlers()


def test_register_handlers_uses_subscription_group_id_when_set() -> None:
    sub = _KafkaSubscription(
        topics=["t"],
        handler=AsyncMock(),
        group_id="override-group",
    )
    node = _build_stub_node_returning(sub)

    client = _client_without_kafka_config()
    worker = Worker(client, nodes=[node], group_id="worker-group")  # type: ignore[list-item]

    worker.register_handlers()

    _, kwargs = client._connection.subscriber.call_args
    assert kwargs["group_id"] == "override-group"


def test_register_handlers_falls_back_to_worker_group_id() -> None:
    sub = _KafkaSubscription(topics=["t"], handler=AsyncMock(), group_id=None)
    node = _build_stub_node_returning(sub)

    client = _client_without_kafka_config()
    worker = Worker(client, nodes=[node], group_id="worker-group")  # type: ignore[list-item]

    worker.register_handlers()

    _, kwargs = client._connection.subscriber.call_args
    assert kwargs["group_id"] == "worker-group"


def test_register_handlers_per_sub_extra_kwargs_override_worker_level() -> None:
    sub = _KafkaSubscription(
        topics=["t"],
        handler=AsyncMock(),
        extra_kwargs={"shared_kw": "from_sub", "sub_only": True},
    )
    node = _build_stub_node_returning(sub)

    client = _client_without_kafka_config()
    worker = Worker(
        client,
        nodes=[node],  # type: ignore[list-item]
        extra_subscribe_kwargs={"shared_kw": "from_worker", "worker_only": "y"},
    )

    worker.register_handlers()

    _, kwargs = client._connection.subscriber.call_args
    assert kwargs["shared_kw"] == "from_sub"
    assert kwargs["worker_only"] == "y"
    assert kwargs["sub_only"] is True


async def test_run_prepares_aggregators_before_registering_handlers() -> None:
    client = _client_without_kafka_config()
    worker = Worker(client)

    manager = MagicMock()
    prepare_mock = AsyncMock()
    register_mock = MagicMock()
    faststream_run_mock = AsyncMock()
    manager.attach_mock(prepare_mock, "_prepare_aggregators")
    manager.attach_mock(register_mock, "register_handlers")
    manager.attach_mock(faststream_run_mock, "faststream_run")

    faststream_instance = MagicMock()
    faststream_instance.run = faststream_run_mock

    with (
        patch.object(worker, "_prepare_aggregators", prepare_mock),
        patch.object(worker, "register_handlers", register_mock),
        patch("calfkit.worker.worker.FastStream", return_value=faststream_instance),
    ):
        await worker.run()

    call_names = [c[0] for c in manager.mock_calls]
    assert call_names == ["_prepare_aggregators", "register_handlers", "faststream_run"]


async def test_run_forwards_extra_run_args_to_faststream() -> None:
    client = _client_without_kafka_config()
    worker = Worker(client)

    faststream_run_mock = AsyncMock()
    faststream_instance = MagicMock()
    faststream_instance.run = faststream_run_mock

    with (
        patch.object(worker, "_prepare_aggregators", AsyncMock()),
        patch.object(worker, "register_handlers", MagicMock()),
        patch(
            "calfkit.worker.worker.FastStream",
            return_value=faststream_instance,
        ) as faststream_cls,
    ):
        await worker.run(log_level="INFO", another="arg")

    faststream_cls.assert_called_once_with(client._connection)
    faststream_run_mock.assert_awaited_once_with(log_level="INFO", another="arg")


async def test_worker_warns_on_low_rehydration_timeout(caplog: object) -> None:
    """A worker that owns aggregator state-store partitions must finish
    rehydration before the broker considers it dead, otherwise it
    triggers a rebalance storm. Worker startup must surface a WARN when
    the configured rebalance_timeout_ms is below the recommended floor."""
    import logging

    from calfkit._vendor.pydantic_ai.models.function import FunctionModel
    from calfkit.nodes.agent import BaseAgentNodeDef

    kafka_config = KafkaConfig(
        bootstrap_servers="broker:9092",
        client_kwargs={"rebalance_timeout_ms": 30_000},
    )
    client = _client_with_kafka_config(kafka_config=kafka_config)

    agent = BaseAgentNodeDef(
        node_id="planner",
        subscribe_topics="planner.input",
        model_client=FunctionModel(lambda messages, info: None),  # type: ignore[arg-type]
    )
    agent.aggregator.setup = AsyncMock()  # type: ignore[method-assign]

    worker = Worker(client, nodes=[agent])
    with caplog.at_level(logging.WARNING, logger="calfkit.client.kafka_config"):  # type: ignore[attr-defined]
        await worker._prepare_aggregators()

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]  # type: ignore[attr-defined]
    assert warnings, "expected a WARN when rebalance_timeout_ms is below floor with an aggregator wired"
    assert any("rebalance_timeout_ms" in r.getMessage() for r in warnings)


async def test_worker_no_warn_when_no_aggregator(caplog: object) -> None:
    """Non-aggregator workers don't pay the rehydration cost, so the
    floor doesn't apply — warning them would be noise that drowns out
    actionable warnings on aggregator-wired workers."""
    import logging

    from calfkit.models import NodeResult, ReturnCall, State
    from calfkit.models.session_context import SessionRunContext
    from calfkit.nodes import BaseNodeDef

    class _StubNode(BaseNodeDef):
        async def run(self, ctx: SessionRunContext) -> NodeResult[State]:
            return ReturnCall(state=ctx.state)

    kafka_config = KafkaConfig(
        bootstrap_servers="broker:9092",
        client_kwargs={"rebalance_timeout_ms": 30_000},
    )
    client = _client_with_kafka_config(kafka_config=kafka_config)
    worker = Worker(client, nodes=[_StubNode(node_id="n", subscribe_topics=["t"])])

    with caplog.at_level(logging.WARNING, logger="calfkit.client.kafka_config"):  # type: ignore[attr-defined]
        await worker._prepare_aggregators()

    # No aggregator => no warning, even with a too-low rebalance_timeout_ms.
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]  # type: ignore[attr-defined]
    rehydration_warnings = [r for r in warnings if "rebalance_timeout_ms" in r.getMessage()]
    assert rehydration_warnings == []


def test_add_nodes_extends_node_list() -> None:
    from calfkit.models import NodeResult, ReturnCall, State
    from calfkit.models.session_context import SessionRunContext
    from calfkit.nodes import BaseNodeDef

    class _StubNode(BaseNodeDef):
        async def run(self, ctx: SessionRunContext) -> NodeResult[State]:
            return ReturnCall(state=ctx.state)

    client = _client_without_kafka_config()
    worker = Worker(client)

    node1 = _StubNode(node_id="n1", subscribe_topics=["t1"])
    node2 = _StubNode(node_id="n2", subscribe_topics=["t2"])
    node3 = _StubNode(node_id="n3", subscribe_topics=["t3"])

    worker.add_nodes(node1, node2)
    worker.add_nodes(node3)

    assert worker._nodes == [node1, node2, node3]
