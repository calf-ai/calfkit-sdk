"""Tests for Worker._prepare_aggregators preconditions.

Specifically: a Worker holding a Client constructed directly via
``BaseClient(...)`` (no ``Client.connect``) won't have ``kafka_config``
populated. The previous design silently defaulted to ``localhost`` and
broke rehydration on every production cluster with broker auth. The
new contract: ``_prepare_aggregators`` raises ``AggregatorStateStoreError``
if any ``BaseAgentNodeDef`` is registered and ``kafka_config is None``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from calfkit.client.base import BaseClient
from calfkit.client.reply_dispatcher import _ReplyDispatcher
from calfkit.nodes.aggregator.errors import AggregatorStateStoreError
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
