"""Tests for client-side broker construction.

Specifically verifies that :meth:`Client.connect` installs the framework's
:class:`FanOutAggregatorPartitioner` on the underlying FastStream
``KafkaBroker``. This is the load-bearing co-partitioning invariant that
the durable fan-out aggregator depends on: messages keyed ``"abc"`` and
``"abc|fan_42"`` must land on the same partition number across all
topics. Without the custom partitioner, the default Kafka partitioner
hashes the composite key verbatim and the state topic loses
co-partitioning with the agent's main / returns topics.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from calfkit.client import Client
from calfkit.nodes.aggregator._partitioner import FanOutAggregatorPartitioner


def test_client_connect_installs_aggregator_partitioner() -> None:
    """Client.connect() must pass FanOutAggregatorPartitioner to KafkaBroker."""
    with patch("calfkit.client.base.KafkaBroker") as mock_broker_cls:
        Client.connect("kafka.example:9092")

    mock_broker_cls.assert_called_once()
    kwargs = mock_broker_cls.call_args.kwargs
    assert "partitioner" in kwargs, "KafkaBroker must be constructed with partitioner="
    assert isinstance(kwargs["partitioner"], FanOutAggregatorPartitioner)


def test_client_connect_rejects_user_supplied_partitioner() -> None:
    """A user passing partitioner= via broker_kwargs would silently override
    ours and break the co-partitioning invariant. Raise a clear error
    instead."""
    with pytest.raises(ValueError, match="partitioner"):
        Client.connect("kafka.example:9092", partitioner=lambda k, a, b: 0)
