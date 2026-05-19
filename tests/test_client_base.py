"""Tests for BaseClient input validation."""

from __future__ import annotations

import pytest

from calfkit.client import Client
from calfkit.models.state import State


async def test_invoke_rejects_correlation_id_with_pipe_delimiter() -> None:
    """correlation_id containing '|' would corrupt the fan-out aggregator's
    composite-key partitioning. Validate at the SDK boundary."""
    client = Client.connect("localhost:9092")
    try:
        with pytest.raises(ValueError, match=r"correlation_id"):
            await client._invoke(
                topic="some.topic",
                reply_topic=client.reply_topic,
                correlation_id="bad|id",
                state=State(),
            )
    finally:
        # close() awaits dispatcher cleanup; broker was never started so
        # broker.stop() is effectively a no-op.
        await client.close()
