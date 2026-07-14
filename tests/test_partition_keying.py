"""The client-side partition-keying seam and the entry publish going through it.

The keying contract (spec §11 / ADR): the Kafka partition key must co-locate every pair
of messages whose handlers mutate the same await-spanning workflow state. Today's policy
keys by ``correlation_id``; the policy is expected to change in a later PR, so call sites
and tests reference the SEAM, never an inline ``correlation_id.encode()`` literal.
"""

from calfkit.client import Client
from calfkit.keying import partition_key
from calfkit.models import State


def test_partition_key_matches_todays_node_side_convention() -> None:
    # Node-side publishes key with correlation_id bytes; the seam must agree so entry
    # messages and continuations share a serialization domain.
    assert partition_key("abc-123") == b"abc-123"
    assert isinstance(partition_key("x"), bytes)


async def test_entry_publish_is_keyed_via_the_seam(monkeypatch) -> None:  # noqa: ANN001
    """The gateway's entry publish carries the seam's partition key — previously it sent
    only ``correlation_id=`` (a FastStream trace/header value, NOT the partition key), so
    entry Calls were keyless and round-robined across lanes."""
    client = Client.connect()
    published: list[dict] = []

    async def spy_publish(message, **kwargs):  # noqa: ANN001, ANN003
        published.append(kwargs)

    monkeypatch.setattr(client._broker, "publish", spy_publish)

    cid, state, overrides = client._build_state_and_overrides(
        "hello",
        correlation_id="cid-entry-1",
        temp_instructions=None,
        message_history=None,
        tool_overrides=None,
        model_settings=None,
        author=None,
    )
    await client._publish_call(topic="some.topic", correlation_id=cid, state=state, overrides=overrides, deps=None)

    assert len(published) == 1
    assert published[0]["key"] == partition_key("cid-entry-1")
    assert published[0]["correlation_id"] == "cid-entry-1"
    assert isinstance(state, State)
