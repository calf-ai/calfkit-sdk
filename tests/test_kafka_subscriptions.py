"""Unit tests for BaseNodeDef.kafka_subscriptions() default behavior.

The introspection method underpins Worker.register_handlers(): the Worker
iterates each node's kafka_subscriptions() and registers an independent
FastStream @broker.subscriber for each spec. The default returns one
subscription mirroring today's per-node wiring; subclasses (forthcoming
agents with aggregator returns subscribers) override to return multiple.

These tests pin the default shape so the contract stays stable.
"""

from calfkit.nodes.base import _KafkaSubscription
from calfkit.nodes.tool import ToolNodeDef


def _sample_tool(x: int, y: int) -> int:
    """Sample tool function used as the source for a ToolNodeDef."""
    return x + y


def test_default_kafka_subscriptions_returns_single_spec() -> None:
    node = ToolNodeDef.create_tool_node(_sample_tool, "tool.in", "tool.out")

    subs = node.kafka_subscriptions()

    assert len(subs) == 1
    assert isinstance(subs[0], _KafkaSubscription)


def test_default_subscription_mirrors_node_fields() -> None:
    node = ToolNodeDef.create_tool_node(_sample_tool, "tool.in", "tool.out")

    sub = node.kafka_subscriptions()[0]

    assert sub.topics == list(node.subscribe_topics)
    # Bound methods don't compare with `is` (each attribute access creates a new
    # bound method object). Verify the method is bound to the same instance and
    # backed by the same underlying function.
    assert sub.handler.__self__ is node
    assert sub.handler.__func__ is type(node).handler
    assert sub.publish_topic == node.publish_topic


def test_default_subscription_leaves_overrides_unset() -> None:
    """group_id, max_workers, listener, ack_policy default to None so the Worker
    fills them from its own configuration."""
    node = ToolNodeDef.create_tool_node(_sample_tool, "tool.in", "tool.out")

    sub = node.kafka_subscriptions()[0]

    assert sub.group_id is None
    assert sub.max_workers is None
    assert sub.listener is None
    assert sub.ack_policy is None
    assert sub.extra_kwargs == {}


def test_default_subscription_publish_topic_passthrough_when_none() -> None:
    """When the node has no publish_topic, the subscription mirrors that absence."""
    # ToolNodeDef.create_tool_node always sets a publish_topic; test the
    # underlying shape via a node with publish_topic=None.
    node = ToolNodeDef.create_tool_node(_sample_tool, "tool.in", "tool.out")
    node.publish_topic = None  # type: ignore[assignment]

    sub = node.kafka_subscriptions()[0]

    assert sub.publish_topic is None
