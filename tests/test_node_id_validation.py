"""PR-3a: ``node_id`` must be a legal Kafka topic-name component.

``node_id`` is interpolated raw into this node's framework topic
(``{node_id}.private.return`` — ``BaseNodeDef._return_topic``). An illegal id yields an
illegal topic, surfacing only as a request-timeout stall into an argument-less
``UnknownTopicOrPartitionError`` on another process. Validation lives in
``BaseNodeSchema.__post_init__`` so it fires for every node kind (the one hook all
subclasses run — agents/consumers via ``BaseNodeDef.__init__``, ``@dataclass`` tool nodes
via their generated ``__init__``).
"""

import pytest

from calfkit.nodes.consumer import ConsumerNode
from calfkit.nodes.node import NodeDef

ILLEGAL = ["a b", "a/b", "a:b", "a#b", "topic,name", "", ".", ".."]
LEGAL = ["orchestrator", "my.agent-1_v2", "a", "A.B_C-1"]


@pytest.mark.parametrize("node_id", ILLEGAL)
def test_illegal_node_id_rejected_at_construction(node_id: str) -> None:
    with pytest.raises(ValueError, match="node_id"):
        NodeDef(node_id=node_id, subscribe_topics=["t"])


@pytest.mark.parametrize("node_id", LEGAL)
def test_legal_node_id_accepted(node_id: str) -> None:
    NodeDef(node_id=node_id, subscribe_topics=["t"])  # must not raise


def test_consumer_node_id_validated_too() -> None:
    # Observers run the same __post_init__ — validation is uniform across kinds.
    with pytest.raises(ValueError, match="node_id"):
        ConsumerNode(node_id="bad id", consume_fn=lambda ctx: None, subscribe_topics=["t"])


def test_max_length_component_node_id_accepted() -> None:
    # A 249-char node_id is a legal topic-name *component* — the charset/length bound is
    # on the component (the assembled topic's total length is the provisioner's concern).
    NodeDef(node_id="a" * 249, subscribe_topics=["t"])  # must not raise


def test_over_length_node_id_rejected() -> None:
    with pytest.raises(ValueError, match="node_id"):
        NodeDef(node_id="a" * 250, subscribe_topics=["t"])


def test_dataclass_node_subclass_validates_node_id_via_post_init() -> None:
    # @dataclass node types (e.g. tool nodes) get a generated __init__ that bypasses
    # BaseNodeDef.__init__; the guard must still fire via the shared __post_init__. A
    # minimal @dataclass(BaseNodeSchema) subclass exercises exactly that bypass path.
    from dataclasses import dataclass

    from calfkit.models.node_schema import BaseNodeSchema

    @dataclass
    class _DataclassNode(BaseNodeSchema):
        pass

    with pytest.raises(ValueError, match="node_id"):
        _DataclassNode(node_id="bad id", subscribe_topics=["t"], publish_topic=None)
