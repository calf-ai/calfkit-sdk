"""PR-3a: ``node_id`` must be a legal Kafka topic-name component.

``node_id`` is interpolated raw into framework topic names — ``{node_id}.private.return``
(``BaseNodeDef._return_topic``) and (PR-4) ``calf.fanout.{node_id}.{state,basestate}``.
An illegal id yields an illegal topic, surfacing only as a request-timeout stall into an
argument-less ``UnknownTopicOrPartitionError`` on another process. Validation lives in
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
