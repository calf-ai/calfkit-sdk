from typing import Any

from calf.broker.broker import Broker
from calf.nodes.base_node import BaseNode
from calf.nodes.registrator import Registrator


class NodeRunner(Registrator):
    def __init__(self, node: BaseNode, *args: Any, **kwargs: Any):
        self.node = node
        super().__init__(*args, **kwargs)

    def register_on(self, broker: Broker, *args: Any, **kwargs: Any) -> None:
        for handler_fn, topics_dict in self.node.bound_registry.items():
            pub = topics_dict.get("publish_topic")
            sub = topics_dict.get("subscribe_topic")
            if pub is not None:
                handler_fn = broker.publisher(pub)(handler_fn)
            if sub is not None:
                handler_fn = broker.subscriber(sub)(handler_fn)
