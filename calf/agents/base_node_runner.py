from calf.broker.broker import Broker
from calf.nodes.base_node import BaseNode
from calf.nodes.registrator import Registrator


class BaseNodeRunner(Registrator):
    def __init__(self, node: BaseNode, *args, **kwargs):
        self.node = node
        super().__init__(*args, **kwargs)

    def register_on(self, broker: Broker, *args, **kwargs):
        handler_fn = broker.subscriber(self.node.get_on_enter_topic())(self.node.on_enter)
        handler_fn = broker.publisher(self.node.get_post_to_topic())(handler_fn)
