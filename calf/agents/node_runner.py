from faststream.kafka import KafkaBroker, KafkaPublisher, KafkaRoute, KafkaRouter

from calf.nodes.base_node import BaseNode


class NodeRunner:
    def __init__(self, node: BaseNode, route_prefix: str, sep: str = "."):
        self.node = node
        self.route_prefix = route_prefix
        self.sep = sep
        self.br = None

    def register(
        self, enter_topic: str, post_to_topic: str
    ):  # TODO: cannot dynamically use topics as this makes it hard to be singly deployable. Unless the input is being supplied donwstream by some constant.
        br = KafkaRouter(
            prefix=f"{self.route_prefix}{self.sep}",
            handlers=(
                KafkaRoute(
                    self.node.on_enter,
                    enter_topic,
                    publishers=(KafkaPublisher(post_to_topic),),
                ),
            ),
        )
        self.br = br
        self.route = KafkaRoute(  # Each atomic, singly deployable node should essentially be a wrapper over a Kafka route. This way, a single route can be placed into a broker to be deployed singly pretty simply, and a runner/orchestrator can configure with it pretty easily
            self.node.on_enter,
            enter_topic,
            publishers=(KafkaPublisher(post_to_topic),),
        )

    @property
    def router(self):
        return self.br

    def bind_to_broker(self, broker):
        broker.include_router(self.br)
