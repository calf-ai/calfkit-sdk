from typing import Any, TypeAlias

from calfkit.broker.broker import BrokerClient
from calfkit.nodes.base_node import BaseNode
from calfkit.nodes.registrator import Registrator


class NodeRunner(Registrator):
    """The NodeRunner deploys a node as a service registered on a broker"""

    def __init__(self, node: BaseNode, *args: Any, **kwargs: Any):
        self.node = node
        super().__init__(*args, **kwargs)

    def register_on(
        self,
        broker: BrokerClient,
        *,
        max_workers: int | None = None,
        # group_id explicitly set as to avoid duplicated processing for separate deployments
        group_id: str = "default",
        extra_publish_kwargs: dict[str, Any] = {},
        extra_subscribe_kwargs: dict[str, Any] = {},
    ) -> None:
        for handler_fn, topics_dict in self.node.bound_registry.items():
            pub: str | None = topics_dict.get("publish_topic")
            sub: str | None = topics_dict.get("subscribe_topic")
            if sub is not None:
                handler_fn = broker.subscriber(
                    sub, max_workers=max_workers, group_id=group_id, **extra_subscribe_kwargs
                )(handler_fn)
            if pub is not None:
                handler_fn = broker.publisher(pub, **extra_publish_kwargs)(handler_fn)


ChatRunner: TypeAlias = NodeRunner
ToolRunner: TypeAlias = NodeRunner
AgentRouterRunner: TypeAlias = NodeRunner
