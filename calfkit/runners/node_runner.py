from typing import Any, TypeAlias

from calfkit.broker.broker import BrokerClient
from calfkit.nodes.base_node import BaseNode
from calfkit.nodes.registrator import Registrator
from calfkit.runners.service import _adapt_filter, _noop_handler


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
            subs: list[str] = topics_dict.get("subscribe_topics", [])
            filter_fn = topics_dict.get("filter")
            for sub in subs:
                subscriber = broker.subscriber(
                    sub, max_workers=max_workers, group_id=group_id, **extra_subscribe_kwargs
                )
                if filter_fn is not None:
                    handler_fn = subscriber(handler_fn, filter=_adapt_filter(filter_fn))
                    subscriber(_noop_handler)
                else:
                    handler_fn = subscriber(handler_fn)
            if pub is not None:
                handler_fn = broker.publisher(pub, **extra_publish_kwargs)(handler_fn)


ChatRunner: TypeAlias = NodeRunner
ToolRunner: TypeAlias = NodeRunner
AgentRouterRunner: TypeAlias = NodeRunner
