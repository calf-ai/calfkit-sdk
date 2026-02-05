from typing import Any

from calfkit.broker.broker import BrokerClient
from calfkit.nodes.base_node import BaseNode


class NodesService:
    def __init__(self, broker: BrokerClient):
        self._broker = broker
        return

    def register_node(
        self,
        node: BaseNode,
        *,
        max_workers: int = 1,
        # group_id explicitly set as to avoid duplicated processing for separate deployments
        group_id: str = "default",
        extra_publish_kwargs: dict[str, Any] = {},
        extra_subscribe_kwargs: dict[str, Any] = {},
    ) -> None:
        for handler_fn, topics_dict in node.bound_registry.items():
            pub = topics_dict.get("publish_topic")
            sub = topics_dict.get("subscribe_topic")
            if sub is not None:
                handler_fn = self._broker.subscriber(
                    sub, max_workers=max_workers, group_id=group_id, **extra_subscribe_kwargs
                )(handler_fn)
            if pub is not None:
                handler_fn = self._broker.publisher(pub, **extra_publish_kwargs)(handler_fn)

    async def run(self) -> None:
        """Blocking function to run registered nodes as services."""
        await self._broker.run_app()
