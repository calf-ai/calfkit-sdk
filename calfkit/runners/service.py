from typing import Any

from calfkit.broker.broker import BrokerClient
from calfkit.nodes.base_node import BaseNode


class NodesService:
    def __init__(self, broker: BrokerClient):
        self._broker = broker
        self._subscribers: list[Any] = []

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
            pub: str | None = topics_dict.get("publish_topic")
            subscribe_topics: list[str] | None = topics_dict.get("subscribe_topics")
            if subscribe_topics is not None:
                for sub_topic in subscribe_topics:
                    subscriber = self._broker.subscriber(
                        sub_topic,
                        max_workers=max_workers,
                        group_id=group_id,
                        **extra_subscribe_kwargs,
                    )
                    handler_fn = subscriber(handler_fn)
                    self._subscribers.append(subscriber)
            else:
                sub: str | None = topics_dict.get("subscribe_topic")
                if sub is not None:
                    subscriber = self._broker.subscriber(
                        sub,
                        max_workers=max_workers,
                        group_id=group_id,
                        **extra_subscribe_kwargs,
                    )
                    handler_fn = subscriber(handler_fn)
                    self._subscribers.append(subscriber)
            if pub is not None:
                handler_fn = self._broker.publisher(pub, **extra_publish_kwargs)(handler_fn)

    async def start_subscribers(self) -> None:
        """Start all registered subscribers.

        Use this to start consumers that were registered after the broker
        has already been started (e.g. dynamically spawned nodes).
        """
        for sub in self._subscribers:
            await sub.start()

    async def run(self) -> None:
        """Blocking function to run registered nodes as services."""
        await self._broker.run_app()
