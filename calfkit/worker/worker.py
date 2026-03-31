import logging
from typing import Any

from faststream import FastStream

from calfkit.client import Client
from calfkit.nodes import BaseNodeDef

logger = logging.getLogger(__name__)


class Worker:
    def __init__(
        self,
        client: Client,
        nodes: list[BaseNodeDef] | None = None,
        max_workers: int = 1,
        group_id: str | None = None,
        extra_publish_kwargs: dict[str, Any] = {},
        extra_subscribe_kwargs: dict[str, Any] = {},
    ):
        self._client = client
        self._nodes = nodes or list()
        self._max_workers = max_workers
        self._group_id = group_id
        self._extra_publish_kwargs = extra_publish_kwargs
        self._extra_subscribe_kwargs = extra_subscribe_kwargs
        self._prepared = False

    def add_nodes(self, *nodes: BaseNodeDef) -> None:
        self._nodes.extend(nodes)

    def register_handlers(self) -> None:
        if self._prepared:
            raise RuntimeError("register_handlers() already called")
        else:
            for node in self._nodes:
                group_id = self._group_id or node.name
                logger.info(
                    "registering node=%s subscribe=%s publish=%s",
                    node.name,
                    node.subscribe_topics,
                    node.publish_topic,
                )
                subscriber = self._client._connection.subscriber(
                    *node.subscribe_topics,
                    group_id=group_id,
                    max_workers=self._max_workers,
                    **self._extra_subscribe_kwargs,
                )
                handler = subscriber(node.handler)
                if node.publish_topic:
                    self._client._connection.publisher(node.publish_topic, **self._extra_publish_kwargs)(handler)

            self._prepared = True

    async def run(self, **extra_run_args: Any) -> None:
        """Blocking method to run worker as a service until stopped."""
        logger.info("worker starting with %d node(s)", len(self._nodes))
        self.register_handlers()
        await FastStream(self._client._connection).run(**extra_run_args)
