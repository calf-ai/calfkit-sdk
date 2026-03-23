from typing import Any

from faststream import FastStream

from calfkit.experimental.client import Client
from calfkit.experimental.nodes.node_def import BaseNodeDef


class Worker:
    def __init__(
        self,
        client: Client,
        nodes: list[BaseNodeDef],
        max_workers: int | None = None,
        group_id: str | None = None,
        extra_publish_kwargs: dict[str, Any] = {},
        extra_subscribe_kwargs: dict[str, Any] = {},
    ):
        self._client = client
        self._nodes = nodes
        self._max_workers = max_workers
        self._group_id = group_id
        self._extra_publish_kwargs = extra_publish_kwargs
        self._extra_subscribe_kwargs = extra_subscribe_kwargs

    def _subscribe_and_publish_setup(self) -> None:
        for node in self._nodes:
            group_id = self._group_id or node.name
            subscriber = self._client._connection.subscriber(
                *node.subscribe_topics,
                group_id=group_id,
                max_workers=self._max_workers,
                **self._extra_subscribe_kwargs,
            )
            node.run = subscriber(node.run)
            if node.publish_topic:
                node.run = self._client._connection.publisher(
                    node.publish_topic, **self._extra_publish_kwargs
                )(node.run)

    async def run(self, **extra_run_args: Any) -> None:
        """Blocking method to run worker as a service until stopped."""
        self._subscribe_and_publish_setup()
        await FastStream(self._client._connection).run(**extra_run_args)
