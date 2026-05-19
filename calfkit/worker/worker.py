import logging
from typing import Any

from faststream import FastStream

from calfkit.client import Client
from calfkit.nodes import BaseNodeDef
from calfkit.nodes.agent import BaseAgentNodeDef

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

    async def _prepare_aggregators(self) -> None:
        """Run :meth:`FanOutAggregator.setup` for each agent node.

        Requires the broker to be connected (so ``broker.config.admin_client``
        is available). Called from :meth:`run` before
        :meth:`register_handlers`.
        """
        broker = self._client._connection
        # Ensure broker is connected so admin_client is available.
        await broker.connect()
        for node in self._nodes:
            if isinstance(node, BaseAgentNodeDef):
                main_topic = node.subscribe_topics[0]
                await node.aggregator.setup(broker, node_id=node.node_id, main_topic=main_topic)

    def register_handlers(self) -> None:
        if self._prepared:
            raise RuntimeError("register_handlers() already called")

        for node in self._nodes:
            for sub in node.kafka_subscriptions():
                group_id = sub.group_id or self._group_id or node.name
                max_workers = sub.max_workers if sub.max_workers is not None else self._max_workers

                # Per-subscription extra_kwargs override Worker-level values on conflict.
                merged_extra_kwargs: dict[str, Any] = {**self._extra_subscribe_kwargs, **sub.extra_kwargs}
                if sub.listener is not None:
                    merged_extra_kwargs["listener"] = sub.listener
                if sub.ack_policy is not None:
                    merged_extra_kwargs["ack_policy"] = sub.ack_policy

                logger.info(
                    "registering node=%s topics=%s group_id=%s publish=%s",
                    node.name,
                    sub.topics,
                    group_id,
                    sub.publish_topic,
                )
                subscriber = self._client._connection.subscriber(
                    *sub.topics,
                    group_id=group_id,
                    max_workers=max_workers,
                    **merged_extra_kwargs,
                )
                handler = subscriber(sub.handler)
                if sub.publish_topic:
                    self._client._connection.publisher(sub.publish_topic, **self._extra_publish_kwargs)(handler)

        self._prepared = True

    async def run(self, **extra_run_args: Any) -> None:
        """Blocking method to run worker as a service until stopped.

        Order of operations:

        1. Connect broker and run :meth:`FanOutAggregator.setup` for each
           agent (provisions per-agent topics and wires the state store +
           rebalance listener).
        2. Register handlers (now reads each agent's expanded
           ``kafka_subscriptions`` including the aggregator returns
           subscriber).
        3. Hand off to FastStream's lifecycle.
        """
        logger.info("worker starting with %d node(s)", len(self._nodes))
        await self._prepare_aggregators()
        self.register_handlers()
        await FastStream(self._client._connection).run(**extra_run_args)
