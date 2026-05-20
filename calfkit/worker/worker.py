import logging
from typing import Any

from faststream import FastStream

from calfkit.client import Client
from calfkit.nodes import BaseNodeDef
from calfkit.nodes.agent import BaseAgentNodeDef
from calfkit.nodes.aggregator.errors import AggregatorStateStoreError

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

        # Aggregators need the broker's bootstrap servers + security kwargs
        # to construct their transient AIOKafkaConsumer for state-topic
        # rehydration. Client.connect captures these; if a caller built the
        # client directly via BaseClient(...) and skipped the snapshot,
        # we fail loudly rather than silently defaulting to a localhost
        # placeholder.
        kafka_config = self._client.kafka_config
        has_agent_node = any(isinstance(n, BaseAgentNodeDef) for n in self._nodes)
        if kafka_config is None and has_agent_node:
            raise AggregatorStateStoreError(
                "Worker requires Client.kafka_config to wire up the durable "
                "fan-out aggregator. Either use Client.connect(...) (which "
                "captures kafka_config automatically) or pass kafka_config "
                "explicitly when constructing BaseClient."
            )

        # Only check the rehydration timeout floor when at least one
        # agent node will register a state-store rebalance listener.
        # Workers without aggregators don't pay the rehydration cost, so
        # the floor doesn't apply.
        if has_agent_node and kafka_config is not None:
            kafka_config.check_rehydration_timeout_floor()

        for node in self._nodes:
            if isinstance(node, BaseAgentNodeDef):
                main_topic = node.subscribe_topics[0]
                # kafka_config is not None here: the guard above raised for
                # any agent node when it was None.
                assert kafka_config is not None
                await node.aggregator.setup(
                    broker,
                    node_id=node.node_id,
                    main_topic=main_topic,
                    kafka_config=kafka_config,
                )

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
