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

        Order of operations matters here: all configuration validation
        (kafka_config presence, rehydration timeout floor) runs BEFORE
        ``broker.connect()`` so that a validation failure does not leak
        a connected (and never-closed) broker — aiokafka emits
        "unclosed" warnings on Python exit when that happens.
        ``assert_rehydration_timeout_ok`` reads only KafkaConfig
        attributes; it has no dependency on broker state.
        """
        broker = self._client._connection

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
        # the floor doesn't apply. The assertion raises
        # ``DurabilityConfigError`` if the floor is breached — fail-fast
        # at worker startup is correct because the alternative (a
        # rebalance storm on first reassignment) is operationally severe
        # and requires a restart to recover anyway.
        #
        # MUST run before ``broker.connect()``: if the assertion raises
        # after connect, the broker is left connected and never closed,
        # producing aiokafka "unclosed" warnings on process exit.
        if has_agent_node and kafka_config is not None:
            kafka_config.assert_rehydration_timeout_ok()

        # Connect only after validation passes so a config failure
        # doesn't leak broker resources.
        await broker.connect()

        for node in self._nodes:
            if isinstance(node, BaseAgentNodeDef):
                main_topic = node.subscribe_topics[0]
                # The guard above raises for any agent node when
                # kafka_config is None, so it is non-None here. Use an
                # explicit raise (not ``assert``) because asserts are
                # stripped under ``python -O`` / ``PYTHONOPTIMIZE=1``.
                if kafka_config is None:
                    raise RuntimeError(
                        "kafka_config is required when an aggregator is wired; this should have been caught at the earlier validation guard"
                    )
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

                # Compose the topic list. Add the node's framework-private
                # return inbox (``_return_topic``) to its MAIN public
                # subscription only — where tool ``Call`` returns and
                # ``TailCall`` self-retries are addressed exclusively to this
                # node instance (issue #141 / PR #142). Auxiliary subscriptions
                # (e.g., the aggregator's ``fanout-returns`` subscriber) own
                # their own topics under a separate consumer group and must
                # not also subscribe to ``_return_topic`` — that would split
                # delivery across two consumer groups. Detection uses
                # ``sub.handler == node.handler`` (bound-method ``==`` is
                # semantic identity: same ``__self__`` and same ``__func__``).
                # ``dict.fromkeys`` preserves declared order while removing
                # duplicates, so a user who manually lists
                # ``f'{node_id}.private.return'`` in ``subscribe_topics``
                # doesn't end up with a duplicate registration.
                if sub.handler == node.handler:
                    topics = list(dict.fromkeys([*sub.topics, node._return_topic]))
                else:
                    topics = list(sub.topics)

                logger.info(
                    "registering node=%s topics=%s group_id=%s publish=%s",
                    node.name,
                    topics,
                    group_id,
                    sub.publish_topic,
                )
                subscriber = self._client._connection.subscriber(
                    *topics,
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
