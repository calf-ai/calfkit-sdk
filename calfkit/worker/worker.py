import logging
from typing import Any

from faststream import FastStream

from calfkit.client import Client
from calfkit.mcp._bridge import McpBridge
from calfkit.mcp._dedup import IdempotencyCache
from calfkit.mcp._server import McpServer
from calfkit.nodes import BaseNodeDef
from calfkit.provisioning import TopicProvisioner, topics_for_nodes

logger = logging.getLogger(__name__)


class Worker:
    def __init__(
        self,
        client: Client,
        nodes: list[BaseNodeDef | McpServer] | None = None,
        max_workers: int = 1,
        group_id: str | None = None,
        extra_publish_kwargs: dict[str, Any] = {},
        extra_subscribe_kwargs: dict[str, Any] = {},
        idempotency_cache: IdempotencyCache | None = None,
    ):
        """Initialize a worker.

        Args:
            client: The calfkit Client (Kafka connection).
            nodes: Mixed list of ``BaseNodeDef`` instances and ``McpServer``
                instances. McpServers are segregated and expanded into
                ``McpBridge`` per-tool subscribers at ``_on_startup`` time.
            max_workers: FastStream subscriber concurrency cap.
            group_id: Optional Kafka consumer group override (defaults to
                each node's name).
            extra_publish_kwargs: Forwarded to ``broker.publisher(...)``.
            extra_subscribe_kwargs: Forwarded to ``broker.subscriber(...)``.
            idempotency_cache: Optional shared ``IdempotencyCache`` for
                MCP-bridge dedup. If not provided, a fresh cache is
                constructed with default parameters (1hr TTL, 10k entries).
                One cache is shared across all MCP bridges in this worker
                so multi-tool redeliveries dedup correctly.
        """
        self._client = client
        self._max_workers = max_workers
        self._group_id = group_id
        self._extra_publish_kwargs = extra_publish_kwargs
        self._extra_subscribe_kwargs = extra_subscribe_kwargs
        self._prepared = False
        # Snapshot of the nodes actually wired up by ``register_handlers``.
        # Recorded once on the first (effective) call and used as the single
        # source of truth for ``provision_topics`` — it survives the idempotent
        # second call (which is a no-op once ``_prepared`` is set), so the
        # topic set reflects exactly what was registered (including MCP bridges
        # appended just before registration in ``_on_startup``).
        self._registered_nodes: list[BaseNodeDef] = []

        # MCP-specific state. Segregating at construction time means the
        # ``add_nodes`` API path also routes McpServer instances correctly.
        self._mcp_servers: list[McpServer] = []
        self._mcp_bridges: list[McpBridge] = []
        self._dedup_cache: IdempotencyCache = idempotency_cache if idempotency_cache is not None else IdempotencyCache()

        # Regular (non-MCP) nodes. The list type is widened to allow McpServer
        # in the kwarg signature for ergonomics, but storage is split.
        self._nodes: list[BaseNodeDef] = []
        for node in nodes or []:
            self._add_node(node)

    def add_nodes(self, *nodes: BaseNodeDef | McpServer) -> None:
        """Add nodes after construction.

        Segregates ``McpServer`` instances into the MCP-specific state list
        so they get expanded into ``McpBridge`` subscribers at startup,
        rather than being treated as raw nodes (which would fail because
        McpServer doesn't implement the BaseNodeDef handler contract).
        """
        for node in nodes:
            self._add_node(node)

    def _add_node(self, node: BaseNodeDef | McpServer) -> None:
        """Internal: route a node to the right bucket based on type."""
        if isinstance(node, McpServer):
            self._mcp_servers.append(node)
        else:
            self._nodes.append(node)

    def register_handlers(self) -> None:
        """Register FastStream subscribers + publishers for every node.

        Idempotent: a second call logs a warning and returns. This lets the
        Worker.run() lifecycle hook call it safely even if a test driver
        called it manually first (the existing ``tests/providers.py`` pattern).

        Note: McpBridges constructed in ``_on_startup`` are also registered
        via this method — they're appended to ``_nodes`` immediately before
        the registration loop runs.
        """
        if self._prepared:
            logger.debug("register_handlers() called again; skipping (already prepared)")
            return
        # Record the nodes we are about to register as the single source of
        # truth for ``provision_topics``. Snapshot (not alias) so later
        # ``add_nodes`` calls can't retroactively widen the provisioned set
        # beyond what was actually wired up.
        self._registered_nodes = list(self._nodes)
        for node in self._nodes:
            group_id = self._group_id or node.name
            # Subscribe to the node's public inboxes plus its
            # framework-private return inbox. The latter is where tool
            # ``Call`` returns and ``TailCall`` self-retries are addressed
            # exclusively to this node instance — see
            # ``BaseNodeDef._return_topic`` (issue #141). ``dict.fromkeys``
            # preserves declared order while removing duplicates, so a
            # user who manually lists ``f'{node_id}.private.return'`` in
            # ``subscribe_topics`` doesn't end up with a duplicate entry
            # in registration logs / AsyncAPI / observability tooling.
            topics = list(dict.fromkeys([*node.subscribe_topics, node._return_topic]))
            logger.info(
                "registering node=%s subscribe=%s publish=%s",
                node.name,
                topics,
                node.publish_topic,
            )
            subscriber = self._client._connection.subscriber(
                *topics,
                group_id=group_id,
                max_workers=self._max_workers,
                **self._extra_subscribe_kwargs,
            )
            handler = subscriber(node.handler)
            if node.publish_topic:
                self._client._connection.publisher(node.publish_topic, **self._extra_publish_kwargs)(handler)

        self._prepared = True

    async def provision_topics(self) -> None:
        """Best-effort, opt-in creation of every topic the registered nodes use.

        Idempotent and safe to call directly: this is the explicit entry point
        for the manual ``register_handlers()``-without-``app.run()`` path. When
        ``self._client.provisioning.enabled`` is ``False`` it is a pure no-op
        (no admin client is constructed).

        The topic set is :func:`~calfkit.provisioning.topics_for_nodes` over the
        nodes recorded by :meth:`register_handlers` (the single source of truth):
        each node's ``subscribe_topics``, its framework-private ``_return_topic``,
        its ``publish_topic``, and — for agent nodes — each tool's input
        ``subscribe_topics``. MCP bridges appended in :meth:`_on_startup` are
        included because registration runs before this call.

        Every node's ``_return_topic`` is a framework inbox, so it is passed in
        ``framework_topics`` to ensure user ``topic_configs`` (retention /
        compaction overrides) are never applied to those correlation-keyed
        return inboxes.

        **Experimental** (opt-in; off by default): this API may change or be
        removed in a minor release — calfkit is pre-1.0.

        This is a **development convenience** (rf=1, no ACLs); review before
        relying on it in production, where topics are typically ops-governed.
        See :class:`~calfkit.provisioning.ProvisioningConfig` for the caveats.
        """
        config = self._client.provisioning
        if not config.enabled:
            return

        topics = topics_for_nodes(self._registered_nodes)
        if not topics:
            return

        framework_topics = {node._return_topic for node in self._registered_nodes}

        provisioner = TopicProvisioner.from_connection(
            server_urls=self._client.server_urls,
            config=config,
            security_kwargs=self._client.security_kwargs,
        )
        report = await provisioner.provision(topics, framework_topics=framework_topics)
        logger.info(
            "provisioned topics: %d created, %d existing, %d unauthorized",
            len(report.created),
            len(report.existing),
            len(report.unauthorized),
        )
        if report.unauthorized:
            logger.warning(
                "topic provisioning: %d unauthorized topic(s) NOT created (producers/consumers will stall): %s",
                len(report.unauthorized),
                ", ".join(report.unauthorized),
            )

    async def _on_startup(self) -> None:
        """FastStream startup hook — runs before broker.start().

        FastStream's ``on_startup`` fires before any Kafka subscriber
        begins consuming, so we use this window to:

        1. Open each McpServer's MCP session (subprocess spawn or HTTP
           connect + ``initialize`` + drift-detection sanity check).
        2. Construct one ``McpBridge`` per declared tool, sharing the
           per-worker idempotency cache.
        3. Append bridges to ``_nodes`` so the existing handler-registration
           loop picks them up.
        4. Register all subscribers + publishers via ``register_handlers``.

        Cancellation safety: catches ``BaseException`` (not just
        ``Exception``) so SIGTERM during boot still closes any MCP
        subprocesses we successfully spawned before the cancellation.
        """
        spawned: list[McpServer] = []
        try:
            for server in self._mcp_servers:
                await server._open_bridge_session()
                spawned.append(server)
                for tool_def in server._apply_filters(server._tools):
                    self._mcp_bridges.append(
                        McpBridge(
                            server=server,
                            tool_def=tool_def,
                            dedup_cache=self._dedup_cache,
                        )
                    )
            # Bridges go through the same handler-registration path as
            # native nodes so subscribers are wired identically.
            self._nodes.extend(self._mcp_bridges)
            self.register_handlers()
            # Eager, opt-in topic provisioning. Runs AFTER register_handlers so
            # the MCP bridges just appended are part of the registered set, and
            # BEFORE broker.start() (FastStream starts the broker only after
            # this on_startup hook returns) so every subscriber's inbox exists
            # before consumption begins. Kept inside this try so a provisioning
            # failure still triggers the BaseException cleanup below, closing
            # any MCP sessions opened above. ``provision_topics`` self-guards on
            # the disabled config (a documented no-op), so no guard here.
            await self.provision_topics()
        except BaseException:
            # Cancellation, OSError, any other failure — close every
            # session we successfully opened so subprocesses don't outlive
            # the worker process. Re-raise to let FastStream surface the
            # original failure to the caller.
            logger.exception("Worker._on_startup failed; cleaning up %d open MCP session(s)", len(spawned))
            for s in spawned:
                try:
                    await s._close_bridge_session()
                except Exception:
                    logger.exception("failed to close MCP session for %r during startup cleanup", s.raw_name)
            raise

    async def _on_shutdown(self) -> None:
        """FastStream shutdown hook — close MCP sessions cleanly.

        Runs before ``broker.stop()`` so in-flight bridge handlers can
        still complete (the FastStream subscribers are still alive at this
        point — see the audit's lifecycle ordering).

        Errors in individual session teardown are logged and swallowed so
        a slow / hung MCP server doesn't prevent the other sessions from
        being closed.
        """
        for server in self._mcp_servers:
            if server.session is None:
                continue
            try:
                await server._close_bridge_session()
            except Exception:
                logger.exception("failed to close MCP session for %r during shutdown", server.raw_name)

    async def run(self, **extra_run_args: Any) -> None:
        """Blocking method to run worker as a service until stopped."""
        logger.info(
            "worker starting with %d regular node(s), %d MCP server(s)",
            len(self._nodes),
            len(self._mcp_servers),
        )
        app = FastStream(
            self._client._connection,
            on_startup=[self._on_startup],
            on_shutdown=[self._on_shutdown],
        )
        await app.run(**extra_run_args)
