import logging
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack
from types import TracebackType
from typing import Any

import uuid_utils
from faststream import FastStream
from typing_extensions import Self

from calfkit.client import Client
from calfkit.models.capability import CAPABILITY_VIEW_RESOURCE_KEY, CapabilityRecord
from calfkit.nodes import BaseNodeDef
from calfkit.provisioning import topics_for_nodes
from calfkit.worker.lifecycle import (
    PHASE_PAIRS,
    LifecycleContext,
    LifecycleHookMixin,
    ResourceSetupContext,
    ServingContext,
    SupportsLifecycleHooks,
    _resource_cm,
    _span_cm,
)
from calfkit.worker.worker_config import MCPDiscoveryConfig

logger = logging.getLogger(__name__)


class Worker(LifecycleHookMixin):
    """Hosts nodes against a Kafka broker and manages their lifecycle.

    A worker registers each node's FastStream subscribers/publishers and brackets
    them with lifecycle hooks (``@resource`` / ``on_startup`` / ``after_startup``
    / ``on_shutdown`` / ``after_shutdown``) inherited from
    :class:`~calfkit.worker.lifecycle.LifecycleHookMixin`. Resources opened by
    the worker are merged into every node handler's ``ctx.resources`` (node keys
    win on collision); resources opened by a node are visible only to that node.

    A worker is **single-use**: once started it cannot be restarted; create a new
    ``Worker`` instead. Pick one of three run surfaces:

    =========================  ===============  ======  ============================
    Surface                    Signal handlers  Blocks  Use when
    =========================  ===============  ======  ============================
    ``await worker.run()``     yes              yes     deploying as a service
    ``await worker.start()``   no               no      programmatic/embedded control
    / ``await worker.stop()``
    ``async with worker:``     no               no      tests, short-lived embedding
    =========================  ===============  ======  ============================

    On a failed boot (e.g. the broker can't reach Kafka), ``start()``, ``run()``,
    and ``async with`` all run teardown automatically before re-raising, so a
    failed start never leaks resources.
    """

    def __init__(
        self,
        client: Client,
        nodes: list[BaseNodeDef] | None = None,
        max_workers: int = 1,
        group_id: str | None = None,
        extra_publish_kwargs: dict[str, Any] = {},
        extra_subscribe_kwargs: dict[str, Any] = {},
        id: str | None = None,
        name: str | None = None,
        mcp_discovery: MCPDiscoveryConfig | None = None,
    ):
        """Initialize a worker.

        Args:
            client: The calfkit Client (Kafka connection).
            nodes: List of ``BaseNodeDef`` instances to host.
            max_workers: FastStream subscriber concurrency cap.
            group_id: Optional Kafka consumer group override (defaults to
                each node's name).
            extra_publish_kwargs: Forwarded to ``broker.publisher(...)``.
            extra_subscribe_kwargs: Forwarded to ``broker.subscriber(...)``.
            id: Stable wire identity for this worker (e.g. for fleet presence).
                Validated non-empty; a uuid7 hex is generated when ``None``.
                Read-only after construction — mirrors ``BaseClient.emitter_id``.
            name: Display-only label; defaults to ``id``. Never put on the wire.
            mcp_discovery: Optional tuning for MCP capability discovery
                (topic, catch-up timeout, bootstrap override). Entirely
                optional — the Capability View is auto-registered with
                defaults whenever a hosted agent declares MCP tool selectors.
        """
        if id is not None and not id.strip():
            raise ValueError("id must be a non-empty string or None")
        if name is not None and not name.strip():
            raise ValueError("name must be a non-empty string or None")
        self._client = client
        self._mcp_discovery = mcp_discovery if mcp_discovery is not None else MCPDiscoveryConfig()
        self._max_workers = max_workers
        self._group_id = group_id
        self._extra_publish_kwargs = extra_publish_kwargs
        self._extra_subscribe_kwargs = extra_subscribe_kwargs
        self._prepared = False
        self._id = id if id is not None else uuid_utils.uuid7().hex
        self._name = name if name is not None else self._id
        self._started = False

        # Lifecycle bracket stacks. ``resource`` brackets (callbacks + @resource)
        # are entered before the broker starts and torn down after it stops;
        # ``serving`` brackets run while the broker consumes. Built lazily by the
        # hooks; ``None`` means "not yet entered" so ``_safe_aclose`` is a no-op.
        self._resource_stack: AsyncExitStack | None = None
        self._serving_stack: AsyncExitStack | None = None
        # The FastStream app, built once on start()/run(). ``None`` until then so
        # stop() before start() is a safe no-op rather than an AttributeError.
        self._app: FastStream | None = None
        # Snapshot of the nodes actually wired up by ``register_handlers``.
        # Recorded once on the first (effective) call and used as the single
        # source of truth for ``_declare_startup_topics`` — it survives the
        # idempotent second call (which is a no-op once ``_prepared`` is set), so
        # the topic set reflects exactly what was registered.
        self._registered_nodes: list[BaseNodeDef] = []

        self._nodes: list[BaseNodeDef] = []
        for node in nodes or []:
            self._add_node(node)

    @property
    def id(self) -> str:
        """Stable wire identity for this worker (read-only; set at construction)."""
        return self._id

    @property
    def name(self) -> str:
        """Display-only label (read-only; defaults to ``id``). Never on the wire."""
        return self._name

    def add_nodes(self, *nodes: BaseNodeDef) -> None:
        """Add nodes after construction."""
        for node in nodes:
            self._add_node(node)

    def _add_node(self, node: BaseNodeDef) -> None:
        """Internal: register a node for hosting."""
        # Back-reference so the node's per-message handler can merge this
        # worker's lifecycle resources under its own (see
        # BaseNodeDef._effective_resources / prepare_context). Last writer
        # wins: re-using one node-def instance across workers (a common test
        # pattern) points it at whichever worker is currently hosting it.
        # Hosting the SAME instance in two *concurrently live* workers is
        # unsupported (one shared resource bag, double subscriber
        # registration) — use a separate instance per live worker.
        node._worker = self
        self._nodes.append(node)

    def _maybe_register_capability_view(self) -> None:
        """Auto-register the MCP Capability View resource (spec §8.3).

        Zero user wiring: iff any hosted node declares MCP tool selectors, ONE
        worker-level ``KafkaTable[CapabilityRecord]`` resource is registered
        (idempotent — guarded by resource-name lookup). The table lands in the
        worker bag and reaches every node's ``ctx.resources`` via the existing
        worker-under-node merge.
        """
        if not any(getattr(node, "_tool_selectors", None) for node in self._nodes):
            return
        if any(name == CAPABILITY_VIEW_RESOURCE_KEY for name, _ in self._resource_cms()):
            return
        self.resource(name=CAPABILITY_VIEW_RESOURCE_KEY)(self._capability_view_resource)

    async def _capability_view_resource(self, ctx: ResourceSetupContext["Worker"]) -> AsyncIterator[Any]:
        """Open the Capability View for this worker's lifetime.

        ``KafkaTable.start()`` IS the boot gate: it replays the capability
        topic to its start-time end offsets bounded by ``catchup_timeout``
        (serving degraded, loudly, on expiry) — and because resource setup
        runs before the broker serves, agents never see a half-built view.
        """
        from ktables import KafkaTable  # the one ktables import outside calfkit.mcp

        cfg = self._mcp_discovery
        bootstrap = cfg.bootstrap_servers or self._client.server_urls
        if not bootstrap:
            kwargs = getattr(self._client.broker, "_connection_kwargs", None) or {}
            servers = kwargs.get("bootstrap_servers")
            bootstrap = servers if isinstance(servers, str) else ",".join(servers) if servers else None
        if not bootstrap:
            raise RuntimeError(
                "cannot derive Kafka bootstrap servers for the MCP Capability View "
                "(client built without connect()?); set MCPDiscoveryConfig(bootstrap_servers=...)."
            )
        table: KafkaTable[CapabilityRecord] = KafkaTable.json(
            bootstrap_servers=bootstrap,
            topic=cfg.topic,
            model=CapabilityRecord,
            catchup_timeout=cfg.catchup_timeout,
            # Readers ensure only in dev/CI (spec §3.1); production topics are
            # ops-governed and a missing topic fails setup loudly.
            ensure_topic=self._client._provisioning.enabled,
        )
        await table.start()
        yield table
        await table.stop()

    def register_handlers(self) -> None:
        """Register FastStream subscribers + publishers for every node.

        Idempotent: a second call logs at debug and returns. This lets the
        Worker.run() lifecycle hook call it safely even if a test driver
        called it manually first (the existing ``tests/providers.py`` pattern).
        """
        if self._prepared:
            logger.debug("register_handlers() called again; skipping (already prepared)")
            return
        self._maybe_register_capability_view()
        # Record the nodes we are about to register as the single source of
        # truth for ``_declare_startup_topics``. Snapshot (not alias) so later
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

    def _declare_startup_topics(self) -> None:
        """Declare the registered nodes' topics into the client's startup ensurer.

        The ensurer (the broker's pre-start hook) creates them — when
        provisioning is enabled — at broker start, before any subscriber
        consumes, alongside the client's reply topic. This keeps a single
        provisioning pass over one admin client (FastStream's) for the whole
        process. Declaration itself is cheap and unconditional; the enabled gate
        lives in the ensurer.

        The topic set is :func:`~calfkit.provisioning.topics_for_nodes` over the
        nodes recorded by :meth:`register_handlers` (the single source of truth):
        each node's ``subscribe_topics``, its framework-private ``_return_topic``,
        its ``publish_topic``, and — for agent nodes — each tool's input
        ``subscribe_topics``. Every node's ``_return_topic`` is declared
        ``framework=True`` so user ``topic_configs``
        (retention / compaction) are never applied to those correlation-keyed
        return inboxes.
        """
        ensurer = self._client._startup_ensurer
        ensurer.declare(topics_for_nodes(self._registered_nodes))
        ensurer.declare({node._return_topic for node in self._registered_nodes}, framework=True)

    async def _on_startup(self) -> None:
        """Register handlers + declare topics, before the broker starts.

        Invoked by :meth:`_hook_on_startup` (the wired FastStream ``on_startup``
        hook). Runs before any Kafka subscriber begins consuming: wire up all
        subscribers + publishers, then declare the registered nodes' topics into
        the client's startup ensurer (which provisions them — when provisioning
        is enabled — at ``broker.start()``, before any subscriber consumes,
        alongside the client's reply topic).
        """
        self.register_handlers()
        self._declare_startup_topics()

    # ------------------------------------------------------------------
    # Lifecycle engine: build per-owner brackets and enter them into a phase
    # stack. Owners are the worker itself then its nodes.
    # ------------------------------------------------------------------

    def _owners(self) -> list[SupportsLifecycleHooks]:
        """Owners that participate in the lifecycle, worker first then nodes.

        The return type also exercises ``SupportsLifecycleHooks`` conformance:
        this line fails to type-check if ``Worker``/``BaseNodeDef`` ever drift
        from the structural surface the CM builders rely on.
        """
        return [self, *self._nodes]

    def _make_ctx(self, owner: Any, pair: str) -> Any:
        """Build the callback-span context for ``owner``.

        ``serving`` phases get a read-only-typed resources view plus the broker;
        ``resource`` phases get a writable view so ``on_startup``/
        ``after_shutdown`` callbacks can set ``ctx.resources["key"]``. Both share
        the owner's plain-dict bag; the read-only-ness is type-level.
        """
        (_enter, _exit), has_res = PHASE_PAIRS[pair]
        if not has_res:
            return ServingContext(owner, owner.resources, self._client.broker)
        return LifecycleContext(owner, owner.resources)

    def _owner_cms(self, owner: Any, pair: str) -> list[Any]:
        """Build the async CMs for ``owner`` in ``pair``.

        One resource pattern per owner: if the owner declares ``@resource``
        brackets, those win and any ``on_startup``/``after_shutdown`` callbacks
        are ignored with a warning (mirrors FastAPI's lifespan-vs-on_event rule).
        Otherwise the callback span runs. ``serving`` phases only ever have the
        callback span. Returns ``[]`` when the owner has nothing for this pair.
        """
        (enter, exit_), has_res = PHASE_PAIRS[pair]
        has_callbacks = bool(owner._hooks_for(enter) or owner._hooks_for(exit_))
        if has_res and owner._resource_cms():
            if has_callbacks:
                logger.warning(
                    "%r registers @resource(...) and %s/%s callbacks; the callbacks are ignored. "
                    "Use one resource pattern per owner (like FastAPI's lifespan vs on_event).",
                    owner,
                    enter,
                    exit_,
                )
            setup_ctx = ResourceSetupContext(owner, owner.resources)
            return [_resource_cm(owner, name, genfn, setup_ctx) for name, genfn in owner._resource_cms()]
        if has_callbacks:
            return [_span_cm(owner, enter, exit_, self._make_ctx(owner, pair))]
        return []

    async def _enter_into(self, stack: AsyncExitStack, pair: str) -> None:
        """Enter every owner's CMs for ``pair`` into ``stack``, worker first."""
        for owner in self._owners():
            for cm in self._owner_cms(owner, pair):
                await stack.enter_async_context(cm)

    @staticmethod
    async def _safe_aclose(stack: AsyncExitStack | None) -> None:
        """Close a phase stack if it was entered; teardown logs-never-raises.

        Each CM's own ``finally`` is guarded (``_safe_teardown`` /
        ``_resource_cm``), so ``aclose`` here unwinds the whole stack without
        re-raising teardown errors.
        """
        if stack is not None:
            await stack.aclose()

    # ------------------------------------------------------------------
    # Four FastStream hooks. ``_on_startup`` registers handlers + declares
    # topics; the resource/serving brackets are layered on top.
    # ------------------------------------------------------------------

    async def _hook_on_startup(self) -> None:
        """``on_startup``: register handlers + declare topics, then enter resource brackets.

        If entering the resource stack fails (or is cancelled), roll back the
        resource stack before re-raising the original error so boot fails cleanly.
        """
        await self._on_startup()
        self._resource_stack = AsyncExitStack()
        try:
            await self._enter_into(self._resource_stack, "resource")
        except BaseException:
            await self._safe_aclose(self._resource_stack)
            self._resource_stack = None
            raise

    async def _hook_after_startup(self) -> None:
        """``after_startup``: enter serving brackets while the broker consumes.

        FastStream skips shutdown hooks when ``after_startup`` raises, so a
        failure here must unwind both stacks and stop the broker itself before
        re-raising.
        """
        self._serving_stack = AsyncExitStack()
        try:
            await self._enter_into(self._serving_stack, "serving")
        except BaseException:
            # Mirror the normal shutdown order so an in-flight handler never
            # reads a torn-down resource: serving teardown while the broker is
            # up, then drain (broker.stop), then resource teardown.
            await self._safe_aclose(self._serving_stack)
            self._serving_stack = None
            try:
                await self._client.broker.stop()  # drain in-flight handlers
            except Exception:
                logger.exception("broker.stop() failed during after_startup rollback")
            await self._safe_aclose(self._resource_stack)  # tear down resources post-drain
            self._resource_stack = None
            raise

    async def _hook_on_shutdown(self) -> None:
        """``on_shutdown``: tear down serving brackets.

        Runs before ``broker.stop()`` so in-flight handlers can still complete.
        """
        await self._safe_aclose(self._serving_stack)
        self._serving_stack = None

    async def _hook_after_shutdown(self) -> None:
        """``after_shutdown``: tear down resource brackets (post-drain)."""
        await self._safe_aclose(self._resource_stack)
        self._resource_stack = None

    def _build_app(self) -> FastStream:
        """Build the FastStream app once, wiring all four lifecycle hooks."""
        return FastStream(
            self._client._connection,
            on_startup=[self._hook_on_startup],
            after_startup=[self._hook_after_startup],
            on_shutdown=[self._hook_on_shutdown],
            after_shutdown=[self._hook_after_shutdown],
        )

    def _mark_started(self) -> None:
        if self._started:
            raise RuntimeError("Worker is single-use; create a new Worker to restart")
        self._started = True

    # ------------------------------------------------------------------
    # Three-surface lifecycle: start/stop, async-with, run.
    # ------------------------------------------------------------------

    async def start(self, **run_extra: Any) -> None:
        """Start the worker without installing signal handlers (programmatic use).

        Raises:
            RuntimeError: if the worker was already started (single-use).
        """
        self._mark_started()
        logger.info(
            "worker %s starting with %d node(s)",
            self.id,
            len(self._nodes),
        )
        self._app = self._build_app()
        try:
            await self._app.start(**run_extra)
        except BaseException:
            # FastStream runs no shutdown hooks if startup fails *after*
            # on_startup (e.g. broker.start() can't reach Kafka), which would
            # orphan the resource brackets opened in _hook_on_startup. Python
            # also skips __aexit__ when __aenter__ raises, so `async with
            # worker:` couldn't recover either. Run our own teardown
            # (idempotent) before re-raising so a failed boot never leaks. A
            # failure *in* teardown is logged, not raised, so it never masks the
            # original boot error.
            await self._cleanup_after_failed_start()
            raise

    async def stop(self) -> None:
        """Stop the worker (drains then disconnects the broker).

        A no-op if the worker was never started, so a defensive
        ``try/finally: await worker.stop()`` is safe.

        FastStream's ``stop()`` runs ``on_shutdown`` → ``broker.stop()`` →
        ``after_shutdown`` with no ``try/finally``, so if ``broker.stop()``
        raises (flaky disconnect) or shutdown is cancelled, it skips
        ``after_shutdown`` — where the resource brackets are torn down. We
        therefore release the resource stack in a ``finally`` regardless, so a
        failed/cancelled drain never strands pools or clients.
        Resource teardown still happens *after* the drain attempt, preserving
        the "drain before close" order. ``CancelledError`` still propagates
        after the best-effort teardown attempt.
        """
        if self._app is None:
            return
        try:
            await self._app.stop()
        finally:
            # Idempotent: _hook_after_shutdown nulls the stack on the clean
            # path, so this is a no-op then; it only does real work when
            # after_shutdown was skipped by a failing/cancelled drain.
            await self._safe_aclose(self._resource_stack)
            self._resource_stack = None

    async def _cleanup_after_failed_start(self) -> None:
        """Best-effort teardown after a failed ``start()``/``run()``.

        Runs the shutdown hooks (via ``stop()``) to release any resource
        brackets opened before the failure. A failure *here* is logged, never
        raised, so it cannot mask the original boot error that the caller is
        about to re-raise. ``CancelledError`` still propagates.
        """
        try:
            await self.stop()
        except Exception:
            # Name what may still be open so an operator knows a manual
            # cleanup / restart is warranted rather than assuming graceful release.
            leaked_resources = self._resource_stack is not None
            logger.exception(
                "worker %s teardown after failed start failed; original boot error will be raised (resource brackets still open=%s)",
                self.id,
                leaked_resources,
            )

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.stop()

    async def run(self, **extra_run_args: Any) -> None:
        """Blocking method to run worker as a service until stopped.

        Installs signal handlers and blocks until the worker is signalled.

        Raises:
            RuntimeError: if the worker was already started (single-use).
        """
        self._mark_started()
        logger.info(
            "worker %s starting with %d node(s)",
            self.id,
            len(self._nodes),
        )
        self._app = self._build_app()
        try:
            await self._app.run(**extra_run_args)
        except BaseException:
            # FastStream's run() never reaches its own _shutdown() when startup
            # raises inside the task group, so (as in start()) run our teardown
            # to release resources. stop() is idempotent, so this is safe even
            # when run() already shut down cleanly before failing.
            await self._cleanup_after_failed_start()
            raise
