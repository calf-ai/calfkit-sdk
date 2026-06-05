import logging
from contextlib import AsyncExitStack
from types import TracebackType
from typing import Any

import uuid_utils
from faststream import FastStream
from typing_extensions import Self

from calfkit.client import Client
from calfkit.mcp._bridge import McpBridge
from calfkit.mcp._dedup import IdempotencyCache
from calfkit.mcp._server import McpServer
from calfkit.nodes import BaseNodeDef
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
    failed start never leaks resources or MCP sessions.
    """

    def __init__(
        self,
        client: Client,
        nodes: list[BaseNodeDef | McpServer] | None = None,
        max_workers: int = 1,
        group_id: str | None = None,
        extra_publish_kwargs: dict[str, Any] = {},
        extra_subscribe_kwargs: dict[str, Any] = {},
        idempotency_cache: IdempotencyCache | None = None,
        id: str | None = None,
        name: str | None = None,
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
            id: Stable wire identity for this worker (e.g. for fleet presence).
                Validated non-empty; a uuid7 hex is generated when ``None``.
                Read-only after construction — mirrors ``BaseClient.emitter_id``.
            name: Display-only label; defaults to ``id``. Never put on the wire.
        """
        if id is not None and not id.strip():
            raise ValueError("id must be a non-empty string or None")
        if name is not None and not name.strip():
            raise ValueError("name must be a non-empty string or None")
        self._client = client
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

    @property
    def id(self) -> str:
        """Stable wire identity for this worker (read-only; set at construction)."""
        return self._id

    @property
    def name(self) -> str:
        """Display-only label (read-only; defaults to ``id``). Never on the wire."""
        return self._name

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
            # Back-reference so the node's per-message handler can merge this
            # worker's lifecycle resources under its own (see
            # BaseNodeDef._effective_resources / prepare_context).
            node._worker = self
            self._nodes.append(node)

    def register_handlers(self) -> None:
        """Register FastStream subscribers + publishers for every node.

        Idempotent: a second call logs at debug and returns. This lets the
        Worker.run() lifecycle hook call it safely even if a test driver
        called it manually first (the existing ``tests/providers.py`` pattern).

        Note: McpBridges constructed in ``_on_startup`` are also registered
        via this method — they're appended to ``_nodes`` immediately before
        the registration loop runs.
        """
        if self._prepared:
            logger.debug("register_handlers() called again; skipping (already prepared)")
            return
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

    async def _on_startup(self) -> None:
        """Open MCP sessions and register handlers, before the broker starts.

        Invoked by :meth:`_hook_on_startup` (the wired FastStream ``on_startup``
        hook) and by the rollback paths. Runs before any Kafka subscriber
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
            # native nodes so subscribers are wired identically; give them the
            # same worker back-reference as _add_node so worker-scoped resources
            # merge into bridge handlers too.
            for bridge in self._mcp_bridges:
                bridge._worker = self
            self._nodes.extend(self._mcp_bridges)
            self.register_handlers()
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
        """Close MCP sessions cleanly.

        Invoked by :meth:`_hook_on_shutdown` (the wired FastStream
        ``on_shutdown`` hook) and by the startup / after-startup rollback paths,
        always while the broker is still up so in-flight bridge handlers can
        still complete.

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

    # ------------------------------------------------------------------
    # Lifecycle engine: build per-owner brackets and enter them into a phase
    # stack. Owners are the worker itself then its nodes.
    # ------------------------------------------------------------------

    def _owners(self) -> list[SupportsLifecycleHooks]:
        """Owners that participate in the lifecycle, worker first then nodes.

        ``McpServer`` instances are *not* owners here — they're managed by the
        existing MCP open/close logic, not the generic bracket engine. The
        return type also exercises ``SupportsLifecycleHooks`` conformance: this
        line fails to type-check if ``Worker``/``BaseNodeDef`` ever drift from
        the structural surface the CM builders rely on.
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
    # Four FastStream hooks. MCP open/close is preserved as-is
    # (``_on_startup`` / ``_on_shutdown``); the resource/serving brackets are
    # layered on top.
    # ------------------------------------------------------------------

    async def _hook_on_startup(self) -> None:
        """``on_startup``: open MCP + register handlers, then enter resource brackets.

        If entering the resource stack fails (or is cancelled), roll back the
        resource stack *and* the MCP sessions opened by ``_on_startup`` before
        re-raising the original error so boot fails cleanly.
        """
        # Existing MCP open + register_handlers; this method does its own MCP
        # cleanup if it fails, so a failure here needs no extra MCP teardown.
        await self._on_startup()
        self._resource_stack = AsyncExitStack()
        try:
            await self._enter_into(self._resource_stack, "resource")
        except BaseException:
            await self._safe_aclose(self._resource_stack)
            self._resource_stack = None
            await self._on_shutdown()  # close MCP sessions opened above
            raise

    async def _hook_after_startup(self) -> None:
        """``after_startup``: enter serving brackets while the broker consumes.

        FastStream skips shutdown hooks when ``after_startup`` raises, so a
        failure here must unwind both stacks, close MCP, and stop the broker
        itself before re-raising.
        """
        self._serving_stack = AsyncExitStack()
        try:
            await self._enter_into(self._serving_stack, "serving")
        except BaseException:
            # Mirror the normal shutdown order so an in-flight handler never
            # reads a torn-down resource: serving teardown + MCP close while the
            # broker is up, then drain (broker.stop), then resource teardown.
            await self._safe_aclose(self._serving_stack)
            self._serving_stack = None
            await self._on_shutdown()  # close MCP sessions (broker still up)
            try:
                await self._client.broker.stop()  # drain in-flight handlers
            except Exception:
                logger.exception("broker.stop() failed during after_startup rollback")
            await self._safe_aclose(self._resource_stack)  # tear down resources post-drain
            self._resource_stack = None
            raise

    async def _hook_on_shutdown(self) -> None:
        """``on_shutdown``: tear down serving brackets, then close MCP sessions.

        Runs before ``broker.stop()`` so in-flight handlers can still complete.
        """
        await self._safe_aclose(self._serving_stack)
        self._serving_stack = None
        await self._on_shutdown()

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
            "worker %s starting with %d regular node(s), %d MCP server(s)",
            self.id,
            len(self._nodes),
            len(self._mcp_servers),
        )
        self._app = self._build_app()
        try:
            await self._app.start(**run_extra)
        except BaseException:
            # FastStream runs no shutdown hooks if startup fails *after*
            # on_startup (e.g. broker.start() can't reach Kafka), which would
            # orphan the resource brackets + MCP sessions opened in
            # _hook_on_startup. Python also skips __aexit__ when __aenter__
            # raises, so `async with worker:` couldn't recover either. Run our
            # own teardown (idempotent) before re-raising so a failed boot
            # never leaks. A failure *in* teardown is logged, not raised, so it
            # never masks the original boot error.
            await self._cleanup_after_failed_start()
            raise

    async def stop(self) -> None:
        """Stop the worker (drains then disconnects the broker).

        A no-op if the worker was never started, so a defensive
        ``try/finally: await worker.stop()`` is safe.

        Cancellation caveat: resource/serving teardown is best-effort under
        *cancellation*. If the surrounding task is cancelled mid-shutdown, a
        ``CancelledError`` propagates out of the serving-teardown hook and
        FastStream then skips ``broker.stop()`` and the resource-bracket
        teardown — those resources may not close. The signal-driven
        :meth:`run` surface is unaffected (FastStream runs shutdown to
        completion before cancelling its task group); this only affects
        external cancellation of the programmatic ``start``/``stop`` /
        ``async with`` surfaces.
        """
        if self._app is not None:
            await self._app.stop()

    async def _cleanup_after_failed_start(self) -> None:
        """Best-effort teardown after a failed ``start()``/``run()``.

        Runs the shutdown hooks (via ``stop()``) to release any resource
        brackets / MCP sessions opened before the failure. A failure *here* is
        logged, never raised, so it cannot mask the original boot error that the
        caller is about to re-raise. ``CancelledError`` still propagates.
        """
        try:
            await self.stop()
        except Exception:
            logger.exception("worker %s teardown after failed start failed; original boot error will be raised", self.id)

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
            "worker %s starting with %d regular node(s), %d MCP server(s)",
            self.id,
            len(self._nodes),
            len(self._mcp_servers),
        )
        self._app = self._build_app()
        try:
            await self._app.run(**extra_run_args)
        except BaseException:
            # FastStream's run() never reaches its own _shutdown() when startup
            # raises inside the task group, so (as in start()) run our teardown
            # to release resources + MCP. stop() is idempotent, so this is safe
            # even when run() already shut down cleanly before failing.
            await self._cleanup_after_failed_start()
            raise
