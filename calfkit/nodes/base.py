import inspect
import logging
from abc import abstractmethod
from collections.abc import Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, Annotated, Any, ClassVar

from faststream import Context, Response
from faststream.kafka.annotations import (
    KafkaBroker as BrokerAnnotation,
)
from pydantic import ValidationError

from calfkit._protocol import HDR_EMITTER, HDR_EMITTER_KIND, HDR_ROUTE, NodeKind, decode_header_str
from calfkit._registry import RegistryMixin
from calfkit._routing import match_chain
from calfkit.exceptions import RegistryConfigError
from calfkit.models import (
    Call,
    Next,
    NodeResult,
    ReturnCall,
    Silent,
    State,
    TailCall,
)
from calfkit.models.envelope import Envelope
from calfkit.models.node_schema import BaseNodeSchema
from calfkit.models.session_context import SessionRunContext
from calfkit.worker.lifecycle import LifecycleHookMixin

if TYPE_CHECKING:
    from calfkit.worker.worker import Worker

logger = logging.getLogger(__name__)


GateFunction = Callable[[SessionRunContext], bool | Awaitable[bool]]
"""A predicate evaluated in ``handler()`` before ``run()``. Sync or async; must return ``bool``.

Returning ``False`` (or raising, or returning a non-bool) skips ``run()`` and returns the
envelope unchanged. Gates stack with AND semantics in registration order and short-circuit
on the first rejection.
"""


# ---------------------------------------------------------------------------
# Base node definition
# ---------------------------------------------------------------------------


class BaseNodeDef(BaseNodeSchema, LifecycleHookMixin, RegistryMixin):
    _run_accepts_input: bool
    _worker: "Worker | None" = None
    """Back-reference to the owning worker, set by ``Worker._add_node``. ``None``
    for a node not attached to a worker. Used by :meth:`_effective_resources` to
    merge worker-scoped lifecycle resources under the node's own. Not a dataclass
    field (``BaseNodeDef`` is not a ``@dataclass``), so it never rides the wire."""
    _node_kind: ClassVar[NodeKind] = "node"
    """Coarse classification of this node, stamped onto every outbound publish as the
    ``x-calf-emitter-kind`` Kafka header. Subclasses override to one of the values in
    :data:`~calfkit._protocol.NodeKind`. The ``"client"`` kind is reserved for the
    :class:`~calfkit.client.base.BaseClient` and is not a valid subclass override.
    """

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        sig = inspect.signature(cls.run)
        # Unbound method signature includes self, so self + ctx = 2 params.
        # If > 2, the subclass declared an input parameter (any name).
        cls._run_accepts_input = len(sig.parameters) > 2
        cls._validate_routes()

    @classmethod
    def _validate_routes(cls) -> None:
        """Class-definition checks for ``@handler`` routes (run after collection).

        - An overridden ``run()`` is the implicit ``*`` fallback, so an explicit
          ``@handler('*')`` alongside it is an ambiguous catch-all.
        - The signature/``schema`` pairing must agree (§5.1): a ``(self, ctx, payload)``
          handler requires ``schema=``; a ``(self, ctx)`` handler must not have it.
        """
        run_overridden = cls.run is not BaseNodeDef.run
        if "*" in cls._handlers and run_overridden:
            raise RegistryConfigError(
                f"{cls.__qualname__}: @handler('*') conflicts with the node's run() catch-all "
                "(run() is already the implicit '*' fallback); use a more specific route, or override run() instead."
            )
        for route, info in cls._handler_info.items():
            handler_fn = getattr(cls, cls._handlers[route])
            accepts_payload = len(inspect.signature(handler_fn).parameters) > 2
            if accepts_payload and info.schema is None:
                raise RegistryConfigError(
                    f"{cls.__qualname__}: route {route!r} handler {info.name!r} takes a payload parameter "
                    "but has no schema=; add schema= or drop the parameter."
                )
            if not accepts_payload and info.schema is not None:
                raise RegistryConfigError(
                    f"{cls.__qualname__}: route {route!r} handler {info.name!r} has schema= but takes no payload "
                    "parameter; add a payload parameter or drop schema=."
                )

    def __init__(
        self,
        *,
        node_id: str,
        subscribe_topics: list[str],
        publish_topic: str | None = None,
        gates: list[GateFunction] | None = None,
    ) -> None:
        """Initialize a node definition.

        Args:
            node_id: Unique identifier for the node.
            subscribe_topics: One or more topics the node consumes from.
                Must be non-empty — a node with no public inbox cannot be
                invoked by any client or peer. Without the validation in
                :meth:`BaseNodeSchema.__post_init__`, ``Worker.register_handlers``
                would still wire the node up to ``_return_topic`` (issue #141
                fix), so the node would "register" successfully while being
                functionally unreachable from the outside.
            publish_topic: Optional default topic to publish results to.
            gates: Optional list of predicates evaluated in ``handler()`` before
                ``run()``. Stack with AND semantics in registration order;
                short-circuits on the first ``False``, exception, or non-bool.
                Returning anything other than ``True`` rejects the message:
                ``run()`` is skipped and the envelope is returned unchanged.

        Raises:
            ValueError: If ``subscribe_topics`` is empty. Enforced uniformly
                across all node kinds in :meth:`BaseNodeSchema.__post_init__`.
        """
        super().__init__(
            node_id=node_id,
            subscribe_topics=subscribe_topics,
            publish_topic=publish_topic,
        )
        self.gates: list[GateFunction] = list(gates) if gates else []

    def gate(self, fn: GateFunction) -> GateFunction:
        """Register a gate predicate. Usable as a decorator. Repeatable.

        Multiple gates evaluate in registration order with AND semantics
        (short-circuit on the first ``False``, exception, or non-bool return).
        Returns ``fn`` unchanged so it remains callable.
        """
        self.gates.append(fn)
        return fn

    async def _evaluate_gates(self, ctx: SessionRunContext, correlation_id: str) -> bool:
        for i, gate in enumerate(self.gates):
            try:
                result = gate(ctx)
                if inspect.isawaitable(result):
                    result = await result
                if not isinstance(result, bool):
                    raise TypeError(
                        f"gate[{i}] {getattr(gate, '__name__', repr(gate))} for node={self.node_id} returned {type(result).__name__}; expected bool"
                    )
                if not result:
                    logger.debug(
                        "[%s] gate[%d]=%s rejected node=%s",
                        correlation_id[:8],
                        i,
                        getattr(gate, "__name__", "?"),
                        self.node_id,
                    )
                    return False
            except Exception:
                logger.exception(
                    "[%s] gate[%d]=%s raised for node=%s; treating as reject",
                    correlation_id[:8],
                    i,
                    getattr(gate, "__name__", "?"),
                    self.node_id,
                )
                return False
        return True

    # TODO: consider multiple abstract methods per node based on the incoming communication pattern,
    # like a delgation or emit. So the communication-specific handler can properly handle it
    @abstractmethod
    async def run(self, ctx: SessionRunContext, *args: Any, **kwargs: Any) -> NodeResult[State]:
        """Runs the node's logic using provided context.

        Subclasses that need per-invocation input can add an optional parameter
        (any name). The framework inspects the signature at class definition time
        and will pass ``current_frame.input_args`` automatically if the parameter is declared.

        Args:
            ctx: Session context containing mutable state and the user-provided
                dependencies dict (treat ``ctx.deps`` as read-only).

        Raises:
            NotImplementedError: If node subclass does not implement the run() method.

        Returns:
            NodeResult[State]: Execution results persisted via modified state wrapped
            in the NodeResult type. Different NodeResult types define how results
            should be communicated.
        """
        raise NotImplementedError()

    async def prepare_context(
        self,
        envelope: Envelope,
        emitter_node_id: str | None = None,
        emitter_node_kind: str | None = None,
        correlation_id: str | None = None,
    ) -> SessionRunContext:
        ctx = envelope.context.model_copy(deep=True)
        current_frame = envelope.internal_workflow_state.current_frame
        if current_frame.overrides:
            ctx.state.overrides = current_frame.overrides
        ctx._stamp_transport(correlation_id=correlation_id, emitter_node_id=emitter_node_id, emitter_node_kind=emitter_node_kind)
        ctx._resources = self._effective_resources()
        ctx._frame_id = current_frame.frame_id
        return ctx

    def _effective_resources(self) -> dict[str, Any]:
        """The resources a per-message handler sees: worker-scoped merged under
        this node's own (node wins on key collision).

        Returns a fresh shallow copy each call so handler code can't corrupt the
        node's or the worker's shared bag. ``{}`` when neither owns resources.
        """
        worker = self._worker
        if worker is None:
            return dict(self.resources)
        return {**worker.resources, **self.resources}

    def _emitter_headers(self) -> dict[str, str]:
        return {HDR_EMITTER: self.node_id, HDR_EMITTER_KIND: self._node_kind}

    def _headers_for_call(self, call: Call[Any]) -> dict[str, str]:
        """Emitter headers, plus the ``x-calf-route`` header when a ``Call`` addresses
        a sub-route of a downstream routed node (ingress-only, ``Call`` only)."""
        if call.route is None:
            return self._emitter_headers()
        return {**self._emitter_headers(), HDR_ROUTE: call.route}

    async def _publish_action(self, output: NodeResult[State], envelope: Envelope, correlation_id: str, broker: BrokerAnnotation) -> Envelope:
        publish_envelope: Envelope

        if isinstance(output, list) and all(isinstance(item, Call) for item in output):
            # Parallel fan-out: publish each Call with independent workflow_state
            for call in output:
                wf_copy = envelope.internal_workflow_state.model_copy(deep=True)
                wf_copy.invoke_frame(call, self._return_topic, payload=call.body)
                publish_envelope = Envelope(
                    context=SessionRunContext(state=call.state, deps=envelope.context.deps),
                    internal_workflow_state=wf_copy,
                )
                await broker.publish(
                    publish_envelope,
                    topic=wf_copy.current_frame.target_topic,
                    correlation_id=correlation_id,
                    key=correlation_id.encode(),
                    headers=self._headers_for_call(call),
                )
            return envelope

        elif isinstance(output, Call):
            # push to callstack and call the target topic
            envelope.internal_workflow_state.invoke_frame(output, self._return_topic, payload=output.body)
            publish_envelope = Envelope(
                context=SessionRunContext(state=output.state, deps=envelope.context.deps),
                internal_workflow_state=envelope.internal_workflow_state,
            )
            target_topic = envelope.internal_workflow_state.current_frame.target_topic
            logger.debug("[%s] Call target=%s frame_pushed node=%s", correlation_id[:8], target_topic, self.node_id)
            await broker.publish(
                publish_envelope,
                topic=target_topic,
                correlation_id=correlation_id,
                key=correlation_id.encode(),
                headers=self._headers_for_call(output),
            )
        elif isinstance(output, ReturnCall):
            # unwind current frame and return to previous topic
            frame = envelope.internal_workflow_state.unwind_frame()
            publish_envelope = Envelope(
                context=SessionRunContext(state=output.state, deps=envelope.context.deps),
                internal_workflow_state=envelope.internal_workflow_state,
            )
            if frame.callback_topic is None:
                # Fire-and-forget terminal: no requester to return to. Skip the
                # point-to-point callback publish, but still return
                # ``publish_envelope`` below so the worker's @publisher broadcasts
                # the terminal result to ``publish_topic`` (the traceability channel).
                logger.debug("[%s] ReturnCall no-callback fire-and-forget terminal node=%s", correlation_id[:8], self.node_id)
            else:
                logger.debug("[%s] ReturnCall callback=%s node=%s", correlation_id[:8], frame.callback_topic, self.node_id)
                await broker.publish(
                    publish_envelope,
                    topic=frame.callback_topic,
                    correlation_id=correlation_id,
                    key=correlation_id.encode(),
                    headers=self._emitter_headers(),
                )

        elif isinstance(output, TailCall):
            # tailcall optimization: replace current call frame with new tailcall
            frame = envelope.internal_workflow_state.unwind_frame()
            envelope.internal_workflow_state.invoke_frame(output, frame.callback_topic)
            publish_envelope = Envelope(
                context=SessionRunContext(state=output.state, deps=envelope.context.deps),
                internal_workflow_state=envelope.internal_workflow_state,
            )
            target_topic = envelope.internal_workflow_state.current_frame.target_topic
            logger.debug("[%s] TailCall target=%s node=%s", correlation_id[:8], target_topic, self.node_id)
            await broker.publish(
                publish_envelope,
                topic=target_topic,
                correlation_id=correlation_id,
                key=correlation_id.encode(),
                headers=self._emitter_headers(),
            )

        elif isinstance(output, Silent):
            logger.warning(
                "node (%s) ran and was silent with no explicit publish. This is the end of this event-stream, any state modifications will not be carried downstream.",  # noqa: E501
                self.name,
            )
            publish_envelope = envelope
        else:
            logger.error("Return type is unknown or invalid so the message was not published anywhere.")
            publish_envelope = envelope

        return publish_envelope

    async def _call_run(self, ctx: SessionRunContext, input_args: Sequence[Any] | None) -> NodeResult[State]:
        """Invoke ``run``, passing the per-message input args when its signature accepts them."""
        if self._run_accepts_input and input_args is not None:
            return await self.run(ctx, *input_args)
        return await self.run(ctx)

    async def _dispatch_routed(
        self,
        ctx: SessionRunContext,
        route: str,
        payload: Any,
        input_args: Sequence[Any] | None,
        *,
        awaiting_reply: bool,
        correlation_id: str,
    ) -> NodeResult[State] | None:
        """Dispatch ``route`` to matched handlers as a Chain of Responsibility.

        Runs matched handlers most-specific → most-general (§6.3). A handler that
        returns :class:`~calfkit.models.Next` declines and the chain advances; the
        first non-``Next`` result is terminal and short-circuits. A handler with a
        ``schema`` whose body fails validation is skipped (logged, callback-aware
        level). If no handler handles the route, the node's overridden ``run()`` is
        the implicit ``*`` fallback; absent that, returns ``None`` (no match).
        """
        cls = type(self)
        for r in match_chain(route, cls._handlers):
            info = cls._handler_info[r]
            method = self.get_handler(r)
            if info.schema is not None:
                try:
                    validated = info.schema.model_validate(payload)
                except ValidationError:
                    level = logging.WARNING if awaiting_reply else logging.DEBUG
                    logger.log(
                        level,
                        "[%s] route=%s handler=%s body failed %s validation; skipping to next handler",
                        correlation_id[:8],
                        route,
                        info.name,
                        info.schema.__name__,
                    )
                    continue
                result: NodeResult[State] | Next = await method(ctx, validated)
            else:
                result = await method(ctx)
            if isinstance(result, Next):
                continue
            return result
        if cls.run is not BaseNodeDef.run:
            return await self._call_run(ctx, input_args)
        return None

    async def handler(
        self,
        envelope: Envelope,
        correlation_id: Annotated[str, Context()],
        headers: Annotated[dict[str, Any], Context("message.headers")],
        broker: BrokerAnnotation,
    ) -> Response:
        raw_emitter = headers.get(HDR_EMITTER)
        emitter = decode_header_str(raw_emitter)
        if emitter is None:
            logger.warning(
                "[%s] inbound to node=%s has no usable %s header (got %s); emitter unknown",
                correlation_id[:8],
                self.node_id,
                HDR_EMITTER,
                "None" if raw_emitter is None else type(raw_emitter).__name__,
            )
        emitter_kind = decode_header_str(headers.get(HDR_EMITTER_KIND))
        logger.debug("[%s] handler entered node=%s emitter=%s kind=%s", correlation_id[:8], self.node_id, emitter, emitter_kind)
        ctx = await self.prepare_context(envelope, emitter_node_id=emitter, emitter_node_kind=emitter_kind, correlation_id=correlation_id)

        if not await self._evaluate_gates(ctx, correlation_id):
            body: Envelope = envelope
        else:
            frame = envelope.internal_workflow_state.current_frame
            route = decode_header_str(headers.get(HDR_ROUTE))
            if route is not None and type(self)._handlers:
                output = await self._dispatch_routed(
                    ctx,
                    route,
                    frame.payload,
                    frame.input_args,
                    awaiting_reply=frame.callback_topic is not None,
                    correlation_id=correlation_id,
                )
                if output is None:
                    # No matching handler (and no run() fallback): a stuck workflow
                    # if a caller is awaiting a return, else a fire-and-forget no-op.
                    level = logging.WARNING if frame.callback_topic is not None else logging.DEBUG
                    logger.log(
                        level,
                        "[%s] no handler matched route=%s on node=%s; registered=%s",
                        correlation_id[:8],
                        route,
                        self.node_id,
                        tuple(type(self)._handlers),
                    )
                    return Response(envelope, headers=self._emitter_headers())
            else:
                output = await self._call_run(ctx, frame.input_args)
            logger.debug("[%s] node=%s produced action=%s", correlation_id[:8], self.node_id, type(output).__name__)
            body = await self._publish_action(output, envelope, correlation_id, broker)

        return Response(body, headers=self._emitter_headers())

    @property
    def id(self) -> str:
        return self.node_id

    @property
    def name(self) -> str:
        return self.node_id

    @property
    def _return_topic(self) -> str:
        """Framework-private return inbox for this node instance.

        Used as the ``callback_topic`` written into the call frame when this
        node issues a tool ``Call`` (so the tool's ``ReturnCall`` knows where
        to route back), and as the ``target_topic`` for the framework's
        built-in all-invalid ``TailCall`` self-retry in
        :meth:`BaseAgentNodeDef.run`. Must be uniquely owned by this
        ``node_id`` — sharing it with another node's consumer group would
        re-introduce the co-tenant tool-return leak (issue #141).
        :meth:`Worker.register_handlers` automatically subscribes the node to
        this topic under the worker's configured ``group_id`` (defaults to
        the node's own ``node_id``).

        The value is recomputed from ``node_id`` on every access. Do not
        mutate ``node_id`` after the node has been registered with a worker:
        the worker's subscription is bound to the old topic, so tool
        ``ReturnCall`` responses (which target the call frame's
        ``callback_topic``) and built-in ``TailCall`` self-retries (which
        target ``_return_topic`` directly) would silently route into a topic
        with no consumer. Ordinary tool ``Call`` publishes are unaffected —
        they target the tool's input topic, not ``_return_topic``.
        """
        return f"{self.node_id}.private.return"
