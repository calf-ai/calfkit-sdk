import inspect
import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Annotated, Any, ClassVar

from faststream import Context, Response
from faststream.kafka.annotations import (
    KafkaBroker as BrokerAnnotation,
)
from pydantic import ValidationError

from calfkit._protocol import HDR_EMITTER, HDR_EMITTER_KIND, HDR_ROUTE, NodeKind, decode_header_str
from calfkit._registry import RegistryMixin, handler
from calfkit._routing import is_concrete_route_key, match_chain
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


def _accepts_extra_param(fn: Callable[..., Any]) -> bool:
    """True if ``fn`` declares a parameter beyond ``(self, ctx)`` — i.e. it takes a
    run input-arg / route payload. (Unbound signature: ``self`` + ``ctx`` = 2.)"""
    return len(inspect.signature(fn).parameters) > 2


def _stuck_level(awaiting_reply: bool) -> int:
    """``WARNING`` when a caller is awaiting a reply (an unmatched/malformed/declined
    route stalls that workflow), else ``DEBUG`` (a fire-and-forget no-op)."""
    return logging.WARNING if awaiting_reply else logging.DEBUG


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
        cls._validate_routes()

    @classmethod
    def _validate_routes(cls) -> None:
        """Class-definition checks for ``@handler`` routes (run after collection).

        The signature/``schema`` pairing must agree: a ``(self, ctx, payload)``
        handler requires ``schema=``; a ``(self, ctx)`` handler must not have it.
        ``run()`` is the inherited ``@handler('*')`` catch-all and is validated by
        the same rule — so an override that takes a payload (e.g. a tool node) must
        re-decorate ``@handler('*', schema=...)``. A user-defined second ``@handler('*')``
        collides with ``run`` and is rejected by the registry's uniqueness check.
        """
        for route, info in cls._handler_info.items():
            handler_fn = getattr(cls, cls._handlers[route])
            accepts_payload = _accepts_extra_param(handler_fn)
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

    @handler("*")
    async def run(self, ctx: SessionRunContext) -> NodeResult[State] | Next:
        """The node's default catch-all handler — registered at route ``'*'`` (the
        least-specific pattern, dispatched last in the Chain of Responsibility).

        The base implementation **declines** (returns :class:`~calfkit.models.Next`)
        so a node that defines only specific ``@handler`` routes skips an unmatched
        message instead of erroring. Override to give the node default behavior; an
        override resolves as the ``'*'`` handler with no need to re-apply ``@handler``.
        A node that needs a per-invocation typed body re-decorates
        ``@handler('*', schema=...)`` and takes a ``payload`` parameter (e.g. a tool node).

        Args:
            ctx: Session context containing mutable state and the user-provided
                dependencies dict (treat ``ctx.deps`` as read-only).

        Returns:
            A :class:`~calfkit.models.NodeResult` (a control-flow action), or
            :class:`~calfkit.models.Next` to decline and end the chain (the default).
        """
        return Next()

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

    async def _dispatch_routed(
        self,
        ctx: SessionRunContext,
        route: str | None,
        payload: Any,
        *,
        awaiting_reply: bool,
        correlation_id: str,
    ) -> NodeResult[State] | None:
        """Dispatch ``route`` to matched handlers as a Chain of Responsibility.

        Runs matched handlers most-specific → most-general. A handler that returns
        :class:`~calfkit.models.Next` (or ``None`` — e.g. a missing return) declines
        and the chain advances; the first other result is terminal and short-circuits.
        A handler with a ``schema`` whose body fails validation is skipped (logged,
        callback-aware level). ``run()`` is the inherited ``'*'`` handler dispatched
        last; the base ``run()`` declines (returns ``Next``), so a node with no real
        match returns ``None``. ``route`` is ``None`` for a header-less message — only
        the ``'*'`` handler matches it.
        """
        cls = type(self)
        if route is not None and not is_concrete_route_key(route):
            # Present-but-malformed inbound key (empty segment / trailing dot / wildcard):
            # never partial-matches a specific handler — only the "*"/run fallback catches it.
            # (A None route is the normal header-less case, not malformed.)
            level = _stuck_level(awaiting_reply)
            logger.log(
                level,
                "[%s] malformed inbound route=%r on node=%s; only a catch-all/run fallback can handle it",
                correlation_id[:8],
                route,
                self.node_id,
            )
        for r in match_chain(route, cls._handlers):
            info = cls._handler_info[r]
            method = self.get_handler(r)
            if info.schema is not None:
                try:
                    validated = info.schema.model_validate(payload)
                except ValidationError:
                    level = _stuck_level(awaiting_reply)
                    logger.log(
                        level,
                        "[%s] route=%s handler=%s body failed %s validation; skipping to next handler",
                        correlation_id[:8],
                        route,
                        info.name,
                        info.schema.__name__,
                    )
                    continue
                result: NodeResult[State] | Next | None = await method(ctx, validated)
            else:
                result = await method(ctx)
            if result is None or isinstance(result, Next):
                # Decline (Next, or a handler that simply returned nothing) → advance.
                continue
            return result
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
            output = await self._dispatch_routed(
                ctx,
                route,
                frame.payload,
                awaiting_reply=frame.callback_topic is not None,
                correlation_id=correlation_id,
            )
            if output is None:
                # Every matched handler declined (the base run() '*' fallback always
                # declines). Stuck workflow if a caller awaits a return, else a no-op.
                level = _stuck_level(frame.callback_topic is not None)
                logger.log(
                    level,
                    "[%s] no handler matched route=%s on node=%s; registered=%s",
                    correlation_id[:8],
                    route,
                    self.node_id,
                    tuple(type(self)._handlers),
                )
                if frame.payload is not None:
                    # A producer attached a body that no schema handler consumed — surface
                    # it (callback-aware level) so a silently-dropped payload is observable.
                    logger.log(
                        level,
                        "[%s] inbound body was not consumed by any handler on node=%s (no '*' schema handler matched)",
                        correlation_id[:8],
                        self.node_id,
                    )
                return Response(envelope, headers=self._emitter_headers())
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
