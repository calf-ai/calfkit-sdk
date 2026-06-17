import inspect
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum, auto
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, Final, Literal, TypeVar, cast

import uuid_utils
from aiokafka.errors import KafkaError  # type: ignore[import-untyped]
from faststream import Context, Response
from faststream.kafka.annotations import (
    KafkaBroker as BrokerAnnotation,
)
from pydantic import ValidationError

from calfkit._protocol import HDR_EMITTER, HDR_EMITTER_KIND, HDR_KIND, HDR_ROUTE, MessageKind, NodeKind, decode_header_str
from calfkit._registry import RegistryMixin, handler
from calfkit._routing import is_concrete_route_key, match_chain
from calfkit.exceptions import RegistryConfigError, SeamContractError
from calfkit.models import (
    Call,
    Next,
    NodeResult,
    ReturnCall,
    Silent,
    State,
    TailCall,
)
from calfkit.models._coerce import _coerce_to_parts
from calfkit.models.envelope import Envelope
from calfkit.models.fanout import EnvelopeSnapshot, FanoutOpen, FanoutOutcome, SlotRef
from calfkit.models.node_result import _UNSET, _extract_output, extract_lenient
from calfkit.models.node_schema import BaseNodeSchema
from calfkit.models.reply import ReturnMessage
from calfkit.models.seam_context import SeamContext
from calfkit.models.session_context import SessionRunContext
from calfkit.nodes._fanout_store import (
    FANOUT_STORE_KEY,
    CloseAbandon,
    CloseAbort,
    CloseResume,
    CloseSpurious,
    FanoutBatchStore,
    FanoutStoreUnavailableError,
    FoldAbort,
    FoldComplete,
    FoldParked,
    FoldStray,
    abort_batch,
    close_batch,
    fold_sibling,
)
from calfkit.nodes._seams import AFTER_NODE, BEFORE_NODE, ON_CALLEE_ERROR, ON_NODE_ERROR, SEAM_NAMES, run_chain
from calfkit.worker.lifecycle import LifecycleHookMixin

if TYPE_CHECKING:
    from calfkit.worker.worker import Worker

logger = logging.getLogger(__name__)

_SeamFn = TypeVar("_SeamFn", bound=Callable[..., Any])
"""Preserves a seam handler's concrete type through the registration decorators (the
``gate()`` return-fn-unchanged precedent), so a decorated handler stays directly callable."""

# A seam constructor parameter: one handler, a list of handlers, or unset. Typed loosely
# (``Callable``) for now; the §6.3 per-seam typed aliases (``BeforeNodeSeam[StateT]`` etc.,
# which require ``BaseNodeDef`` to become ``Generic[StateT, OutputT]``) are a typing
# refinement deferred to step 3, where the pipeline consumes the typed signatures.
_SeamArg = Callable[..., Any] | list[Callable[..., Any]] | None

# Positional arity each seam invokes its handlers with: ``before_node(ctx)`` takes one;
# the rest take two (``ctx`` + ``output``/``fault``). Validated at registration (spec §6.8).
_SEAM_ARITY = {BEFORE_NODE: 1, AFTER_NODE: 2, ON_NODE_ERROR: 2, ON_CALLEE_ERROR: 2}

# The publishable NodeResult action types (sans the ``list[Call]`` fan-out, handled in
# :func:`_is_action`). ``Next`` is route-CoR vocabulary, not a publishable action.
_ACTION_TYPES: tuple[type, ...] = (Call, ReturnCall, TailCall)


def _is_action(value: Any) -> bool:
    """True if ``value`` is a NodeResult action (spec §6.3 tier-2): a boundary seam
    returning one executes it as-is, rather than coercing it to output. A ``list[Call]``
    (fan-out) counts; a ``list[ContentPart]`` (a parts *value*) does not."""
    if isinstance(value, _ACTION_TYPES):
        return True
    return isinstance(value, list) and bool(value) and all(isinstance(item, Call) for item in value)


def _accepts_extra_param(fn: Callable[..., Any]) -> bool:
    """True if ``fn`` declares a parameter beyond ``(self, ctx)`` — i.e. it takes a
    route payload (which then requires a ``schema=``). (Unbound signature: ``self`` + ``ctx`` = 2.)"""
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
# Staged-pipeline outcome vocabulary (fault-rail spec §6.8) — return-only subset
# ---------------------------------------------------------------------------


# These are dataclasses (not ``_PipelineSentinel`` enum members) so the fault rail can add the
# payload-carrying ``_BatchFaulted(report)`` arm additively, without an enum→union migration.
@dataclass(frozen=True)
class _BatchClosed:
    """The aggregation stage resolved — proceed to the body. Either a completed fan-out
    closure (the durable snapshot's state/stack/deps restored onto ``ctx``+``envelope``, its
    outcomes materialized) or a stateless single-call continuation (no batch registered)."""


@dataclass(frozen=True)
class _BatchOpen:
    """The fan-out batch is still open — park via the no-reply mirror (``_CONSUMED``). Either
    an incomplete sibling fold, or a no-op close (a spurious/abandoned/aborted re-entry)."""


# TODO(fault rail PR-6): add ``_BatchFaulted(report: ErrorReport)`` to this union — an unhandled
# callee fault closes the batch through the fault arm. Needs the fault wire model (PR-5).


class _PipelineSentinel(Enum):
    """Non-action outcomes of the staged pipeline (§6.8) — narrowable singletons, distinct from
    the publishable :data:`NodeResult` actions the handler routes to ``_publish_action``."""

    CONSUMED = auto()
    """A parked fan-out fold or a self-published re-entry: no publishable action this hop (the
    output is still owed by pending siblings). The handler emits the no-reply broadcast mirror."""

    DECLINED = auto()
    """Every handler in the body's route Chain-of-Responsibility declined (all-declined terminal)."""


_CONSUMED: Final = _PipelineSentinel.CONSUMED
_DECLINED: Final = _PipelineSentinel.DECLINED


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
    is_caller_capable: ClassVar[bool] = True
    """Whether this node type handles ``Call``s and their ``ReturnCall`` continuations over
    its own workflow state (agent, tool, MCP toolbox, custom ``BaseNodeDef`` subclasses);
    ``False`` only for observers (``ConsumerNode``), which just consume. Subclasses may
    override it. Load-bearing for registration: such nodes are pinned to ``max_workers=1``
    because handling a continuation is an await-spanning read-modify-write of workflow state
    — the agent's tool-call batch aggregation today, the in-node fan-out fold next — that a
    no-affinity ``max_workers>1`` coroutine pool would race."""

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
        before_node: _SeamArg = None,
        after_node: _SeamArg = None,
        on_node_error: _SeamArg = None,
        on_callee_error: _SeamArg = None,
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
        # The four policy-seam chains (spec §6.1), populated by the constructor params
        # (first) and the instance decorators (after); consulted by the staged pipeline (step 3).
        self._chains: dict[str, list[Callable[..., Any]]] = {name: [] for name in SEAM_NAMES}
        self._register_seam_params(BEFORE_NODE, before_node)
        self._register_seam_params(AFTER_NODE, after_node)
        self._register_seam_params(ON_NODE_ERROR, on_node_error)
        self._register_seam_params(ON_CALLEE_ERROR, on_callee_error)

    def _register_seam_params(self, seam: str, value: _SeamArg) -> None:
        """Normalize a seam constructor param (one handler / a list / unset) and register
        each in order — so a constructor-supplied chain precedes any later decorator
        entries (spec §6.1 chain-order)."""
        if value is None:
            return
        handlers = value if isinstance(value, list) else [value]
        for fn in handlers:
            self._register_seam(seam, fn)

    def _register_seam(self, seam: str, fn: Callable[..., Any]) -> None:
        """Validate and append one handler to a seam chain (spec §6.8 — registration-time,
        never mid-message). Shared by the constructor params and the instance decorators.

        Validates the handler is callable and accepts the seam's positional arity
        (``before_node(ctx)``; the rest ``(ctx, output/fault)``) — so a wrong-shaped
        handler fails loudly at startup, not silently at first fire."""
        if not callable(fn):
            raise RegistryConfigError(f"node={self.node_id}: {seam} handler must be callable, got {type(fn).__name__}")
        arity = _SEAM_ARITY[seam]
        try:
            inspect.signature(fn).bind(*([None] * arity))
        except TypeError as exc:
            extra = ", fault/output" if arity == 2 else ""
            raise RegistryConfigError(
                f"node={self.node_id}: {seam} handler {getattr(fn, '__name__', fn)!r} must accept {arity} positional arg(s) (ctx{extra}); {exc}"
            ) from exc
        self._chains[seam].append(fn)

    def before_node(self, fn: _SeamFn) -> _SeamFn:
        """Register a ``before_node`` handler. Usable as an instance decorator; repeatable.

        Appends to the same chain the ``before_node=`` constructor param feeds (constructor
        entries first, then decorated), and returns ``fn`` unchanged so it stays directly
        unit-testable (the ``gate()`` precedent)."""
        self._register_seam(BEFORE_NODE, fn)
        return fn

    def after_node(self, fn: _SeamFn) -> _SeamFn:
        """Register an ``after_node`` handler (instance decorator; repeatable; see :meth:`before_node`)."""
        self._register_seam(AFTER_NODE, fn)
        return fn

    def on_node_error(self, fn: _SeamFn) -> _SeamFn:
        """Register an ``on_node_error`` handler (instance decorator; repeatable; see :meth:`before_node`)."""
        self._register_seam(ON_NODE_ERROR, fn)
        return fn

    def on_callee_error(self, fn: _SeamFn) -> _SeamFn:
        """Register an ``on_callee_error`` handler (instance decorator; repeatable; see :meth:`before_node`)."""
        self._register_seam(ON_CALLEE_ERROR, fn)
        return fn

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
    async def run(self, ctx: SessionRunContext) -> NodeResult[State] | Next | None:
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
        frame = envelope.internal_workflow_state.current_frame_or_none
        if frame is not None and frame.overrides:
            ctx.state.overrides = frame.overrides
        ctx._stamp_transport(correlation_id=correlation_id, emitter_node_id=emitter_node_id, emitter_node_kind=emitter_node_kind)
        ctx._resources = self._effective_resources()
        ctx._frame_id = frame.frame_id if frame is not None else None
        # Stamp the per-delivery reply AFTER the copy and UNCONDITIONALLY: model_copy
        # preserves private attrs, and on a fresh deserialize the inbound context's
        # _reply is None anyway, so source it from the envelope's reply field — setting
        # None on a call-kind delivery clears any stale value (mirrors _frame_id above).
        ctx._reply = envelope.reply
        return ctx

    def _build_seam_context(self, run_ctx: SessionRunContext, envelope: Envelope, headers: dict[str, Any], kind: MessageKind) -> SeamContext[State]:
        """Build the capability-scoped :class:`SeamContext` the four seams receive (spec §6.3).

        Sourced from the already-prepared ``run_ctx`` + the inbound frame: ``state`` is the
        SAME object as ``run_ctx.state`` (a ``before_node`` mutation transforms the input the
        body then runs on), ``deps``/``resources``/identity are carried read-only, and the
        stage-scoped fields (``failing_call``/``exception``) start empty (set by the pipeline
        during their stages). ``route`` is exposed on call-kind ingress only (§6.3)."""
        frame = envelope.internal_workflow_state.current_frame_or_none
        return SeamContext(
            state=run_ctx.state,  # SHARED with the body's run_ctx — the input-transform channel
            deps=run_ctx.deps,
            resources=run_ctx.resources,
            payload=frame.payload if frame is not None else None,
            node_id=self.node_id,
            correlation_id=run_ctx.correlation_id,
            emitter_node_id=run_ctx.emitter_node_id,
            route=decode_header_str(headers.get(HDR_ROUTE)) if kind == "call" else None,
            delivery_kind=kind,
            awaiting_reply=frame.callback_topic is not None if frame is not None else False,
        )

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

    def _headers(self, kind: MessageKind, *, route: str | None = None) -> dict[str, str]:
        """Outbound headers for one publish: emitter id/kind + the ``x-calf-kind``
        delivery classification (spec §4.1), plus ``x-calf-route`` when a ``Call``
        addresses a sub-route of a downstream routed node (ingress-only, ``Call`` only)."""
        h = {HDR_EMITTER: self.node_id, HDR_EMITTER_KIND: self._node_kind, HDR_KIND: kind}
        if route is not None:
            h[HDR_ROUTE] = route
        return h

    async def _publish_action(
        self, output: NodeResult[State], envelope: Envelope, correlation_id: str, broker: BrokerAnnotation
    ) -> tuple[Envelope, MessageKind]:
        """Publish the node's action point-to-point and return the envelope to mirror
        on ``publish_topic`` plus its ``x-calf-kind`` (spec §4). The kind is known here
        per branch but not at the ``handler`` ``Response``, so it is returned upward."""
        publish_envelope: Envelope
        kind: MessageKind = "call"

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
                    headers=self._headers("call", route=call.route),
                )
            # No-reply hop: the mirror is the inbound envelope — clear any inbound reply
            # so a return this node was processing is not re-broadcast under its own
            # emitter to its observers (the I3 leak class).
            envelope.reply = None
            return envelope, "call"

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
                headers=self._headers("call", route=output.route),
            )
        elif isinstance(output, ReturnCall):
            # unwind current frame and return to previous topic, carrying the reply slot
            frame = envelope.internal_workflow_state.unwind_frame()
            reply = ReturnMessage(in_reply_to=frame.frame_id, tag=frame.tag, parts=_coerce_to_parts(output.value))
            publish_envelope = Envelope(
                context=SessionRunContext(state=output.state, deps=envelope.context.deps),
                internal_workflow_state=envelope.internal_workflow_state,
                reply=reply,
            )
            kind = "return"
            if frame.callback_topic is None:
                # Fire-and-forget terminal: no requester to return to. Skip the
                # point-to-point callback publish, but still return
                # ``publish_envelope`` below so the worker's @publisher broadcasts
                # the terminal result (with its reply) to ``publish_topic``.
                logger.debug("[%s] ReturnCall no-callback fire-and-forget terminal node=%s", correlation_id[:8], self.node_id)
            else:
                logger.debug("[%s] ReturnCall callback=%s node=%s", correlation_id[:8], frame.callback_topic, self.node_id)
                try:
                    await broker.publish(
                        publish_envelope,
                        topic=frame.callback_topic,
                        correlation_id=correlation_id,
                        key=correlation_id.encode(),
                        headers=self._headers("return"),
                    )
                except KafkaError:
                    # Point-to-point delivery failed (e.g. a send(reply_to=...)
                    # topic missing with auto-create off, or unauthorized).
                    # Losing it must not also take down the publish_topic
                    # broadcast below — the documented traceability fallback —
                    # which only fires if this handler returns publish_envelope.
                    # No retry/DLQ here: redelivery policy belongs to the
                    # fault rail (#193 successor).
                    logger.exception(
                        "[%s] terminal result could not be delivered to callback_topic=%s node=%s; "
                        "the terminal envelope is still returned for the publish_topic broadcast (no retry)",
                        correlation_id,
                        frame.callback_topic,
                        self.node_id,
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
                headers=self._headers("call"),
            )

        elif isinstance(output, Silent):
            logger.warning(
                "node (%s) ran and was silent with no explicit publish. This is the end of this event-stream, any state modifications will not be carried downstream.",  # noqa: E501
                self.name,
            )
            envelope.reply = None  # no-reply hop: don't re-broadcast an inbound reply (I3)
            publish_envelope = envelope
        else:
            logger.error("Return type is unknown or invalid so the message was not published anywhere.")
            envelope.reply = None  # no-reply hop: don't re-broadcast an inbound reply (I3)
            publish_envelope = envelope

        return publish_envelope, kind

    async def _publish_reentry(self, envelope: Envelope, correlation_id: str, broker: BrokerAnnotation) -> None:
        """Self-publish the fan-out closure re-entry to this node's own return inbox.

        A bespoke, frame-preserving self-return (NOT a ``ReturnCall``, which pops): the
        node addresses ITSELF so its fan-out frame stays current, and the close (§4.3)
        rebuilds context from the durable basestate. Deltas from a normal ``ReturnCall``:
        no pop; ``in_reply_to`` = the fan-out frame's own id; ``parts=[]``; target = own
        ``_return_topic``; ``key=correlation_id`` (same partition, single-writer); context
        ``state``/``deps`` cleared (rebuilt at close); **not** broadcast-mirrored — a direct
        point-to-point publish, never returned for the ``@publisher`` mirror.
        """
        frame = envelope.internal_workflow_state.current_frame  # the fan-out frame — NOT popped
        reentry = Envelope(
            context=SessionRunContext(state=State(), deps={}),  # cleared — rebuilt from basestate at close
            internal_workflow_state=envelope.internal_workflow_state,
            reply=ReturnMessage(in_reply_to=frame.frame_id, tag=frame.tag, parts=[]),
        )
        await broker.publish(
            reentry,
            topic=self._return_topic,
            correlation_id=correlation_id,
            key=correlation_id.encode(),
            headers=self._headers("return"),
        )

    @property
    def _is_fanout_capable(self) -> bool:
        """Whether this node folds durable fan-out batches in-node. ``False`` for every
        node type except a non-sequential agent (which overrides this) — it gates the
        staged handler's fan-out recognition and the OPEN dispatch path."""
        return False

    def _resolve_fanout_store(self, ctx: SessionRunContext) -> FanoutBatchStore:
        """The node's durable fan-out store, from the resource bag.

        Production: a node-owned ``@resource`` (ktables). Offline tests inject a fake
        (``agent.resources[FANOUT_STORE_KEY] = fake``). Fan-out cannot proceed without it,
        so a missing store is a misconfiguration (raise), never a silent skip."""
        store = ctx.resources.get(FANOUT_STORE_KEY)
        if store is None:
            raise RuntimeError(
                f"node={self.node_id} fanned out but no FanoutBatchStore is registered under "
                f"{FANOUT_STORE_KEY!r}; a fan-out-capable agent needs its durable store resource."
            )
        return cast(FanoutBatchStore, store)

    # ── seam conversions (generic, sealed; spec §6.9) ────────────────────────────
    # Additive in step 1 — wired into the staged pipeline (stage 3/5/6) in step 3.

    def _coerce_output(self, ctx: SeamContext[State], value: Any) -> NodeResult[State]:
        """Convert a seam's plain return value into the node's output action (spec §6.9).

        The value becomes a ``ReturnCall`` carrying ``ctx.state``; the value→parts
        coercion happens once later, at the publish chokepoint (§4.5). Used for every
        value-substituting seam (``before_node`` short-circuit, ``on_node_error``
        recovery, ``after_node`` replacement) so seam handling needs no per-seam cases.

        The §6.2 teaching guards reject return types that can never be a node output,
        loudly (``SeamContractError`` → ``on_node_error`` → fault), never silently.
        """
        # bool BEFORE any int-accepting path: bool subclasses int, and a gate's
        # `return True` ported to a seam must not become the node's output (§6.2).
        if isinstance(value, bool):
            raise SeamContractError(
                "a seam returned a bool, which is never a node output; return None to proceed, substitute a real output, or raise to reject."
            )
        # The session record / the context itself must not be serialized out as the
        # node's answer — the reflexive functional idiom; mutate ctx.state instead (§6.2).
        if isinstance(value, (State, SeamContext)):
            raise SeamContractError(
                "a seam returned the session State or the SeamContext itself, which is not a node "
                "output; transform the input by mutating ctx.state in place and returning None."
            )
        # bytes has no parts arm (§4.5) — reject rather than guess an encoding.
        if isinstance(value, bytes):
            raise SeamContractError("a seam returned bytes, which has no parts representation; return str, structured data, or a model.")
        return ReturnCall[State](state=ctx.state, value=value)

    @property
    def _seam_output_type(self) -> Any:
        """The output type ``after_node``'s view is projected to (spec §6.3).

        The base is untyped (``_UNSET`` → lenient auto-detect, never raises). A typed
        node — the agent — overrides this to its declared output type so the projection
        becomes strict: a type-breaking output then faults at the seam, not downstream
        (the agent's override lands with its body migration, step 4)."""
        return _UNSET

    def _output_view(self, ctx: SeamContext[State], output: NodeResult[State]) -> Any:
        """Project a terminal output into the typed view ``after_node`` inspects (spec §6.9).

        ``None`` for a non-terminal action (``Call``/``TailCall``/fan-out list) — ``after_node``
        guards a *produced* output, and those produce nothing yet. The value is coerced to
        parts first (so a raw body value and an agent's parts project uniformly), then
        projected: lenient on an untyped node (``None`` on an unprojectable output, never a
        fault), strict against :attr:`_seam_output_type` on a typed node."""
        if not isinstance(output, ReturnCall):
            return None
        parts = _coerce_to_parts(output.value)
        if not parts:
            return None  # empty output → nothing for after_node to guard
        output_type = self._seam_output_type
        if output_type is _UNSET:
            return extract_lenient(parts)
        return _extract_output(parts, output_type)

    def _interpret(self, ctx: SeamContext[State], value: Any) -> NodeResult[State]:
        """Interpret a boundary seam's return (spec §6.3/§6.8 two-tier rule): a NodeResult
        action executes as-is; any other value is coerced to the node's output. Used for
        ``before_node`` short-circuits and ``on_node_error`` recovery values."""
        return value if _is_action(value) else self._coerce_output(ctx, value)

    async def _apply_after(self, ctx: SeamContext[State], output: NodeResult[State]) -> NodeResult[State]:
        """Stage 6 (spec §6.8): run the ``after_node`` chain over the output's typed view.

        Short-circuits when no ``after_node`` is registered — there is nothing to guard, and
        the view (which can raise on a type-breaking output) is not computed. A handler
        returning a value REPLACES the output (coerced); ``None`` keeps it; an action
        violates ``after_node``'s values-only contract (§6.1) → ``SeamContractError``."""
        if not self._chains[AFTER_NODE]:
            return output
        view = self._output_view(ctx, output)
        if view is None:
            return output  # non-terminal / empty output → nothing to guard
        post = await run_chain(self._chains[AFTER_NODE], ctx, view)
        if post is None:
            return output
        if _is_action(post):
            raise SeamContractError("after_node returns a value, not an action; use before_node/on_node_error for actions.")
        return self._coerce_output(ctx, post)

    def _classify(self, headers: dict[str, Any]) -> MessageKind:
        """Classify the inbound delivery kind from the ``x-calf-kind`` header (§6.8 stage-0).

        Missing ⇒ ``"call"`` (a producer that didn't stamp it / a fresh ingress). Return-only:
        only ``call`` and ``return`` exist; an unrecognized value falls back to ``"call"``
        (the pre-rail body path). The kind selects stage routing in :meth:`_execute`
        (``return`` ⇒ the aggregation stage; ``call`` ⇒ straight to the body).

        TODO(fault rail PR-6): widen to ``"fault"`` (``MessageKind`` gains the arm) and add the
        unknown-value→ERROR-and-ignore + the kind↔reply-slot disagreement→stray checks
        (fault-rail spec §6.8 stage-0). Both need the fault wire model, not yet in tree.
        """
        return "return" if decode_header_str(headers.get(HDR_KIND)) == "return" else "call"

    def _classify_fanout(self, envelope: Envelope) -> Literal["sibling", "reentry"] | None:
        """Recognize a fan-out continuation on a fan-out-capable node.

        ``"sibling"`` (a marked sibling reply → fold), ``"reentry"`` (the self-published
        closure → close), or ``None`` (normal ingress / single-call continuation). The
        marker rides the node's OWN frame (``fanout_id`` set), so it is the top frame when
        a fan-out continuation re-enters; the reply slot's ``in_reply_to`` then tells a
        sibling callee (≠ the frame id) from the re-entry (== it)."""
        if not self._is_fanout_capable:
            return None
        frame = envelope.internal_workflow_state.current_frame_or_none
        if frame is None or frame.fanout_id is None:
            return None
        reply = envelope.reply
        if reply is None:
            return None  # a marked frame with no reply slot is not a fold/close continuation
        return "reentry" if reply.in_reply_to == frame.frame_id else "sibling"

    async def _handle_fanout_open(
        self, ctx: SessionRunContext, calls: list[Call[State]], envelope: Envelope, correlation_id: str, broker: BrokerAnnotation
    ) -> Response:
        """OPEN a durable fan-out batch, then publish the marked siblings (§4.1).

        Pre-mint a callee slot id per ``Call``; register the batch (basestate snapshot THEN
        state, awaiting acks before any sibling publishes — so *registration ⟹ basestate*);
        then publish each sibling on its pre-minted id, with the node's OWN frame marked so
        the marker survives the callee's return-pop. Returns the no-reply mirror — the
        output is owed by the pending siblings."""
        store = self._resolve_fanout_store(ctx)
        fanout_id = envelope.internal_workflow_state.current_frame.frame_id
        slot_ids = [uuid_utils.uuid7().hex for _ in calls]
        reg = FanoutOpen(
            fanout_id=fanout_id,
            node_id=self.node_id,
            expected=[SlotRef(frame_id=fid, tag=call.tag) for fid, call in zip(slot_ids, calls)],
        )
        snapshot = EnvelopeSnapshot(state=ctx.state, stack=envelope.internal_workflow_state, deps=dict(ctx.deps))
        try:
            await store.open(fanout_id, reg, snapshot)
            for call, slot_id in zip(calls, slot_ids):
                wf_copy = envelope.internal_workflow_state.model_copy(deep=True)
                wf_copy.mark_fanout()  # mark the node's OWN (current top) frame, before the callee push
                wf_copy.invoke_frame(call, self._return_topic, payload=call.body, frame_id=slot_id, tag=call.tag)
                sibling = Envelope(
                    context=SessionRunContext(state=call.state, deps=envelope.context.deps),
                    internal_workflow_state=wf_copy,
                )
                await broker.publish(
                    sibling,
                    topic=wf_copy.current_frame.target_topic,
                    correlation_id=correlation_id,
                    key=correlation_id.encode(),
                    headers=self._headers("call", route=call.route),
                )
        except (KafkaError, FanoutStoreUnavailableError) as exc:
            # §4.4 dispatch-abort: the batch may be durably registered (basestate+state written) but
            # cannot complete — a sibling publish failed, or the store failed at OPEN. Tombstone both
            # records so the orphan open batch is not leaked (reclaimed otherwise only by #220); any
            # sibling already published then post_closure-stray-floors against the tombstone at its
            # fold. ERROR-log + strand: the ACK_FIRST offset is already committed, so the inbound is
            # not redelivered.
            logger.error(
                "[%s] fan-out OPEN failed batch=%s node=%s; aborting + caller strands: %r",
                correlation_id[:8],
                fanout_id,
                self.node_id,
                exc,
            )
            await abort_batch(store, fanout_id)
            # TODO(fault rail PR-6): escalate a typed fault to the caller (spec §4.4) instead of
            # stranding — the abort+tombstone here is the return-only precursor, matching the
            # _aggregate fold/close abort arms the rail likewise upgrades to escalation.
        envelope.reply = None  # no-reply mirror (I3): nothing was returned point-to-point this hop
        # (the siblings owe the output; or, on abort above, both records are tombstoned and the caller strands)
        return Response(envelope, headers=self._headers("call"))

    async def _aggregate(
        self, ctx: SessionRunContext, envelope: Envelope, correlation_id: str, broker: BrokerAnnotation
    ) -> _BatchOpen | _BatchClosed:
        """The durable fan-out fold/close stage (fault-rail §6.8 stage-2 made durable; in-node
        spec §4.2/§4.3). Runs on ``kind == "return"`` deliveries.

        - A non-fan-out return is a **stateless continuation** → ``_BatchClosed`` (run the body).
        - A **marked sibling reply** folds into the durable batch → ``_BatchOpen`` (park); on the
          completing fold it self-publishes the closure re-entry (a fresh delivery), still parked.
        - The self-published **re-entry** closes the batch: the durable snapshot's state/stack/deps
          are restored onto ``ctx``+``envelope`` and the materialized outcomes resume the body →
          ``_BatchClosed``. A spurious/abandoned/aborted close is a no-op park → ``_BatchOpen``.

        TODO(fault rail PR-6): add the ``_BatchFaulted`` arm (an unhandled callee fault closes the
        batch through the fault group), the per-sibling stage-1 ``on_callee_error`` on fault
        deliveries, and the ``seam_budgets`` tally — all need the fault wire model.
        """
        match self._classify_fanout(envelope):
            case None:
                return _BatchClosed()  # stateless single-call continuation → body
            case "sibling":
                store = self._resolve_fanout_store(ctx)
                frame = envelope.internal_workflow_state.current_frame
                fanout_id = frame.frame_id  # == frame.fanout_id, the batch key
                reply = envelope.reply
                assert reply is not None  # guaranteed by _classify_fanout == "sibling"
                if reply.in_reply_to is None:
                    logger.error(
                        "[%s] malformed sibling reply (no in_reply_to) on fan-out node=%s; not folding",
                        correlation_id[:8],
                        self.node_id,
                    )
                    return _BatchOpen()
                tag = reply.tag
                result = ctx.state.tool_results.get(tag) if tag is not None else None
                outcome = FanoutOutcome(slot=reply.in_reply_to, tag=tag, result=result)
                match await fold_sibling(store, fanout_id, outcome):
                    case FoldComplete():
                        try:
                            await self._publish_reentry(envelope, correlation_id, broker)
                        except KafkaError:
                            logger.error(
                                "[%s] fan-out re-entry publish failed batch=%s node=%s; aborting + caller strands",
                                correlation_id[:8],
                                fanout_id,
                                self.node_id,
                            )
                            await abort_batch(store, fanout_id)
                            # TODO(fault rail PR-6): on a permanent re-entry-publish failure, escalate a typed fault to the
                            # caller (spec §4.4) instead of stranding — the abort+tombstone here is the return-only precursor.
                    case FoldStray(reason=reason):
                        logger.warning("[%s] fan-out stray (%s) slot=%s node=%s", correlation_id[:8], reason, reply.in_reply_to, self.node_id)
                    case FoldAbort(reason=reason):
                        logger.error(
                            "[%s] fan-out fold abort (%s) batch=%s node=%s; caller strands",
                            correlation_id[:8],
                            reason,
                            fanout_id,
                            self.node_id,
                        )
                    case FoldParked():
                        pass
                return _BatchOpen()  # park (no-reply mirror) — the output is still owed by siblings
            case "reentry":
                store = self._resolve_fanout_store(ctx)
                fanout_id = envelope.internal_workflow_state.current_frame.frame_id
                match await close_batch(store, fanout_id):
                    case CloseResume(snapshot=snapshot):
                        self._restore_from_snapshot(ctx, envelope, snapshot)
                        return _BatchClosed()  # resume the body on the restored context
                    case CloseSpurious():
                        logger.warning("[%s] fan-out spurious re-entry (incomplete) batch=%s node=%s", correlation_id[:8], fanout_id, self.node_id)
                    case CloseAbandon():
                        logger.debug("[%s] fan-out re-entry on a closed batch=%s node=%s; abandoning", correlation_id[:8], fanout_id, self.node_id)
                    case CloseAbort(reason=reason):
                        logger.error(
                            "[%s] fan-out close abort (%s) batch=%s node=%s; caller strands",
                            correlation_id[:8],
                            reason,
                            fanout_id,
                            self.node_id,
                        )
                return _BatchOpen()  # no-op park for a spurious/abandoned/aborted re-entry

    def _restore_from_snapshot(self, ctx: SessionRunContext, envelope: Envelope, snapshot: EnvelopeSnapshot) -> None:
        """Restore the closure context from the durable snapshot (in-node spec §4.3 stage-0).

        ``_publish_reentry`` cleared the re-entry envelope's context, so the basestate snapshot
        (its outcomes already materialized into ``snapshot.state`` by ``close_batch``) is the sole
        restore source. Overwrites ``ctx``'s state/deps and the envelope's stack so the resumed
        body runs on the conversation as of fan-out and its ``ReturnCall`` unwinds the original
        (unmarked) fan-out frame back to its caller. ``resources`` are node-local — re-stamped
        from the bag, never snapshotted.
        """
        frame = snapshot.stack.current_frame_or_none
        # The snapshot's overrides are authoritative-by-capture (baked into snapshot.state.overrides
        # at OPEN); do NOT re-stamp them from the restored frame here — the capture is the source of truth.
        ctx.state = snapshot.state
        ctx.deps = snapshot.deps
        ctx._frame_id = frame.frame_id if frame is not None else None
        ctx._resources = self._effective_resources()
        ctx._reply = None
        envelope.internal_workflow_state = snapshot.stack
        envelope.context = SessionRunContext(state=snapshot.state, deps=snapshot.deps)

    async def _execute(
        self,
        ctx: SessionRunContext,
        kind: MessageKind,
        envelope: Envelope,
        route: str | None,
        payload: Any,
        *,
        awaiting_reply: bool,
        correlation_id: str,
        broker: BrokerAnnotation,
    ) -> NodeResult[State] | _PipelineSentinel:
        """The staged inner pipeline (fault-rail §6.8 ``_execute``) — return-only subset.

        Stage 2 (:meth:`_aggregate`) runs on ``return`` deliveries: an open batch parks
        (``_CONSUMED``); a closed batch or a stateless continuation falls through to the body.
        Stage 4 is the body — today's route Chain-of-Responsibility (:meth:`_dispatch_routed`),
        untouched; an all-declined body is ``_DECLINED``.

        TODO(fault rail PR-6): stage 1 ``on_callee_error`` (before stage 2, on ``fault`` kind),
        stage 3 ``before_node`` (after ``_aggregate``, before the body), stage 6 ``after_node``
        (wrapping the output), and the ``_BatchFaulted`` arm — all need the seam machinery + the
        fault wire model. Gates currently occupy the stage-3 slot from the handler; the rail
        deletes gates and adds ``before_node`` here.
        """
        if kind == "return":
            match await self._aggregate(ctx, envelope, correlation_id, broker):
                case _BatchOpen():
                    return _CONSUMED
                case _BatchClosed():
                    pass  # fan-out close resume OR stateless continuation → run the body
        result = await self._dispatch_routed(ctx, route, payload, awaiting_reply=awaiting_reply, correlation_id=correlation_id)
        if result is None:
            return _DECLINED
        return result

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
                    # TODO(#201): "skip to next handler" is correct for an optional specific
                    # route, but for a reply-owing tool node whose only handler is the '*'
                    # catch-all this declines without publishing a ReturnCall — the agent then
                    # relies on its reply-TTL. Make a reply-owing catch-all schema rejection
                    # produce a FailedToolCall reply (error-propagation work).
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
        """The closed per-delivery pipeline (fault-rail §6.8) — return-only subset.

        stage-0 (emitter decode → :meth:`_classify` → :meth:`prepare_context`) → gates →
        :meth:`_execute` (stage-2 :meth:`_aggregate` + stage-4 the body) → output disposition.
        Returns a broadcast-mirror :class:`Response` whose ``x-calf-kind`` is the hop's kind.

        TODO(fault rail PR-6): stage-0 ``_stray_check``/``_floor_stray`` (return-only has no
        stage-0 strays — fan-out strays floor inside :meth:`_aggregate`, closure-recognition is in
        :meth:`_classify_fanout`); the fault boundary wrapping ``_execute`` (try/except →
        ``on_node_error``/``_publish_fault``) — pre-rail, body exceptions propagate (dropped under
        ACK_FIRST → the caller strands, preserving today's at-most-once behavior); ``before_node``
        replaces the gate block below. All need the seam machinery + the fault wire model.
        """
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
        kind = self._classify(headers)
        logger.debug("[%s] handler entered node=%s emitter=%s kind=%s", correlation_id[:8], self.node_id, emitter, kind)
        ctx = await self.prepare_context(envelope, emitter_node_id=emitter, emitter_node_kind=emitter_kind, correlation_id=correlation_id)

        if not await self._evaluate_gates(ctx, correlation_id):
            envelope.reply = None  # no-reply mirror (gate rejected): don't re-broadcast an inbound reply (I3)
            return Response(envelope, headers=self._headers("call"))

        frame = envelope.internal_workflow_state.current_frame_or_none
        payload = frame.payload if frame is not None else None
        awaiting_reply = frame.callback_topic is not None if frame is not None else False
        route = decode_header_str(headers.get(HDR_ROUTE))
        output = await self._execute(
            ctx,
            kind,
            envelope,
            route,
            payload,
            awaiting_reply=awaiting_reply,
            correlation_id=correlation_id,
            broker=broker,
        )

        if output is _CONSUMED:
            # A parked fan-out fold or a self-published re-entry: no publishable action this hop,
            # the output is still owed by the pending siblings.
            envelope.reply = None  # no-reply mirror (I3)
            return Response(envelope, headers=self._headers("call"))
        if output is _DECLINED:
            # No matched handler produced a terminal result: every match declined (returned
            # Next/None). The '*' handler (run) is always in the chain — the base run() always
            # declines; an overridden run() may also decline, or a schema'd '*' run may have
            # REJECTED the body (logged above). Stuck workflow if a caller awaits a return, else a
            # fire-and-forget no-op. The body_note surfaces a present-but-unconsumed payload (e.g. a
            # malformed tool ToolCallRef) so a dropped body is observable, not silent.
            level = _stuck_level(awaiting_reply)
            body_note = " — a body was not consumed (rejected by a schema handler, or unmatched)" if payload is not None else ""
            logger.log(
                level,
                "[%s] no handler produced a result for route=%s on node=%s; registered=%s%s",
                correlation_id[:8],
                route,
                self.node_id,
                tuple(type(self)._handlers),
                body_note,
            )
            envelope.reply = None  # no-reply mirror (no result): don't re-broadcast an inbound reply (I3)
            return Response(envelope, headers=self._headers("call"))

        # Fan-out OPEN: a fan-out-capable node whose body returned a parallel batch (N >= 2 Calls)
        # registers the durable batch + publishes the marked siblings. A non-capable node's
        # list[Call] stays the plain parallel publish in _publish_action; a 1-element list is a
        # single stateless continuation (not a durable batch) and reroutes there too.
        if self._is_fanout_capable and isinstance(output, list) and len(output) >= 2 and all(isinstance(c, Call) for c in output):
            return await self._handle_fanout_open(ctx, output, envelope, correlation_id, broker)

        logger.debug("[%s] node=%s produced action=%s", correlation_id[:8], self.node_id, type(output).__name__)
        body, pubkind = await self._publish_action(output, envelope, correlation_id, broker)
        return Response(body, headers=self._headers(pubkind))

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
