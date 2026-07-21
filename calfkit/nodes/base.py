import inspect
import logging
from collections.abc import Callable
from dataclasses import dataclass, replace
from enum import Enum, auto
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, Final, Literal, TypeVar, cast

import uuid_utils
from aiokafka.errors import MessageSizeTooLargeError  # type: ignore[import-untyped]
from faststream import Context, Response
from faststream.kafka.annotations import (
    KafkaBroker as BrokerAnnotation,
)
from pydantic import PydanticSchemaGenerationError, TypeAdapter, ValidationError

from calfkit._protocol import HDR_EMITTER, HDR_EMITTER_KIND, HDR_ERROR_TYPE, HDR_KIND, HDR_ROUTE, HDR_WIRE, MessageKind, NodeKind, decode_header_str
from calfkit._registry import RegistryMixin, handler
from calfkit._routing import is_concrete_route_key, match_chain
from calfkit.controlplane.advert import AdvertRegistryMixin
from calfkit.exceptions import NodeFaultError, RegistryConfigError, SeamContractError, safe_exc_message
from calfkit.models import (
    Call,
    Next,
    NodeResult,
    ReturnCall,
    State,
    TailCall,
)
from calfkit.models._coerce import _coerce_to_parts
from calfkit.models.envelope import Envelope
from calfkit.models.error_report import ErrorReport, FaultTypes, FrameRef
from calfkit.models.fanout import EnvelopeSnapshot, FanoutOpen, FanoutOutcome, SlotRef
from calfkit.models.node_result import _UNSET, _extract_output, extract_lenient
from calfkit.models.node_schema import BaseNodeSchema
from calfkit.models.payload import ContentPart
from calfkit.models.reply import FaultMessage, ReturnMessage
from calfkit.models.seam_context import CalleeResult, SeamContext
from calfkit.models.session_context import SessionRunContext, WorkflowState
from calfkit.nodes._fanout_store import (
    FANOUT_STORE_KEY,
    CloseAbandon,
    CloseAbort,
    CloseResume,
    CloseSpurious,
    FanoutBatchStore,
    FoldAbort,
    FoldComplete,
    FoldParked,
    FoldStray,
    SiblingPending,
    abort_batch,
    classify_sibling,
    close_batch,
    record_outcome,
)
from calfkit.nodes._seams import (
    AFTER_NODE,
    BEFORE_NODE,
    ON_CALLEE_ERROR,
    ON_NODE_ERROR,
    SEAM_NAMES,
    _Minted,
    run_chain,
    run_chain_guarded,
    validate_positional_arity,
)
from calfkit.nodes._steps import HopStepLedger, Observed
from calfkit.worker.lifecycle import LifecycleHookMixin

if TYPE_CHECKING:
    from calfkit.worker.worker import Worker

logger = logging.getLogger(__name__)

_SeamFn = TypeVar("_SeamFn", bound=Callable[..., Any])
"""Preserves a seam handler's concrete type through the registration decorators (which
return the handler fn unchanged), so a decorated handler stays directly callable."""

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


@dataclass(frozen=True)
class _BatchFaulted:
    """The aggregation / stage-1 stage resolved to a FAULT — escalate it up the node's own rail
    (§6.8). **Returned, never raised** (R5): raising would trip the node's OWN ``on_node_error``
    (the swallow trap that could convert a callee fault into a success). In step 3 it carries a
    received callee fault escalating by default (§8); step 4 also produces it when a fan-out batch
    closes with unhandled sibling faults (the fault group)."""

    report: ErrorReport


class _PipelineSentinel(Enum):
    """The payload-less park outcome of the staged pipeline (§6.8) — a narrowable singleton,
    distinct from the publishable :data:`NodeResult` actions the handler routes to
    ``_publish_action``. The decline (:class:`_Declined`) and fault (:class:`_BatchFaulted`)
    outcomes carry payloads and are their own dataclasses."""

    CONSUMED = auto()
    """A parked fan-out fold or a self-published re-entry: no publishable action this hop (the
    output is still owed by pending siblings). The handler emits the no-reply broadcast mirror."""


_CONSUMED: Final = _PipelineSentinel.CONSUMED


@dataclass(frozen=True)
class _Declined:
    """The body produced no terminal result — every matched handler declined, or a handler's
    schema rejected the body (§10). Carries ``reason`` (the dispatcher's discriminator) so the
    handler can auto-fault a reply-owing delivery (``calf.delivery.rejected`` with
    ``details.reason``) — #201 closed by construction — while a fire-and-forget no-output stays a
    DEBUG no-op. ``reason`` is one of
    :attr:`~calfkit.models.error_report.FaultTypes.REASON_SCHEMA_REJECTED` / ``REASON_ALL_DECLINED``."""

    reason: str


@dataclass(frozen=True)
class _Stray:
    """A stage-0 stray: the inbound delivery's reply-slot SHAPE disagrees with its asserted
    ``x-calf-kind`` (§4.1 rule 3 — ``call`` ↔ no reply, ``return`` ↔ ``ReturnMessage``,
    ``fault`` ↔ ``FaultMessage``). Floored and ignored before the seams by
    :meth:`BaseNodeDef._floor_stray`, never run as work — a junk/foreign delivery must never
    fault the node's own live invocation. ``kind`` is the asserted (header) kind, for the log."""

    kind: MessageKind


class _SeamAccidentError(Exception):
    """An accidental (non-``NodeFaultError``) raise from a BOUNDARY seam — ``before_node`` or
    ``after_node`` (§6.5). :meth:`BaseNodeDef._execute` wraps it so the stage-5 arm can distinguish
    it from a BODY raise (§6.7): a boundary-seam accident routes to ``on_node_error`` WITH the handled
    inbound fault, if any, chained in ``causes``; a body raise chains nothing. The mint rule still
    holds — a ``NodeFaultError`` from a seam is NOT wrapped (it propagates verbatim). Carries the
    original exception so synthesis sees the true error_type/traceback."""

    def __init__(self, original: Exception) -> None:
        # No args message: the fault path must never itself raise, and repr(original) on a hostile
        # __repr__ could. The unwrap reads .original; synthesis re-derives the human message via the
        # guarded _safe_exc_str. (Round-2: drop the unguarded, never-read repr.)
        super().__init__()
        self.original = original


# ── slot-outcome vocabulary (what a callee slot resolves to in stage 1 / the fan-out fold, §6.9) ──
@dataclass(frozen=True)
class _SlotResolved:
    """A callee slot resolved to CONTENT — a plain return (``handled=False``) or an
    ``on_callee_error`` substitute (``handled=True``). Recorded as a ``CalleeResult``; the agent
    materializes it into the model conversation (a ``ToolReturn``, or a ``RetryPromptPart`` for a
    ``calf.retry``-marked part). Carries the slot identity so the uniform stage-1 path resolves a
    return and a fault the same way (the identity is not always on ``ctx.failing_call``)."""

    frame_id: str
    tag: str | None
    target_topic: str | None
    parts: list[ContentPart]
    handled: bool


@dataclass(frozen=True)
class _SlotFailed:
    """A callee slot that FAILED — its fault was unhandled (``on_callee_error`` declined) or a
    coercion/materialization error. Recorded as a ``CalleeResult`` carrying the fault; **never
    materialized** into the model conversation — the fault escalates at closure (§6.9)."""

    frame_id: str
    tag: str | None
    target_topic: str | None
    report: ErrorReport


# ---------------------------------------------------------------------------
# Base node definition
# ---------------------------------------------------------------------------


class BaseNodeDef(BaseNodeSchema, LifecycleHookMixin, RegistryMixin, AdvertRegistryMixin):
    _worker: "Worker | None" = None
    """Back-reference to the owning worker, set by ``Worker._add_node``. ``None``
    for a node not attached to a worker. Used by :meth:`_effective_resources` to
    merge worker-scoped lifecycle resources under the node's own. Not a dataclass
    field (``BaseNodeDef`` is not a ``@dataclass``), so it never rides the wire."""
    _node_kind: ClassVar[NodeKind] = "node"
    """Coarse classification of this node, stamped onto every outbound publish as the
    ``x-calf-emitter-kind`` Kafka header. Subclasses override to one of the values in
    :data:`~calfkit._protocol.NodeKind`. The ``"client"`` kind is reserved for the
    :class:`~calfkit.client.Client` and is not a valid subclass override.
    """
    is_caller_capable: ClassVar[bool] = True
    """Whether this node type handles ``Call``s and their ``ReturnCall`` continuations over
    its own workflow state (agent, tool, MCP toolbox, custom ``BaseNodeDef`` subclasses);
    ``False`` only for observers (``ConsumerNode``), which just consume. Subclasses may
    override it. This is why EVERY node registers via the key-ordered subscriber
    (parallel across correlations, strictly serial and in-order per ``correlation_id`` —
    the partition key): handling a continuation is an await-spanning read-modify-write of
    per-correlation workflow state — the agent's tool-call batch aggregation, the in-node
    fan-out fold — that a no-affinity ``max_workers>1`` coroutine pool would race.
    Registration itself no longer branches on this flag (observers share the one
    consumption model, ADR-0042); it gates control-plane advert registration."""

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
        before_node: _SeamArg = None,
        after_node: _SeamArg = None,
        on_node_error: _SeamArg = None,
        on_callee_error: _SeamArg = None,
    ) -> None:
        """Initialize a node definition.

        Args:
            node_id: Unique identifier for the node.
            subscribe_topics: One or more topics the node consumes from.
                Must be non-empty for node kinds whose only inbound traffic
                arrives here — a Consumer / Tool with no public inbox cannot be
                invoked by any client or peer. Without the validation in
                :meth:`BaseNodeSchema.__post_init__`, ``Worker.register_handlers``
                would still wire the node up to ``_return_topic`` (issue #141
                fix), so the node would "register" successfully while being
                functionally unreachable from the outside. Agents are exempt
                (``BaseNodeSchema._reachable_without_public_inbox``): they are
                always addressable on their name-derived private input inbox
                (``agent.{name}.private.input``, ADR-0017), so an empty list is
                valid for an agent.
            publish_topic: Optional default topic to publish results to.
            before_node, after_node, on_node_error, on_callee_error: Optional policy-seam
                handlers — a single callable or a list (spec §6.1). Constructor entries
                precede any decorator-registered handlers in the same chain.

        Raises:
            ValueError: If ``subscribe_topics`` is empty for a node kind that is
                not reachable without it. Enforced in
                :meth:`BaseNodeSchema.__post_init__` (agents are exempt).
        """
        super().__init__(
            node_id=node_id,
            subscribe_topics=subscribe_topics,
            publish_topic=publish_topic,
        )
        # The constructor seam params register first (decorator entries append after, §6.1
        # chain-order). The chains themselves are lazily created by the ``_chains`` property.
        self._register_seam_params(BEFORE_NODE, before_node)
        self._register_seam_params(AFTER_NODE, after_node)
        self._register_seam_params(ON_NODE_ERROR, on_node_error)
        self._register_seam_params(ON_CALLEE_ERROR, on_callee_error)

    @property
    def _chains(self) -> dict[str, list[Callable[..., Any]]]:
        """The four policy-seam chains (spec §6.1), consulted by the staged pipeline.

        Lazily initialized so EVERY node type has them — including ``@dataclass`` node types
        (``BaseToolNodeDef``, the MCP toolbox) whose auto-generated ``__init__`` bypasses
        ``BaseNodeDef.__init__`` entirely (the same reason topic validation lives in
        ``BaseNodeSchema.__post_init__``). Stored under a distinct ``__dict__`` key so the
        property name stays read-only."""
        chains = self.__dict__.get("_seam_chains")
        if chains is None:
            chains = {name: [] for name in SEAM_NAMES}
            self.__dict__["_seam_chains"] = chains
        return chains

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

        Rejects registration on an observer (``ConsumerNode``, ``is_caller_capable=False``)
        outright (§6.6): observers consume via their own body and never call, so there is
        nothing for a seam to guard. Otherwise validates the handler is callable and accepts
        the seam's positional arity (``before_node(ctx)``; the rest ``(ctx, output/fault)``) —
        so a wrong-shaped handler fails loudly at startup, not silently at first fire."""
        if not self.is_caller_capable:
            raise RegistryConfigError(
                f"node={self.node_id}: cannot register a {seam} seam on an observer (ConsumerNode); "
                f"observers consume via their own body and never call, so there is nothing for a seam to "
                f"guard. Policy seams exist only on caller-capable nodes."
            )
        if not callable(fn):
            raise RegistryConfigError(f"node={self.node_id}: {seam} handler must be callable, got {type(fn).__name__}")
        arity = _SEAM_ARITY[seam]
        # follow_wrapped=False (D6a): validate the WRAPPER's own arity, not a functools.wraps'd inner —
        # call time never unwraps. Shared with the agent's on_tool_error arity-3 check (validate_positional_arity).
        extra = ", fault/output" if arity == 2 else ""
        validate_positional_arity(fn, arity, node_id=self.node_id, kind=seam, param_desc=f"ctx{extra}")
        self._chains[seam].append(fn)

    def before_node(self, fn: _SeamFn) -> _SeamFn:
        """Register a ``before_node`` handler. Usable as an instance decorator; repeatable.

        Appends to the same chain the ``before_node=`` constructor param feeds (constructor
        entries first, then decorated), and returns ``fn`` unchanged so it stays directly
        unit-testable."""
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

    @handler("*")
    async def run(self, ctx: SessionRunContext) -> NodeResult[State] | Observed[State] | Next | None:
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
            Framework node kinds may pair the action with step facts via ``Observed``
            (step-emission spec §3.1b) — user handlers return plain actions.
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
        ctx._stamp_transport(correlation_id=correlation_id, emitter_node_id=emitter_node_id, emitter_node_kind=emitter_node_kind)
        ctx._resources = self._effective_resources()
        ctx._frame_id = frame.frame_id if frame is not None else None
        # Derive the messaging cycle guard's ancestor chain (ADR-0016): the (caller_node_id,
        # caller_node_kind) of every suspended call on the inbound stack. The agent's resolver rejects a
        # message_agent whose target is already here (a ring). Frames with no caller (the public entry,
        # escalation hops) are filtered. Workflow-state-sourced like _frame_id, so stamped here.
        ctx._ancestor_callers = frozenset(
            (f.caller_node_id, f.caller_node_kind)
            for f in envelope.internal_workflow_state.call_stack._internal_list
            if f.caller_node_id is not None and f.caller_node_kind is not None
        )
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

    def _resync_seam_context(self, seam_ctx: SeamContext[State], run_ctx: SessionRunContext, envelope: Envelope) -> None:
        """Re-sync the (shared) ``seam_ctx`` from ``run_ctx`` + the inbound frame after
        ``_aggregate``. A no-op for a stateless continuation (``run_ctx`` unchanged); for a
        fan-out CLOSE it propagates the restored snapshot ``state``/``deps``/frame so
        ``before_node`` (fires at closure, §6.4) and ``after_node`` see the resumed
        conversation, not the pre-close (cleared) re-entry state."""
        frame = envelope.internal_workflow_state.current_frame_or_none
        seam_ctx.state = run_ctx.state
        seam_ctx.deps = run_ctx.deps
        seam_ctx.payload = frame.payload if frame is not None else None
        seam_ctx.awaiting_reply = frame.callback_topic is not None if frame is not None else False

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
        h = {HDR_EMITTER: self.node_id, HDR_EMITTER_KIND: self._node_kind, HDR_KIND: kind, HDR_WIRE: Envelope.WIRE}
        if route is not None:
            h[HDR_ROUTE] = route
        return h

    async def _flush_steps(
        self, ledger: HopStepLedger, snapshot: WorkflowState, correlation_id: str, broker: BrokerAnnotation, *, disposition: NodeResult[State] | None
    ) -> None:
        """The hop-exit flush (step-emission spec §3.4, I6): invoked at EVERY exit of
        ``_handle_delivery`` — the chokepoint path, the parked ``_CONSUMED`` arm, the fire-and-forget
        declined arm, and immediately BEFORE every fault publish. This helper is the SINGLE
        best-effort guard (I1) and owns the identity-argument derivation: one ``try/except``
        log-and-drop encloses the guarded root read (``Stack.root`` RAISES on an empty stack — the
        frameless fire-and-forget flows), the identity computation, and the flush call itself
        (``flush`` is total by construction, E2 — replacing it wholesale still lands in this guard).
        It never short-circuits the action/fault publish that follows; a raise here would otherwise
        escape to FastStream and — under ACK_FIRST — hang the run."""
        try:
            stack = snapshot.call_stack
            root_callback = stack.root.callback_topic if stack else None
            frame = stack.peek() if stack else None
            await ledger.flush(
                broker,
                disposition=disposition,
                depth=len(stack),
                frame_id=frame.frame_id if frame is not None else "",
                correlation_id=correlation_id,
                emitter=self.node_id,
                emitter_kind=self._node_kind,
                root_callback=root_callback,
            )
        except Exception:
            logger.warning(
                "[%s] step flush failed on node=%s; dropping (best-effort, run unaffected)",
                correlation_id[:8],
                self.node_id,
                exc_info=True,
            )

    def _no_reply_mirror(self, envelope: Envelope) -> Response:
        """The broadcast mirror for a hop that produces NO reply this delivery: clear the inbound
        reply (I3 — a no-reply hop must not re-broadcast the inbound reply under this node's emitter
        to its observers) and return the call-kind ``Response``. Shared by the floor paths, the
        parked/``_CONSUMED`` fan-out fold, and the fire-and-forget no-output arms."""
        envelope.reply = None
        return Response(envelope, headers=self._headers("call"))

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
                wf_copy.invoke_frame(
                    call,
                    self._return_topic,
                    payload=call.body,
                    tag=call.tag,
                    marker=call.marker,
                    caller_node_id=self.node_id,
                    caller_node_kind=self._node_kind,
                )
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
            envelope.internal_workflow_state.invoke_frame(
                output,
                self._return_topic,
                payload=output.body,
                tag=output.tag,
                marker=output.marker,
                caller_node_id=self.node_id,
                caller_node_kind=self._node_kind,
            )
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
            reply = ReturnMessage(in_reply_to=frame.frame_id, tag=frame.tag, marker=frame.marker, parts=_coerce_to_parts(output.value))
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
                # The publish failure is NOT swallowed here (the old traceability fallback):
                # it propagates to the handler's publish guard, which faults the caller on the
                # pre-mutation snapshot (scenario 42). The fault path is what carries the
                # broadcast mirror now — so a lost delivery is still observable, as a fault.
                await broker.publish(
                    publish_envelope,
                    topic=frame.callback_topic,
                    correlation_id=correlation_id,
                    key=correlation_id.encode(),
                    headers=self._headers("return"),
                )

        elif isinstance(output, TailCall):
            # TailCall = the SAME pending call retargeted (§4.2/§15): preserve frame_id/tag/
            # callback_topic on the replacement frame (a fresh frame_id would orphan the caller's slot —
            # the eventual reply's in_reply_to must match the id the caller registered), clearing only
            # payload (TailCall carries no body — the traveling State is its input) and fanout_id (a
            # TailCall is never fan-out-marked). `invoke_frame` would mint a fresh id.
            frame = envelope.internal_workflow_state.unwind_frame()
            retargeted = replace(frame, target_topic=output.target_topic, payload=None, fanout_id=None)
            envelope.internal_workflow_state.call_stack.push(retargeted)
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

    def _stack_snapshot(self, envelope: Envelope) -> WorkflowState:
        """A deep copy of the inbound call stack, captured BEFORE ``_execute`` mutates it,
        so ``_publish_fault`` addresses the pre-mutation caller (spec §6.8 / scenario 42)."""
        return envelope.internal_workflow_state.model_copy(deep=True)

    def _fault_from_exception(
        self, exc: Exception, ctx: SeamContext[State] | None, snapshot: WorkflowState, correlation_id: str, *, cause: ErrorReport | None = None
    ) -> ErrorReport:
        """Synthesize a node-own ``calf.exception`` fault, capturing the call-stack topology from the
        PRE-mutation ``snapshot`` (spec §4.3/§4.4 / ADR-0003): ``frame_chain`` is the traceback analog
        (one ``FrameRef`` per pending frame) and ``origin_frame_id`` is the node's own answering frame.
        The seam ``ctx`` carries no stack, so the snapshot is the sole source. ``cause`` chains the §6.8
        recovery-then-failure prior report. Logs the synthesis ERROR with the traceback, at origin (§13)."""
        frame_chain = [FrameRef(frame_id=f.frame_id, target_topic=f.target_topic) for f in snapshot.call_stack._internal_list]
        origin = snapshot.current_frame_or_none
        report = ErrorReport.from_exception(
            exc,
            node=self,
            ctx=ctx,
            cause=cause,
            frame_chain=frame_chain,
            origin_frame_id=origin.frame_id if origin is not None else None,
        )
        # §13: synthesis is logged ERROR with the traceback, at origin (the 'why did my node fault'
        # anchor). exception.type carries the forensic class hint the removed class-name details
        # breadcrumb gave; the guard is necessary — build_safe's fallback arm leaves exception=None.
        logger.error(
            "[%s] fault synthesized at node=%s error_type=%s exception.type=%s: %s",
            correlation_id[:8],
            self.node_id,
            report.error_type,
            report.exception.type if report.exception else None,
            report.message,
            exc_info=exc,
        )
        return report

    async def _publish_fault(
        self, report: ErrorReport, snapshot: WorkflowState, inbound: Envelope, correlation_id: str, broker: BrokerAnnotation
    ) -> tuple[Envelope, MessageKind]:
        """Publish a typed fault on the node's success rail (P2) and return the fault-bearing
        envelope for the broadcast mirror (spec §4.2/§6.8/§13).

        Mirrors the ``ReturnCall`` arm against the PRE-MUTATION ``snapshot``: pop the answered
        frame, mint ``FaultMessage(in_reply_to=popped.frame_id, tag=popped.tag, marker=popped.marker, error=report)``,
        and publish to the popped ``callback_topic`` with ``x-calf-kind=fault`` + ``x-calf-error-type``.
        The rung-1 mirror carries the inbound ``context`` UNCHANGED (handler mutations die with the
        faulted turn, §4.2). A frameless / fire-and-forget terminal (no callback) is floored (ERROR +
        full report JSON, §13). Escalation NEVER wraps (§4.4): the same ``report`` is re-addressed each hop.

        On an oversized publish the carriage is degraded down the §D1 ladder rather than dropped:
        full → **lean** (empty context + a topology-only stack, ``state_elided=True``, spec D2) → lean +
        ``to_minimal()`` report → floor. So an oversized turn — the incident's shape, where the run state
        (not the report) blows the budget — still delivers the error to the caller instead of becoming a
        new silent drop (state-elision spec, superseding the old report-only strip). A non-size publish
        failure (dead broker) at any rung cannot be published-around, so it floors with that rung's mirror.
        """
        frame = snapshot.current_frame_or_none
        callback_topic = frame.callback_topic if frame is not None else None
        in_reply_to = frame.frame_id if frame is not None else None
        tag = frame.tag if frame is not None else None
        marker = frame.marker if frame is not None else None  # echo the answered frame's marker (captured pre-pop, like tag)
        if frame is not None:
            snapshot.unwind_frame()  # pop the answered frame; the remaining stack travels for the next hop

        topology: WorkflowState | None = None
        # Propagation (spec D3): if the inbound fault being answered was ITSELF state-elided, the rung-1
        # mirror carries that inbound (already-empty) context, so its flag must stay True or it would lie
        # on the next escalation hop. A ReturnMessage inbound (a fan-out close re-entry, state restored
        # from the durable basestate) is NOT elided — the flag correctly does not propagate through a close.
        inbound_elided = isinstance(inbound.reply, FaultMessage) and inbound.reply.state_elided

        def _mirror(r: ErrorReport, *, lean: bool) -> Envelope:
            # A LEAN mirror (spec D2) drops the run state that can overflow the size limit — an empty
            # context + a topology-only stack (routing skeleton kept) — and flags the elision. A full
            # mirror carries the inbound context UNCHANGED (handler mutations die with the turn, §4.2).
            # TODO(state-elided-receiver-policy): a lean carriage drops the run state a registered
            #   on_callee_error/on_tool_error recovery would read (single-call substitute-continuation),
            #   so recovery on an elided fault can be incoherent. Whether the framework should bypass
            #   recovery on a state-elided fault is DELIBERATELY UNDECIDED — see D4 of
            #   docs/designs/oversized-fault-state-elision-spec.md. Receivers are behaviorally untouched
            #   here; the flag exists so that policy can be chosen later without a wire migration.
            nonlocal topology
            if lean:
                if topology is None:
                    topology = snapshot.to_topology()  # lazy: the happy path (rung 1) never builds it
                context, wf = SessionRunContext(state=State(), deps={}), topology
            else:
                context, wf = inbound.context, snapshot
            return Envelope(
                context=context,
                internal_workflow_state=wf,
                reply=FaultMessage(in_reply_to=in_reply_to, tag=tag, marker=marker, error=r, state_elided=lean or inbound_elided),
            )

        if callback_topic is None:
            # Frameless / fire-and-forget terminal: no caller to answer → floor (§13). The
            # broadcast mirror still fires where a publish_topic exists (the returned envelope).
            mirror = _mirror(report, lean=False)
            logger.error(
                "[%s] terminal fault floored (no callback_topic) node=%s error_type=%s report=%s",
                correlation_id[:8],
                self.node_id,
                report.error_type,
                report.model_dump_json(),
            )
            return mirror, "fault"

        # The §D1 degradation ladder — a flat, always-three-rung table, so the "the loop always runs"
        # invariant the floor below depends on is visible right here. The expensive part (the lean
        # topology projection) is still built lazily inside ``_mirror``, so a rung-1 success — the common
        # case — never pays for it; ``to_minimal()`` is a small field copy with no serialization, built up
        # front because deferring it would buy nothing and cost the reader this clarity.
        rungs: list[tuple[ErrorReport, bool]] = [
            (report, False),  # rung 1 — full carriage + full report
            (report, True),  # rung 2 — lean carriage (state elided) + full report
            (report.to_minimal(), True),  # rung 3 — lean carriage + minimal report
        ]
        for rung_report, lean in rungs:
            mirror = _mirror(rung_report, lean=lean)
            try:
                await broker.publish(
                    mirror,
                    topic=callback_topic,
                    correlation_id=correlation_id,
                    key=correlation_id.encode(),
                    headers=self._headers("fault") | {HDR_ERROR_TYPE: rung_report.error_type},
                )
            except MessageSizeTooLargeError:
                # This rung overflowed the producer's size limit; degrade to the next, leaner rung. If
                # this was the last rung, the loop exhausts and floors below (spec D1 — never silent).
                logger.warning(
                    "[%s] fault publish to callback_topic=%s exceeded the carriage budget node=%s (%s carriage); degrading (spec D1)",
                    correlation_id[:8],
                    callback_topic,
                    self.node_id,
                    "lean" if lean else "full",
                )
                continue
            except Exception:
                # A non-size publish failure (e.g. a dead/unreachable broker): you cannot publish your way
                # out of it, so floor (§6.8 log-only-guarded — never re-enter the fault path). Log the REPORT
                # in full: the broadcast mirror rides this same producer, so on a dead broker it cannot
                # deliver either — this ERROR is then the only record the fault ever existed.
                logger.exception(
                    "[%s] fault delivery to callback_topic=%s failed node=%s; flooring report=%s",
                    correlation_id[:8],
                    callback_topic,
                    self.node_id,
                    rung_report.model_dump_json(),
                )
                return mirror, "fault"
            # §13: each escalation hop is logged WARNING — error_type / origin / remaining stack depth
            # (after the answered frame is popped) — the teaching load for "why didn't my handler fire."
            logger.warning(
                "[%s] fault escalated one hop node=%s error_type=%s origin=%s remaining_depth=%d",
                correlation_id[:8],
                self.node_id,
                rung_report.error_type,
                rung_report.origin_node_id,
                len(snapshot.call_stack._internal_list),
            )
            return mirror, "fault"

        # Every rung overflowed on size → floor with the leanest mirror built (lean carriage + minimal
        # report). After the loop ``mirror``/``rung_report`` hold the final (rung-3 lean+minimal) attempt —
        # the ladder always has three rungs, so both are bound. This ERROR is the last silent-drop guard
        # and, here, the ONLY record: the broadcast mirror rides the same producer that just rejected this
        # very envelope on size, so it cannot carry the fault to ops taps either — hence the full report JSON.
        logger.error(
            "[%s] fault delivery to callback_topic=%s failed even after eliding the run state and stripping to the minimal report"
            " node=%s; flooring report=%s",
            correlation_id[:8],
            callback_topic,
            self.node_id,
            rung_report.model_dump_json(),
        )
        return mirror, "fault"

    async def _fault_response(
        self, report: ErrorReport, snapshot: WorkflowState, inbound: Envelope, correlation_id: str, broker: BrokerAnnotation
    ) -> Response:
        """Publish a fault and wrap its broadcast mirror as the handler's :class:`Response`
        (so the worker's ``@publisher`` mirrors the fault on ``publish_topic``, §13)."""
        mirror, fkind = await self._publish_fault(report, snapshot, inbound, correlation_id, broker)
        headers = self._headers(fkind)
        if isinstance(mirror.reply, FaultMessage):
            # Stamp x-calf-error-type on the broadcast mirror too (§4.2/§13) so faults are broker-
            # filterable on the broadcast rail, matching the point-to-point publish. Source it from the
            # SENT report (mirror.reply) — the minimal report if _publish_fault degraded to the ladder's
            # last rung (state-elision spec D1). to_minimal preserves error_type, so the header is stable.
            headers = headers | {HDR_ERROR_TYPE: mirror.reply.error.error_type}
        return Response(mirror, headers=headers)

    async def _floor_stage0(
        self, exc: Exception, envelope: Envelope, headers: dict[str, Any], correlation_id: str, broker: BrokerAnnotation
    ) -> Response:
        """Handle a stage-0 failure (classify / context build raised) BELOW the seams (§4.1/§6.8).

        No ``ctx`` exists yet, so no seam runs. The disposition reads the RAW ``x-calf-kind``
        header (the original ``_classify`` may itself have raised, so it must not be re-invoked):

        - **call-kind ingress** (missing / ``call``): a caller awaits a reply on the inbound top
          frame, so fault it where the stack is readable (or floor, if frameless) — it never
          hangs. The fault is ``calf.exception`` (a decoded-but-internally-broken envelope is an
          unexpected internal failure; ``calf.delivery.undecodable`` is the distinct PRE-handler
          decode floor).
        - **return / fault delivery**: junk or an internal error on the return inbox must NEVER
          fault the node's own live invocation — floor only (ERROR; the inbound ``ErrorReport`` in
          full if a readable ``FaultMessage``), returning the cleared no-reply mirror (I3).
        """
        hdr_kind = decode_header_str(headers.get(HDR_KIND))
        if hdr_kind is None or hdr_kind == "call":
            snapshot = self._stack_snapshot(envelope)
            report = self._fault_from_exception(exc, None, snapshot, correlation_id)
            return await self._fault_response(report, snapshot, envelope, correlation_id, broker)
        inbound_report = envelope.reply.error.model_dump_json() if isinstance(envelope.reply, FaultMessage) else None
        logger.error(
            "[%s] stage-0 failure on a %s delivery node=%s; flooring (a live invocation is not faulted): %r inbound_report=%s",
            correlation_id[:8],
            hdr_kind,
            self.node_id,
            exc,
            inbound_report,
        )
        return self._no_reply_mirror(envelope)

    def _floor_unknown_kind(self, envelope: Envelope, headers: dict[str, Any], correlation_id: str) -> Response:
        """Floor + drop an unclassifiable delivery (§4.1 rule 2): an unrecognized ``x-calf-kind`` is
        ERROR-logged (with the inbound :class:`FaultMessage` report in full, when one is readable)
        and the delivery is ignored — a node must not run work it cannot classify. Returns the
        cleared no-reply mirror (I3); never faults a live invocation (no callback publish)."""
        raw = decode_header_str(headers.get(HDR_KIND))
        inbound_report = envelope.reply.error.model_dump_json() if isinstance(envelope.reply, FaultMessage) else None
        logger.error(
            "[%s] unrecognized x-calf-kind=%r node=%s; ignoring the delivery (a node must not run unclassifiable work) inbound_report=%s",
            correlation_id[:8],
            raw,
            self.node_id,
            inbound_report,
        )
        return self._no_reply_mirror(envelope)

    @property
    def _is_fanout_capable(self) -> bool:
        """Whether this node folds durable fan-out batches in-node. ``False`` for every
        node kind except an agent (which overrides this) — the fan-out disjunct of
        :attr:`_needs_durable_batch`, which gates the staged handler's fan-out recognition
        and the OPEN dispatch path."""
        return False

    @property
    def _needs_durable_batch(self) -> bool:
        """Whether this node needs the durable fan-out snapshot/restore machinery (decision 1(b)).

        True iff the node can parallel fan out (:attr:`_is_fanout_capable`) **or** it can dispatch an
        ``isolate_state`` call — i.e. it carries a ``Messaging`` handle (set by ``Agent(peers=[Messaging…])``).
        For every agent the disjunction collapses to ``True`` via the first disjunct (an agent is always
        fan-out capable); the
        decoupling from :attr:`_is_fanout_capable` is retained deliberately as future-proofing for a
        non-agent node kind that carries Messaging handles and would need the snapshot machinery for its
        ``isolate_state`` dispatches without folding parallel batches. Gates ``_classify_fanout`` and the
        OPEN trigger."""
        return self._is_fanout_capable or bool(getattr(self, "_messaging_handles", None))

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
        # Next is route-dispatch vocabulary (the CoR decline sentinel), not a seam output. It is not an
        # action, so _is_action lets it through to here; reject it loudly rather than coerce a garbage
        # empty DataPart (§6.2 — return None to proceed / substitute a real output / raise to reject).
        if isinstance(value, Next):
            raise SeamContractError(
                "a seam returned Next, which is route-dispatch vocabulary, not a node output; "
                "return None to proceed, substitute a real output, or raise to reject."
            )
        # Observed is the framework BODY's fact carriage (step-emission spec §3.1b): seams run on
        # plain NodeResults and never see facts — reject it loudly rather than serialize it out.
        if isinstance(value, Observed):
            raise SeamContractError(
                "a seam returned Observed, which is the framework body's step-fact carriage, not a seam "
                "output; return the plain action/value instead — seams never declare step facts."
            )
        # Output-position validation (scenario 44): a TYPED node validates the substitute against its
        # declared output type, so a type-breaking value fails HERE (→ a fault at the seam), not as a
        # DeserializationError in the caller's process. This is the ONE caller-capable exception to
        # "custom nodes are free" (§6.3) — the agent overrides _seam_output_type to final_output_type.
        # An UNSET (custom) node and an unschematizable output type both SKIP (a valid exotic-output
        # config must not crash); on_callee_error substitutes never reach here (they materialize at the
        # slot via _resolve_slot, §6.9 — exempt by construction).
        output_type = self._seam_output_type
        if output_type is not _UNSET:
            try:
                TypeAdapter(output_type).validate_python(value)
            except (PydanticSchemaGenerationError, AttributeError, TypeError):
                # Unschematizable: a bare exotic class (PydanticSchemaGenerationError) OR a pydantic-ai
                # OutputSpec WRAPPER INSTANCE — ToolOutput(M)/NativeOutput(M)/PromptedOutput(M), where
                # TypeAdapter(<instance>) raises AttributeError ('no __mro__')/TypeError. Degrade to a
                # lenient SKIP (the documented exotic-OutputSpec contract) — never a spurious seam fault.
                logger.debug("node=%s seam output type is unschematizable; skipping output-position validation", self.node_id)
            except ValidationError as exc:
                raise SeamContractError(
                    f"a seam substitute does not match node {self.node_id!r}'s declared output type "
                    f"{getattr(output_type, '__name__', output_type)!r}: {exc}"
                ) from exc
        return ReturnCall[State](state=ctx.state, value=value)

    @property
    def _seam_output_type(self) -> Any:
        """The output type ``after_node``'s view is projected to (spec §6.3).

        The base is untyped (``_UNSET`` → lenient auto-detect, never raises). A typed
        node — the agent — overrides this to its declared output type so the projection
        becomes strict: a type-breaking output then faults at the seam, not downstream
        (the agent overrides this to its declared ``final_output_type``)."""
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
        try:
            return _extract_output(parts, output_type)
        except (PydanticSchemaGenerationError, AttributeError, TypeError):
            # An unschematizable declared output type — a bare exotic class OR an OutputSpec wrapper
            # instance (TypeAdapter raises AttributeError/TypeError: no __mro__) — falls back to the
            # lenient view rather than crash; a type-breaking VALUE still fails (DeserializationError)
            # and propagates as a fault.
            return extract_lenient(parts)

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
        post = await run_chain(self._chains[AFTER_NODE], ctx, view, seam_name=AFTER_NODE)
        if post is None:
            return output
        if _is_action(post):
            raise SeamContractError("after_node returns a value, not an action; use before_node/on_node_error for actions.")
        return self._coerce_output(ctx, post)

    def _resolve_slot(self, ctx: SeamContext[State], outcome: _SlotResolved | _SlotFailed) -> None:
        """Record a resolved callee slot on ``ctx.callee_results`` (spec §6.9 — a sealed generic
        conversion). The base only RECORDS the ``CalleeResult``; the agent overrides to ALSO
        materialize the slot into the model conversation (the SDK's single per-type codec). Driven
        by the uniform stage-1 path (a return or a fault) and the fan-out closure."""
        if isinstance(outcome, _SlotResolved):
            result = CalleeResult(
                frame_id=outcome.frame_id, tag=outcome.tag, target_topic=outcome.target_topic, parts=outcome.parts, handled=outcome.handled
            )
        else:
            result = CalleeResult(frame_id=outcome.frame_id, tag=outcome.tag, target_topic=outcome.target_topic, fault=outcome.report)
        ctx.callee_results.append(result)

    async def _resolve_callee(
        self,
        seam_ctx: SeamContext[State],
        kind: MessageKind,
        reply: ReturnMessage | FaultMessage,
        target_topic: str | None,
    ) -> _SlotResolved | _SlotFailed:
        """Stage 1 (fault-rail §6.8/§6.9): resolve ONE callee slot — UNIFORM for a return AND a fault,
        single-call AND fan-out. A return resolves directly (``on_callee_error`` runs only for faults).
        A fault runs the ``on_callee_error`` chain: a substitute value resolves the slot
        (``handled=True``, coerced to parts — a wire-unsafe substitute becomes
        ``calf.slot.materialization_failed``); ``None`` (declined) fails the slot with the inbound report
        (escalates at closure); a raise is SLOT-SCOPED, never a node-own failure (§6.5) — a
        ``NodeFaultError`` honored verbatim, anything else wrapped ``calf.exception``, both chaining the
        inbound fault via ``causes``. Carries the slot identity (frame_id/tag/target_topic) so the caller
        resolves or folds it with no in-process correlation map.
        """
        frame_id = reply.in_reply_to or ""  # stray-checked: a fold/continuation reply always carries it
        tag = reply.tag
        if kind == "return":
            assert isinstance(reply, ReturnMessage)  # stray-check guarantees return ⇔ ReturnMessage
            return _SlotResolved(frame_id=frame_id, tag=tag, target_topic=target_topic, parts=reply.parts, handled=False)
        assert isinstance(reply, FaultMessage)  # stray-check guarantees fault ⇔ FaultMessage
        report = reply.error
        seam_ctx.failing_call = CalleeResult(frame_id=frame_id, tag=tag, target_topic=target_topic, marker=reply.marker, fault=report)
        if reply.state_elided and self._chains[ON_CALLEE_ERROR]:
            # TODO(state-elided-receiver-policy): the recovery chain is about to run on a fault whose run
            #   state was elided to fit the producer's size limit (state-elision spec D2), so a single-call
            #   substitute then continues over EMPTY state — which can be incoherent. Behavior is UNCHANGED
            #   (the handler still runs); this WARNING only makes the deliberately-undecided corner visible
            #   if it occurs. Whether the framework should bypass recovery on an elided fault is open — see
            #   D4 of docs/designs/oversized-fault-state-elision-spec.md.
            logger.warning(
                "[%s] running on_callee_error on a state-elided fault node=%s tag=%s error_type=%s; recovery sees no run state (spec D4)",
                seam_ctx.correlation_id[:8],
                self.node_id,
                tag,
                report.error_type,
            )
        try:
            handled = await run_chain(self._chains[ON_CALLEE_ERROR], seam_ctx, report, seam_name=ON_CALLEE_ERROR)
        except NodeFaultError as nfe:
            # The per-slot transformation gesture (§6.5): the minted fault is honored verbatim, the
            # inbound chained via causes — the `raise ... from` analog, done at the slot.
            minted = nfe.report.model_copy(update={"causes": [*nfe.report.causes, report]})
            return _SlotFailed(frame_id=frame_id, tag=tag, target_topic=target_topic, report=minted)
        except Exception as exc:
            # An accidental raise is SLOT-scoped (§6.5): wrapped calf.exception with the inbound chained
            # — routing it to on_node_error would mint a whole-invocation outcome mid-batch (double
            # replies, leaked batches). It propagates outward (the batch/escalation axis), not sideways.
            chained = ErrorReport.from_exception(exc, node=self, ctx=seam_ctx, cause=report)
            return _SlotFailed(frame_id=frame_id, tag=tag, target_topic=target_topic, report=chained)
        finally:
            seam_ctx.failing_call = None  # set during on_callee_error ONLY
        if handled is None:
            return _SlotFailed(frame_id=frame_id, tag=tag, target_topic=target_topic, report=report)
        try:
            # I2's slot-materialization call site: the eager `to_json` inside coercion guards wire-safety.
            parts = _coerce_to_parts(handled)
        except Exception as exc:
            # A wire-unsafe substitute is slot-scoped too (§6.5/§6.9) — mark the slot failed
            # deterministically so the batch still closes; it never hangs.
            mat = ErrorReport.build_safe(
                error_type=FaultTypes.SLOT_MATERIALIZATION_FAILED,
                message=f"on_callee_error substitute is not wire-serializable: {safe_exc_message(exc)}",
                origin_node_id=self.node_id,
                causes=[report],
            )
            return _SlotFailed(frame_id=frame_id, tag=tag, target_topic=target_topic, report=mat)
        return _SlotResolved(frame_id=frame_id, tag=tag, target_topic=target_topic, parts=parts, handled=True)

    def _classify(self, headers: dict[str, Any]) -> MessageKind | None:
        """Classify the inbound delivery kind from the ``x-calf-kind`` header (§4.1 / §6.8 stage-0).

        Trusts the header — a producer-side fact (§4.1). Missing ⇒ ``"call"`` (the raw-producer
        ingress norm); ``"call"``/``"return"``/``"fault"`` map to themselves. An UNRECOGNIZED value
        ⇒ ``None`` ("ignore"): a node must not execute work it cannot classify (the forward-compat
        rule, §4.1 rule 2; the handler floors + drops it via :meth:`_floor_unknown_kind`). The kind
        selects stage routing in :meth:`_execute` (``return`` ⇒ aggregation, ``fault`` ⇒ stage-1
        escalation, ``call`` ⇒ the body).

        A pure header→kind mapping by design: the kind↔reply-slot-shape *agreement* is a separate,
        body-aware concern owned by :meth:`_stray_check` (run after the context build).
        """
        raw = decode_header_str(headers.get(HDR_KIND))
        if raw is None or raw == "call":
            return "call"
        if raw == "return":
            return "return"
        if raw == "fault":
            return "fault"
        return None

    def _stray_check(self, kind: MessageKind, envelope: Envelope) -> _Stray | None:
        """Check the kind ↔ reply-slot-shape agreement (§4.1 rule 3 / §6.7), run BEFORE the seams.

        The single publish chokepoint stamps ``x-calf-kind`` and the reply slot together, so a
        calfkit-produced delivery always agrees (``call`` ↔ no reply, ``return`` ↔ ``ReturnMessage``,
        ``fault`` ↔ ``FaultMessage``). A disagreement is a foreign/malformed delivery — a ``_Stray``,
        floored and ignored (:meth:`_floor_stray`); it must never reach the seams or fault the node's
        own live invocation (without this, a ``fault``-kind delivery with no slot would ``reply.error``
        → ``AttributeError`` and escalate the whole invocation). Pure and body-aware, by design
        distinct from header classification (:meth:`_classify`)."""
        reply = envelope.reply
        agrees = (
            (kind == "call" and reply is None)
            or (kind == "return" and isinstance(reply, ReturnMessage))
            or (kind == "fault" and isinstance(reply, FaultMessage))
        )
        return None if agrees else _Stray(kind=kind)

    def _floor_stray(self, stray: _Stray, envelope: Envelope, correlation_id: str) -> Response:
        """Floor + drop a stray (kind ↔ slot disagreement, §6.7), never run as work.

        A readable :class:`FaultMessage` under a disagreeing header takes the floor — ERROR with the
        full report + the broadcast mirror (kind/error-type headers stamped) so ops can tap it where
        a ``publish_topic`` exists — but NO point-to-point publish (a stray has no caller relationship
        to answer). Any other disagreement is WARNING + ignore, returning the cleared no-reply mirror
        (I3). Either way the node's own live invocation is untouched (P1's floor arm)."""
        reply = envelope.reply
        if isinstance(reply, FaultMessage):
            logger.error(
                "[%s] stray fault (asserted kind=%s ↔ slot disagreement) node=%s; flooring report=%s",
                correlation_id[:8],
                stray.kind,
                self.node_id,
                reply.error.model_dump_json(),
            )
            return Response(envelope, headers=self._headers("fault") | {HDR_ERROR_TYPE: reply.error.error_type})
        logger.warning(
            "[%s] stray %s (kind ↔ slot disagreement) node=%s; ignoring (the live invocation is untouched)",
            correlation_id[:8],
            stray.kind,
            self.node_id,
        )
        return self._no_reply_mirror(envelope)

    def _classify_fanout(self, envelope: Envelope) -> Literal["sibling", "reentry"] | None:
        """Recognize a fan-out continuation on a node that needs the durable batch machinery.

        ``"sibling"`` (a marked sibling reply → fold), ``"reentry"`` (the self-published
        closure → close), or ``None`` (normal ingress / single-call continuation). The
        marker rides the node's OWN frame (``fanout_id`` set), so it is the top frame when
        a fan-out continuation re-enters; the reply slot's ``in_reply_to`` then tells a
        sibling callee (≠ the frame id) from the re-entry (== it). Gated on
        ``_needs_durable_batch`` (decision 1(b)) so any degenerate one-element batch's
        continuations classify + fold, not just a true parallel fan-out's."""
        if not self._needs_durable_batch:
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
        fanout_id = envelope.internal_workflow_state.current_frame.frame_id
        store: FanoutBatchStore | None = None
        try:
            store = self._resolve_fanout_store(ctx)
            slot_ids = [uuid_utils.uuid7().hex for _ in calls]
            reg = FanoutOpen(
                fanout_id=fanout_id,
                node_id=self.node_id,
                expected=[SlotRef(frame_id=fid, tag=call.tag, target_topic=call.target_topic) for fid, call in zip(slot_ids, calls)],
            )
            snapshot = EnvelopeSnapshot(state=ctx.state, stack=envelope.internal_workflow_state, deps=dict(ctx.deps))
            await store.open(fanout_id, reg, snapshot)
            for call, slot_id in zip(calls, slot_ids):
                wf_copy = envelope.internal_workflow_state.model_copy(deep=True)
                wf_copy.mark_fanout()  # mark the node's OWN (current top) frame, before the callee push
                wf_copy.invoke_frame(
                    call,
                    self._return_topic,
                    payload=call.body,
                    frame_id=slot_id,
                    tag=call.tag,
                    marker=call.marker,
                    caller_node_id=self.node_id,
                    caller_node_kind=self._node_kind,
                )
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
        except Exception as exc:
            # §4.4 dispatch-abort (C1): ANY failure here — a missing/dead store (``_resolve_fanout_store``
            # raises), a ``store.open`` error, or a sibling publish raise (Kafka OR not, e.g. a
            # ``PydanticSerializationError`` serializing the sibling state) — must NOT escape. The OPEN
            # dispatch is a terminal-publish path the §6.8 fault boundary must enclose (P1): an escape
            # under ACK_FIRST drops the message and strands the caller forever. Tombstone if the batch may
            # be registered (``store`` resolved ⇒ ``open`` may have written records), and escalate a fault
            # ONCE to the caller — the inbound stack (D's own fan-out frame on top) addresses it, so no
            # basestate read is needed. Any sibling already published then post_closure-stray-floors at its fold.
            logger.error("[%s] fan-out OPEN failed batch=%s node=%s; aborting + escalating: %r", correlation_id[:8], fanout_id, self.node_id, exc)
            if store is not None:
                await abort_batch(store, fanout_id)
            report = self._fanout_abort_report(FaultTypes.REASON_DISPATCH_FAILED)
            return await self._fault_response(report, self._stack_snapshot(envelope), envelope, correlation_id, broker)
        return self._no_reply_mirror(envelope)  # success path — the siblings owe the output this hop

    async def _aggregate(
        self,
        run_ctx: SessionRunContext,
        seam_ctx: SeamContext[State],
        kind: MessageKind,
        envelope: Envelope,
        ledger: HopStepLedger,
        correlation_id: str,
        broker: BrokerAnnotation,
    ) -> _BatchOpen | _BatchClosed | _BatchFaulted:
        """The durable fold/close + stage-1 stage (fault-rail §6.8 stage-2; in-node spec §4.2/§4.3).
        Runs on ``return`` AND ``fault`` deliveries.

        - A non-fan-out delivery is a **stateless continuation** (§6.7): resolve the one callee slot
          (``_resolve_callee`` — a return materializes; a fault runs ``on_callee_error``). Handled ⇒
          ``_BatchClosed`` (run the body); unhandled ⇒ ``_BatchFaulted`` (escalate, body skipped).
        - A **marked sibling reply** classifies (stray-check BEFORE the seams, decision 10), resolves
          stage-1 per sibling, and folds the outcome → ``_BatchOpen`` (park); the completing fold
          self-publishes the closure re-entry.
        - The self-published **re-entry** closes the batch: restore the snapshot, then ANY unhandled
          fault ⇒ the fault group (``_BatchFaulted``, skipping before_node/body/after_node); ALL
          resolved ⇒ materialize each via ``_resolve_slot`` and ``_BatchClosed`` (resume the body).
        """
        match self._classify_fanout(envelope):
            case None:
                # Single-call continuation: resolve the one slot uniformly (a return materializes; a
                # fault runs on_callee_error). Unhandled ⇒ escalate; handled ⇒ materialize + run body.
                reply = envelope.reply
                assert reply is not None  # kind in (return, fault) ⇒ a reply slot (stray-checked)
                outcome = await self._resolve_callee(seam_ctx, kind, reply, target_topic=None)
                # Stage-1 fold mint (step-emission spec §3.2, I4): the same slot value that drives
                # durable materialization drives the mint, gated on the echoed marker (unmarked
                # replies mint nothing). fold_failed here + the hop-exit flush give the single-call
                # arm the SAME failed closure as a parking sibling (topology symmetry, L18b).
                if isinstance(outcome, _SlotFailed):
                    if reply.marker is not None:
                        ledger.fold_failed(reply.marker)
                    return _BatchFaulted(outcome.report)  # escalate, unwrapped; the body never runs
                if reply.marker is not None:
                    ledger.folded(reply.marker, outcome.parts)
                self._resolve_slot(seam_ctx, outcome)  # materialize into the agent's private bookkeeping (I4)
                return _BatchClosed()
            case "sibling":
                return await self._fold_sibling_reply(run_ctx, seam_ctx, kind, envelope, ledger, correlation_id, broker)
            case "reentry":
                # The close/re-entry hop mints NOTHING — its outcomes already trickled on their
                # sibling fold hops (the double-emit guard is placement, not a flag; spec §3.2).
                return await self._close_fanout_batch(run_ctx, seam_ctx, envelope, correlation_id, broker)

    async def _fold_sibling_reply(
        self,
        run_ctx: SessionRunContext,
        seam_ctx: SeamContext[State],
        kind: MessageKind,
        envelope: Envelope,
        ledger: HopStepLedger,
        correlation_id: str,
        broker: BrokerAnnotation,
    ) -> _BatchOpen | _BatchFaulted:
        """Fold one marked sibling reply (in-node spec §4.2): classify (stray-check BEFORE the seams,
        decision 10 / §6.7) → stage-1 ``on_callee_error`` on a LIVE slot → record. A stray parks
        (``_BatchOpen`` — the output is owed by the still-pending siblings); a node-own abort (store
        death / a failed re-entry publish) tombstones and escalates ``_BatchFaulted`` (§4.4)."""
        store = self._resolve_fanout_store(run_ctx)
        fanout_id = envelope.internal_workflow_state.current_frame.frame_id  # == frame.fanout_id, the batch key
        reply = envelope.reply
        assert reply is not None  # guaranteed by _classify_fanout == "sibling"
        if reply.in_reply_to is None:
            logger.error("[%s] malformed sibling reply (no in_reply_to) on fan-out node=%s; not folding", correlation_id[:8], self.node_id)
            return _BatchOpen()
        match await classify_sibling(store, fanout_id, reply.in_reply_to):
            case FoldStray(reason=reason):
                logger.warning("[%s] fan-out stray (%s) slot=%s node=%s", correlation_id[:8], reason, reply.in_reply_to, self.node_id)
                return _BatchOpen()
            case FoldAbort(reason=reason):
                # The store died during classify (tombstoned best-effort): escalate ONCE to the caller.
                logger.error("[%s] fan-out classify abort (%s) batch=%s node=%s; escalating", correlation_id[:8], reason, fanout_id, self.node_id)
                return _BatchFaulted(self._fanout_abort_report(reason))
            case SiblingPending(slot_ref=slot_ref):
                # Stage 1 runs ONLY on a live pending slot (the stray check above precedes the seams). The
                # failing tool's identity now rides the echoed ``reply.marker``, read inside ``_resolve_callee``.
                outcome = await self._resolve_callee(seam_ctx, kind, reply, target_topic=slot_ref.target_topic)
                # Stage-1 fold mint (step-emission spec §3.2, I4/I5): minted HERE at the sibling fold —
                # the park-arm flush publishes it as the trickle; an unhandled sibling fault keeps the
                # same failed closure as the single-call arm (topology symmetry). Marker-gated.
                if reply.marker is not None:
                    if isinstance(outcome, _SlotFailed):
                        ledger.fold_failed(reply.marker)
                    else:
                        ledger.folded(reply.marker, outcome.parts)
                return await self._record_and_maybe_close(store, fanout_id, self._slot_to_fanout_outcome(outcome), envelope, correlation_id, broker)

    async def _record_and_maybe_close(
        self, store: FanoutBatchStore, fanout_id: str, outcome: FanoutOutcome, envelope: Envelope, correlation_id: str, broker: BrokerAnnotation
    ) -> _BatchOpen | _BatchFaulted:
        """Record the folded outcome (park); on the completing fold, self-publish the closure re-entry.
        A node-own abort (store death, or a permanently-failed re-entry publish) tombstones and
        escalates ``_BatchFaulted`` (§4.4); otherwise parks (``_BatchOpen``)."""
        match await record_outcome(store, fanout_id, outcome):
            case FoldComplete():
                try:
                    await self._publish_reentry(envelope, correlation_id, broker)
                    return _BatchOpen()  # parked — the re-entry is a fresh delivery that resumes the body
                except Exception:
                    # A permanent re-entry-publish failure (round-4 §4.4): tombstone + escalate ONCE,
                    # so a fully-folded batch never leaks a complete-but-unclosed corpse. Catch is broad
                    # (matching the C1 OPEN-dispatch posture) so a NON-Kafka raise (e.g. a serialization
                    # error on the re-entry envelope) gets the precise FANOUT_ABORTED(reentry_failed)
                    # attribution here rather than the generic calf.exception via the boundary backstop.
                    logger.error("[%s] fan-out re-entry publish failed batch=%s node=%s; escalating", correlation_id[:8], fanout_id, self.node_id)
                    await abort_batch(store, fanout_id)
                    return _BatchFaulted(self._fanout_abort_report(FaultTypes.REASON_REENTRY_FAILED))
            case FoldParked():
                return _BatchOpen()
            case FoldAbort(reason=reason):
                logger.error("[%s] fan-out record abort (%s) batch=%s node=%s; escalating", correlation_id[:8], reason, fanout_id, self.node_id)
                return _BatchFaulted(self._fanout_abort_report(reason))

    def _slot_to_fanout_outcome(self, outcome: _SlotResolved | _SlotFailed) -> FanoutOutcome:
        """Project a stage-1 slot outcome into the durable :class:`FanoutOutcome` (parts XOR fault). A
        fan-out sibling always carries ``target_topic`` (sourced from its ``SlotRef``)."""
        assert outcome.target_topic is not None  # a fan-out sibling sources target_topic from the SlotRef
        tt = outcome.target_topic
        if isinstance(outcome, _SlotResolved):
            return FanoutOutcome(slot=outcome.frame_id, tag=outcome.tag, target_topic=tt, handled=outcome.handled, parts=outcome.parts)
        return FanoutOutcome(slot=outcome.frame_id, tag=outcome.tag, target_topic=tt, handled=False, fault=outcome.report)

    async def _close_fanout_batch(
        self, run_ctx: SessionRunContext, seam_ctx: SeamContext[State], envelope: Envelope, correlation_id: str, broker: BrokerAnnotation
    ) -> _BatchOpen | _BatchClosed | _BatchFaulted:
        """Close the batch on its self-published re-entry (in-node spec §4.3). Restore the snapshot, then
        the three-way: ANY unhandled fault ⇒ the fault group (``_BatchFaulted`` — skips
        before_node/body/after_node, §7.3); ALL resolved ⇒ materialize each (``_resolve_slot``) and
        ``_BatchClosed`` (resume the body). A spurious/abandoned/aborted re-entry is a no-op park."""
        store = self._resolve_fanout_store(run_ctx)
        fanout_id = envelope.internal_workflow_state.current_frame.frame_id
        match await close_batch(store, fanout_id):
            case CloseResume(snapshot=snapshot, outcomes=outcomes):
                self._restore_from_snapshot(run_ctx, envelope, snapshot)
                # Re-sync the (shared) seam_ctx onto the RESTORED state BEFORE materializing, so the
                # outcomes land in snapshot.state (run_ctx.state), not the cleared re-entry state.
                self._resync_seam_context(seam_ctx, run_ctx, envelope)
                failed = [o for o in outcomes if o.fault is not None]
                if failed:
                    # Any unhandled fault fails the whole batch (§7.3): escalate the group, NO body.
                    return _BatchFaulted(self._build_fault_group(outcomes, failed))
                for o in outcomes:
                    resolved = _SlotResolved(frame_id=o.slot, tag=o.tag, target_topic=o.target_topic, parts=o.parts or [], handled=o.handled)
                    self._resolve_slot(seam_ctx, resolved)
                return _BatchClosed()  # resume the body on the restored, materialized context
            case CloseSpurious():
                logger.warning("[%s] fan-out spurious re-entry (incomplete) batch=%s node=%s", correlation_id[:8], fanout_id, self.node_id)
                return _BatchOpen()  # no-op park for a spurious re-entry (the batch is still open)
            case CloseAbandon():
                logger.debug("[%s] fan-out re-entry on a closed batch=%s node=%s; abandoning", correlation_id[:8], fanout_id, self.node_id)
                return _BatchOpen()  # no-op park for an already-closed batch
            case CloseAbort(reason=reason):
                # The close can't complete (store death / missing basestate, tombstoned by close_batch):
                # escalate ONCE to the caller. The re-entry's stack carries the caller (the marked
                # fan-out frame), so the handler's pre-mutation snapshot addresses it (no basestate read).
                logger.error("[%s] fan-out close abort (%s) batch=%s node=%s; escalating", correlation_id[:8], reason, fanout_id, self.node_id)
                return _BatchFaulted(self._fanout_abort_report(reason))

    def _fanout_abort_report(self, reason: str) -> ErrorReport:
        """Synthesize the fault for an ABORTED fan-out batch (in-node spec §4.4): the batch could not
        complete due to a node-own infra failure — the durable store died, its basestate was missing, or
        a sibling/re-entry publish failed — NOT a callee fault. ``details.reason`` discriminates so a
        caller can branch (``find(FaultTypes.FANOUT_ABORTED)`` → ``details[REASON]``)."""
        return ErrorReport.build_safe(
            error_type=FaultTypes.FANOUT_ABORTED,
            message=f"fan-out batch aborted ({reason})",
            origin_node_id=self.node_id,
            details={FaultTypes.REASON: reason},
        )

    def _in_batch_work(self, envelope: Envelope) -> bool:
        """True iff this delivery is processing while a fan-out batch is OPEN — the current (top) frame
        carries the ``fanout_id`` marker (the node holds ZERO in-process batch state post-PR-4, so the
        marker is the discriminator). A node-own raise here must ABORT (tombstone + escalate once), NOT
        ``_publish_fault``/recover — the still-pending siblings owe the output, so faulting the caller
        directly would double-reply (spec §6.8 / R4)."""
        frame = envelope.internal_workflow_state.current_frame_or_none
        return frame is not None and frame.fanout_id is not None

    async def _publish_abort(
        self, report: ErrorReport, snapshot: WorkflowState, inbound: Envelope, ctx: SessionRunContext, correlation_id: str, broker: BrokerAnnotation
    ) -> Response:
        """Abort the open batch a node-own fault interrupted: tombstone both records, then escalate the
        fault ONCE to the caller (in-node spec §4.4). The caller rides the inbound delivery's stack — the
        marked fan-out frame is on top of ``snapshot``, so ``_fault_response`` pops it and addresses the
        caller with NO basestate read (the abort fires on store/publish trouble — the worst moment to
        read the store)."""
        frame = snapshot.current_frame_or_none
        if frame is not None and frame.fanout_id is not None:
            # Guard the store resolution: _publish_abort runs from INSIDE the boundary's except handler,
            # so a raise here (e.g. the store resource absent) is an exception-in-except that would escape
            # to FastStream (C1). Best-effort tombstone; escalate the fault to the caller regardless.
            try:
                store = self._resolve_fanout_store(ctx)
            except Exception:
                logger.exception(
                    "[%s] fan-out abort could not resolve the store batch=%s node=%s; escalating without tombstone",
                    correlation_id[:8],
                    frame.fanout_id,
                    self.node_id,
                )
            else:
                await abort_batch(store, frame.fanout_id)  # frame_id == fanout_id == batch key
        return await self._fault_response(report, snapshot, inbound, correlation_id, broker)

    def _build_fault_group(self, outcomes: list[FanoutOutcome], failed: list[FanoutOutcome]) -> ErrorReport:
        """Build the closing batch's fault from its unhandled-fault slots (§4.4/§7.3). Carries the
        per-slot topology in ``details`` (lean — never the success VALUES, §4.3). A SINGLE unhandled
        fault FLATTENS to the bare child fault (identity preserved, §4.4), the topology copied onto its
        ``details``; 2+ compose a ``calf.fault_group`` carrying them in ``causes``."""
        causes = [o.fault for o in failed if o.fault is not None]
        topology: dict[str, Any] = {
            "slots": [{"tag": o.tag, "target_topic": o.target_topic, "status": "failed" if o.fault is not None else "ok"} for o in outcomes],
            "ok": sum(1 for o in outcomes if o.fault is None),
            "failed": len(causes),
        }
        if len(causes) == 1:
            child = causes[0]
            return child.model_copy(update={"details": {**child.details, FaultTypes.FANOUT_TOPOLOGY: topology}})
        group = ErrorReport.build_safe(
            error_type=FaultTypes.FAULT_GROUP,
            message=f"fan-out batch closed with {len(causes)} unhandled fault(s)",
            origin_node_id=self.node_id,
            causes=causes,
        )
        group.details[FaultTypes.FANOUT_TOPOLOGY] = topology  # framework-reserved calf.* key (set after build_safe)
        return group

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
        ctx.state = snapshot.state
        ctx.deps = snapshot.deps
        ctx._frame_id = frame.frame_id if frame is not None else None
        ctx._resources = self._effective_resources()
        ctx._reply = None
        envelope.internal_workflow_state = snapshot.stack
        envelope.context = SessionRunContext(state=snapshot.state, deps=snapshot.deps)

    async def _execute(
        self,
        run_ctx: SessionRunContext,
        seam_ctx: SeamContext[State],
        kind: MessageKind,
        envelope: Envelope,
        route: str | None,
        payload: Any,
        ledger: HopStepLedger,
        *,
        awaiting_reply: bool,
        correlation_id: str,
        broker: BrokerAnnotation,
    ) -> NodeResult[State] | _PipelineSentinel | _BatchFaulted | _Declined:
        """The staged inner pipeline (fault-rail §6.8 ``_execute``).

        Stage 2 (:meth:`_aggregate`) on ``return`` deliveries: an open batch parks
        (``_CONSUMED``); a closed batch / stateless continuation falls through (re-syncing
        ``seam_ctx`` from the possibly-restored ``run_ctx``). Stage 3 ``before_node`` may
        short-circuit the body with an output. Stage 4 is the body (:meth:`_dispatch_routed`);
        an all-declined / schema-rejected body is a ``_Declined(reason)``. Stage 6 ``after_node`` (:meth:`_apply_after`)
        wraps the produced output. The body runs on ``run_ctx`` (``SessionRunContext``); the
        seams run on ``seam_ctx`` (``SeamContext``, sharing ``run_ctx.state``).

        Stages 1+2 (``return``/``fault`` kinds, :meth:`_aggregate`): the uniform slot resolution +
        fold/close. An open batch parks (``_CONSUMED``); an unhandled fault (single-call escalation or
        the closing batch's fault group) returns ``_BatchFaulted`` — RETURNED, never raised (R5), so it
        never trips the node's own ``on_node_error``; a resolved continuation / completed close falls
        through to the body. The body never runs for an unhandled fault.
        """
        if kind in ("return", "fault"):  # ── stages 1+2: on_callee_error + fold/close (_aggregate) ──
            match await self._aggregate(run_ctx, seam_ctx, kind, envelope, ledger, correlation_id, broker):
                case _BatchOpen():
                    return _CONSUMED
                case _BatchFaulted() as faulted:
                    return faulted  # escalate (single-call unhandled fault, or the closing fault group)
                case _BatchClosed():
                    # A fan-out close restored run_ctx + the stack; re-sync the shared seam_ctx so
                    # before_node (fires at closure, §6.4) and after_node see the resumed state.
                    self._resync_seam_context(seam_ctx, run_ctx, envelope)
        # ── stage 3: before_node ── (+ its short-circuit after_node). A non-NodeFaultError raise from a
        # BOUNDARY seam is wrapped _SeamAccidentError so the stage-5 arm chains the handled inbound fault
        # (§6.5); a NodeFaultError propagates verbatim (the mint rule). The body (stage 4) is NOT
        # wrapped — a body raise is §6.7 calf.exception with nothing chained.
        try:
            pre = await run_chain(self._chains[BEFORE_NODE], seam_ctx, seam_name=BEFORE_NODE)
            if pre is not None:
                return await self._apply_after(seam_ctx, self._interpret(seam_ctx, pre))
        except NodeFaultError:
            raise
        except Exception as exc:
            raise _SeamAccidentError(exc) from exc
        result = await self._dispatch_routed(run_ctx, route, payload, awaiting_reply=awaiting_reply, correlation_id=correlation_id)  # stage 4: body
        if isinstance(result, Observed):
            # The Observed unwrap (step-emission spec §3.1b): immediately after the body returns and
            # BEFORE after_node — seams run on the plain NodeResult and never see facts. A raising
            # body never surfaces its facts (I6's completed-occurrence rule holds by position).
            ledger.absorb(result.facts)
            result = result.action
        if isinstance(result, _Declined):
            return result  # propagate the decline + its reason to the §10 auto-fault disposition
        try:
            return await self._apply_after(seam_ctx, result)  # ── stage 6: after_node ──
        except NodeFaultError:
            raise
        except Exception as exc:
            raise _SeamAccidentError(exc) from exc

    async def _dispatch_routed(
        self,
        ctx: SessionRunContext,
        route: str | None,
        payload: Any,
        *,
        awaiting_reply: bool,
        correlation_id: str,
    ) -> NodeResult[State] | Observed[State] | _Declined:
        """Dispatch ``route`` to matched handlers as a Chain of Responsibility.

        Runs matched handlers most-specific → most-general. A handler that returns
        :class:`~calfkit.models.Next` (or ``None`` — e.g. a missing return) declines
        and the chain advances; the first other result is terminal and short-circuits.
        A handler with a ``schema`` whose body fails validation is skipped (logged,
        callback-aware level). ``run()`` is the inherited ``'*'`` handler dispatched
        last; the base ``run()`` declines (returns ``Next``), so a node with no real
        match returns a :class:`_Declined` carrying WHY it declined (§10): ``reason`` is
        ``schema_rejected`` if any matched handler's schema rejected the body, else
        ``all_declined`` — the discriminator the §10 auto-fault writer needs. ``route``
        is ``None`` for a header-less message — only the ``'*'`` handler matches it.
        """
        cls = type(self)
        schema_rejected = False
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
                    # Record WHY for the §10 auto-fault: a reply-owing terminal where a schema
                    # rejected the body faults the caller ``calf.delivery.rejected``
                    # (reason=schema_rejected) — #201 closed by construction (the caller no longer
                    # relies on its reply-TTL). Skipping to the next handler stays correct for an
                    # optional specific route a later handler may still match.
                    schema_rejected = True
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
                result: NodeResult[State] | Observed[State] | Next | None = await method(ctx, validated)
            else:
                result = await method(ctx)
            if result is None or isinstance(result, Next):
                # Decline (Next, or a handler that simply returned nothing) → advance.
                continue
            return result
        return _Declined(FaultTypes.REASON_SCHEMA_REJECTED if schema_rejected else FaultTypes.REASON_ALL_DECLINED)

    def _decode_emitter(self, headers: dict[str, Any], correlation_id: str) -> tuple[str | None, str | None]:
        """Decode the inbound emitter id/kind headers — the per-delivery prelude shared by the
        caller-capable and observer paths. The emitter is diagnostic (logging + ``ctx`` stamping),
        never load-bearing, so a missing/garbled id (a non-calfkit producer) WARNs but never fails."""
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
        return emitter, decode_header_str(headers.get(HDR_EMITTER_KIND))

    async def handler(
        self,
        envelope: Envelope,
        correlation_id: Annotated[str, Context()],
        headers: Annotated[dict[str, Any], Context("message.headers")],
        broker: BrokerAnnotation,
    ) -> Response:
        """The FastStream per-delivery entrypoint. Single-sources the dependency-injection
        annotations (``correlation_id``/``headers``/``broker``) and forwards to the polymorphic
        :meth:`_handle_delivery` — caller-capable nodes run the §6.8 fault pipeline; observers
        (:class:`~calfkit.nodes.consumer.ConsumerNode`) run the observe-only consume path (§6.6).
        Kept thin so a node family varies its delivery handling by overriding ``_handle_delivery``,
        never by re-declaring the FastStream contract."""
        return await self._handle_delivery(envelope, correlation_id, headers, broker)

    async def _handle_delivery(self, envelope: Envelope, correlation_id: str, headers: dict[str, Any], broker: BrokerAnnotation) -> Response:
        """The caller-capable per-delivery pipeline (fault-rail §6.8).

        Emitter decode → stage-0 guard (:meth:`_classify` → :meth:`prepare_context` →
        :meth:`_build_seam_context` → :meth:`_stray_check`) → the fault boundary around
        :meth:`_execute` (the seam stages + the body) → output disposition + the publish guard.
        A node-own raise becomes a typed fault on the success rail (P1 — no silent drop); a
        ``NodeFaultError`` is the mint gesture (bypasses ``on_node_error``, §6.5); a failed terminal
        publish faults the caller (scenario 42). Returns a broadcast-mirror :class:`Response` whose
        ``x-calf-kind`` is the hop's kind. Observers (``ConsumerNode``) override this with the
        observe-only path — they never enter the fault pipeline (§6.6).
        """
        emitter, emitter_kind = self._decode_emitter(headers, correlation_id)
        # ── stage-0 guard: no user code may run before a context exists (§6.8 / R1) ──
        # A raise in classify/context-build is handled BELOW the seams (no ctx yet): a
        # call-kind ingress faults the caller where the stack is readable, a return/fault
        # delivery floors only (junk must not fault a live invocation, §4.1). Never escapes.
        try:
            kind = self._classify(headers)
            if kind is None:  # unrecognized x-calf-kind → ERROR-log + ignore (§4.1 rule 2)
                return self._floor_unknown_kind(envelope, headers, correlation_id)
            logger.debug("[%s] handler entered node=%s emitter=%s kind=%s", correlation_id[:8], self.node_id, emitter, kind)
            ctx = await self.prepare_context(envelope, emitter_node_id=emitter, emitter_node_kind=emitter_kind, correlation_id=correlation_id)
            seam_ctx = self._build_seam_context(ctx, envelope, headers, kind)
            stray = self._stray_check(kind, envelope)  # kind ↔ slot agreement, BEFORE the seams (§6.7)
            if stray is not None:
                return self._floor_stray(stray, envelope, correlation_id)
        except Exception as exc:
            return await self._floor_stage0(exc, envelope, headers, correlation_id, broker)

        frame = envelope.internal_workflow_state.current_frame_or_none
        payload = frame.payload if frame is not None else None
        awaiting_reply = frame.callback_topic is not None if frame is not None else False
        route = decode_header_str(headers.get(HDR_ROUTE))
        # The pre-mutation caller address every _publish_fault below addresses (§6.8 / scenario 42).
        snapshot = self._stack_snapshot(envelope)
        # The hop's step ledger (step-emission spec §3.1c): a per-delivery local, threaded through
        # _execute → _aggregate → _fold_sibling_reply, flushed once at EVERY exit below (I6) — it
        # dies with the hop (I2). Floors above fire before it exists (stage 0 is pre-ledger).
        ledger = HopStepLedger()
        # §6.5: a fault-kind delivery's inbound fault — chained in causes ONLY if a before_node/after_node
        # accident later routes to on_node_error (captured pre-mutation; it is the fault on_callee_error
        # handled, since a `calf.exception` fault escalates before the seams ever run).
        inbound_fault = envelope.reply.error if isinstance(envelope.reply, FaultMessage) else None
        try:
            output = await self._execute(
                ctx,
                seam_ctx,
                kind,
                envelope,
                route,
                payload,
                ledger,
                awaiting_reply=awaiting_reply,
                correlation_id=correlation_id,
                broker=broker,
            )
        except NodeFaultError as nfe:
            # The mint rule (§6.5): a deliberate typed fault converts verbatim, BYPASSING on_node_error.
            await self._flush_steps(ledger, snapshot, correlation_id, broker, disposition=None)
            return await self._fault_response(nfe.report, snapshot, envelope, correlation_id, broker)
        except Exception as caught:
            # ── stage 5: on_node_error — the node's own work raised uncaught ──
            # §6.5: a before_node/after_node accident (wrapped _SeamAccidentError) chains the handled inbound
            # fault, if any; a BODY raise (§6.7) chains nothing. Unwrap to the true exception for synthesis.
            if isinstance(caught, _SeamAccidentError):
                node_exc, inbound_cause = caught.original, inbound_fault
            else:
                node_exc, inbound_cause = caught, None
            if self._in_batch_work(envelope):
                # Mid-batch (a marked sibling delivery): recovery is FORBIDDEN — the still-pending
                # siblings owe the output, so on_node_error → _publish_fault would double-reply. Abort
                # (tombstone + escalate ONCE) instead (spec §6.8 / R4 / R5). _in_batch_work is False once
                # the close has restored the unmarked frame, so a body raise at close recovers normally.
                report = self._fault_from_exception(node_exc, seam_ctx, snapshot, correlation_id)
                await self._flush_steps(ledger, snapshot, correlation_id, broker, disposition=None)
                return await self._publish_abort(report, snapshot, envelope, ctx, correlation_id, broker)
            seam_ctx.exception = node_exc
            report = self._fault_from_exception(node_exc, seam_ctx, snapshot, correlation_id, cause=inbound_cause)
            recovery = await run_chain_guarded(self._chains[ON_NODE_ERROR], seam_ctx, report)
            if isinstance(recovery, _Minted):  # a NodeFaultError raised inside the chain (§6.5)
                await self._flush_steps(ledger, snapshot, correlation_id, broker, disposition=None)
                return await self._fault_response(recovery.report, snapshot, envelope, correlation_id, broker)
            if recovery is None:  # all handlers declined → the original fault escalates
                await self._flush_steps(ledger, snapshot, correlation_id, broker, disposition=None)
                return await self._fault_response(report, snapshot, envelope, correlation_id, broker)
            # Recovered: on_node_error is done, so clear ctx.exception (set during on_node_error ONLY,
            # §6.3) before the recovery value passes after_node. SINGLE-SHOT — a raise while processing
            # the recovery is terminal, chaining the original as a cause (§6.8).
            seam_ctx.exception = None
            try:
                output = await self._apply_after(seam_ctx, self._interpret(seam_ctx, recovery))
            except NodeFaultError as nfe:
                # §6.5 mint rule is ABSOLUTE ("anywhere — any seam, any body"): a deliberate fault from the
                # recovery-path after_node converts VERBATIM, NOT downgraded. The §6.8 single-shot sketch's
                # bare except predates the rule; a mint is the node's typed decision, not the accident that
                # arm chains-the-original for.
                await self._flush_steps(ledger, snapshot, correlation_id, broker, disposition=None)
                return await self._fault_response(nfe.report, snapshot, envelope, correlation_id, broker)
            except Exception as exc2:
                report2 = self._fault_from_exception(exc2, seam_ctx, snapshot, correlation_id, cause=report)
                await self._flush_steps(ledger, snapshot, correlation_id, broker, disposition=None)
                return await self._fault_response(report2, snapshot, envelope, correlation_id, broker)

        if output is _CONSUMED:
            # A parked fan-out fold or a self-published re-entry: no publishable action this hop,
            # the output is still owed by the pending siblings. The park-arm flush IS the trickle
            # (I5): a parked sibling fold's result step publishes as that reply folds.
            await self._flush_steps(ledger, snapshot, correlation_id, broker, disposition=None)
            return self._no_reply_mirror(envelope)
        if isinstance(output, _Declined):
            # No matched handler produced a terminal result: every match declined, or a schema
            # rejected the body (``output.reason`` discriminates). On a REPLY-OWING delivery this
            # auto-faults the caller (``calf.delivery.rejected`` + ``details.reason``, §10) — #201
            # closed by construction, the caller never hangs. A fire-and-forget no-output stays a
            # no-op (DEBUG; the stream-filter case, no reply owed). ``body_note`` surfaces a
            # present-but-unconsumed payload (e.g. a malformed tool ToolCallRef) — observable, not silent.
            body_note = " — a body was not consumed (rejected by a schema handler, or unmatched)" if payload is not None else ""
            if awaiting_reply:
                logger.warning(
                    "[%s] no handler produced a result for route=%s on node=%s (reason=%s); auto-faulting %s; registered=%s%s",
                    correlation_id[:8],
                    route,
                    self.node_id,
                    output.reason,
                    FaultTypes.DELIVERY_REJECTED,
                    tuple(type(self)._handlers),
                    body_note,
                )
                report = ErrorReport.build_safe(
                    error_type=FaultTypes.DELIVERY_REJECTED,
                    message=f"node {self.node_id!r} produced no result for this delivery ({output.reason})",
                    origin_node_id=self.node_id,
                    details={FaultTypes.REASON: output.reason},
                )
                await self._flush_steps(ledger, snapshot, correlation_id, broker, disposition=None)
                return await self._fault_response(report, snapshot, envelope, correlation_id, broker)
            logger.debug(
                "[%s] no handler produced a result for route=%s on node=%s (reason=%s); fire-and-forget no-op; registered=%s%s",
                correlation_id[:8],
                route,
                self.node_id,
                output.reason,
                tuple(type(self)._handlers),
                body_note,
            )
            # The fire-and-forget declined arm still flushes (near-always an empty no-op; a frameless
            # inbound has an EMPTY stack — the helper's guarded root read covers it).
            await self._flush_steps(ledger, snapshot, correlation_id, broker, disposition=None)
            return self._no_reply_mirror(envelope)  # no result to publish: don't re-broadcast the inbound reply

        if isinstance(output, _BatchFaulted):
            # A callee fault escalating up this node's rail (§6.8): RETURNED from _execute, never
            # raised, so it never tripped this node's own on_node_error. Re-address it to the caller
            # on the pre-mutation snapshot — the same report, unwrapped (§4.4). The flush FIRST: a
            # single-call fold_failed mint (or a resolved fold whose close then faulted) publishes
            # before the fault — the fold and the escalation are two phases of one hop (L18b/L18i).
            await self._flush_steps(ledger, snapshot, correlation_id, broker, disposition=None)
            return await self._fault_response(output.report, snapshot, envelope, correlation_id, broker)

        # ── Step emission — the disposition chokepoint (step-emission spec §3.2/§3.4). ──
        # `output` is now a publishable happy-path action: mint the call half for every outgoing
        # marked Call (the pair law's dispatch side, I4), then flush the hop's whole ledger BEFORE
        # the action branches to fan-out OPEN or _publish_action below — minting at the chokepoint
        # (not inside the publish paths) is what puts the call steps into the very flush that
        # precedes the action. The helper owns the best-effort guard (I1) and the terminal gate
        # reads the disposition (a ReturnCall drops agent_message events, any depth).
        ledger.note_dispatch(output)
        await self._flush_steps(ledger, snapshot, correlation_id, broker, disposition=output)

        # Fan-out OPEN: register a durable batch + publish the marked siblings when the caller's state
        # must survive the call independently of the round-trip (decision 1(b) / L13) — a true fan-out
        # (N >= 2 Calls) OR any `isolate_state` Call (a lone `message_agent` whose peer authors its own
        # history). A bare Call is normalized to a one-element list first so a flagged bare Call cannot
        # slip through to _publish_action's bare-Call fast path (which would skip the snapshot, C1). An
        # *unflagged* single Call / 1-element list stays a stateless continuation and reroutes to the
        # plain publish in _publish_action below. Gated on `_needs_durable_batch` (the retained 1(b)
        # decoupling — see its docstring), so a lone `message_agent` opens its degenerate batch here.
        fanout_calls = [output] if isinstance(output, Call) else output
        if (
            self._needs_durable_batch
            and isinstance(fanout_calls, list)
            and all(isinstance(c, Call) for c in fanout_calls)
            and (len(fanout_calls) >= 2 or any(c.isolate_state for c in fanout_calls))
        ):
            # The OPEN dispatch is a terminal-publish path; guard it like the publish guard below so a
            # raise that _handle_fanout_open's own abort somehow missed still faults the caller on the
            # pre-mutation snapshot, never escapes to FastStream (C1 / P1 — defense-in-depth).
            try:
                return await self._handle_fanout_open(ctx, fanout_calls, envelope, correlation_id, broker)
            except Exception as exc:
                report = self._fault_from_exception(exc, seam_ctx, snapshot, correlation_id)
                # The chokepoint flush above already drained the ledger, so this is near-always an
                # empty no-op (spec §3.4's accepted over-report edge; _handle_fanout_open's own
                # internal abort arm is deliberately NOT threaded — same drained-ledger argument).
                await self._flush_steps(ledger, snapshot, correlation_id, broker, disposition=None)
                return await self._fault_response(report, snapshot, envelope, correlation_id, broker)

        logger.debug("[%s] node=%s produced action=%s", correlation_id[:8], self.node_id, type(output).__name__)
        # ── publish guard: a transport/size failure on the success rail NEVER re-enters
        # on_node_error; it faults the caller directly on the pre-mutation snapshot (§6.8 / scenario 42).
        # No _in_batch_work arm here (unlike stage-5): _publish_action is only ever reached with an
        # UNMARKED frame — a marked sibling delivery parks (_CONSUMED) and the close restores the unmarked
        # frame before the body runs — so a publish failure here is never mid-batch. A fan-out OPEN's
        # sibling-publish failure is owned by _handle_fanout_open's own abort, not this guard.
        try:
            body, pubkind = await self._publish_action(output, envelope, correlation_id, broker)
        except Exception as exc:
            report = self._fault_from_exception(exc, seam_ctx, snapshot, correlation_id)
            # Drained by the chokepoint flush above → an empty no-op; exactly one StepMessage still
            # published for this hop (I6 holds by drain — spec §3.4's accepted over-report edge).
            await self._flush_steps(ledger, snapshot, correlation_id, broker, disposition=None)
            return await self._fault_response(report, snapshot, envelope, correlation_id, broker)
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

    @property
    def _private_input_topic(self) -> str:
        """Framework-private, name-scoped INBOUND inbox for this node instance (ADR-0017).

        The inbound parallel to :meth:`_return_topic` (the continuation inbox): a
        deterministic ``{node_kind}.{name}.private.input`` any caller can derive from
        this node's name alone — the addressing primitive agent-to-agent messaging
        builds on (the ``AgentCard`` carries no topic; the caller derives
        ``agent.{name}.private.input``). It is contributed at registration like
        ``_return_topic`` — read by :meth:`Worker.register_handlers` and the startup
        provisioner and flagged **framework-owned** (never receives user
        ``topic_configs``) — *not* appended into ``subscribe_topics`` in ``__init__``
        (the ``@dataclass`` tool-node ``__init__`` bypasses ``BaseNodeDef.__init__``, so a
        list-append there would be silently missed; a property sidesteps init-time
        divergence across every node kind, regardless of which ``__init__`` ran).

        **Universal, dormant for non-agents in v1.** Every node kind exposes and
        subscribes to its inbox, but only agents are dispatched to today — for a tool
        or consumer the inbox is provisioned and consumed yet receives no traffic. A
        stray non-``ToolCallRef`` body delivered here is **not** silently swallowed: a
        caller-capable node classifies it ``"call"`` (no ``x-calf-kind`` header) and
        enters the body, where it declines / schema-rejects and is disposed (a DEBUG
        no-op, or a WARNING auto-fault if the delivery is reply-owing); a consumer
        observes it (ERROR-floored on failure). Harmless in v1 only because no producer
        targets it — the dormancy contract is "no producer," not "a safe sink".

        Recomputed from identity on every access (like ``_return_topic``); do not mutate
        ``node_id`` after registration. Uses ``self.name``, which aliases ``node_id``
        today; the divergence from ``_return_topic``'s ``self.node_id`` is intentional
        name-centric addressing (spec §4.1), not an inconsistency to "fix".
        """
        return f"{self._node_kind}.{self.name}.private.input"
