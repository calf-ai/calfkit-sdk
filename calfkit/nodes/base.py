import inspect
import logging
from abc import abstractmethod
from collections.abc import Awaitable, Callable
from typing import Annotated, Any, ClassVar

from faststream import Context, Response
from faststream.kafka.annotations import (
    KafkaBroker as BrokerAnnotation,
)

from calfkit._protocol import HDR_EMITTER, HDR_EMITTER_KIND, NodeKind, decode_header_str
from calfkit.models import (
    Call,
    NodeResult,
    ReturnCall,
    Silent,
    State,
    TailCall,
)
from calfkit.models.envelope import Envelope
from calfkit.models.node_schema import BaseNodeSchema
from calfkit.models.session_context import SessionRunContext

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


class BaseNodeDef(BaseNodeSchema):
    _run_accepts_input: bool
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
            ctx: Session context containing mutable state and immutable dependencies.

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
    ) -> SessionRunContext:
        ctx = envelope.context.model_copy(deep=True)
        if envelope.internal_workflow_state.current_frame.overrides:
            ctx.state.overrides = envelope.internal_workflow_state.current_frame.overrides
        ctx._emitter_node_id = emitter_node_id
        ctx._emitter_node_kind = emitter_node_kind
        return ctx

    def _emitter_headers(self) -> dict[str, str]:
        return {HDR_EMITTER: self.node_id, HDR_EMITTER_KIND: self._node_kind}

    async def _publish_action(self, output: NodeResult[State], envelope: Envelope, correlation_id: str, broker: BrokerAnnotation) -> Envelope:
        publish_envelope: Envelope

        if isinstance(output, list) and all(isinstance(item, Call) for item in output):
            # Parallel fan-out: publish each Call with independent workflow_state
            for call in output:
                wf_copy = envelope.internal_workflow_state.model_copy(deep=True)
                wf_copy.invoke_frame(call, self._return_topic)
                publish_envelope = Envelope(
                    context=SessionRunContext(state=call.state, deps=envelope.context.deps),
                    internal_workflow_state=wf_copy,
                )
                await broker.publish(
                    publish_envelope,
                    topic=wf_copy.current_frame.target_topic,
                    correlation_id=correlation_id,
                    key=correlation_id.encode(),
                    headers=self._emitter_headers(),
                )
            return envelope

        elif isinstance(output, Call):
            # push to callstack and call the target topic
            envelope.internal_workflow_state.invoke_frame(output, self._return_topic)
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
                headers=self._emitter_headers(),
            )
        elif isinstance(output, ReturnCall):
            # unwind current frame and return to previous topic
            frame = envelope.internal_workflow_state.unwind_frame()
            publish_envelope = Envelope(
                context=SessionRunContext(state=output.state, deps=envelope.context.deps),
                internal_workflow_state=envelope.internal_workflow_state,
            )
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
        ctx = await self.prepare_context(envelope, emitter_node_id=emitter, emitter_node_kind=emitter_kind)

        if not await self._evaluate_gates(ctx, correlation_id):
            body: Envelope = envelope
        else:
            if self._run_accepts_input and envelope.internal_workflow_state.current_frame.input_args is not None:
                output = await self.run(ctx, *envelope.internal_workflow_state.current_frame.input_args)
            else:
                output = await self.run(ctx)
            logger.debug("[%s] run() returned action=%s node=%s", correlation_id[:8], type(output).__name__, self.node_id)
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
