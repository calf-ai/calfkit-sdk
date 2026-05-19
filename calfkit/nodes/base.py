import inspect
import logging
from abc import abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Annotated, Any, ClassVar

from faststream import Context, Response
from faststream.kafka.annotations import (
    KafkaBroker as BrokerAnnotation,
)

from calfkit._protocol import (
    HDR_EMITTER,
    HDR_EMITTER_KIND,
    HDR_FANOUT_ID,
    HDR_FRAME_ID,
    NodeKind,
    decode_header_str,
)
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
# Kafka subscription registration spec
# ---------------------------------------------------------------------------


@dataclass
class _KafkaSubscription:
    """A Kafka subscriber registration spec returned by ``BaseNodeDef.kafka_subscriptions()``.

    The :class:`~calfkit.worker.worker.Worker` iterates the list returned by
    ``kafka_subscriptions()`` and registers each spec as an independent FastStream
    ``@broker.subscriber``. The default ``BaseNodeDef`` returns a single spec that
    mirrors today's wiring (handler on ``subscribe_topics`` with optional
    ``publish_topic`` wrapper). Subclasses that need additional subscribers — e.g.,
    agents that own a fan-out aggregator's returns subscriber — override
    ``kafka_subscriptions()`` to return multiple specs.

    Attributes carrying ``None`` (``group_id``, ``max_workers``) inherit the
    Worker-level default at registration time. ``listener`` and ``ack_policy``
    forward to the FastStream subscriber factory only when set.
    """

    topics: list[str]
    """Topics this subscriber consumes from."""

    handler: Callable[..., Any]
    """Async callable that processes each message. Same shape as
    :meth:`BaseNodeDef.handler`; FastStream supplies the parameters via its
    ``Context()`` and broker annotations."""

    group_id: str | None = None
    """Consumer group id. ``None`` means inherit Worker default (Worker uses its
    own ``group_id`` or falls back to ``node.name``)."""

    listener: Any | None = None
    """Optional ``aiokafka.ConsumerRebalanceListener`` for partition-assignment
    callbacks. Forwarded to ``broker.subscriber(listener=...)`` when set."""

    max_workers: int | None = None
    """Per-subscription override of ``Worker.max_workers``. ``None`` means inherit
    the Worker default."""

    ack_policy: Any | None = None
    """Optional ``faststream.middlewares.AckPolicy``. ``None`` lets FastStream
    pick its own default."""

    publish_topic: str | None = None
    """If set, the handler's return value is published to this topic by wrapping
    it with FastStream's ``@broker.publisher``. Only the node's main subscription
    typically uses this; auxiliary subscriptions (e.g., the aggregator's returns
    subscriber) leave it ``None``."""

    extra_kwargs: dict[str, Any] = field(default_factory=dict)
    """Additional kwargs forwarded to ``broker.subscriber()``. Per-subscription
    values override Worker-level ``extra_subscribe_kwargs`` on conflict."""


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
            publish_topic: Optional default topic to publish results to.
            gates: Optional list of predicates evaluated in ``handler()`` before
                ``run()``. Stack with AND semantics in registration order;
                short-circuits on the first ``False``, exception, or non-bool.
                Returning anything other than ``True`` rejects the message:
                ``run()`` is skipped and the envelope is returned unchanged.
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

    def _publish_headers(
        self,
        inbound_headers: dict[str, Any] | None = None,
        *,
        forward_fanout: bool = False,
    ) -> dict[str, str]:
        """Build outbound publish headers.

        Always stamps :data:`HDR_EMITTER` and :data:`HDR_EMITTER_KIND`.
        When ``forward_fanout=True``, also forwards :data:`HDR_FANOUT_ID`
        and :data:`HDR_FRAME_ID` from ``inbound_headers`` (if present) —
        used by the ``ReturnCall`` publish path so a tool's return carries
        the same fan-out identity as the inbound ``Call`` from the agent.
        """
        headers = self._emitter_headers()
        if forward_fanout and inbound_headers is not None:
            for fwd in (HDR_FANOUT_ID, HDR_FRAME_ID):
                value = decode_header_str(inbound_headers.get(fwd))
                if value is not None:
                    headers[fwd] = value
        return headers

    def kafka_subscriptions(self) -> list[_KafkaSubscription]:
        """Return the list of Kafka subscriber registrations this node needs.

        Default: one subscription mirroring today's behavior — ``self.handler`` on
        ``self.subscribe_topics`` with the optional ``self.publish_topic`` wrapper.
        Subclasses that need additional subscribers (e.g., agents owning a fan-out
        aggregator's returns subscriber) override this to return multiple specs.

        The Worker iterates the returned list and registers each spec as an
        independent FastStream ``@broker.subscriber``.
        """
        return [
            _KafkaSubscription(
                topics=list(self.subscribe_topics),
                handler=self.handler,
                publish_topic=self.publish_topic,
            )
        ]

    async def _publish_action(
        self,
        output: NodeResult[State],
        envelope: Envelope,
        correlation_id: str,
        broker: BrokerAnnotation,
        inbound_headers: dict[str, Any] | None = None,
    ) -> Envelope:
        """Publish the framework-level result of a ``run()`` invocation.

        Subclasses (e.g. :class:`BaseAgentNodeDef`) override this to inject
        aggregator semantics on the ``list[Call]`` parallel-fan-out branch
        before delegating back here for everything else.

        ``inbound_headers``: the headers of the message that triggered this
        invocation. Used to forward :data:`HDR_FANOUT_ID` /
        :data:`HDR_FRAME_ID` from the inbound to ``ReturnCall`` publishes,
        so tools returning to an agent's fan-out carry the right batch
        identity for the aggregator. Forward only on ``ReturnCall``; other
        publish kinds either generate new identity (parallel fan-out) or
        leave the headers untouched.
        """
        publish_envelope: Envelope

        if isinstance(output, list) and all(isinstance(item, Call) for item in output):
            # Parallel fan-out: publish each Call with independent workflow_state.
            # Subclasses that need aggregator semantics override _publish_action
            # to intercept this branch before delegating back.
            for call in output:
                wf_copy = envelope.internal_workflow_state.model_copy(deep=True)
                wf_copy.invoke_frame(call, self.subscribe_topics[0])
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
            envelope.internal_workflow_state.invoke_frame(output, self.subscribe_topics[0])
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
                # Forward HDR_FANOUT_ID/HDR_FRAME_ID so the aggregator can
                # identify which fan-out batch a tool's return belongs to.
                headers=self._publish_headers(inbound_headers, forward_fanout=True),
            )

        elif isinstance(output, TailCall):
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
            body = await self._publish_action(output, envelope, correlation_id, broker, inbound_headers=headers)

        return Response(body, headers=self._emitter_headers())

    @property
    def id(self) -> str:
        return self.node_id

    @property
    def name(self) -> str:
        return self.node_id

    @property
    def _return_topic(self) -> str:
        return f"{self.node_id}.private.return"
