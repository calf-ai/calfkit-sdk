from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from types import MappingProxyType
from typing import Any, Generic

import uuid_utils
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

from calfkit._types import DepsT, StackItemT, StateT
from calfkit.models.actions import _Call
from calfkit.models.payload import ContentPart
from calfkit.models.reply import ReturnMessage
from calfkit.models.state import OverridesState, State

_EMPTY_RESOURCES: Mapping[str, Any] = MappingProxyType({})


@dataclass
class Stack(Generic[StackItemT]):
    _internal_list: list[StackItemT] = field(default_factory=list)

    def push(self, item: StackItemT) -> None:
        self._internal_list.append(item)

    def pop(self) -> StackItemT:
        try:
            return self._internal_list.pop()
        except Exception as e:
            raise Exception("An exception occurred when popping from execution stack") from e

    def peek(self) -> StackItemT:
        try:
            return self._internal_list[-1]
        except Exception as e:
            raise Exception("An exception occurred when peeking from execution stack") from e

    def is_empty(self) -> bool:
        return not self._internal_list


@dataclass(frozen=True)
class CallFrame:
    target_topic: str
    callback_topic: str | None  # return address; ``None`` = fire-and-forget, no requester to return to
    frame_id: str = field(default_factory=lambda: uuid_utils.uuid7().hex)
    overrides: OverridesState | None = field(default=None)
    payload: Any = field(default=None)
    """Optional producer-supplied body validated against a handler's ``schema``.
    Read by a routed ``@handler`` (via the ``x-calf-route`` header) or, for a
    routeless body, by the target's inherited ``@handler('*')`` ``run`` (e.g. a tool
    node validating a ``ToolCallRef``). ``None`` when the producer sent no body."""
    tag: str | None = field(default=None)
    """Caller-set opaque correlation token, echoed verbatim on the reply
    (``ReturnMessage.tag``) when this frame unwinds. The agent sets it to ``tool_call_id`` on
    fan-out tool ``Call``s so a sibling reply is self-describing — the durable fold reads that
    sibling's result from ``state.tool_results[reply.tag]``. Transport metadata, never content.
    ``None`` on frames whose producer set no ``Call.tag`` (single/sequential calls, escalation hops)."""
    fanout_id: str | None = field(default=None)
    """Fan-out batch marker (= the fan-out node's OWN inbound ``frame_id``, which is
    also the batch key for the durable tables). At fan-out dispatch it is stamped on the
    node's **own** frame within each sibling's stack copy — *not* on the pushed callee
    frame — so it survives the callee's return-pop and is the top frame when the sibling
    reply re-enters this node, routing that reply into the durable fold rather than the
    stateless-continuation path. A marked frame therefore has ``fanout_id == frame_id``.
    ``None`` on every non-sibling frame (single calls, escalation hops); the closure
    re-entry is built from the pre-stamp snapshot, so the continuation is unmarked by
    construction."""


CallFrameStack = Stack[CallFrame]


class WorkflowState(BaseModel):
    """The current control state for the routing and metadata representing the workflow. Framework-level wiring."""  # noqa: E501

    model_config = ConfigDict(extra="ignore")
    call_stack: CallFrameStack
    metadata: Any = Field(
        default=None,
        description="Additional data that can be accessed programmatically by the application.",
    )

    @property
    def current_frame(self) -> CallFrame:
        return self.call_stack.peek()

    @property
    def current_frame_or_none(self) -> CallFrame | None:
        """The top call frame, or ``None`` when the stack is empty.

        A frameless inbound envelope reaches a terminal-sink consumer that taps a
        producer's ``publish_topic`` after that producer's ``ReturnCall`` unwound
        the stack. Unlike :attr:`current_frame` (which raises on an empty stack),
        this lets the base handler treat a missing frame as "no per-invocation
        control-flow metadata" instead of failing.
        """
        return None if self.call_stack.is_empty() else self.call_stack.peek()

    def unwind_frame(self) -> CallFrame:
        return self.call_stack.pop()

    def invoke_frame(
        self, call: _Call, callback_topic: str | None, payload: Any = None, *, frame_id: str | None = None, tag: str | None = None
    ) -> None:
        if call.target_topic is None:
            raise Exception("")
        # ``frame_id`` lets the fan-out OPEN pre-mint each callee slot id, so the
        # published callee frame *is* that id and a reply's ``in_reply_to`` matches the
        # registered slot directly; ``None`` keeps the default fresh-uuid7 mint. ``tag``
        # (the caller's tool_call_id) rides the callee frame and is echoed on its reply,
        # making a sibling reply self-describing. ``invoke_frame`` never sets the
        # ``fanout_id`` marker — that rides the node's OWN frame, not the pushed callee.
        if frame_id is None:
            frame = CallFrame(target_topic=call.target_topic, callback_topic=callback_topic, payload=payload, tag=tag)
        else:
            frame = CallFrame(target_topic=call.target_topic, callback_topic=callback_topic, payload=payload, frame_id=frame_id, tag=tag)
        return self.call_stack.push(frame)

    def mark_fanout(self) -> None:
        """Stamp the current (top) frame as the fan-out origin: set ``fanout_id`` to the
        frame's OWN ``frame_id`` (the batch key). ``CallFrame`` is frozen, so this replaces
        the top frame with a marked copy. The marker rides the node's own frame — *not* the
        pushed callee frames — so it survives the callee's return-pop and is the top frame
        when a sibling reply re-enters this node, routing it into the durable fold."""
        frame = self.call_stack.pop()
        self.call_stack.push(replace(frame, fanout_id=frame.frame_id))


class BaseSessionRunContext(BaseModel, Generic[StateT, DepsT]):
    """Base generic context for a session — state, deps, and per-hop identity."""

    state: StateT
    """The app state. Mutable."""

    deps: DepsT
    """User-provided dependencies for the execution. A JSON-serializable mapping
    (it crosses the Kafka boundary). The same ``dict`` the producer passed to
    ``Client.start(deps=...)``; tools read it as ``ctx.deps["key"]``."""

    _correlation_id: str | None = PrivateAttr(default=None)
    _emitter_node_id: str | None = PrivateAttr(default=None)
    _emitter_node_kind: str | None = PrivateAttr(default=None)
    _frame_id: str | None = PrivateAttr(default=None)
    _resources: Mapping[str, Any] | None = PrivateAttr(default=None)
    _reply: ReturnMessage | None = PrivateAttr(default=None)

    @property
    def correlation_id(self) -> str:
        """The correlation id that ties this hop to its invocation.

        Sourced from the inbound Kafka message ``correlation_id`` (stamped by
        ``BaseNodeDef.prepare_context`` server-side, the consumer handler, or the
        client's reply dispatcher) — never from the envelope body. Backed by a
        ``PrivateAttr`` so it cannot be spoofed via the model constructor, mirroring
        :attr:`emitter_node_id`. Unlike the emitter id it is always present once a
        handler has prepared the context (the transport guarantees a value), so the
        accessor is typed ``str`` and raises if read before stamping (rather than
        ``assert``, which ``python -O`` would strip — letting ``None`` leak into
        ``correlation_id[:8]`` log/key sites as a confusing ``TypeError``).
        """
        if self._correlation_id is None:
            raise RuntimeError(
                "correlation_id is unset; the context was read outside a handler that stamps it "
                "(prepare_context / consumer handler / reply dispatcher)."
            )
        return self._correlation_id

    @property
    def emitter_node_id(self) -> str | None:
        """Node id of the most-recent publisher of this event (the immediate hop sender).

        Set by ``BaseNodeDef.prepare_context`` (server side) or the client's reply
        dispatcher (client side) from the inbound ``x-calf-emitter`` Kafka header.
        Backed by a ``PrivateAttr`` so it never rides on the wire and cannot be
        spoofed via the model constructor.
        """
        return self._emitter_node_id

    @property
    def emitter_node_kind(self) -> str | None:
        """Coarse classification of the emitter (one of ``NodeKind``).

        Companion to :attr:`emitter_node_id`, sourced from the inbound
        ``x-calf-emitter-kind`` Kafka header.
        """
        return self._emitter_node_kind

    @property
    def frame_id(self) -> str | None:
        """Per-invocation identifier of the current call frame on the workflow stack.

        Set by ``BaseNodeDef.prepare_context`` from the inbound frame
        (``envelope.internal_workflow_state.current_frame.frame_id``), so it is the id
        of the frame this delivery is running under. Every ``Call`` published by the
        framework pushes a fresh ``CallFrame`` with a UUID7-generated ``frame_id``, so
        parallel invocations of the same node (which share a single ``correlation_id``)
        are still uniquely identifiable per invocation. No longer tied to any in-process
        aggregation (durable fan-out is keyed by ``fanout_id`` in the store, not by this
        field). Backed by a ``PrivateAttr`` so it never rides on the wire and cannot be
        spoofed via the model constructor.
        """
        return self._frame_id

    @property
    def resources(self) -> Mapping[str, Any]:
        """The owner's lifecycle-managed resources (read-only by type).

        Populated server-side by ``BaseNodeDef.prepare_context`` with a *shallow
        copy* of the node's (and its worker's) resource bag, so mutating it can't
        corrupt the shared bag or other handlers. Typed ``Mapping`` so
        ``ctx.resources["k"] = ...`` is a type error at dev time (mirrors how
        ``deps`` is treated read-only). Returns an empty mapping when unset (e.g.
        a context built outside a handler), so reads never raise.

        Backed by a ``PrivateAttr`` so it never rides on the wire and cannot be
        spoofed via the model constructor, and stamped *after* the
        ``model_copy(deep=True)`` in ``prepare_context``: the inbound context's
        ``_resources`` is unset (``None``) at copy time, so the deep copy never
        duplicates live resources. The stored value is a plain ``dict`` (not a
        proxy), so the framework's deep copy is mechanically safe; avoid
        deep-copying a *stamped* context in application code, though, since the
        values are live resource objects (pools, clients) meant to be shared, not
        duplicated.
        """
        if self._resources is None:
            return _EMPTY_RESOURCES
        return self._resources

    @property
    def output_parts(self) -> list[ContentPart]:
        """Content parts carried by this delivery's reply slot (spec §4).

        ``[]`` on call-kind deliveries (no reply) and on a context built outside a
        handler (unstamped) — empty, never raises. The reply-sourced replacement for
        the retired ``state.final_output_parts`` read: a consumer gate that dropped
        intermediate hops via ``bool(ctx.state.final_output_parts)`` now uses
        ``bool(ctx.output_parts)``. Backed by ``_reply`` (stamped by ``prepare_context``
        / the client reply dispatcher), so it never rides the wire.
        """
        return self._reply.parts if self._reply is not None else []

    def _stamp_transport(self, *, correlation_id: str | None, emitter_node_id: str | None, emitter_node_kind: str | None) -> None:
        """Stamp transport-sourced identity onto this context.

        ``correlation_id`` and the emitter ids come from the inbound Kafka message
        (header / FastStream ``Context()``), never the envelope body. Called by
        ``BaseNodeDef.prepare_context`` (on a freshly-copied context), the consumer
        handler, and the client's reply dispatcher, so the three sites cannot drift.
        ``frame_id`` is workflow-state-sourced, not transport-sourced, so it is
        stamped separately by ``prepare_context``.
        """
        self._correlation_id = correlation_id
        self._emitter_node_id = emitter_node_id
        self._emitter_node_kind = emitter_node_kind


SessionRunContext = BaseSessionRunContext[State, dict[str, Any]]
