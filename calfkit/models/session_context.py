from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Generic

import uuid_utils
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

from calfkit._types import DepsT, StackItemT, StateT
from calfkit.models.actions import _Call
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

    def unwind_frame(self) -> CallFrame:
        return self.call_stack.pop()

    def invoke_frame(self, call: _Call, callback_topic: str | None, payload: Any = None) -> None:
        if call.target_topic is None:
            raise Exception("")
        frame = CallFrame(
            target_topic=call.target_topic,
            callback_topic=callback_topic,
            payload=payload,
        )
        return self.call_stack.push(frame)


class BaseSessionRunContext(BaseModel, Generic[StateT, DepsT]):
    """Base generic context for a session — state, deps, and per-hop identity."""

    state: StateT
    """The app state. Mutable."""

    deps: DepsT
    """User-provided dependencies for the execution. A JSON-serializable mapping
    (it crosses the Kafka boundary). The same ``dict`` the producer passed to
    ``Client.invoke_node(deps=...)``; tools read it as ``ctx.deps["key"]``."""

    _correlation_id: str | None = PrivateAttr(default=None)
    _emitter_node_id: str | None = PrivateAttr(default=None)
    _emitter_node_kind: str | None = PrivateAttr(default=None)
    _frame_id: str | None = PrivateAttr(default=None)
    _resources: Mapping[str, Any] | None = PrivateAttr(default=None)

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

        Set by ``BaseNodeDef.prepare_context`` from
        ``envelope.internal_workflow_state.current_frame.frame_id``. Every
        ``Call`` published by the framework pushes a fresh ``CallFrame`` with a
        UUID7-generated ``frame_id``, so parallel invocations of the same node
        (which share a single ``correlation_id``) are still uniquely
        identifiable per invocation. Used by the agent to key its in-memory
        parallel tool-batch aggregation dict so concurrent fan-outs do not
        collide. Backed by a ``PrivateAttr`` so it never rides on the wire and
        cannot be spoofed via the model constructor.
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
