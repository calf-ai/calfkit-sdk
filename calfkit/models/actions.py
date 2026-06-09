from dataclasses import dataclass
from typing import Any, Generic

from typing_extensions import TypeAliasType, TypeVar

from calfkit._routing import is_concrete_route_key
from calfkit._types import StateT


@dataclass
class Reply(Generic[StateT]):
    """Terminal: send value back to whoever called this node (pops reply_stack).
    Does not require a topic address to reply to, it is handled by the framework.
    Similar mental model to completing an async Promise or Future with a result."""

    value: StateT


@dataclass
class Delegate(Generic[StateT]):
    """Terminal: forward to another node, expect result back (pushes reply_stack)."""

    topic: str
    value: StateT | None = None


@dataclass
class _Call(Generic[StateT]):
    """Call another node, providing mutable state.

    The callee callbacks the caller (w/ ``state``) when it completes, within the
    current correlation id / flow; for a fire-and-forget terminal the callstack
    pops to the previous caller instead.

    Args:
        target_topic: The topic of the target node to call.
        state: Mutable state returned back to the caller on callback.
    """

    target_topic: str
    state: StateT


@dataclass(init=False)
class Call(Generic[StateT], _Call[StateT]):
    """Call another node, providing mutable state.
    The target will callback the caller (w/ State) when complete.

    Optionally carries header-route-dispatch metadata: ``route`` (a concrete route
    key, stamped as the ``x-calf-route`` header on the publish) and ``body`` (an
    optional payload validated against the target handler's ``schema``). These live
    on ``Call`` only — ``TailCall``/``ReturnCall`` never carry a route. ``init=False``
    keeps the custom keyword-only ``route``/``body`` constructor while letting them
    participate in ``__eq__``/``__repr__``."""

    route: str | None = None
    body: Any | None = None

    def __init__(self, target_topic: str, state: StateT, *, route: str | None = None, body: Any | None = None) -> None:
        if route is not None and not is_concrete_route_key(route):
            raise ValueError(
                f"Call route {route!r} must be a concrete key — non-empty, '.'-delimited words, no empty "
                "segments, no wildcard. ('*' is a route pattern for @handler, not a producer route key.)"
            )
        # A routeless ``body`` is intentionally allowed: it lands in ``CallFrame.payload``
        # and is read by the target node's inherited ``@handler('*')`` ``run`` when that
        # ``run`` declares a ``schema`` (e.g. a tool node validating a ``ToolCallRef``).
        super().__init__(target_topic, state)
        self.route = route
        self.body = body


class TailCall(Generic[StateT], _Call[StateT]):
    """Call another node with no expectation for response callback.
    If current execution has a callback committment to its callee, the tailcallee inherits it."""


@dataclass
class ReturnCall(Generic[StateT]):
    """Finish the node's execution and callback the caller."""

    state: StateT


@dataclass
class Sequential(Generic[StateT]):
    """Sequentially forward a shared state from node to node,
    where the final state is returned to caller."""

    topics: list[str]  # passed to topics in this list in order index 0 -> n
    value: StateT | None = None


@dataclass
class Emit(Generic[StateT]):
    """Terminal: fire-and-forget publish to a topic. No reply expected."""

    value: StateT
    topic: str


@dataclass
class Parallel(Generic[StateT]):
    """Parallel fan-out of delegates and calls. Developer manages result aggregation via store."""

    delegates: list[Delegate[StateT] | Call[StateT]]


@dataclass
class Silent:
    """Silent end of node execution, no explicit publish. End of event stream."""


@dataclass
class Next:
    """Routing control returned by a route handler that *declines* to handle the
    message: advance the Chain of Responsibility to the next, more-general matching
    handler. By contract it makes no state change and no publish. It is **not** a
    publish action — the dispatcher consumes it, so it is intentionally absent from
    the :data:`NodeResult` union and never reaches ``_publish_action``."""


_T = TypeVar("_T")

NodeResult = TypeAliasType(
    "NodeResult",
    Silent | Call[_T] | list[Call[_T]] | ReturnCall[_T] | TailCall[_T],
    type_params=(_T,),
)
"""All possible return types from a node's ``run`` method."""
