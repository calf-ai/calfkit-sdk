from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Generic

from typing_extensions import TypeAliasType, TypeVar

from calfkit._types import StateT
from calfkit._vendor.pydantic_ai.tools import DeferredToolCallResult as ToolCallResult
from calfkit.models.state import State


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
    input_args: Sequence[Any] | None = None


@dataclass(init=False)
class _Call(Generic[StateT]):
    """Call another node, and provide a mutable state and arguments.
    The target will callback the caller (w/ State) when complete."""

    # callback occurs within the current correlation id / flow

    target_topic: str
    state: StateT
    input_args: Sequence[Any] | None

    def __init__(
        self,
        target_topic: str,
        state: StateT,
        *input_args: Any,
    ):
        """Create a call object to another node. Used to call another node.

        Args:

            target_topic (str): The topic of the target node to call.

            request_callback (bool): Whether to request a callback from the target node.
            The callback will return the processed state back to the caller.
            The callback occurs within the current correlation id / flow.
            If no callback is requested, at the end of this node's execution, the callstack pops and the previous caller gets its callback.

            state (StateT | None, optional): Mutable state which will be returned back if callback is requested. Defaults to None.

            *input_args (Any, optional): Pass in any arguments the node's run() method accepts.

        Returns:
            Call: The call object
        """  # noqa: E501

        self.target_topic = target_topic
        self.state = state
        self.input_args = input_args or None


class Call(Generic[StateT], _Call[StateT]):
    """Call another node, and provide a mutable state and arguments.
    The target will callback the caller (w/ State) when complete."""


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


_T = TypeVar("_T")

NodeResult = TypeAliasType(
    "NodeResult",
    Silent | Call[_T] | list[Call[_T]] | ReturnCall[_T] | TailCall[_T],
    type_params=(_T,),
)
"""All possible return types from a node's ``run`` method."""


@dataclass
class PendingToolBatch:
    """Tracks an in-flight parallel tool call batch for one correlation chain."""

    expected_tool_call_ids: frozenset[str]

    # State snapshot at fan-out time, with tool_calls registered
    base_state: State

    # map of tool call IDs to tool call results
    collected_results: dict[str, ToolCallResult | Any] = field(default_factory=dict)

    @property
    def is_complete(self) -> bool:
        return self.expected_tool_call_ids == frozenset(self.collected_results.keys())
