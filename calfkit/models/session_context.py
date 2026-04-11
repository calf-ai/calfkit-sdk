from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Generic

import uuid_utils
from pydantic import BaseModel, ConfigDict, Field

from calfkit._types import DepsT, StackItemT, StateT
from calfkit.models.actions import _Call
from calfkit.models.state import OverridesState, State


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
    callback_topic: str  # return address
    input_args: Sequence[Any] | None = field(default=None)
    frame_id: str = field(default_factory=lambda: uuid_utils.uuid7().hex)
    overrides: OverridesState | None = field(default=None)


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

    def invoke_frame(self, call: _Call, callback_topic: str) -> None:
        if call.target_topic is None:
            raise Exception("")
        frame = CallFrame(
            target_topic=call.target_topic,
            callback_topic=callback_topic,
            input_args=call.input_args,
        )
        return self.call_stack.push(frame)


class Deps(BaseModel):
    """immutable dependencies for agent executions"""

    model_config = ConfigDict(extra="ignore", frozen=True)
    correlation_id: str
    provided_deps: dict[str, Any] = Field(description="user-provided agent dependencies")


class BaseSessionRunContext(BaseModel, Generic[StateT, DepsT]):
    """Base generic context for a session — just state + deps."""

    state: StateT
    """The app state. Mutable."""

    deps: DepsT
    """Dependencies for the execution. Immutable."""


SessionRunContext = BaseSessionRunContext[State, Deps]
