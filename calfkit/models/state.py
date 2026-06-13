import logging
from dataclasses import dataclass, field
from typing import Annotated, Any, ClassVar, Generic, Literal

from pydantic import BaseModel, ConfigDict, Discriminator, Field, Tag, field_validator
from typing_extensions import Self, TypeVar

from calfkit._vendor.pydantic_ai.exceptions import ModelRetry
from calfkit._vendor.pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    RetryPromptPart,
    ToolCallPart,
    ToolReturn,
)
from calfkit.models.tool_dispatch import ToolBinding


class BaseAgentActivityState(BaseModel):
    model_config = ConfigDict(extra="ignore")


class OverridesState(BaseAgentActivityState):
    """State for storing any override objects"""

    model_config = ConfigDict(extra="ignore")
    override_agent_tools: list[ToolBinding] | None = None
    model_settings: dict[str, Any] | None = None


class CoreMessageState(BaseAgentActivityState):
    """The state for committed messages and structured objects"""

    model_config = ConfigDict(extra="ignore")
    uncommitted_message: ModelMessage | None = None
    message_history: list[ModelMessage] = Field(default_factory=list, description="Append-only message history list")

    temp_instructions: str | None = None

    def latest_tool_calls(self) -> list[ToolCallPart]:
        pending_tool_calls = list()
        for msg in reversed(self.message_history):
            if isinstance(msg, ModelRequest):
                break
            elif isinstance(msg.tool_calls, list):
                pending_tool_calls.extend(msg.tool_calls)
        return pending_tool_calls

    def extend_with_responses(self, messages: list[ModelMessage], author: str) -> None:
        """Append run-produced messages, stamping author identity on un-named responses.

        Every ``ModelResponse`` in ``messages`` whose ``name`` is still ``None`` is
        tagged with ``author`` (the producing agent's id) before the messages are
        appended to the canonical ``message_history``. The ``if m.name is None``
        guard makes this idempotent — re-stamping an already-stamped list is a no-op
        (no current pydantic-ai provider sets ``ModelResponse.name``, so any non-None
        name here is one calfkit already applied; §4). ``ModelRequest``s are untouched.
        """
        for m in messages:
            if isinstance(m, ModelResponse) and m.name is None:
                m.name = author
        self.message_history.extend(messages)

    def stage_message(self, message: ModelMessage) -> None:
        self.uncommitted_message = message

    def commit_message_to_history(self) -> None:
        if self.uncommitted_message is None:
            err_msg = "The staged message(uncommitted_message) is None, can't be committed to history."
            logging.error(err_msg)
            raise RuntimeError(err_msg)
        self.message_history.append(self.uncommitted_message)
        self.uncommitted_message = None


class FailedToolCall(BaseModel):
    """Wire-format representation of a tool that raised on the worker side.

    Stored in ``State.tool_results`` in the same slot a successful ``ToolReturn``
    would occupy; consumers must handle both shapes. The ``marker_kind`` literal
    is the discriminator tag consulted by ``_calf_tool_result_discriminator`` so
    this value reconstructs as a typed instance (rather than a plain ``dict``)
    when ``State`` round-trips through JSON across the Kafka boundary.

    String fields are clamped to bounded lengths (see ``_MAX_LENGTHS``) rather
    than rejected for wire-size safety: a tool that raises with an enormous
    ``str(exc)`` must not itself cause construction to raise inside the worker's
    ``except Exception`` block (which would prevent the failure reply from ever
    being published and hang the agent).
    """

    model_config = ConfigDict(extra="ignore", frozen=True)
    tool_name: str
    tool_call_id: str = Field(min_length=1)
    exc_type: str
    exc_message: str
    marker_kind: Literal["calfkit-tool-error"] = "calfkit-tool-error"

    _MAX_LENGTHS: ClassVar[dict[str, int]] = {
        "tool_name": 256,
        "tool_call_id": 128,
        "exc_type": 256,
        "exc_message": 4096,
    }

    @field_validator("tool_name", "tool_call_id", "exc_type", "exc_message", mode="before")
    @classmethod
    def _clamp_string_fields(cls, v: Any, info: Any) -> Any:
        if isinstance(v, str):
            limit = cls._MAX_LENGTHS.get(info.field_name)
            if limit is not None and len(v) > limit:
                return v[:limit]
        return v

    @classmethod
    def build_safe(cls, *, tool_name: str, tool_call_id: str, exc_type: str, exc_message: str) -> Self:
        """Construct a marker that **never raises**, for use inside a worker's error path.

        The normal constructor rejects an empty ``tool_call_id`` (``min_length=1``) and
        could raise inside an ``except`` block — re-introducing the silent agent hang
        this marker exists to prevent (oversized strings are already clamped, not
        rejected, so the empty id is the realistic failure). On any construction error,
        fall back to sentinel identifiers so the failure reply is always published.
        """
        try:
            return cls(tool_name=tool_name, tool_call_id=tool_call_id, exc_type=exc_type, exc_message=exc_message)
        except Exception:
            return cls(
                tool_name=tool_name or "<unknown>",
                tool_call_id=tool_call_id or "<missing>",
                exc_type="FailedToolCallConstructionError",
                exc_message=f"could not construct marker for original exc_type={exc_type!r}",
            )


def _calf_tool_result_discriminator(x: Any) -> str | None:
    """Tag extractor for the ``CalfToolResult`` discriminated union.

    Mirrors ``calfkit._vendor.pydantic_ai.tools._deferred_tool_call_result_discriminator``
    with an added ``marker_kind`` branch for ``FailedToolCall``. Without this, a
    serialized marker arrives as a plain ``dict`` after a Kafka hop because the
    upstream discriminator only recognizes ``kind`` / ``part_kind``.

    Returns only string tags; non-string values are treated as missing so the
    union falls through to the ``Any`` arm rather than silently misrouting.
    Keep this in sync with the upstream helper if a new pydantic-ai tag is added.
    """
    for key in ("marker_kind", "kind", "part_kind"):
        v = x.get(key) if isinstance(x, dict) else getattr(x, key, None)
        if isinstance(v, str):
            return v
    return None


CalfToolResult = Annotated[
    Annotated[ToolReturn, Tag("tool-return")]
    | Annotated[ModelRetry, Tag("model-retry")]
    | Annotated[RetryPromptPart, Tag("retry-prompt")]
    | Annotated[FailedToolCall, Tag("calfkit-tool-error")],
    Discriminator(_calf_tool_result_discriminator),
]
"""Calfkit-side tagged union for values stored in ``State.tool_results``.

Flattens pydantic-ai's ``DeferredToolCallResult`` (``ToolReturn`` / ``ModelRetry``
/ ``RetryPromptPart``) and adds calfkit's ``FailedToolCall``. Flattening (rather
than ``DeferredToolCallResult | FailedToolCall``) is required because Pydantic's
discriminated-union machinery does not compose nested ``Discriminator``
annotations reliably.
"""


class InFlightToolsState(BaseAgentActivityState):
    """State of all in-progress tool calls and results.
    Tool calls and results are in-progress when all tool calls have not been completed and committed to message history."""  # noqa: E501

    model_config = ConfigDict(extra="ignore")

    tool_calls: dict[str, ToolCallPart] = Field(default_factory=dict)

    tool_results: dict[str, CalfToolResult | Any] = Field(default_factory=dict)

    def add_tool_call(self, tool_call: ToolCallPart) -> None:
        self.tool_calls[tool_call.tool_call_id] = tool_call

    def add_tool_result(self, tool_call_id: str, tool_result: CalfToolResult | Any) -> None:
        self.tool_results[tool_call_id] = tool_result

    def get_tool_result(self, tool_call_id: str) -> CalfToolResult | Any | None:
        return self.tool_results.get(tool_call_id)

    def all_call_ids_complete(self, *call_ids: str) -> bool:
        for call_id in call_ids:
            _ = self.tool_calls[call_id]
            if call_id not in self.tool_results:
                return False
        return True


class State(CoreMessageState, InFlightToolsState):
    """A flat unified state tracking all current states within an agent's loop.
    Inheritance is ordered by priority, so important state fields take precedence."""

    model_config = ConfigDict(extra="ignore")
    metadata: Any = Field(
        default=None,
        description="Additional data that can be accessed programmatically by the application but is not sent to the LLM.",  # noqa: E501
    )
    overrides: OverridesState | None = None


AgentStateSubsetT = TypeVar("AgentStateSubsetT", CoreMessageState, InFlightToolsState)

PartialState = Annotated[CoreMessageState | InFlightToolsState | Any, Discriminator("kind")]


class NodeConsumeState(BaseModel, Generic[AgentStateSubsetT]):
    """Used to partially consume a subset of the complete agent state"""

    model_config = ConfigDict(extra="ignore")
    run_state: AgentStateSubsetT


@dataclass
class PendingToolBatch:
    """Tracks an in-flight parallel tool call batch for one correlation chain."""

    expected_tool_call_ids: frozenset[str]

    # State snapshot at fan-out time, with tool_calls registered
    base_state: State

    # map of tool call IDs to tool call results
    collected_results: dict[str, CalfToolResult | Any] = field(default_factory=dict)

    @property
    def is_complete(self) -> bool:
        return self.expected_tool_call_ids == frozenset(self.collected_results.keys())
