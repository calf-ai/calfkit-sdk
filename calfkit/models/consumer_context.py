from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic

from pydantic import TypeAdapter

from calfkit._types import OutputT
from calfkit._vendor.pydantic_ai.messages import ModelMessage
from calfkit.models.node_result import _UNSET, project_output
from calfkit.models.payload import ContentPart
from calfkit.models.state import State

if TYPE_CHECKING:
    from calfkit.models.session_context import SessionRunContext


@dataclass(frozen=True)
class ConsumerContext(Generic[OutputT]):
    """Node-side context handed to a ``@consumer`` sink, once per inbound envelope.

    Speaks the same vocabulary as :class:`~calfkit.models.tool_context.ToolContext`
    (``ctx.deps`` / ``ctx.resources`` / ``ctx.correlation_id``) but is shaped for a
    terminal sink: it carries the projected ``output`` and the full session
    ``state``, and none of the tool/LLM-run machinery. Treat it, its ``state``, and
    its ``deps`` as read-only.

    Build one with :meth:`from_run_context` from the consumer's stamped
    :class:`~calfkit.models.session_context.SessionRunContext`; don't construct the
    dataclass directly.
    """

    output: OutputT | None
    """Deserialized final output (typed via ``output_type``).

    ``None`` on intermediate hops — call-kind deliveries with no reply slot (e.g.
    tool completions, mid-loop agent hops). Populated only when the upstream node
    emitted a terminal return carrying reply parts."""

    state: State
    """Full session state at this hop (message history, in-flight tool
    calls/results, metadata, overrides, …). Shared with the envelope, not copied —
    treat as read-only."""

    correlation_id: str
    """The correlation id that ties this hop to its invocation."""

    output_parts: list[ContentPart] = field(default_factory=list)
    """The raw reply parts this observation was projected from (spec §4). ``[]`` on an
    intermediate hop. Captured from the delivery's reply slot, not ``state``."""

    emitter_node_id: str | None = None
    """Node id of the upstream emitter (``x-calf-emitter`` header), or ``None``."""

    emitter_node_kind: str | None = None
    """Coarse classification of the emitter (``x-calf-emitter-kind`` header)."""

    deps: Mapping[str, Any] = field(default_factory=dict)
    """Inbound producer dependencies — the same mapping the producer passed to
    ``Client.start(deps=...)``. Read as ``ctx.deps["key"]``, mirroring tools."""

    resources: Mapping[str, Any] = field(default_factory=dict)
    """The consuming node's lifecycle-managed resources (read-only by type). Read as
    ``ctx.resources["key"]``, mirroring how tools read ``ctx.resources["key"]``.
    Empty when the node owns no resources."""

    # Holds a mutable Pydantic model (state); declare unhashability explicitly so
    # static checkers and runtime introspection agree (mirrors NodeResult).
    __hash__ = None  # type: ignore[assignment]

    @classmethod
    def from_run_context(
        cls,
        ctx: SessionRunContext,
        output_type: type[Any] = _UNSET,
        *,
        type_adapter: TypeAdapter[Any] | None = None,
    ) -> ConsumerContext[Any]:
        """Build from the consumer's stamped ``SessionRunContext``.

        Uses ``strict=False`` (consumer semantics): an intermediate hop with no
        reply slot yields ``output=None`` instead of raising. ``resources`` is
        sourced from ``ctx.resources`` (stamped by the node's ``prepare_context``),
        so the consumer needs no separate resources plumbing.

        Raises:
            DeserializationError / pydantic.ValidationError: only when the reply
            parts are present but don't match ``output_type``.
        """
        return cls(
            output=project_output(ctx._reply, output_type, strict=False, type_adapter=type_adapter),
            output_parts=ctx._reply.parts if ctx._reply is not None else [],
            state=ctx.state,
            correlation_id=ctx.correlation_id,
            emitter_node_id=ctx.emitter_node_id,
            emitter_node_kind=ctx.emitter_node_kind,
            deps=ctx.deps,
            resources=ctx.resources,
        )

    @property
    def message_history(self) -> list[ModelMessage]:
        """Convenience: ``state.message_history``."""
        return self.state.message_history

    @property
    def metadata(self) -> Any:
        """Convenience: ``state.metadata``."""
        return self.state.metadata
