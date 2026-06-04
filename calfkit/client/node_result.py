from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Generic

from calfkit._types import OutputT
from calfkit._vendor.pydantic_ai.messages import ModelMessage
from calfkit.models import ContentPart, State


@dataclass(frozen=True)
class NodeResult(Generic[OutputT]):
    """Client-facing projection of the session state after a node returns.

    A ``NodeResult`` is what callers receive in two places:

    * :meth:`Client.execute_node` / :meth:`InvocationHandle.result` — the
      final reply from an agent invocation.
    * The user function of a :class:`ConsumerNodeDef` — one per envelope on
      every subscribed topic, including intermediate hops.

    The ``state`` field is the full session :class:`~calfkit.models.State` at
    the moment this envelope was published, exposing message history, in-flight
    tool calls/results, application metadata, runtime overrides, and any other
    state fields. ``message_history``, ``output_parts``, and ``metadata`` are
    convenience properties that read through ``state``.

    Treat ``NodeResult``, its ``state``, and its ``deps`` as read-only. The
    dataclass is frozen, but ``state`` is a mutable Pydantic model and ``deps``
    is a mutable dict (both shared with the envelope, not copied):

    * **Consumer path**: the consumer never republishes (no ``publish_topic``),
      so mutations have no observable downstream effect. They can still
      surprise other code holding the same ``NodeResult`` instance.
    * **Client path** (:meth:`Client.execute_node` / :meth:`InvocationHandle.result`):
      the caller's lifetime owns the instance — mutations are caller-visible
      and may corrupt any other code holding a parallel reference (caches,
      retry layers, etc.).

    ``NodeResult`` is intentionally unhashable (``__hash__ = None``): the
    underlying ``state`` field is a mutable Pydantic model and cannot be
    placed in a set or used as a dict key safely. Use ``correlation_id`` if
    you need a hashable identifier.
    """

    output: OutputT | None
    """Deserialized final output (typed via ``output_type``).

    ``None`` on intermediate hops — envelopes whose ``state.final_output_parts``
    is empty (e.g. agent hops mid-tool-call, tool completions). Populated when
    the upstream node emitted a terminal envelope with final output parts.
    Client-side strict-mode results (the default) always have ``output``
    populated; consumer-side results may not.
    """

    state: State
    """Full session state at this hop. Includes:

    * ``message_history`` — cumulative conversation
    * ``final_output_parts`` — agent's structured/text output (empty on
      intermediate hops)
    * ``tool_calls`` / ``tool_results`` — in-flight tool batch (keyed by
      ``tool_call_id``)
    * ``metadata`` — application-level metadata
    * ``overrides`` — agent tool overrides applied to this invocation (or
      ``None`` if unset)
    * ``uncommitted_message`` / ``temp_instructions`` — agent-loop scratch;
      usually ``None`` on terminal hops, may be populated mid-loop and is not
      part of the public contract
    """

    correlation_id: str
    """The correlation ID that ties this result to its invocation."""

    emitter_node_id: str | None = None
    """Node id of the node that emitted this reply (sourced from the
    ``x-calf-emitter`` Kafka header). May be ``None`` if the upstream
    producer didn't stamp the header (e.g. a non-calfkit publisher)."""

    emitter_node_kind: str | None = None
    """Coarse classification of the emitter (one of ``NodeKind``), sourced
    from the ``x-calf-emitter-kind`` Kafka header. May be ``None`` if not
    stamped."""

    deps: dict[str, Any] = field(default_factory=dict)
    """Inbound user-provided dependencies — the same ``dict`` the producer passed
    to ``Client.invoke_node(deps=...)``, carried forward on every publish. Read
    it as ``result.deps["key"]``, mirroring how tools read ``ctx.deps["key"]``.
    Empty ``{}`` when the invocation set no deps.

    Like ``state``, this is the same dict instance carried on the envelope (no
    defensive copy) — treat it as read-only (see the read-only note above)."""

    # NodeResult holds a mutable Pydantic model (state); the dataclass-
    # synthesized __hash__ would recursively try to hash unhashable fields and
    # raise at use-time. Declare unhashability explicitly so static type
    # checkers and runtime introspection agree.
    __hash__ = None  # type: ignore[assignment]

    @property
    def output_parts(self) -> list[ContentPart]:
        """Convenience: ``state.final_output_parts``."""
        return self.state.final_output_parts

    @property
    def message_history(self) -> list[ModelMessage]:
        """Convenience: ``state.message_history``."""
        return self.state.message_history

    @property
    def metadata(self) -> Any:
        """Convenience: ``state.metadata``."""
        return self.state.metadata
