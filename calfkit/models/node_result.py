from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic

from pydantic import TypeAdapter

from calfkit._types import OutputT
from calfkit._vendor.pydantic_ai.messages import ModelMessage
from calfkit.exceptions import DeserializationError
from calfkit.models.payload import ContentPart, DataPart, TextPart
from calfkit.models.state import State

if TYPE_CHECKING:
    from calfkit.models.envelope import Envelope
    from calfkit.models.reply import ReturnMessage
    from calfkit.models.session_context import SessionRunContext

_UNSET: Any = object()


@dataclass(frozen=True)
class InvocationResult(Generic[OutputT]):
    """Client-facing projection of the session state after a node returns.

    A ``InvocationResult`` is what callers receive in two places:

    * :meth:`Client.execute` / :meth:`InvocationHandle.result` — the
      final reply from an agent invocation.

    The ``state`` field is the full session :class:`~calfkit.models.State` at
    the moment this envelope was published, exposing message history, in-flight
    tool calls/results, application metadata, runtime overrides, and any other
    state fields. ``message_history``, ``output_parts``, and ``metadata`` are
    convenience properties that read through ``state``.

    Treat ``InvocationResult``, its ``state``, and its ``deps`` as read-only. The
    dataclass is frozen, but ``state`` is a mutable Pydantic model and ``deps``
    is the envelope's dict typed as a read-only ``Mapping`` (both shared with the
    envelope, not copied — the ``Mapping`` annotation makes accidental
    ``result.deps[...] = ...`` a type error; the runtime object is still that
    shared dict):

    * **Consumer path**: the consumer never republishes (no ``publish_topic``),
      so mutations have no observable downstream effect. They can still
      surprise other code holding the same ``InvocationResult`` instance.
    * **Client path** (:meth:`Client.execute` / :meth:`InvocationHandle.result`):
      the caller's lifetime owns the instance — mutations are caller-visible
      and may corrupt any other code holding a parallel reference (caches,
      retry layers, etc.).

    ``InvocationResult`` is intentionally unhashable (``__hash__ = None``): the
    underlying ``state`` field is a mutable Pydantic model and cannot be
    placed in a set or used as a dict key safely. Use ``correlation_id`` if
    you need a hashable identifier.

    Build one with the :meth:`from_envelope` / :meth:`from_context` alternative
    constructors rather than calling the dataclass directly — they project the
    output from the delivery's reply slot and source identity from the
    transport-stamped context.
    """

    output: OutputT | None
    """Deserialized final output (typed via ``output_type``).

    ``None`` on intermediate hops — call-kind deliveries with no reply slot (e.g.
    agent hops mid-tool-call, tool completions). Populated when the upstream node
    emitted a terminal return carrying reply parts. Client-side strict-mode results
    (the default) always have ``output`` populated; consumer-side results may not.
    """

    state: State
    """Full session state at this hop. Includes:

    * ``message_history`` — cumulative conversation
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

    output_parts: list[ContentPart] = field(default_factory=list)
    """The raw reply parts this result was projected from (spec §4). ``[]`` on an
    intermediate hop. Captured at construction from the delivery's reply slot, not
    read through ``state`` (the retired ``final_output_parts``)."""

    emitter_node_id: str | None = None
    """Node id of the node that emitted this reply (sourced from the
    ``x-calf-emitter`` Kafka header). May be ``None`` if the upstream
    producer didn't stamp the header (e.g. a non-calfkit publisher)."""

    emitter_node_kind: str | None = None
    """Coarse classification of the emitter (one of ``NodeKind``), sourced
    from the ``x-calf-emitter-kind`` Kafka header. May be ``None`` if not
    stamped."""

    deps: Mapping[str, Any] = field(default_factory=dict)
    """Inbound user-provided dependencies — the same mapping the producer passed
    to ``Client.start(deps=...)``, carried forward on every publish. Read
    it as ``result.deps["key"]``, mirroring how tools read ``ctx.deps["key"]``.
    Empty ``{}`` when the invocation set no deps.

    Typed as a read-only ``Mapping`` so accidental ``result.deps[...] = ...`` is a
    type error. Like ``state``, the underlying object is the same dict carried on
    the envelope (no defensive copy), so treat it as read-only at runtime too
    (see the read-only note above)."""

    resources: Mapping[str, Any] = field(default_factory=dict)
    """The consuming node's lifecycle-managed resources (read-only by type).

    Stamped by the consumer handler with a *shallow copy* of the node's resource
    bag. Read it as ``result.resources["key"]``, mirroring how tools read
    ``ctx.resources["key"]``. Typed as a read-only ``Mapping`` so
    ``result.resources[...] = ...`` is a type error at dev time (like ``deps``).
    Empty when the node owns no resources."""

    # InvocationResult holds a mutable Pydantic model (state); the dataclass-
    # synthesized __hash__ would recursively try to hash unhashable fields and
    # raise at use-time. Declare unhashability explicitly so static type
    # checkers and runtime introspection agree.
    __hash__ = None  # type: ignore[assignment]

    @classmethod
    def from_context(
        cls,
        ctx: SessionRunContext,
        output_type: type[Any] = _UNSET,
        *,
        correlation_id: str | None = None,
        strict: bool = True,
        type_adapter: TypeAdapter[Any] | None = None,
        resources: Mapping[str, Any] | None = None,
    ) -> InvocationResult[Any]:
        """Project a (transport-stamped) ``SessionRunContext`` into a ``InvocationResult``.

        This is the core constructor; :meth:`from_envelope` delegates here. The
        context must already be stamped (``prepare_context``, the consumer
        handler, or the client's reply dispatcher) so the emitter ids and
        ``correlation_id`` are populated.

        Args:
            ctx: The stamped session context. ``ctx.state``, ``ctx.emitter_node_id``,
                ``ctx.emitter_node_kind``, and ``ctx.deps`` populate the result.
            output_type: The expected Python type for the deserialized output.

                * **not provided** (``_UNSET``): auto-detect — prefer ``DataPart.data``
                  (returned as a raw dict), fall back to ``TextPart.text`` (str).
                * ``str``: extract the first ``TextPart.text``.
                * **anything else**: extract the first ``DataPart.data`` and validate
                  it through ``TypeAdapter(output_type)`` (or *type_adapter* if
                  provided).
            correlation_id: The transport ``correlation_id`` to surface as
                ``InvocationResult.correlation_id``. Sourced from the transport, never
                from the envelope body. When ``None``, falls back to
                ``ctx.correlation_id`` (which raises if the context was never
                stamped).
            strict: When ``True`` (default — client semantics), raises
                :class:`DeserializationError` if the reply parts are empty or
                don't contain the expected part type. When ``False`` (consumer
                semantics), returns ``output=None`` for an empty/absent reply
                (intermediate hop / tool completion); validation errors on
                *present* parts still propagate.
            type_adapter: An optional pre-built :class:`pydantic.TypeAdapter` to
                use for validating ``DataPart.data`` against *output_type*. When
                ``None`` (default), a new adapter is constructed per call.
                Consumers pre-build at wiring time so schema-generation errors
                surface once at construction rather than per envelope.
            resources: The consuming node's lifecycle-managed resources, surfaced
                as ``InvocationResult.resources`` (read-only by type). Defaults to an
                empty mapping.

        Returns:
            A ``InvocationResult`` whose ``.output`` is typed according to *output_type*
            (or ``None`` when ``strict=False`` and no parts are present).

        Raises:
            DeserializationError: If the expected content part is not found in
                the reply parts (and either ``strict=True`` or the parts
                list is non-empty but lacks the expected shape).
            pydantic.ValidationError: If ``output_type`` is provided and the
                matching ``DataPart.data`` doesn't validate against it.
            pydantic.PydanticSchemaGenerationError: If ``type_adapter`` is ``None``
                and ``output_type`` cannot be schematized by :class:`TypeAdapter`.
        """
        state = ctx.state
        output = project_output(ctx._reply, output_type, strict=strict, type_adapter=type_adapter)

        return cls(
            output=output,
            output_parts=ctx._reply.parts if ctx._reply is not None else [],
            state=state,
            correlation_id=correlation_id if correlation_id is not None else ctx.correlation_id,
            emitter_node_id=ctx.emitter_node_id,
            emitter_node_kind=ctx.emitter_node_kind,
            deps=ctx.deps,
            resources=resources if resources is not None else {},
        )

    @classmethod
    def from_envelope(
        cls,
        envelope: Envelope,
        output_type: type[Any] = _UNSET,
        *,
        correlation_id: str,
        strict: bool = True,
        type_adapter: TypeAdapter[Any] | None = None,
        resources: Mapping[str, Any] | None = None,
    ) -> InvocationResult[Any]:
        """Project an ``Envelope`` into a ``InvocationResult``.

        A thin convenience over :meth:`from_context` for callers holding a full
        envelope whose ``context`` was stamped in place (the client reply
        dispatcher and the consumer handler). See :meth:`from_context` for the
        argument and exception contract; ``correlation_id`` is required here
        because the client always sources it explicitly from the transport.
        """
        return cls.from_context(
            envelope.context,
            output_type,
            correlation_id=correlation_id,
            strict=strict,
            type_adapter=type_adapter,
            resources=resources,
        )

    @property
    def message_history(self) -> list[ModelMessage]:
        """Convenience: ``state.message_history``."""
        return self.state.message_history

    @property
    def metadata(self) -> Any:
        """Convenience: ``state.metadata``."""
        return self.state.metadata


def project_output(
    reply: ReturnMessage | None, output_type: type[Any] = _UNSET, *, strict: bool, type_adapter: TypeAdapter[Any] | None = None
) -> Any:
    """Project the deserialized output from the delivery's reply slot (spec §4.5).

    Shared by :meth:`InvocationResult.from_context` (client, ``strict=True``) and
    :meth:`ConsumerContext.from_run_context` (consumer, ``strict=False``). With
    ``strict=False`` an empty/absent reply (an intermediate hop) yields ``None``;
    otherwise the matching part is extracted/validated per ``output_type`` (raising
    ``DeserializationError``/``ValidationError`` on a present-but-mismatched part).
    """
    parts = reply.parts if reply is not None else []
    if not parts and not strict:
        return None
    return _extract_output(parts, output_type, type_adapter=type_adapter)


def _extract_output(parts: list[Any], output_type: type[Any], type_adapter: TypeAdapter[Any] | None = None) -> Any:
    """Extract and optionally deserialize the output from content parts."""
    if output_type is _UNSET:
        return _extract_auto(parts)
    if output_type is str:
        return _extract_text(parts)
    return _extract_data(parts, output_type, type_adapter=type_adapter)


def _extract_auto(parts: list[Any]) -> Any:
    """Auto-detect: prefer DataPart.data, fall back to TextPart.text."""
    for part in parts:
        if isinstance(part, DataPart):
            return part.data
    for part in parts:
        if isinstance(part, TextPart):
            return part.text
    raise DeserializationError("No DataPart or TextPart found in reply.parts; cannot auto-detect output.")


def _extract_text(parts: list[Any]) -> str:
    """Extract the first TextPart.text."""
    for part in parts:
        if isinstance(part, TextPart):
            return part.text
    raise DeserializationError("No TextPart found in reply.parts; expected output_type=str.")


def _extract_data(parts: list[Any], output_type: type[Any], type_adapter: TypeAdapter[Any] | None = None) -> Any:
    """Extract the first DataPart.data and validate via TypeAdapter.

    Uses *type_adapter* if provided; otherwise constructs a new one (which may
    raise :class:`pydantic.PydanticSchemaGenerationError` if *output_type* is
    unschematizable).
    """
    for part in parts:
        if isinstance(part, DataPart):
            adapter = type_adapter if type_adapter is not None else TypeAdapter(output_type)
            return adapter.validate_python(part.data)
    raise DeserializationError(f"No DataPart found in reply.parts; expected output_type={getattr(output_type, '__name__', str(output_type))}.")
