from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import TypeAdapter

from calfkit.client.node_result import NodeResult
from calfkit.exceptions import DeserializationError
from calfkit.models import DataPart, TextPart
from calfkit.models.envelope import Envelope

_UNSET: Any = object()


def deserialize_to_node_result(
    envelope: Envelope,
    output_type: type[Any] = _UNSET,
    *,
    correlation_id: str,
    strict: bool = True,
    type_adapter: TypeAdapter[Any] | None = None,
    resources: Mapping[str, Any] | None = None,
) -> NodeResult[Any]:
    """Project an ``Envelope`` into a client-facing ``NodeResult``.

    Args:
        envelope: The raw wire-format envelope returned by the agent node.
        correlation_id: The transport ``correlation_id`` of the inbound message
            (the key the reply future resolved on, or the consumer handler's
            ``Context()`` value). Surfaced as ``NodeResult.correlation_id`` —
            sourced from the transport, never from the envelope body.
        output_type: The expected Python type for the deserialized output.

            * **not provided** (``_UNSET``): auto-detect — prefer ``DataPart.data``
              (returned as a raw dict), fall back to ``TextPart.text`` (str).
            * ``str``: extract the first ``TextPart.text``.
            * **anything else**: extract the first ``DataPart.data`` and validate
              it through ``TypeAdapter(output_type)`` (or *type_adapter* if
              provided).
        strict: When ``True`` (default — client semantics), raises
            :class:`DeserializationError` if ``final_output_parts`` is empty or
            doesn't contain the expected part type. When ``False`` (consumer
            semantics), returns ``output=None`` for an empty
            ``final_output_parts`` (intermediate hop / tool completion);
            validation errors on *present* parts still propagate.
        type_adapter: An optional pre-built :class:`pydantic.TypeAdapter` to
            use for validating ``DataPart.data`` against *output_type*. When
            ``None`` (default), a new adapter is constructed per call.
            Consumers pre-build at wiring time so schema-generation errors
            surface once at construction rather than per envelope.
        resources: The consuming node's lifecycle-managed resources, surfaced as
            ``NodeResult.resources`` (read-only by type). Stamped by the consumer
            handler with a shallow copy of the node's resource bag. Defaults to
            an empty mapping.

    Returns:
        A ``NodeResult`` whose ``.output`` is typed according to *output_type*
        (or ``None`` when ``strict=False`` and no parts are present).

    Raises:
        DeserializationError: If the expected content part is not found in
            ``final_output_parts`` (and either ``strict=True`` or the parts
            list is non-empty but lacks the expected shape).
        pydantic.ValidationError: If ``output_type`` is provided and the
            matching ``DataPart.data`` doesn't validate against it.
        pydantic.PydanticSchemaGenerationError: If ``type_adapter`` is ``None``
            and ``output_type`` cannot be schematized by :class:`TypeAdapter`.
    """
    state = envelope.context.state

    if not state.final_output_parts and not strict:
        output: Any = None
    else:
        output = _extract_output(state.final_output_parts, output_type, type_adapter=type_adapter)

    return NodeResult(
        output=output,
        state=state,
        correlation_id=correlation_id,
        emitter_node_id=envelope.context.emitter_node_id,
        emitter_node_kind=envelope.context.emitter_node_kind,
        deps=envelope.context.deps,
        resources=resources if resources is not None else {},
    )


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
    raise DeserializationError("No DataPart or TextPart found in final_output_parts; cannot auto-detect output.")


def _extract_text(parts: list[Any]) -> str:
    """Extract the first TextPart.text."""
    for part in parts:
        if isinstance(part, TextPart):
            return part.text
    raise DeserializationError("No TextPart found in final_output_parts; expected output_type=str.")


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
    raise DeserializationError(f"No DataPart found in final_output_parts; expected output_type={getattr(output_type, '__name__', str(output_type))}.")
