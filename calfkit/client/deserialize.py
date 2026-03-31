from __future__ import annotations

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
) -> NodeResult[Any]:
    """Project an ``Envelope`` into a client-facing ``NodeResult``.

    Args:
        envelope: The raw wire-format envelope returned by the agent node.
        output_type: The expected Python type for the deserialized output.

            * **not provided** (``_UNSET``): auto-detect — prefer ``DataPart.data``
              (returned as a raw dict), fall back to ``TextPart.text`` (str).
            * ``str``: extract the first ``TextPart.text``.
            * **anything else**: extract the first ``DataPart.data`` and validate
              it through ``TypeAdapter(output_type)``.

    Returns:
        A ``NodeResult`` whose ``.output`` is typed according to *output_type*.

    Raises:
        DeserializationError: If the expected content part is not found in
            ``final_output_parts``.
    """
    state = envelope.context.state
    output_parts = state.final_output_parts
    message_history = state.message_history
    metadata = state.metadata
    correlation_id = envelope.context.deps.correlation_id

    output = _extract_output(output_parts, output_type)

    return NodeResult(
        output=output,
        output_parts=output_parts,
        message_history=message_history,
        metadata=metadata,
        correlation_id=correlation_id,
    )


def _extract_output(parts: list[Any], output_type: type[Any]) -> Any:
    """Extract and optionally deserialize the output from content parts."""
    if output_type is _UNSET:
        return _extract_auto(parts)
    if output_type is str:
        return _extract_text(parts)
    return _extract_data(parts, output_type)


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


def _extract_data(parts: list[Any], output_type: type[Any]) -> Any:
    """Extract the first DataPart.data and validate via TypeAdapter."""
    for part in parts:
        if isinstance(part, DataPart):
            adapter: TypeAdapter[Any] = TypeAdapter(output_type)
            return adapter.validate_python(part.data)
    raise DeserializationError(f"No DataPart found in final_output_parts; expected output_type={getattr(output_type, '__name__', str(output_type))}.")
