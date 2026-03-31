from __future__ import annotations

from typing import TYPE_CHECKING

from uuid_utils import uuid7

if TYPE_CHECKING:
    from calfkit.models import Payload, ToolCallPart


def generate_payload_id() -> str:
    """Generate a unique, time sortable payload id"""
    return f"payload_{uuid7().hex}"


def find_first_tool_call_part(payload: Payload) -> ToolCallPart | None:
    """Finds and returns the first tool call part found in the payload

    Args:
        payload (Payload): The payload to search for a ToolCallPart.

    Returns:
        ToolCallPart | None: The first ToolCallPart found, or None.
    """
    from calfkit.models import ToolCallPart as _ToolCallPart

    parts = payload.parts
    for part in parts:
        if isinstance(part, _ToolCallPart):
            return part
    return None
