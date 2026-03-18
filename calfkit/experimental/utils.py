from uuid_utils import uuid7

from calfkit.experimental.payload_model import Payload, ToolCallPart


def generate_payload_id() -> str:
    """Generate a unique, time sortable payload id"""
    return f"payload_{uuid7().hex}"


def find_first_tool_call_part(payload: Payload) -> ToolCallPart | None:
    """Finds and returns the first tool call part found in the payload

    Args:
        payload (Payload): _description_

    Returns:
        ToolCallPart | None: _description_
    """
    parts = payload.parts
    for part in parts:
        if isinstance(part, ToolCallPart):
            return part
    return None
