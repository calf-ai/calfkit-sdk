"""Message utility functions for calf SDK.

This module provides utilities for working with pydantic_ai ModelMessage types,
including message history manipulation and transformation.
"""

from pydantic_ai import ModelMessage, ModelRequest, ModelResponse, SystemPromptPart


def patch_system_prompts(
    base: list[ModelMessage],
    incoming: list[ModelMessage],
) -> list[ModelMessage]:
    """Patch system prompts in message history.

    If incoming messages contain system prompts, they replace any existing
    system prompts in base. System prompts are consolidated and placed at
    the front of the history.

    If incoming has no system prompts, returns base unmodified.

    Args:
        base: The existing message history to patch.
        incoming: The new messages that may contain replacement system prompts.

    Returns:
        A new message history with system prompts patched.

    Examples:
        >>> from pydantic_ai import ModelRequest, SystemPromptPart
        >>> base = [ModelRequest(parts=[SystemPromptPart("old system")])]
        >>> incoming = [ModelRequest(parts=[SystemPromptPart("new system")])]
        >>> result = patch_system_prompts(base, incoming)
        >>> len(result)
        1
        >>> result[0].parts[0].content
        'new system'
    """
    incoming_system_parts: list[SystemPromptPart] = []
    for msg in incoming:
        if isinstance(msg, ModelResponse):
            continue
        for part in msg.parts:
            if isinstance(part, SystemPromptPart):
                incoming_system_parts.append(part)

    if not incoming_system_parts:
        return base

    system_msg = ModelRequest(parts=incoming_system_parts)
    result: list[ModelMessage] = []
    for msg in base:
        if isinstance(msg, ModelRequest):
            non_system_parts = [p for p in msg.parts if not isinstance(p, SystemPromptPart)]
            if non_system_parts:
                result.append(ModelRequest(parts=non_system_parts))
        else:
            result.append(msg)

    return [system_msg] + result
