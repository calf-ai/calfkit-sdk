"""Message utility functions for calf SDK.

This module provides utilities for working with pydantic_ai ModelMessage types,
including message history manipulation and transformation.
"""

from calfkit._vendor.pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    RetryPromptPart,
    SystemPromptPart,
    ToolCallPart,
    ToolReturnPart,
)


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


def append_system_prompt(
    messages: list[ModelMessage],
    addition: str,
) -> list[ModelMessage]:
    """Append a system prompt part to the message history.

    Iterates the message list in reverse to find the last SystemPromptPart
    and inserts a new SystemPromptPart immediately after it. This ensures
    the addition is always the final system prompt part in the history.

    If no SystemPromptPart exists, a new ModelRequest is prepended.

    Args:
        messages: The message history.
        addition: The system prompt text to add.

    Returns:
        A new message list with the system prompt addition applied.
    """
    result = list(messages)

    for i in range(len(result) - 1, -1, -1):
        msg = result[i]
        if not isinstance(msg, ModelRequest):
            continue
        for j in range(len(msg.parts) - 1, -1, -1):
            if isinstance(msg.parts[j], SystemPromptPart):
                new_parts = list(msg.parts)
                new_parts.insert(j + 1, SystemPromptPart(addition))
                result[i] = ModelRequest(parts=new_parts)
                return result

    return [ModelRequest(parts=[SystemPromptPart(addition)])] + result


def validate_tool_call_pairs(messages: list[ModelMessage]) -> bool:
    """Validate that all tool calls have corresponding tool results.

    Iterates through messages in reverse order to verify that every ToolCallPart
    has a matching ToolReturnPart or RetryPromptPart with the same tool_call_id.

    The first time a tool call is found without a matching result, the function
    returns False immediately.

    Args:
        messages: List of ModelMessage to validate.

    Returns:
        True if all tool calls have matching results, False otherwise.
    """
    seen_result_ids: set[str] = set()

    for message in reversed(messages):
        if isinstance(message, ModelRequest):
            for req_part in message.parts:
                if isinstance(req_part, (ToolReturnPart, RetryPromptPart)):
                    seen_result_ids.add(req_part.tool_call_id)
        elif isinstance(message, ModelResponse):
            for resp_part in message.parts:
                if isinstance(resp_part, ToolCallPart):
                    if resp_part.tool_call_id not in seen_result_ids:
                        return False

    return True
