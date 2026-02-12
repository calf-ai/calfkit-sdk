"""Message utilities for calf SDK.

This module provides utilities for working with pydantic_ai ModelMessage types,
including message history manipulation and transformation.
"""

from .utils import append_system_prompt, patch_system_prompts, validate_tool_call_pairs

__all__ = ["append_system_prompt", "patch_system_prompts", "validate_tool_call_pairs"]
