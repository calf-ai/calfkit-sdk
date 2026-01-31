"""Message utilities for calf SDK.

This module provides utilities for working with pydantic_ai ModelMessage types,
including message history manipulation and transformation.
"""

from .util import patch_system_prompts

__all__ = ["patch_system_prompts"]
