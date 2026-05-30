"""MCP ↔ calfkit type adapters.

Two responsibilities:

1. **Tool result mapping** — translate ``mcp.types.CallToolResult`` into the
   calfkit-side shape that ``McpBridge.run`` (Phase 3) will store via
   ``State.add_tool_result``. Maps:

   - ``isError=True`` → ``RetryPromptPart`` (LLM-visible, retryable).
   - ``isError=False`` → ``ToolReturn`` with ``structuredContent`` if present,
     else concatenated text. See ``docs/mcp-adaptor-design.md`` §9.

2. **Error classification** — translate raised MCP / transport exceptions
   into ``FailedToolCall`` markers (operator-visible, NOT LLM-retryable),
   consistent with the existing native-tool failure path at
   ``calfkit/nodes/tool.py:138-184``. See design doc §8.

The distinction is load-bearing:

- ``isError`` = the tool *ran* and *reported* an error (typed feedback for
  the LLM) → RetryPromptPart.
- Raised exception = something prevented the tool from running cleanly
  (connection lost, timeout, server crash, serialization) → FailedToolCall.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from mcp.shared.exceptions import McpError
from mcp.types import (
    AudioContent,
    CallToolResult,
    EmbeddedResource,
    ImageContent,
    ResourceLink,
    TextContent,
)

from calfkit._vendor.pydantic_ai.messages import RetryPromptPart, ToolReturn
from calfkit.models.state import FailedToolCall

if TYPE_CHECKING:
    from collections.abc import Iterable

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Tool result adaptation
# ---------------------------------------------------------------------------


def adapt_call_tool_result(result: CallToolResult, *, tool_call_id: str) -> ToolReturn:
    """Map a successful MCP ``CallToolResult`` into a calfkit ``ToolReturn``.

    Preference order for the LLM-facing return value:

    1. ``structured_content`` if present (dict, matches the tool's
       declared ``outputSchema``). This is the typed primary surface.
    2. Otherwise, the concatenated text content blocks.
    3. Non-text content blocks (image/audio/resource) are summarised as
       placeholder strings — v1 does not pass multi-modal content to the
       LLM. The placeholder is honest about the loss; the original ``meta``
       and structured content stay on the ``ToolReturn.metadata``.

    Must NOT be called with ``result.isError=True`` — that case maps to
    :func:`build_retry_prompt_from_error_result` instead. The caller (the
    Phase 3 bridge) is responsible for branching.
    """
    if result.isError:
        raise ValueError(
            "adapt_call_tool_result called with isError=True; caller must dispatch error results to build_retry_prompt_from_error_result()"
        )

    if result.structuredContent is not None:
        return_value: Any = result.structuredContent
    else:
        return_value = _flatten_content(result.content)

    return ToolReturn(
        return_value=return_value,
        metadata={
            "tool_call_id": tool_call_id,
            "mcp_meta": result.meta,
            "is_error": False,
        },
    )


def build_retry_prompt_from_error_result(
    result: CallToolResult,
    *,
    tool_name: str,
    tool_call_id: str,
) -> RetryPromptPart:
    """Map an MCP ``isError=True`` result into a ``RetryPromptPart``.

    The MCP server ran the tool and returned a structured error response —
    typed feedback for the LLM to act on. We surface the concatenated text
    content as the retry prompt so the LLM can adjust and re-call.
    """
    content_str = _flatten_content_as_string(result.content)
    if not content_str:
        content_str = "(empty error from MCP server)"
    return RetryPromptPart(
        content=content_str,
        tool_name=tool_name,
        tool_call_id=tool_call_id,
    )


# ---------------------------------------------------------------------------
# Content-block flattening
# ---------------------------------------------------------------------------


def _flatten_content(blocks: Iterable[Any]) -> str | list[Any]:
    """Flatten an MCP content-block list to a value the LLM can consume.

    Returns:
        - Empty string if no blocks
        - A single string if exactly one block (typical case)
        - A list of strings if multiple blocks (preserves ordering for the LLM)
    """
    parts: list[str] = []
    for b in blocks:
        parts.append(_describe_content_block(b))
    if not parts:
        return ""
    if len(parts) == 1:
        return parts[0]
    return parts


def _flatten_content_as_string(blocks: Iterable[Any]) -> str:
    """Same as :func:`_flatten_content` but always returns a string.

    Used for error-result content where the LLM expects a string-shaped
    retry prompt.
    """
    parts = [_describe_content_block(b) for b in blocks]
    return "\n".join(p for p in parts if p)


def _describe_content_block(block: Any) -> str:
    """Render one MCP content block as a string the LLM can read.

    Non-text content (image/audio/resource) is summarised as a placeholder.
    v1 does not pass multi-modal payloads to providers — see design doc §9
    for the rationale and roadmap.
    """
    if isinstance(block, TextContent):
        return block.text
    if isinstance(block, ImageContent):
        return f"[image: {block.mimeType}]"
    if isinstance(block, AudioContent):
        return f"[audio: {block.mimeType}]"
    if isinstance(block, ResourceLink):
        return f"[resource: {block.uri}]"
    if isinstance(block, EmbeddedResource):
        res = block.resource
        # TextResourceContents has .text; BlobResourceContents has .blob.
        text = getattr(res, "text", None)
        if isinstance(text, str):
            return text
        uri = getattr(res, "uri", "<unknown>")
        mime = getattr(res, "mimeType", "application/octet-stream")
        return f"[resource: {uri} ({mime})]"
    # Defensive fallback for future content types we don't know about.
    block_type = getattr(block, "type", type(block).__name__)
    return f"[unknown content: {block_type}]"


# ---------------------------------------------------------------------------
# Error classification
# ---------------------------------------------------------------------------


def classify_mcp_error(
    exc: BaseException,
    *,
    tool_name: str,
    tool_call_id: str,
) -> FailedToolCall:
    """Map a raised MCP / transport exception to a ``FailedToolCall`` marker.

    All MCP-side failures from this function are **operator-visible** —
    they will eventually raise as ``ToolExecutionError`` at the agent
    (matching the existing native-tool path at ``calfkit/nodes/agent.py:238-255``).

    LLM-retryable errors (``CallToolResult.isError=True``) do NOT come
    through here — they are handled by
    :func:`build_retry_prompt_from_error_result` directly in the bridge.

    Returns:
        A typed ``FailedToolCall`` instance ready to be stored via
        ``State.add_tool_result``.
    """
    exc_type, exc_message = _classify_exc_typename(exc)
    return FailedToolCall(
        tool_name=tool_name,
        tool_call_id=tool_call_id,
        exc_type=exc_type,
        exc_message=exc_message,
    )


def _classify_exc_typename(exc: BaseException) -> tuple[str, str]:
    """Pick the right ``(exc_type, exc_message)`` label for a raised exception.

    The label is operator-facing and goes into logs / metrics, so it
    should be specific enough to triage:

    - ``McpError`` carries a JSON-RPC error code — surface as
      ``McpError(<code>)`` so e.g. timeouts and method-not-found are
      visually distinct in tracing systems.
    - ``asyncio.TimeoutError`` becomes ``McpTimeout`` (the practical
      meaning).
    - Anything else falls through to the exception's class name.
    """
    if isinstance(exc, McpError):
        # mcp's MCPError carries .error: ErrorData with .code and .message.
        # Defensive getattr: future SDK versions might restructure.
        err = getattr(exc, "error", None)
        if err is not None:
            code = getattr(err, "code", "?")
            msg = getattr(err, "message", None) or str(exc)
            return f"McpError({code})", str(msg)
        return "McpError", _safe_str(exc)
    if isinstance(exc, asyncio.TimeoutError):
        return "McpTimeout", _safe_str(exc) or "MCP call timed out"
    return type(exc).__name__, _safe_str(exc)


def _safe_str(exc: BaseException) -> str:
    """Best-effort string of an exception, robust against broken ``__str__``.

    Mirrors ``calfkit/nodes/tool.py::_safe_exc_message`` — a bare ``str(e)``
    can itself raise inside our ``except`` handler if the exception's
    ``__str__`` is broken; we must never let that happen on the failure
    path because the silent-hang failure mode this code exists to prevent
    is exactly "the worker dies trying to construct the failure reply."
    """
    try:
        return str(exc)
    except Exception:
        try:
            return repr(exc)
        except Exception:
            return f"<unprintable {type(exc).__name__}>"
