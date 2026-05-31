"""Unit tests for ``calfkit.mcp._adapt``.

Coverage focus:
- `adapt_call_tool_result` prefers structuredContent over text.
- Content-block flattening handles text, image, audio, resource_link,
  embedded_resource, and unknown types.
- `build_retry_prompt_from_error_result` produces RetryPromptPart with
  tool_name + tool_call_id and non-empty content (empty MCP content
  becomes a placeholder so the LLM has something to retry against).
- `classify_mcp_error` labels exceptions specifically: McpError(<code>),
  McpTimeout, fallback to exception class name.
- `_safe_str` survives broken `__str__`.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from mcp.shared.exceptions import McpError
from mcp.types import (
    AudioContent,
    BlobResourceContents,
    CallToolResult,
    EmbeddedResource,
    ErrorData,
    ImageContent,
    ResourceLink,
    TextContent,
    TextResourceContents,
)

from calfkit._vendor.pydantic_ai.messages import RetryPromptPart, ToolReturn
from calfkit.mcp._adapt import (
    _describe_content_block,
    _flatten_content,
    _flatten_content_as_string,
    _safe_str,
    adapt_call_tool_result,
    build_retry_prompt_from_error_result,
    classify_mcp_error,
)
from calfkit.models.state import FailedToolCall

# ---------------------------------------------------------------------------
# adapt_call_tool_result — happy path
# ---------------------------------------------------------------------------


def test_adapt_structured_content_wins() -> None:
    """structured_content is the primary surface when present; text is dropped."""
    result = CallToolResult(
        content=[TextContent(type="text", text="human-friendly summary")],
        structuredContent={"id": "abc", "score": 0.9},
        isError=False,
    )
    tr = adapt_call_tool_result(result, tool_call_id="tc-1")

    assert isinstance(tr, ToolReturn)
    assert tr.return_value == {"id": "abc", "score": 0.9}
    assert tr.metadata == {"tool_call_id": "tc-1", "mcp_meta": None, "is_error": False}


def test_adapt_text_fallback_when_no_structured() -> None:
    """Without structured_content, the text blocks are flattened."""
    result = CallToolResult(
        content=[TextContent(type="text", text="hello")],
        isError=False,
    )
    tr = adapt_call_tool_result(result, tool_call_id="tc-2")
    assert tr.return_value == "hello"


def test_adapt_empty_content() -> None:
    """No content + no structured → empty string."""
    result = CallToolResult(content=[], isError=False)
    tr = adapt_call_tool_result(result, tool_call_id="tc-3")
    assert tr.return_value == ""


def test_adapt_multiple_text_blocks_returns_list() -> None:
    """Multiple text blocks preserve ordering as a list."""
    result = CallToolResult(
        content=[
            TextContent(type="text", text="first"),
            TextContent(type="text", text="second"),
        ],
        isError=False,
    )
    tr = adapt_call_tool_result(result, tool_call_id="tc-4")
    assert tr.return_value == ["first", "second"]


def test_adapt_mcp_meta_passed_through_to_metadata() -> None:
    """meta field on CallToolResult flows into ToolReturn.metadata for audit."""
    result = CallToolResult(
        content=[TextContent(type="text", text="ok")],
        isError=False,
        _meta={"trace_id": "xyz"},
    )
    tr = adapt_call_tool_result(result, tool_call_id="tc-5")
    assert tr.metadata is not None
    assert tr.metadata["mcp_meta"] == {"trace_id": "xyz"}


def test_adapt_rejects_error_result() -> None:
    """isError=True is the caller's responsibility — adapt_call_tool_result
    must NOT silently coerce it. The bridge should branch on isError.
    """
    result = CallToolResult(
        content=[TextContent(type="text", text="permission denied")],
        isError=True,
    )
    with pytest.raises(ValueError, match="isError=True"):
        adapt_call_tool_result(result, tool_call_id="tc-err")


# ---------------------------------------------------------------------------
# Content block descriptors
# ---------------------------------------------------------------------------


def test_describe_text_block() -> None:
    assert _describe_content_block(TextContent(type="text", text="hello")) == "hello"


def test_describe_image_block() -> None:
    blk = ImageContent(type="image", data="base64-blob", mimeType="image/png")
    assert _describe_content_block(blk) == "[image: image/png]"


def test_describe_audio_block() -> None:
    blk = AudioContent(type="audio", data="base64-blob", mimeType="audio/wav")
    assert _describe_content_block(blk) == "[audio: audio/wav]"


def test_describe_resource_link() -> None:
    blk = ResourceLink(type="resource_link", name="doc.pdf", uri="file:///doc.pdf")
    assert _describe_content_block(blk) == "[resource: file:///doc.pdf]"


def test_describe_embedded_resource_text() -> None:
    """Embedded text resources surface as their text content."""
    res = TextResourceContents(uri="resource://greeting", text="hello world", mimeType="text/plain")
    blk = EmbeddedResource(type="resource", resource=res)
    assert _describe_content_block(blk) == "hello world"


def test_describe_embedded_resource_blob() -> None:
    """Embedded blob resources surface as a placeholder with URI and MIME."""
    res = BlobResourceContents(uri="resource://attach.bin", blob="base64-blob", mimeType="application/octet-stream")
    blk = EmbeddedResource(type="resource", resource=res)
    assert _describe_content_block(blk) == "[resource: resource://attach.bin (application/octet-stream)]"


class _UnknownBlock:
    """Future content type we don't recognise yet."""

    type = "video"


def test_describe_unknown_block_fallback() -> None:
    """Future MCP content types get a typed placeholder, not an exception."""
    assert _describe_content_block(_UnknownBlock()) == "[unknown content: video]"


# ---------------------------------------------------------------------------
# Flatten helpers
# ---------------------------------------------------------------------------


def test_flatten_content_empty() -> None:
    assert _flatten_content([]) == ""


def test_flatten_content_single() -> None:
    assert _flatten_content([TextContent(type="text", text="solo")]) == "solo"


def test_flatten_content_multiple() -> None:
    blocks = [
        TextContent(type="text", text="a"),
        TextContent(type="text", text="b"),
    ]
    assert _flatten_content(blocks) == ["a", "b"]


def test_flatten_content_as_string_joins_with_newlines() -> None:
    blocks = [
        TextContent(type="text", text="line 1"),
        TextContent(type="text", text="line 2"),
    ]
    assert _flatten_content_as_string(blocks) == "line 1\nline 2"


def test_flatten_content_as_string_drops_empty_parts() -> None:
    """Empty descriptions don't add blank lines (don't yield "line 1\\n\\nline 2")."""
    # No way to get an empty description from real content types, but the
    # join logic skips falsy values defensively.
    assert _flatten_content_as_string([]) == ""


# ---------------------------------------------------------------------------
# build_retry_prompt_from_error_result
# ---------------------------------------------------------------------------


def test_error_result_to_retry_prompt() -> None:
    result = CallToolResult(
        content=[TextContent(type="text", text="permission denied: needs scope X")],
        isError=True,
    )
    rp = build_retry_prompt_from_error_result(result, tool_name="gmail.send", tool_call_id="tc-x")

    assert isinstance(rp, RetryPromptPart)
    assert rp.tool_name == "gmail.send"
    assert rp.tool_call_id == "tc-x"
    assert "permission denied" in str(rp.content)


def test_error_result_with_empty_content_uses_placeholder() -> None:
    """An error result with no content blocks still produces a usable retry
    prompt — empty content would leave the LLM with nothing to act on.
    """
    result = CallToolResult(content=[], isError=True)
    rp = build_retry_prompt_from_error_result(result, tool_name="t", tool_call_id="tc-y")
    assert isinstance(rp, RetryPromptPart)
    assert rp.content  # non-empty
    assert "empty error" in str(rp.content)


# ---------------------------------------------------------------------------
# classify_mcp_error
# ---------------------------------------------------------------------------


def test_classify_mcp_error_carries_code() -> None:
    """McpError surfaces with its JSON-RPC error code for operator triage."""
    err = McpError(ErrorData(code=-32603, message="internal error", data=None))
    ftc = classify_mcp_error(err, tool_name="t", tool_call_id="tc-a")
    assert isinstance(ftc, FailedToolCall)
    assert ftc.tool_name == "t"
    assert ftc.tool_call_id == "tc-a"
    assert ftc.exc_type == "McpError(-32603)"
    assert "internal error" in ftc.exc_message


def test_classify_asyncio_timeout() -> None:
    """asyncio.TimeoutError labels as McpTimeout for practical operator meaning."""
    ftc = classify_mcp_error(asyncio.TimeoutError(), tool_name="t", tool_call_id="tc-b")
    assert ftc.exc_type == "McpTimeout"


def test_classify_generic_exception_fallback() -> None:
    """Unknown exceptions get their class name as the label."""
    ftc = classify_mcp_error(RuntimeError("boom"), tool_name="t", tool_call_id="tc-c")
    assert ftc.exc_type == "RuntimeError"
    assert ftc.exc_message == "boom"


def test_classify_handles_mcp_error_without_error_attr() -> None:
    """Defensive: future SDK versions might restructure .error.

    Use a subclass that hides .error so the fallback path runs.
    """

    class StubMcpError(McpError):
        def __init__(self) -> None:
            # bypass parent __init__ to deliberately omit .error
            Exception.__init__(self, "stub")

        @property
        def error(self) -> Any:  # type: ignore[override]
            raise AttributeError("no error attr in this SDK version")

    # Falls back to "McpError" without code
    ftc = classify_mcp_error(StubMcpError(), tool_name="t", tool_call_id="tc-d")
    assert ftc.exc_type == "McpError"


# ---------------------------------------------------------------------------
# _safe_str
# ---------------------------------------------------------------------------


def test_safe_str_normal_exception() -> None:
    assert _safe_str(ValueError("hello")) == "hello"


def test_safe_str_broken_str() -> None:
    """If __str__ raises, fall back to repr; if repr raises, last-resort fallback."""

    class BrokenStrError(Exception):
        def __str__(self) -> str:
            raise RuntimeError("broken str")

    result = _safe_str(BrokenStrError())
    # repr() still works on the exception
    assert "BrokenStrError" in result


def test_safe_str_broken_str_and_repr() -> None:
    """Both __str__ AND __repr__ broken — last-resort fallback fires."""

    class FullyBrokenError(Exception):
        def __str__(self) -> str:
            raise RuntimeError("no str")

        def __repr__(self) -> str:
            raise RuntimeError("no repr")

    assert _safe_str(FullyBrokenError()) == "<unprintable FullyBrokenError>"


# ---------------------------------------------------------------------------
# JSON-safety of adapted return values
# ---------------------------------------------------------------------------


def test_adapted_return_value_is_json_safe() -> None:
    """The ToolReturn.return_value must be JSON-serializable so it survives
    the Kafka envelope wire (see existing pattern in calfkit/nodes/tool.py:121).
    """
    result = CallToolResult(
        content=[TextContent(type="text", text="hi")],
        structuredContent={"k": "v", "n": 1, "list": [1, 2]},
        isError=False,
    )
    tr = adapt_call_tool_result(result, tool_call_id="tc-json")
    # Must survive JSON round-trip
    json.dumps(tr.return_value)
