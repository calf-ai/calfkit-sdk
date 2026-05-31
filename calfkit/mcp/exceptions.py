"""Exception hierarchy for the calfkit MCP adaptor.

The taxonomy distinguishes:

- ``McpConfigError`` — user-visible misconfiguration (parsed at boot, never
  reached at runtime). Raised by ``_config.py`` (mcp.json parse failures,
  unset ``$VAR`` substitutions) and by ``McpServer`` constructor validation.

- ``McpTransportError`` — operator-visible runtime failure (subprocess crash,
  HTTP connect refused, MCP ``initialize`` timeout). Maps to ``FailedToolCall``
  at the bridge boundary so agents raise ``ToolExecutionError`` (operators
  see it, the LLM does not get to retry it).

- ``McpProtocolError`` — server returned a well-formed MCP error response
  (``MCPError`` from ``call_tool``, ``InitializeResult`` capability mismatch).
  Also maps to ``FailedToolCall``.

- ``McpToolDriftError`` — reserved for the v1.1 strict-mode opt-in where the
  bridge hard-fails on declared-vs-server tool drift. v1 logs a warning
  rather than raising (see ``docs/mcp-v1-plan.md`` §11 Q7).

Tool-semantic errors (``CallToolResult.is_error=True``) do NOT raise here —
they are returned as ``RetryPromptPart`` content by ``_adapt.py`` so the LLM
can retry. See ``docs/mcp-adaptor-design.md`` §8 and §9.

The base class is named :class:`CalfkitMcpError` (not ``McpError``) so it
does not collide with the MCP SDK's ``mcp.shared.exceptions.McpError``
that is raised by ``ClientSession.call_tool`` on JSON-RPC failures. A user
catching the calfkit base will not accidentally swallow SDK protocol errors.
"""

from __future__ import annotations


class CalfkitMcpError(Exception):
    """Base class for all calfkit MCP adaptor exceptions.

    Distinct from ``mcp.shared.exceptions.McpError`` (the SDK's JSON-RPC
    error type) — catching this base does NOT catch SDK errors raised by
    ``call_tool``. Operator code that wants to handle the SDK's protocol
    errors should ``except mcp.shared.exceptions.McpError`` directly.
    """


class McpConfigError(CalfkitMcpError):
    """User-visible misconfiguration. Raised at parse / construction time."""


class McpTransportError(CalfkitMcpError):
    """Transport-layer failure (subprocess crash, HTTP connect refused, timeout)."""


class McpProtocolError(CalfkitMcpError):
    """MCP protocol-layer failure (server-side MCPError, capability mismatch)."""


class McpToolDriftError(CalfkitMcpError):
    """Reserved for v1.1 strict-mode opt-in.

    The bridge worker would raise this if a tool declared in ``tools=`` is not
    advertised by the MCP server's ``tools/list``. v1 only logs a warning;
    this class is defined now so the strict-mode upgrade is purely additive.
    """
