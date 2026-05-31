"""calfkit MCP adaptor — see ``docs/mcp-v1-plan.md``.

Public exports:

- ``McpServer`` — polymorphic server reference (works in both
  ``Worker(nodes=[...])`` and ``Agent(tools=[...])``).
- ``McpToolDef`` — schema descriptor for one tool; produced by
  ``calfkit mcp codegen`` and accepted by ``McpServer(tools=[...])``.
- ``McpToolAnnotations`` — annotation hints carried by ``McpToolDef``.
- ``McpServers`` — Phase 6 ``mcp.json`` interop helper (not yet
  re-exported; see the impl plan).

The factory/sugar ``from calfkit import mcp`` lives at the calfkit
top level and is added in Phase 6.
"""

from calfkit.mcp._server import McpServer
from calfkit.mcp._tool_def import McpToolAnnotations, McpToolDef

__all__ = [
    "McpServer",
    "McpToolAnnotations",
    "McpToolDef",
]
