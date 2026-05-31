"""calfkit MCP adaptor — see ``docs/mcp-v1-plan.md``.

Public exports:

- ``McpServer`` — polymorphic server reference (works in both
  ``Worker(nodes=[...])`` and ``Agent(tools=[...])``).
- ``McpServers`` — dict-like collection loaded from an ``mcp.json``
  config (or in-memory dict) with separately-supplied schemas.
- ``McpToolDef`` — schema descriptor for one tool; produced by
  ``calfkit mcp codegen`` and accepted by ``McpServer(tools=[...])``.
- ``McpToolAnnotations`` — annotation hints carried by ``McpToolDef``.
- ``mcp`` — factory callable / namespace; see ``calfkit.mcp._factory``.

The factory is also re-exported at the calfkit top level as
``from calfkit import mcp`` for ergonomic compact-form imports.
"""

from calfkit.mcp._factory import McpServers, mcp
from calfkit.mcp._server import McpServer
from calfkit.mcp._tool_def import McpToolAnnotations, McpToolDef

__all__ = [
    "McpServer",
    "McpServers",
    "McpToolAnnotations",
    "McpToolDef",
    "mcp",
]
