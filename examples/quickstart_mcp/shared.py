"""Shared MCP server declaration — imported by both agent_service.py
and tools_service.py.

The same ``McpServer`` object plays two roles depending on the worker:
- In ``Worker(nodes=[everything])`` (tools_service) it expands into one
  ``McpBridge`` per declared tool and owns the MCP subprocess.
- In ``Agent(tools=[everything])`` (agent_service) it yields one
  ``BaseToolNodeSchema`` per tool for the agent's LLM-facing registry.

No subprocess is spawned at import time — that happens at Worker.run().
"""

from calfkit import mcp
from everything_schemas import Everything

everything = mcp("npx -y @modelcontextprotocol/server-everything", tools=Everything.ALL)
