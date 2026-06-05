"""Small importable nodes module for ``calfkit topics provision`` CLI tests.

The CLI resolves ``--nodes module:attr``; these attrs give it a single node,
a list of nodes, and a list that mixes in an ``McpServer`` (which the command
must warn-and-skip).
"""

from __future__ import annotations

from calfkit.mcp._server import McpServer
from calfkit.nodes.tool import ToolNodeDef


def _tool_node(name: str, sub: str, pub: str) -> ToolNodeDef:
    def _fn(x: int) -> int:
        return x

    _fn.__name__ = name
    return ToolNodeDef.create_tool_node(func=_fn, subscribe_topics=sub, publish_topic=pub)


# A single node (attr resolves to one BaseNodeDef, not a collection).
single = _tool_node("echo", sub="echo.in", pub="echo.out")

# A collection of nodes.
nodes = [
    _tool_node("alpha", sub="alpha.in", pub="alpha.out"),
    _tool_node("beta", sub="beta.in", pub="beta.out"),
]

# A collection that mixes in an McpServer — the CLI must skip it with a note.
# Constructing via ``stdio`` does NOT open a session (no subprocess spawned),
# so this is safe to import in a unit test.
mixed = [
    _tool_node("gamma", sub="gamma.in", pub="gamma.out"),
    McpServer.stdio("echo", name="some_mcp", tools=[]),
]
