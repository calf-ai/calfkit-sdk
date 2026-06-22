"""Small importable nodes module for ``ck topics provision`` CLI tests.

The CLI resolves ``--nodes module:attr``; these attrs give it a single node
and a list of nodes.
"""

from __future__ import annotations

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

# An attr that resolves to a plain object that is NOT a node (no
# ``subscribe_topics`` / ``_return_topic``). The CLI must reject it with an
# actionable error rather than letting an AttributeError escape as exit 1.
not_a_node = object()

# An attr that is an empty iterable — resolves to zero nodes. ``ck run``
# must treat "nothing resolved" as an error (exit 2), not silently start an
# idle worker.
empty_list: list = []
