"""§8.0 — RegistryMixin wired into BaseNodeDef.

A node subclass collects its ``@handler``-decorated methods (RegistryMixin's
``__init_subclass__`` runs) *and* validates routes (BaseNodeDef's own
``__init_subclass__`` runs ``_validate_routes``) — the two cooperate via ``super()``.
``run`` is the inherited ``@handler('*')`` catch-all, so it is registered alongside
the node's own routes.
"""

from typing import Any

from calfkit._registry import handler
from calfkit.models import Silent
from calfkit.models.session_context import SessionRunContext
from calfkit.nodes.node import NodeDef


def test_node_subclass_collects_handlers_and_run_is_registered_as_star() -> None:
    class GreeterNode(NodeDef):
        @handler("greet")
        async def on_greet(self, ctx: SessionRunContext) -> Any:
            return Silent()

        async def run(self, ctx: SessionRunContext) -> Any:
            return Silent()

    # RegistryMixin.__init_subclass__ ran on the node → handler collected.
    assert "greet" in GreeterNode.routes()
    # BaseNodeDef.run is the inherited @handler('*') catch-all → registered too
    # (cooperative super() across the two __init_subclass__ hooks).
    assert "*" in GreeterNode.routes()
    assert GreeterNode._handlers["*"] == "run"
