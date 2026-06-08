"""§8.0 — RegistryMixin wired into BaseNodeDef.

A node subclass must collect its ``@handler``-decorated methods (RegistryMixin's
``__init_subclass__`` runs) *and* still set ``_run_accepts_input`` (BaseNodeDef's
own ``__init_subclass__`` still runs) — i.e. the two cooperate via ``super()``.
"""

from typing import Any

from calfkit._registry import handler
from calfkit.models import Silent
from calfkit.models.session_context import SessionRunContext
from calfkit.nodes.node import NodeDef


def test_node_subclass_collects_handlers_and_still_sets_run_accepts_input() -> None:
    class GreeterNode(NodeDef):
        @handler("greet")
        async def on_greet(self, ctx: SessionRunContext) -> Any:
            return Silent()

        async def run(self, ctx: SessionRunContext) -> Any:
            return Silent()

    # RegistryMixin.__init_subclass__ ran on the node → handler collected.
    assert "greet" in GreeterNode.routes()
    # BaseNodeDef.__init_subclass__ still ran (cooperative super()) → introspection set.
    assert GreeterNode._run_accepts_input is False
