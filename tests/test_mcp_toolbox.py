"""`calfkit.mcp`'s call-side surface after ADR-0045: the `MCPToolbox` alias and the node rewire.

Distributed agent hosts reference a toolbox by name with zero deployment knowledge
(no connection params, no secrets) — spelled `Toolboxes(...)`, with `MCPToolbox` a
code-level alias of the `Toolbox` entry spec for MCP-flavored call sites. The node's
own selector behavior calls the resolution kernel directly, so passing the node
eagerly and naming its Toolbox resolve identically.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from calfkit.models.capability import CapabilityRecord, CapabilityToolDef
from calfkit.models.tool_dispatch import ToolSelector


def make_record(toolbox_id: str = "github", tool_names: tuple[str, ...] = ("search", "create_issue")) -> CapabilityRecord:
    now = datetime.now(tz=timezone.utc)
    return CapabilityRecord(
        started_at=now,
        last_heartbeat_at=now,
        heartbeat_interval=30.0,
        node_kind="toolbox",
        dispatch_topic=f"mcp_server.{toolbox_id}",
        tools=[CapabilityToolDef(name=n, parameters_json_schema={"type": "object", "properties": {}}) for n in tool_names],
        content_updated_at=now,
    )


def make_toolbox(name: str = "github"):
    from calfkit.mcp.mcp_toolbox import MCPToolboxNode
    from calfkit.mcp.mcp_transport import StreamableHttpParameters

    return MCPToolboxNode(name, connection_params=StreamableHttpParameters(url="http://unused.local/mcp"))


class TestDirectConstruction:
    """The whole point of #212: constructible with zero deployment knowledge."""

    def test_resolves_by_name_only(self) -> None:
        from calfkit.mcp import MCPToolbox
        from calfkit.nodes.toolbox import Toolboxes

        result = Toolboxes(MCPToolbox("github")).resolve_tools({"github": make_record()})
        # C1: toolbox tools are namespaced <node_id>__<tool> for the LLM.
        assert [b.name for b in result.bindings] == ["github__search", "github__create_issue"]

    def test_include_kwarg_filters(self) -> None:
        from calfkit.mcp import MCPToolbox
        from calfkit.nodes.toolbox import Toolboxes

        entry = MCPToolbox("github", include=("search",))  # include uses BARE names (C5)
        result = Toolboxes(entry).resolve_tools({"github": make_record()})
        assert [b.name for b in result.bindings] == ["github__search"]

    def test_include_accepts_any_sequence_normalized_to_tuple(self) -> None:
        from calfkit.mcp import MCPToolbox

        ref = MCPToolbox("github", include=["search"])
        assert ref.include == ("search",)

    def test_frozen_and_hashable(self) -> None:
        from calfkit.mcp import MCPToolbox

        ref = MCPToolbox("github", include=("search",))
        with pytest.raises(Exception):
            ref.include = ("other",)  # type: ignore[misc]
        assert len({ref, MCPToolbox("github", include=("search",))}) == 1  # value semantics

    def test_is_not_a_tool_selector(self) -> None:
        # INVERTED post-ADR-0045: the alias is an entry spec, not a selector — the old
        # `tools=[MCPToolbox("gh")]` idiom fails loud with the wrap-it teaching error.
        from calfkit.mcp import MCPToolbox

        assert not isinstance(MCPToolbox("github"), ToolSelector)


class TestNodeParity:
    def test_select_is_gone_scoping_is_direct_construction(self) -> None:
        from calfkit.mcp import MCPToolbox

        assert not hasattr(make_toolbox(), "select")
        entry = MCPToolbox("github", include=("search",))
        assert entry.name == "github" and entry.include == ("search",)

    def test_toolbox_resolution_matches_naming_its_toolbox(self) -> None:
        from calfkit.nodes.toolbox import Toolboxes

        view: dict[str, Any] = {"github": make_record()}
        via_toolbox = make_toolbox().resolve_tools(view)
        via_selector = Toolboxes("github").resolve_tools(view)
        assert [b.name for b in via_toolbox.bindings] == [b.name for b in via_selector.bindings]
        assert via_toolbox == via_selector  # identical SelectorResult (frozen value): node and selector resolve the same

    def test_scoped_selector_is_gone(self) -> None:
        import calfkit.mcp.mcp_toolbox as mod

        assert not hasattr(mod, "_ScopedSelector")


class TestPackageSurface:
    def test_paired_exports(self) -> None:
        from calfkit.mcp import MCPToolbox, MCPToolboxNode, StdioServerParameters, StreamableHttpParameters  # noqa: F401


class TestDeployingARefFailsLoudAndEarly:
    def test_add_nodes_rejects_refs_with_teaching_message(self) -> None:
        from calfkit.client import Client
        from calfkit.mcp import MCPToolbox
        from calfkit.worker.worker import Worker

        worker = Worker(Client.connect("kafka:9092"))
        with pytest.raises(TypeError, match="MCPToolboxNode\\(") as excinfo:
            worker.add_nodes(MCPToolbox("github"))  # type: ignore[arg-type]
        assert "reference" in str(excinfo.value).lower()

    def test_add_nodes_rejects_arbitrary_non_nodes_too(self) -> None:
        from calfkit.client import Client
        from calfkit.worker.worker import Worker

        worker = Worker(Client.connect("kafka:9092"))
        with pytest.raises(TypeError):
            worker.add_nodes("not a node")  # type: ignore[arg-type]


class TestDispatchStripsToolboxPrefix:
    """C4/C6: ``MCPToolboxNode.run`` strips its own ``<node_id>__`` prefix so the MCP
    server is called with the BARE tool name — robust to a server tool name or a toolbox
    name that legitimately contains ``__``. The agent dispatches the namespaced name; the
    strip is local to the node, so the agent's dispatch path stays generic."""

    @pytest.mark.parametrize(
        ("toolbox_name", "advertised_name", "expected_server_name"),
        [
            ("github", "github__search", "search"),  # ordinary case
            ("my__server", "my__server__a__b", "a__b"),  # embedded __ in BOTH names (C6)
            ("github", "search", "search"),  # already bare -> removeprefix is a no-op
        ],
    )
    async def test_run_calls_server_with_bare_tool_name(self, toolbox_name: str, advertised_name: str, expected_server_name: str) -> None:
        from unittest.mock import AsyncMock, MagicMock

        from mcp import ClientSession
        from mcp.types import CallToolResult as MCPCallToolResult
        from mcp.types import TextContent

        from calfkit.mcp.mcp_toolbox import MCPToolboxNode
        from calfkit.mcp.mcp_transport import StreamableHttpParameters
        from calfkit.models.state import State
        from calfkit.models.tool_dispatch import ToolCallRef
        from tests.test_tool_errors import _make_ctx

        node = MCPToolboxNode(toolbox_name, connection_params=StreamableHttpParameters(url="http://unused.local/mcp"))
        session = MagicMock(spec=ClientSession)  # spec => passes run()'s isinstance(session, ClientSession) guard
        session.call_tool = AsyncMock(return_value=MCPCallToolResult(content=[TextContent(type="text", text="ok")]))

        ctx = _make_ctx(State())
        ctx._resources = {node._session_resource_key: session}
        payload = ToolCallRef(tool_call_id="c1", args={"x": 1}, name=advertised_name)

        await node.run(ctx, payload)

        # The server receives the BARE name; the namespaced prefix never crosses the MCP boundary.
        session.call_tool.assert_awaited_once_with(name=expected_server_name, arguments={"x": 1})
