"""#212: MCPToolbox — the public, identity-only handle to an MCP toolbox.

Distributed agent hosts reference a toolbox by name with zero deployment
knowledge (no connection params, no secrets). The handle is the call-side
counterpart to the hosting MCPToolboxNode (peer-node pattern); `select()` mints
one from a node instance; the node's own selector behavior delegates to its
handle so both resolve identically.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from calfkit.models.capability import CapabilityRecord, CapabilityToolDef
from calfkit.models.tool_dispatch import ToolSelector, split_tool_declarations


def make_record(toolbox_id: str = "github", tool_names: tuple[str, ...] = ("search", "create_issue")) -> CapabilityRecord:
    return CapabilityRecord(
        toolbox_id=toolbox_id,
        dispatch_topic=f"mcp_server.{toolbox_id}",
        tools=[CapabilityToolDef(name=n, parameters_json_schema={"type": "object", "properties": {}}) for n in tool_names],
        published_at=datetime.now(tz=timezone.utc),
    )


def make_toolbox(name: str = "github"):
    from calfkit.mcp.mcp_toolbox import MCPToolboxNode
    from calfkit.mcp.mcp_transport import StreamableHttpParameters

    return MCPToolboxNode(name, connection_params=StreamableHttpParameters(url="http://unused.local/mcp"))


class TestDirectConstruction:
    """The whole point of #212: constructible with zero deployment knowledge."""

    def test_resolves_by_name_only(self) -> None:
        from calfkit.mcp import MCPToolbox

        ref = MCPToolbox("github")
        result = ref.resolve_tools({"github": make_record()})
        assert [b.name for b in result.bindings] == ["search", "create_issue"]
        assert not result.strict

    def test_include_and_strict_kwargs(self) -> None:
        from calfkit.mcp import MCPToolbox

        ref = MCPToolbox("github", include=("search",), strict=True)
        result = ref.resolve_tools({"github": make_record()})
        assert [b.name for b in result.bindings] == ["search"]
        assert result.strict

    def test_include_accepts_any_sequence_normalized_to_tuple(self) -> None:
        from calfkit.mcp import MCPToolbox

        ref = MCPToolbox("github", include=["search"])
        assert ref.include == ("search",)

    def test_frozen_and_hashable(self) -> None:
        from calfkit.mcp import MCPToolbox

        ref = MCPToolbox("github", include=("search",))
        with pytest.raises(Exception):
            ref.strict = True  # type: ignore[misc]
        assert len({ref, MCPToolbox("github", include=("search",))}) == 1  # value semantics

    def test_satisfies_tool_selector_and_splits_as_selector(self) -> None:
        from calfkit.mcp import MCPToolbox

        ref = MCPToolbox("github")
        assert isinstance(ref, ToolSelector)
        bindings, selectors = split_tool_declarations([ref])
        assert bindings == [] and selectors == [ref]


class TestMintingAndParity:
    def test_select_returns_the_public_ref_type(self) -> None:
        from calfkit.mcp import MCPToolbox

        ref = make_toolbox().select(include=["search"], strict=True)
        assert isinstance(ref, MCPToolbox)
        assert ref.name == "github" and ref.include == ("search",) and ref.strict

    def test_toolbox_resolution_delegates_to_its_ref(self) -> None:
        from calfkit.mcp import MCPToolbox

        view: dict[str, Any] = {"github": make_record()}
        via_toolbox = make_toolbox().resolve_tools(view)
        via_ref = MCPToolbox("github").resolve_tools(view)
        assert [b.name for b in via_toolbox.bindings] == [b.name for b in via_ref.bindings]
        assert via_toolbox.toolbox_id == via_ref.toolbox_id

    def test_scoped_selector_is_gone(self) -> None:
        import calfkit.mcp.mcp_toolbox as mod

        assert not hasattr(mod, "_ScopedSelector")


class TestPackageSurface:
    def test_paired_exports(self) -> None:
        from calfkit.mcp import MCPToolbox, MCPToolboxNode, StdioServerParameters, StreamableHttpParameters  # noqa: F401


class TestDeployingARefFailsLoudAndEarly:
    def test_add_nodes_rejects_refs_with_teaching_message(self) -> None:
        from calfkit.client.client import Client
        from calfkit.mcp import MCPToolbox
        from calfkit.worker.worker import Worker

        worker = Worker(Client.connect("kafka:9092"))
        with pytest.raises(TypeError, match="MCPToolboxNode\\(") as excinfo:
            worker.add_nodes(MCPToolbox("github"))  # type: ignore[arg-type]
        assert "reference" in str(excinfo.value).lower()

    def test_add_nodes_rejects_arbitrary_non_nodes_too(self) -> None:
        from calfkit.client.client import Client
        from calfkit.worker.worker import Worker

        worker = Worker(Client.connect("kafka:9092"))
        with pytest.raises(TypeError):
            worker.add_nodes("not a node")  # type: ignore[arg-type]
