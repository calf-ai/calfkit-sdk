"""ADR-0017: every node exposes a deterministic, name-scoped private INPUT topic.

``_private_input_topic = f"{node_kind}.{name}.private.input"`` — the inbound-inbox
parallel to ``_return_topic`` (the continuation inbox). Universal across node kinds
(agents are the first consumer; non-agent inboxes are dormant in v1). The property
reads the ``_node_kind`` ClassVar + ``name``, so it resolves even on ``@dataclass``
tool/MCP nodes whose generated ``__init__`` bypasses ``BaseNodeDef.__init__``.
"""

from __future__ import annotations

from calfkit.mcp.mcp_toolbox import MCPToolboxNode
from calfkit.mcp.mcp_transport import StreamableHttpParameters
from calfkit.nodes.agent import Agent
from calfkit.nodes.consumer import ConsumerNode
from calfkit.nodes.node import NodeDef
from calfkit.nodes.tool import agent_tool
from calfkit.providers.pydantic_ai.model_client import PydanticModelClient


class _FakeModel(PydanticModelClient):
    @property
    def model_name(self) -> str:
        return "fake"

    @property
    def system(self) -> str:
        return "fake"

    async def request(self, *args: object, **kwargs: object) -> object:
        raise NotImplementedError


def _add(a: int, b: int) -> int:
    """Add two integers."""
    return a + b


class TestPrivateInputTopicDerivation:
    """The topic is ``{node_kind}.{name}.private.input`` for every node kind."""

    def test_agent_kind(self) -> None:
        agent = Agent("planner", subscribe_topics="planner.in", model_client=_FakeModel())
        assert agent._private_input_topic == "agent.planner.private.input"

    def test_tool_kind(self) -> None:
        # @dataclass tool node: its generated __init__ bypasses BaseNodeDef.__init__,
        # so the property must read the _node_kind ClassVar + name, not an __init__ value.
        tool = agent_tool(_add, name="add")
        assert tool._private_input_topic == "tool.add.private.input"

    def test_consumer_kind(self) -> None:
        consumer = ConsumerNode(name="watcher", consume_fn=lambda ctx: None, subscribe_topics=["watcher.in"])
        assert consumer._private_input_topic == "consumer.watcher.private.input"

    def test_toolbox_kind(self) -> None:
        toolbox = MCPToolboxNode("docs_server", connection_params=StreamableHttpParameters(url="http://unused.local/mcp"))
        assert toolbox._private_input_topic == "toolbox.docs_server.private.input"

    def test_bare_node_kind(self) -> None:
        node = NodeDef(node_id="raw", subscribe_topics=["raw.in"])
        assert node._private_input_topic == "node.raw.private.input"

    def test_pins_the_name_node_id_alias(self) -> None:
        # The property uses self.name per spec §4.1; name aliases node_id today. Pin the
        # equality so a future name/node_id divergence trips loudly here (ADR-0017).
        agent = Agent("planner", subscribe_topics="planner.in", model_client=_FakeModel())
        assert agent._private_input_topic == f"agent.{agent.node_id}.private.input"
