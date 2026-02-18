import asyncio
from abc import ABC
from collections.abc import Callable
from typing import Any

from faststream.kafka.annotations import (
    KafkaBroker as BrokerAnnotation,
)

from calfkit.broker.broker import BrokerClient
from calfkit.nodes.agent_router_node import AgentRouterNode
from calfkit.nodes.base_node import BaseNode, publish_to, subscribe_to
from calfkit.nodes.base_tool_node import BaseToolNode
from calfkit.nodes.chat_node import ChatNode
from calfkit.runners.service import NodesService
from calfkit.stores.in_memory import InMemoryMessageHistoryStore


class AgentDispatcher(BaseNode, ABC):
    _on_enter_topic_name = "dispatch_request"
    _post_to_topic_name = "dispatch_response"

    def __init__(
        self,
        *,
        tool_nodes: list[BaseToolNode] | None = None,
        tool_nodes_factory: Callable[[str], list[BaseToolNode]] | None = None,
        **kwargs: Any,
    ):
        self.tool_nodes = tool_nodes
        self.tool_nodes_factory = tool_nodes_factory
        super().__init__(**kwargs)

    @subscribe_to(_on_enter_topic_name)
    @publish_to(_post_to_topic_name)
    async def _dispatch(
        self,
        system_prompt: str,
        group_id: str,
        broker: BrokerAnnotation,
    ) -> str:
        try:
            tools = self.tool_nodes
            if tools is None and self.tool_nodes_factory is not None:
                tools = self.tool_nodes_factory(group_id)
            router_node = AgentRouterNode(
                chat_node=ChatNode(),
                tool_nodes=tools,
                name=group_id,
                message_history_store=InMemoryMessageHistoryStore(),
                system_prompt=system_prompt,
            )
            service = NodesService(broker)  # type: ignore[arg-type]
            service.register_node(router_node, group_id=group_id)
            if tools is not None:
                for tool in tools:
                    service.register_node(tool)

            await service.start_subscribers()
            return "Success"
        except Exception as e:
            return f"{e}"


if __name__ == "__main__":

    async def main() -> None:
        dispatcher_node = AgentDispatcher()
        broker = BrokerClient(bootstrap_servers="localhost:9092")
        service = NodesService(broker)
        service.register_node(dispatcher_node)
        await service.run()

    asyncio.run(main())
