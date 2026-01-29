from httpx import Timeout
from pydantic_ai.models import Model

from calf.agents.base_node_runner import BaseNodeRunner
from calf.nodes.base_node import BaseNode
from calf.nodes.chat_node import ChatNode


class ChatRunner(BaseNodeRunner):
    """Entity for server/worker-side ops. Mainly used for server-side deployment.
    Pass in a client"""

    def __init__(
        self, *, model_client: Model | None = None, chat_node: BaseNode | None = None, **kwargs
    ):
        self.model_client = model_client
        self.node = chat_node
        if self.node is None:
            if model_client is None:
                raise Exception("When chat node is None, model client must exist")
            self.node = ChatNode(
                model_client,
            )
        super().__init__(self.node, **kwargs)
