from importlib.metadata import version

from calfkit.client import Client, InvocationHandle, NodeResult
from calfkit.models import ToolContext
from calfkit.nodes import (
    Agent,
    BaseNodeDef,
    ConsumerFn,
    ConsumerNodeDef,
    FanOutAggregator,
    GateFunction,
    NodeDef,
    ToolNodeDef,
    agent_tool,
    consumer,
)
from calfkit.providers import AnthropicModelClient, OpenAIModelClient, OpenAIResponsesModelClient
from calfkit.worker import Worker

__version__ = version("calfkit")
__all__ = [
    "__version__",
    # client
    "Client",
    "InvocationHandle",
    "NodeResult",
    # models
    "ToolContext",
    # nodes
    "Agent",
    "BaseNodeDef",
    "ConsumerFn",
    "ConsumerNodeDef",
    "FanOutAggregator",
    "GateFunction",
    "NodeDef",
    "ToolNodeDef",
    "agent_tool",
    "consumer",
    # providers
    "AnthropicModelClient",
    "OpenAIModelClient",
    "OpenAIResponsesModelClient",
    # worker
    "Worker",
]
