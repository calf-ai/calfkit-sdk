from importlib.metadata import version

from calfkit.client import Client, InvocationHandle, KafkaConfig, NodeResult
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
from calfkit.nodes.aggregator import (
    AggregatedReturn,
    AggregatorBatch,
    AggregatorError,
    AggregatorMergeError,
    AggregatorStateStoreError,
    MergeErrorPolicy,
)
from calfkit.providers import AnthropicModelClient, OpenAIModelClient, OpenAIResponsesModelClient
from calfkit.worker import Worker

__version__ = version("calfkit")
__all__ = [
    "__version__",
    # aggregator
    "AggregatedReturn",
    "AggregatorBatch",
    "AggregatorError",
    "AggregatorMergeError",
    "AggregatorStateStoreError",
    "MergeErrorPolicy",
    # client
    "Client",
    "InvocationHandle",
    "KafkaConfig",
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
