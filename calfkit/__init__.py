from importlib.metadata import version

# Wire-protocol header constants are exported at the top level for
# consumer-side monitoring (e.g., counting degraded merges via the
# HDR_DEGRADED_MERGE header on aggregator-published envelopes). They live
# in calfkit._protocol so the wire constants stay free of circular import
# dependencies, but the public surface is re-exported here so users have
# a stable import path.
from calfkit._protocol import HDR_DEGRADED_MERGE, HDR_FANOUT_ID, HDR_FRAME_ID
from calfkit.client import Client, InvocationHandle, KafkaConfig, NodeResult
from calfkit.exceptions import CalfkitError, DurabilityConfigError
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
    # exceptions
    "CalfkitError",
    "DurabilityConfigError",
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
    # protocol header constants
    "HDR_DEGRADED_MERGE",
    "HDR_FANOUT_ID",
    "HDR_FRAME_ID",
    # providers
    "AnthropicModelClient",
    "OpenAIModelClient",
    "OpenAIResponsesModelClient",
    # worker
    "Worker",
]
