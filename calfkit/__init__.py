from importlib.metadata import version

from calfkit.broker import BrokerClient
from calfkit.gates import DecisionGate, GateResult, load_gate, register_gate
from calfkit.messages import append_system_prompt, patch_system_prompts, validate_tool_call_pairs
from calfkit.nodes import (
    AgentRouterNode,
    BaseNode,
    BaseToolNode,
    ChatNode,
    Registrator,
    ToolContext,
    agent_tool,
    entrypoint,
    publish_to,
    returnpoint,
    subscribe_to,
)
from calfkit.providers import OpenAIModelClient
from calfkit.runners import (
    AgentRouterRunner,
    ChatRunner,
    NodeRunner,
    NodesService,
    RouterServiceClient,
    ToolRunner,
)
from calfkit.stores import InMemoryMessageHistoryStore, MessageHistoryStore

__version__ = version("calfkit")
__all__ = [
    "__version__",
    # broker
    "BrokerClient",
    # gates
    "DecisionGate",
    "GateResult",
    "load_gate",
    "register_gate",
    # messages
    "append_system_prompt",
    "patch_system_prompts",
    "validate_tool_call_pairs",
    # nodes
    "AgentRouterNode",
    "BaseNode",
    "BaseToolNode",
    "ChatNode",
    "Registrator",
    "ToolContext",
    "agent_tool",
    "entrypoint",
    "publish_to",
    "returnpoint",
    "subscribe_to",
    # providers
    "OpenAIModelClient",
    # runners
    "AgentRouterRunner",
    "ChatRunner",
    "NodeRunner",
    "NodesService",
    "RouterServiceClient",
    "ToolRunner",
    # stores
    "InMemoryMessageHistoryStore",
    "MessageHistoryStore",
]
