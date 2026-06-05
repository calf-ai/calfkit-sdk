from importlib.metadata import version

from calfkit.client import Client, InvocationHandle, NodeResult
from calfkit.mcp import mcp
from calfkit.models import ToolContext
from calfkit.nodes import Agent, BaseNodeDef, ConsumerFn, ConsumerNodeDef, GateFunction, NodeDef, ToolNodeDef, agent_tool, consumer
from calfkit.providers import AnthropicModelClient, OpenAIModelClient, OpenAIResponsesModelClient
from calfkit.provisioning import ProvisioningConfig
from calfkit.worker import Worker

__version__ = version("calfkit")
__all__ = [
    "__version__",
    # client
    "Client",
    "InvocationHandle",
    "NodeResult",
    # mcp (factory; full surface at calfkit.mcp)
    "mcp",
    # models
    "ToolContext",
    # nodes
    "Agent",
    "BaseNodeDef",
    "ConsumerFn",
    "ConsumerNodeDef",
    "GateFunction",
    "NodeDef",
    "ToolNodeDef",
    "agent_tool",
    "consumer",
    # providers
    "AnthropicModelClient",
    "OpenAIModelClient",
    "OpenAIResponsesModelClient",
    # provisioning (config only; full surface at calfkit.provisioning)
    "ProvisioningConfig",
    # worker
    "Worker",
]
