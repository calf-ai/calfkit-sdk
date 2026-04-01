from importlib.metadata import version

from calfkit.client import Client, InvocationHandle, NodeResult
from calfkit.models import ToolContext
from calfkit.nodes import Agent, BaseNodeDef, NodeDef, ToolNodeDef, agent_tool
from calfkit.providers import OpenAIModelClient
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
    "NodeDef",
    "ToolNodeDef",
    "agent_tool",
    # providers
    "OpenAIModelClient",
    # worker
    "Worker",
]
