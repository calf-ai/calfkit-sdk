from importlib.metadata import version

from calfkit.client import Client, InvocationHandle, InvocationResult
from calfkit.exceptions import DeserializationError, LifecycleConfigError, NodeFaultError, ToolExecutionError
from calfkit.models import ErrorReport, FaultTypes, ToolContext
from calfkit.nodes import Agent, BaseNodeDef, ConsumerFn, ConsumerNode, NodeDef, ToolNodeDef, agent_tool, consumer
from calfkit.providers import AnthropicModelClient, OpenAIModelClient, OpenAIResponsesModelClient
from calfkit.provisioning import ProvisioningConfig
from calfkit.worker import LifecycleContext, ResourceSetupContext, ServingContext, Worker

__version__ = version("calfkit")
__all__ = [
    "__version__",
    # client
    "Client",
    "InvocationHandle",
    "InvocationResult",
    # models
    "ErrorReport",
    "FaultTypes",
    "ToolContext",
    # nodes
    "Agent",
    "BaseNodeDef",
    "ConsumerFn",
    "ConsumerNode",
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
    # worker + lifecycle
    "Worker",
    "LifecycleContext",
    "ResourceSetupContext",
    "ServingContext",
    # exceptions
    "DeserializationError",
    "LifecycleConfigError",
    "NodeFaultError",
    "ToolExecutionError",
]
