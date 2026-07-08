from importlib.metadata import version

from calfkit.client import (
    AgentGateway,
    AgentInfo,
    AgentMessageEvent,
    Client,
    Dispatch,
    EventStream,
    HandoffEvent,
    InvocationHandle,
    InvocationResult,
    Mesh,
    MeshViewConfig,
    RunCompleted,
    RunEvent,
    RunFailed,
    ToolboxInfo,
    ToolCallEvent,
    ToolInfo,
    ToolNodeInfo,
    ToolResultEvent,
    ToolSpec,
)
from calfkit.controlplane import ControlPlaneConfig, ControlPlaneRecord, ControlPlaneStamp, ControlPlaneView, advertises
from calfkit.exceptions import ClientClosedError, ClientTimeoutError, DeserializationError, LifecycleConfigError, MeshUnavailableError, NodeFaultError
from calfkit.models import ErrorReport, ExceptionInfo, FaultTypes, ToolContext
from calfkit.models.payload import retry_text_part
from calfkit.nodes import (
    Agent,
    AgentSeamContext,
    BaseNodeDef,
    ConsumerFn,
    ConsumerNode,
    NodeDef,
    ToolCall,
    ToolErrorHandler,
    ToolNodeDef,
    Tools,
    agent_tool,
    consumer,
    render_fault_for_model,
    surface_to_model,
)
from calfkit.peers import Handoff, Messaging
from calfkit.providers import AnthropicModelClient, OpenAIModelClient, OpenAIResponsesModelClient
from calfkit.provisioning import ProvisioningConfig
from calfkit.tuning import FanoutConfig, KTableReaderTuning
from calfkit.worker import LifecycleContext, ResourceSetupContext, ServingContext, Worker

__version__ = version("calfkit")
__all__ = [
    "__version__",
    # client
    "AgentGateway",
    "Client",
    "Dispatch",
    "EventStream",
    "InvocationHandle",
    "InvocationResult",
    "RunCompleted",
    "RunEvent",
    "RunFailed",
    # client — intermediate step events (members of RunEvent)
    "AgentMessageEvent",
    "ToolCallEvent",
    "ToolResultEvent",
    "HandoffEvent",
    # client — mesh view (caller-side directory)
    "Mesh",
    "MeshViewConfig",
    "AgentInfo",
    "ToolInfo",
    "ToolNodeInfo",
    "ToolboxInfo",
    "ToolSpec",
    # models
    "ErrorReport",
    "ExceptionInfo",
    "FaultTypes",
    "ToolContext",
    # nodes
    "Agent",
    "BaseNodeDef",
    "ConsumerFn",
    "ConsumerNode",
    "NodeDef",
    "ToolNodeDef",
    "Tools",
    "agent_tool",
    "consumer",
    # agent tool-error reception (on_tool_error surface). ``ToolCall`` is calfkit's public name for
    # the vendored model-request tool-call type (``.tool_name``/``.args``) an on_tool_error handler
    # receives — DISTINCT from the wire ``calfkit.models.ToolCallPart`` (``.kwargs``).
    "AgentSeamContext",
    "ToolCall",
    "ToolErrorHandler",
    "render_fault_for_model",
    "surface_to_model",
    "retry_text_part",
    # peers (agent-to-agent)
    "Handoff",
    "Messaging",
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
    # control plane
    "ControlPlaneConfig",
    "ControlPlaneRecord",
    "ControlPlaneStamp",
    "ControlPlaneView",
    "advertises",
    # ktables tuning
    "FanoutConfig",
    "KTableReaderTuning",
    # exceptions
    "ClientClosedError",
    "ClientTimeoutError",
    "DeserializationError",
    "LifecycleConfigError",
    "MeshUnavailableError",
    "NodeFaultError",
]
