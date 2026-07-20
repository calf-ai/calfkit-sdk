from importlib.metadata import version
from typing import TYPE_CHECKING, Any

from calfkit.client import (
    DEFAULT_MAX_MESSAGE_BYTES,
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
from calfkit.peers import Handoff, Messaging
from calfkit.provisioning import ProvisioningConfig
from calfkit.tuning import FanoutConfig, KTableReaderTuning
from calfkit.worker import LifecycleContext, ResourceSetupContext, ServingContext

if TYPE_CHECKING:
    from calfkit.nodes import (
        Agent,
        AgentSeamContext,
        BaseNodeDef,
        ConsumerFn,
        ConsumerNode,
        NodeDef,
        Toolbox,
        Toolboxes,
        ToolCall,
        ToolErrorHandler,
        ToolNodeDef,
        Tools,
        agent_tool,
        consumer,
        render_fault_for_model,
        surface_to_model,
    )
    from calfkit.nodes import Toolbox as MCPToolbox
    from calfkit.providers import AnthropicModelClient, OpenAIModelClient, OpenAIResponsesModelClient
    from calfkit.worker import Worker

__version__ = version("calfkit")
__all__ = [
    "__version__",
    # client
    "AgentGateway",
    "Client",
    "DEFAULT_MAX_MESSAGE_BYTES",
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
    "MCPToolbox",
    "Toolbox",
    "Toolboxes",
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

# Maps each deferred export to (module, attribute). The attribute usually matches the export
# name; ``MCPToolbox`` is the one rename (a root-level compatibility alias for ``Toolbox``).
_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    **{
        name: ("calfkit.nodes", name)
        for name in (
            "Agent",
            "AgentSeamContext",
            "BaseNodeDef",
            "ConsumerFn",
            "ConsumerNode",
            "NodeDef",
            "Toolbox",
            "Toolboxes",
            "ToolCall",
            "ToolErrorHandler",
            "ToolNodeDef",
            "Tools",
            "agent_tool",
            "consumer",
            "render_fault_for_model",
            "surface_to_model",
        )
    },
    "MCPToolbox": ("calfkit.nodes", "Toolbox"),
    **{name: ("calfkit.providers", name) for name in ("AnthropicModelClient", "OpenAIModelClient", "OpenAIResponsesModelClient")},
    "Worker": ("calfkit.worker", "Worker"),
}

_LAZY_SUBMODULES = {"nodes", "providers"}


def __getattr__(name: str) -> Any:
    from importlib import import_module

    if name in _LAZY_SUBMODULES:
        # The import system sets the submodule on the package, so __getattr__ won't fire
        # for it again — no need to cache in globals() here.
        return import_module(f"calfkit.{name}")

    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module_name, attr_name = target
    value = getattr(import_module(module_name), attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__) | _LAZY_SUBMODULES)
