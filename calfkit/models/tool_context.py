import dataclasses

from calfkit._vendor.pydantic_ai._run_context import RunContext
from calfkit.experimental._types import DepsT
from calfkit.experimental.base_models.session_context import Deps


@dataclasses.dataclass(kw_only=True)
class ToolContext(RunContext[Deps]):
    """RunContext subclass for distributed tool execution.

    Injected as the first parameter of @agent_tool functions.
    Hidden from the LLM tool schema.

    ``deps`` is inherited from ``RunContext``. Since the EventEnvelope is
    serialized as JSON over Kafka, ``deps`` must be a JSON-serializable
    value (e.g. dict, str, int, list).
    """

    agent_name: str | None = None
