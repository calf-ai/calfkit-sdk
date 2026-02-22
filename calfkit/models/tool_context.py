import dataclasses

from calfkit._vendor.pydantic_ai._run_context import RunContext


@dataclasses.dataclass(kw_only=True)
class ToolContext(RunContext):
    """RunContext subclass for distributed tool execution.

    Injected as the first parameter of @agent_tool functions.
    Hidden from the LLM tool schema.
    """

    agent_name: str | None = None
