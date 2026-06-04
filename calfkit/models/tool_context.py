import dataclasses
from typing import Any

from calfkit._vendor.pydantic_ai._run_context import RunContext


@dataclasses.dataclass(kw_only=True)
class ToolContext(RunContext[dict[str, Any]]):
    """RunContext subclass for distributed tool execution.

    Injected as the first parameter of @agent_tool functions.
    Hidden from the LLM tool schema.

    ``deps`` is inherited from ``RunContext`` and is the user-provided
    dependencies mapping — the same ``dict`` the producer passed to
    ``Client.invoke_node(deps=...)``. Read it as ``ctx.deps["key"]``. Since the
    envelope is serialized as JSON over Kafka, ``deps`` values must be
    JSON-serializable.
    """

    agent_name: str | None = None

    @property
    def correlation_id(self) -> str:
        """The calfkit correlation id for this run.

        Alias of the inherited pydantic-ai ``run_id`` (calfkit always constructs
        a :class:`ToolContext` with ``run_id`` set to the correlation id), exposed
        under calfkit's own vocabulary so tool authors use the same name as the
        rest of the SDK (``NodeResult.correlation_id``, ``Client.execute_node``).
        """
        assert self.run_id is not None, "ToolContext was constructed without a run_id (correlation id)."
        return self.run_id
