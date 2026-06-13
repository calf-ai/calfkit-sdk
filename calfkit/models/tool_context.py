import dataclasses
from collections.abc import Mapping
from typing import Any

from calfkit._vendor.pydantic_ai._run_context import RunContext


@dataclasses.dataclass(kw_only=True)
class ToolContext(RunContext[dict[str, Any]]):
    """RunContext subclass for distributed tool execution.

    Injected as the first parameter of @agent_tool functions.
    Hidden from the LLM tool schema.

    ``deps`` is inherited from ``RunContext`` and is the user-provided
    dependencies mapping — the same ``dict`` the producer passed to
    ``Client.start(deps=...)``. Read it as ``ctx.deps["key"]``. Since the
    envelope is serialized as JSON over Kafka, ``deps`` values must be
    JSON-serializable.
    """

    agent_name: str | None = None

    resources: Mapping[str, Any] = dataclasses.field(default_factory=dict, kw_only=True)
    """The owning node's lifecycle-managed resources (read-only by type).

    Stamped by ``ToolNodeDef.run`` with a *shallow copy* of the node's resource
    bag, so mutating it can't corrupt the shared bag. Read as
    ``ctx.resources["key"]``. Typed as a read-only ``Mapping`` so
    ``ctx.resources[...] = ...`` is a type error at dev time (like ``deps``).
    Empty when the node owns no resources."""

    @property
    def correlation_id(self) -> str:
        """The calfkit correlation id for this run.

        Alias of the inherited pydantic-ai ``run_id`` (calfkit always constructs
        a :class:`ToolContext` with ``run_id`` set to the correlation id), exposed
        under calfkit's own vocabulary so tool authors use the same name as the
        rest of the SDK (``InvocationResult.correlation_id``, ``Client.execute``).
        """
        if self.run_id is None:
            raise RuntimeError("ToolContext was constructed without a run_id (correlation id).")
        return self.run_id
