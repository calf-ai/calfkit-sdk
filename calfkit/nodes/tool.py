import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, ClassVar

import pydantic_core
from typing_extensions import Self

from calfkit._protocol import NodeKind
from calfkit._registry import handler
from calfkit._vendor.pydantic_ai import Tool
from calfkit._vendor.pydantic_ai.exceptions import ModelRetry
from calfkit._vendor.pydantic_ai.messages import RetryPromptPart, ToolReturn
from calfkit.exceptions import safe_exc_message
from calfkit.models import SessionRunContext, Silent, State, ToolContext
from calfkit.models.actions import NodeResult, ReturnCall
from calfkit.models.node_schema import BaseToolNodeSchema
from calfkit.models.state import FailedToolCall
from calfkit.models.tool_dispatch import ToolCallRef
from calfkit.nodes.base import BaseNodeDef, GateFunction

logger = logging.getLogger(__name__)


@dataclass
class BaseToolNodeDef(BaseToolNodeSchema, BaseNodeDef):
    _node_kind: ClassVar[NodeKind] = "tool"
    _tool: Tool
    gates: list[GateFunction] = field(default_factory=list)

    def validate_call_args(self, args_dict: dict[str, Any]) -> Any:
        """Validate ``args_dict`` against this tool's argument schema.

        Raises ``pydantic.ValidationError`` on mismatch. Used by the agent to
        fail-fast on LLM-produced malformed args before dispatching the call
        across the Kafka boundary.

        Note: tools with unannotated parameters (which default to ``Any`` in
        pydantic-ai's function-schema) or with ``**kwargs`` (where extra fields
        are allowed) bypass meaningful validation here — the worker-side
        ``except Exception`` catch is the safety net for those.
        """
        return self._tool.function_schema.validator.validate_python(args_dict)


class ToolNodeDef(BaseToolNodeDef):
    @classmethod
    def create_tool_node(
        cls,
        func: Callable[..., Any],
        subscribe_topics: str | list[str],
        publish_topic: str,
        gates: list[GateFunction] | None = None,
    ) -> Self:
        if not isinstance(subscribe_topics, (list, tuple)):
            subscribe_topics = [subscribe_topics]
        tool = Tool(func)
        return cls(
            node_id=f"tool_{func.__name__}",
            tool_schema=tool.tool_def,
            subscribe_topics=subscribe_topics,
            publish_topic=publish_topic,
            _tool=tool,
            gates=list(gates) if gates else [],
        )

    @handler("*", schema=ToolCallRef)
    async def run(self, ctx: SessionRunContext, payload: ToolCallRef) -> NodeResult[State]:  # type: ignore[override]
        tool_call_id = payload.tool_call_id
        logger.debug(
            "[%s] tool run entered tool=%s tool_call_id=%s emitter=%s",
            ctx.correlation_id[:8],
            self.name,
            tool_call_id,
            ctx.emitter_node_id,
        )
        tool_call_part = ctx.state.get_tool_call(tool_call_id)
        if tool_call_part is None:
            logger.warning(
                "tool node reached but no matching tool call found in run state for tool_call_id=%s",
                tool_call_id,
            )
            return Silent()

        tool_call_ctx = ToolContext(
            deps=ctx.deps,
            agent_name=ctx.emitter_node_id,
            tool_call_id=tool_call_part.tool_call_id,
            tool_name=tool_call_part.tool_name,
            messages=ctx.state.message_history,
            run_id=ctx.correlation_id,
            resources=self._effective_resources(),
        )

        # TODO(#143): bounded retries / backoff for non-ModelRetry exceptions.
        # ModelRetry below provides LLM-visible retry per pydantic-ai semantics
        # but is not yet rate-limited on the deferred path.
        try:
            result = await self._tool.function_schema.call(tool_call_part.args_as_dict(), tool_call_ctx)
            # Construct the ToolReturn and eagerly verify it is wire-safe BEFORE
            # storing in state. FastStream's envelope serialization at publish
            # time would raise PydanticSerializationError on a non-serializable
            # return_value, killing the worker handler before the reply
            # publishes — the silent-hang failure mode this module exists to
            # prevent. By serializing inside the try block, any failure flows
            # through ``except Exception`` below and surfaces as a FailedToolCall.
            tool_return = ToolReturn(return_value=result, metadata={"tool_call_id": tool_call_part.tool_call_id})
            pydantic_core.to_json(tool_return)
        except ModelRetry as e:
            logger.warning(
                "[%s] tool=%s raised ModelRetry: %s",
                ctx.correlation_id[:8],
                self.name,
                e.message,
            )
            ctx.state.add_tool_result(
                tool_call_part.tool_call_id,
                RetryPromptPart(
                    content=e.message,
                    tool_name=tool_call_part.tool_name,
                    tool_call_id=tool_call_part.tool_call_id,
                ),
            )
            return ReturnCall[State](state=ctx.state)
        except Exception as e:
            logger.exception(
                "[%s] tool=%s tool_call_id=%s raised %s; surfacing FailedToolCall to agent",
                ctx.correlation_id[:8],
                self.name,
                tool_call_part.tool_call_id,
                type(e).__name__,
            )
            # ``build_safe`` never raises (it falls back to sentinel identifiers if the
            # marker itself can't be constructed, e.g. an empty ``tool_call_id``), so the
            # failure reply is always published and the agent never hangs on reply-TTL.
            marker = FailedToolCall.build_safe(
                tool_name=tool_call_part.tool_name,
                tool_call_id=tool_call_part.tool_call_id,
                exc_type=type(e).__name__,
                exc_message=safe_exc_message(e),
            )
            ctx.state.add_tool_result(tool_call_part.tool_call_id, marker)
            return ReturnCall[State](state=ctx.state)

        # ``tool_return`` was constructed and serialization-verified inside the
        # try block above; reuse it rather than constructing twice.
        ctx.state.add_tool_result(tool_call_part.tool_call_id, tool_return)

        logger.debug("[%s] tool completed tool=%s", ctx.correlation_id[:8], self.name)
        return ReturnCall[State](state=ctx.state)


def agent_tool(func: Callable[..., Any] | Callable[..., Awaitable[Any]]) -> ToolNodeDef:
    """Decorator to turn a function into a deployable tool node that agents can call"""
    subscribe_topic = f"tool.{func.__name__}.input"
    publish_topic = f"tool.{func.__name__}.output"
    tool_node = ToolNodeDef.create_tool_node(func=func, subscribe_topics=subscribe_topic, publish_topic=publish_topic)

    return tool_node
