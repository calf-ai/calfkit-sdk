import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, ClassVar

import pydantic_core
from typing_extensions import Self

from calfkit._protocol import NodeKind
from calfkit._vendor.pydantic_ai import Tool
from calfkit._vendor.pydantic_ai.exceptions import ModelRetry
from calfkit._vendor.pydantic_ai.messages import RetryPromptPart, ToolReturn
from calfkit.models import SessionRunContext, Silent, State, ToolContext
from calfkit.models.actions import NodeResult, ReturnCall
from calfkit.models.node_schema import BaseToolNodeSchema
from calfkit.models.state import FailedToolCall
from calfkit.nodes.base import BaseNodeDef, GateFunction

logger = logging.getLogger(__name__)


def _safe_exc_message(e: BaseException) -> str:
    """Best-effort string of an exception, robust against broken ``__str__``.

    A bare ``str(e)`` can itself raise (if the exception's ``__str__`` is
    broken or its args don't coerce). Inside the worker's ``except Exception``
    block that would propagate out, prevent the FailedToolCall from being
    constructed, and re-introduce the silent-hang failure mode this module
    exists to prevent. Mirrors stdlib ``traceback._some_str`` with a ``repr``
    fallback.
    """
    try:
        return str(e)
    except Exception:
        try:
            return repr(e)
        except Exception:
            return f"<unprintable {type(e).__name__}>"


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

    async def run(self, ctx: SessionRunContext, tool_call_id: str) -> NodeResult[State]:
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
            resources=MappingProxyType(self.resources),
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
            # Construct the marker defensively: a validator on FailedToolCall (e.g.,
            # min_length on tool_call_id) could itself raise inside this except block
            # and re-introduce the silent-hang failure mode we exist to prevent.
            try:
                marker: FailedToolCall = FailedToolCall(
                    tool_name=tool_call_part.tool_name,
                    tool_call_id=tool_call_part.tool_call_id,
                    exc_type=type(e).__name__,
                    exc_message=_safe_exc_message(e),
                )
            except Exception as construct_err:
                logger.exception(
                    "[%s] tool=%s tool_call_id=%s failed to construct FailedToolCall (%s); using fallback sentinel marker",
                    ctx.correlation_id[:8],
                    self.name,
                    tool_call_part.tool_call_id,
                    type(construct_err).__name__,
                )
                # Fallback: preserve real ``tool_name`` / ``tool_call_id`` from
                # ``tool_call_part`` when they are valid strings (so operators
                # don't lose the correlation key in the raised ToolExecutionError);
                # only substitute sentinels for fields that are themselves
                # problematic. ``isinstance(..., str)`` and truthy guards keep
                # construction total even if the originals are the cause of the
                # primary failure (e.g. empty ``tool_call_id``).
                fallback_tool_name = (
                    tool_call_part.tool_name if isinstance(tool_call_part.tool_name, str) and tool_call_part.tool_name else "<unknown>"
                )
                fallback_tool_call_id = (
                    tool_call_part.tool_call_id if isinstance(tool_call_part.tool_call_id, str) and tool_call_part.tool_call_id else "<missing>"
                )
                marker = FailedToolCall(
                    tool_name=fallback_tool_name,
                    tool_call_id=fallback_tool_call_id,
                    exc_type="FailedToolCallConstructionError",
                    exc_message=f"Failed to construct primary marker: {_safe_exc_message(construct_err)}",
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
