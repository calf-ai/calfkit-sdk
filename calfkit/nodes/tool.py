import logging
from collections.abc import Awaitable, Callable
from dataclasses import KW_ONLY, dataclass
from typing import Any, ClassVar

import pydantic_core
from typing_extensions import Self

from calfkit._protocol import NodeKind
from calfkit._registry import handler
from calfkit._vendor.pydantic_ai import Tool
from calfkit._vendor.pydantic_ai.exceptions import ModelRetry
from calfkit._vendor.pydantic_ai.tools import ToolDefinition
from calfkit.controlplane import ControlPlaneStamp, advertises
from calfkit.models import SessionRunContext, State, ToolContext
from calfkit.models.actions import NodeResult, ReturnCall
from calfkit.models.capability import CAPABILITY_TOPIC, CapabilityRecord, CapabilityToolDef
from calfkit.models.payload import retry_text_part
from calfkit.models.tool_dispatch import ToolBinding, ToolCallRef
from calfkit.nodes.base import BaseNodeDef

logger = logging.getLogger(__name__)


@dataclass
class BaseToolNodeDef(BaseNodeDef):
    _node_kind: ClassVar[NodeKind] = "tool"
    _tool: Tool
    _: KW_ONLY
    tool_schema: ToolDefinition

    def tool_bindings(self) -> list[ToolBinding]:
        """This node's single binding — satisfies the ``ToolProvider`` protocol.

        ``dispatch_topic`` is the node's public inbox (``subscribe_topics[0]``);
        the validator is the bound :meth:`validate_call_args`, so the agent
        fail-fasts on malformed LLM args before the Kafka boundary.
        """
        return [
            ToolBinding(
                tool_def=self.tool_schema,
                dispatch_topic=self.subscribe_topics[0],
                validator=self.validate_call_args,
            )
        ]

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

    @advertises(topic=CAPABILITY_TOPIC, record=CapabilityRecord)
    def _capability_advert(self, stamp: ControlPlaneStamp) -> CapabilityRecord:
        """Advertise this node's single tool on the shared capability plane (always-on).

        Static-schema advertiser: the tool surface is fixed at construction, so the factory
        reads ``tool_schema`` directly — no session, no cache to prime, nothing that can fail
        at publish time. ``content_updated_at`` is the process boot time (``stamp.started_at``):
        the content never changes, so a stable, non-``now()`` value satisfies the substrate's
        no-``now()``-in-factory contract. ``node_kind`` rides on the stamp (the worker stamps
        it from ``_node_kind="tool"``), so it is not set here. ``subscribe_topics[0]`` is always
        safe: ``BaseNodeSchema`` rejects an empty list at construction.
        """
        return CapabilityRecord(
            **stamp.model_dump(),
            dispatch_topic=self.subscribe_topics[0],
            tools=[
                CapabilityToolDef(
                    name=self.tool_schema.name,
                    description=self.tool_schema.description,
                    parameters_json_schema=self.tool_schema.parameters_json_schema,
                )
            ],
            content_updated_at=stamp.started_at,
        )


class ToolNodeDef(BaseToolNodeDef):
    @classmethod
    def create_tool_node(
        cls,
        func: Callable[..., Any],
        subscribe_topics: str | list[str],
        publish_topic: str,
        *,
        name: str | None = None,
    ) -> Self:
        # The tool name (``name`` if given, else the function name) IS the node id —
        # the capability key an agent references — and the LLM-facing tool name. No
        # prefix: one name everywhere. ``name`` disambiguates without renaming ``func``.
        if name is not None and not name:
            raise ValueError("name must be non-empty when given")
        if not isinstance(subscribe_topics, (list, tuple)):
            subscribe_topics = [subscribe_topics]
        effective = name or func.__name__
        tool = Tool(func, name=effective)
        return cls(
            node_id=effective,
            tool_schema=tool.tool_def,
            subscribe_topics=subscribe_topics,
            publish_topic=publish_topic,
            _tool=tool,
        )

    @handler("*", schema=ToolCallRef)
    async def run(self, ctx: SessionRunContext, payload: ToolCallRef) -> NodeResult[State]:  # type: ignore[override]
        # The ToolCallRef payload is the authoritative invocation source —
        # name, args, and tool_call_id all come from the ref, never from a
        # ToolCallPart lookup in ctx.state. In parallel mode each fanned-out
        # Call carries a deep state copy holding ALL pending calls, so the
        # payload is the only per-invocation discriminator anyway.
        tool_call_id = payload.tool_call_id
        logger.debug(
            "[%s] tool run entered tool=%s tool_call_id=%s emitter=%s",
            ctx.correlation_id[:8],
            self.name,
            tool_call_id,
            ctx.emitter_node_id,
        )

        tool_call_ctx = ToolContext(
            deps=ctx.deps,
            agent_name=ctx.emitter_node_id,
            tool_call_id=tool_call_id,
            tool_name=payload.name,
            messages=ctx.state.message_history,
            run_id=ctx.correlation_id,
            resources=self._effective_resources(),
        )

        # TODO(#143): bounded retries / backoff for non-ModelRetry exceptions.
        # ModelRetry stays a model-visible recoverable (rendered at origin, §4.5); any OTHER
        # exception ESCAPES to the chokepoint (on_node_error → the fault rail), no longer captured
        # into a FailedToolCall — that terminal carriage is now the rail's ErrorReport.
        try:
            result = await self._tool.function_schema.call(payload.args, tool_call_ctx)
            # B1 eager wire-safety (fault-rail decision 2): a non-serializable result raises HERE so
            # it escapes to the chokepoint and faults via ``on_node_error`` (giving the dev's edge
            # seam its chance), instead of killing the envelope serialization mid-publish (the
            # silent-hang failure mode) or faulting directly at the publish-guard coercion.
            pydantic_core.to_json(result)
        except ModelRetry as e:
            logger.warning("[%s] tool=%s raised ModelRetry: %s", ctx.correlation_id[:8], self.name, e.message)
            # Render at origin to a calf.retry-marked TextPart on the reply slot — the RAW message
            # (option 1): the agent hydrates the RetryPromptPart so the provider renders the
            # fix-and-retry suffix exactly once. NOT a tool_results blob-write, NOT the fault rail.
            return ReturnCall[State](state=ctx.state, value=[retry_text_part(e.message)])

        logger.debug("[%s] tool completed tool=%s", ctx.correlation_id[:8], self.name)
        # The result rides the reply slot (ReturnCall.value -> reply.parts at the chokepoint), not a
        # state.tool_results blob-write; the calling agent materializes it at the callee slot
        # (``_resolve_slot``) keyed by the echoed ``tag``.
        return ReturnCall[State](state=ctx.state, value=result)


def agent_tool(func: Callable[..., Any] | Callable[..., Awaitable[Any]], *, name: str | None = None) -> ToolNodeDef:
    """Turn a function into a deployable tool node that agents can call.

    Usable bare (``@agent_tool``) or as a call (``agent_tool(fn, name="x")``). The tool
    name — ``name`` if given, else the function name — drives the node id (the capability
    key an agent references), the LLM-facing tool name, and the ``tool.<name>.input`` /
    ``.output`` topics, so one name is the only thing to reason about.
    """
    if name is not None and not name:
        raise ValueError("name must be non-empty when given")
    effective = name or func.__name__
    subscribe_topic = f"tool.{effective}.input"
    publish_topic = f"tool.{effective}.output"
    return ToolNodeDef.create_tool_node(func=func, subscribe_topics=subscribe_topic, publish_topic=publish_topic, name=effective)
