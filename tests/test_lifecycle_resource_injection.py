"""Phase 3 — per-surface lifecycle resource injection.

These cover the *wiring* (stamping) side of resource injection: each node
surface reads its own ``self.resources`` bag (from ``LifecycleHookMixin``) and
exposes a read-only view of it to user code on the live handler/run path.

* ``BaseNodeDef.prepare_context`` stamps ``ctx._resources``.
* ``ToolNodeDef.run`` builds ``ToolContext(resources=...)``.
* ``ConsumerNodeDef.handler`` threads ``resources`` into ``NodeResult``.

See ``docs/research/node-worker-lifecycle-hooks-plan-v7.md`` §3.5.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest
from faststream.kafka import KafkaBroker

from calfkit._protocol import HDR_EMITTER, HDR_EMITTER_KIND
from calfkit.models import ToolContext
from calfkit.models.envelope import Envelope
from calfkit.models.session_context import (
    CallFrame,
    CallFrameStack,
    SessionRunContext,
    WorkflowState,
)
from calfkit.models.state import State
from calfkit.nodes.base import BaseNodeDef
from calfkit.worker.lifecycle import LifecycleHookMixin


class _ProbeNode(BaseNodeDef):
    """Minimal concrete node that records the ctx it ran against."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.seen_ctx: SessionRunContext | None = None

    async def run(self, ctx: SessionRunContext) -> Any:
        self.seen_ctx = ctx
        from calfkit.models import Silent

        return Silent()


def _envelope_with_frame() -> Envelope:
    stack = CallFrameStack()
    stack.push(CallFrame(target_topic="t", callback_topic="cb"))
    return Envelope(
        context=SessionRunContext(state=State(), deps={}),
        internal_workflow_state=WorkflowState(call_stack=stack),
    )


# ---------------------------------------------------------------------------
# Mixin inheritance
# ---------------------------------------------------------------------------


def test_base_node_def_inherits_lifecycle_hook_mixin() -> None:
    assert issubclass(BaseNodeDef, LifecycleHookMixin)


def test_node_subclasses_inherit_lifecycle_hook_mixin() -> None:
    from calfkit.nodes import Agent, ConsumerNodeDef, ToolNodeDef

    assert issubclass(Agent, LifecycleHookMixin)
    assert issubclass(ToolNodeDef, LifecycleHookMixin)
    assert issubclass(ConsumerNodeDef, LifecycleHookMixin)


# ---------------------------------------------------------------------------
# Surface 1: BaseNodeDef.prepare_context stamps ctx._resources
# ---------------------------------------------------------------------------


async def test_prepare_context_stamps_node_resources_onto_ctx() -> None:
    node = _ProbeNode(node_id="probe", subscribe_topics=["probe.in"])
    sentinel = object()
    node.resources["db"] = sentinel

    ctx = await node.prepare_context(_envelope_with_frame(), correlation_id="cid")

    assert ctx.resources["db"] is sentinel


async def test_prepared_ctx_resources_is_read_only() -> None:
    node = _ProbeNode(node_id="probe", subscribe_topics=["probe.in"])
    node.resources["db"] = object()

    ctx = await node.prepare_context(_envelope_with_frame(), correlation_id="cid")

    assert isinstance(ctx.resources, Mapping)
    with pytest.raises(TypeError):
        ctx.resources["other"] = object()  # type: ignore[index]


# ---------------------------------------------------------------------------
# Surface 2: ToolNodeDef.run builds ToolContext(resources=...)
# ---------------------------------------------------------------------------


async def test_tool_run_injects_node_resources_into_tool_context() -> None:
    from calfkit._vendor.pydantic_ai.messages import ModelResponse, ToolCallPart
    from calfkit.nodes import ToolNodeDef

    seen: dict[str, Any] = {}

    def reader(ctx: ToolContext) -> str:
        seen["db"] = ctx.resources["db"]
        return "ok"

    tool_node = ToolNodeDef.create_tool_node(
        func=reader,
        subscribe_topics="tool.reader.input",
        publish_topic="tool.reader.output",
    )
    sentinel = object()
    tool_node.resources["db"] = sentinel

    state = State()
    tool_call_id = "tc-res-001"
    part = ToolCallPart(tool_name="reader", args={}, tool_call_id=tool_call_id)
    state.add_tool_call(part)
    state.message_history.append(ModelResponse(parts=[part]))

    ctx = SessionRunContext(state=state, deps={})
    ctx._correlation_id = "cid"

    await tool_node.run(ctx, tool_call_id)

    assert seen["db"] is sentinel


# ---------------------------------------------------------------------------
# Surface 3: ConsumerNodeDef.handler threads resources into NodeResult
# ---------------------------------------------------------------------------


async def test_consumer_handler_injects_node_resources_into_result() -> None:
    from calfkit.client import NodeResult
    from calfkit.models import TextPart
    from calfkit.nodes import ConsumerNodeDef

    seen: dict[str, Any] = {}

    def sink(result: NodeResult[str]) -> None:
        seen["db"] = result.resources["db"]

    node: ConsumerNodeDef[str] = ConsumerNodeDef(
        node_id="sink",
        subscribe_topics="sink.in",
        consume_fn=sink,
        output_type=str,
    )
    sentinel = object()
    node.resources["db"] = sentinel

    state = State(final_output_parts=[TextPart(text="hi")])
    envelope = Envelope(
        context=SessionRunContext(state=state, deps={}),
        internal_workflow_state=WorkflowState(call_stack=CallFrameStack()),
    )

    await node.handler(
        envelope,
        correlation_id="cid",
        headers={HDR_EMITTER: b"agent", HDR_EMITTER_KIND: b"agent"},
        broker=KafkaBroker(),
    )

    assert seen["db"] is sentinel
