"""PR-6 step 1 — generic seam-value coercion + the §6.2 teaching guards.

Pure unit tests for ``BaseNodeDef._coerce_output`` (spec §6.9 generic conversion:
seam value → ``ReturnCall``) and the §6.2 guards that reject migration-trap return
types (bool / the session State / the SeamContext / bytes) with ``SeamContractError``.
Additive — these are not wired into the live pipeline until step 3. See
notes/pr6-fault-rail-implementation-plan.md §3 step 1 / §4 layer 2.
"""

from __future__ import annotations

import pytest

from calfkit.exceptions import SeamContractError
from calfkit.models.actions import ReturnCall
from calfkit.models.error_report import ErrorReport
from calfkit.models.seam_context import SeamContext
from calfkit.models.state import State
from calfkit.nodes.base import BaseNodeDef


def _node() -> BaseNodeDef:
    return BaseNodeDef(node_id="n", subscribe_topics=["in"])


def _ctx(state: State) -> SeamContext[State]:
    return SeamContext(
        state=state,
        deps={},
        resources={},
        payload=None,
        node_id="n",
        correlation_id="cid",
        emitter_node_id=None,
        route=None,
        delivery_kind="return",
        awaiting_reply=False,
        callee_results=[],
    )


class TestCoerceOutput:
    def test_wraps_value_in_returncall_carrying_ctx_state(self) -> None:
        # The generic conversion (spec §6.9): a seam value becomes the node's
        # output via ReturnCall(state=ctx.state, value=...); value→parts is deferred
        # to the publish chokepoint.
        node, state = _node(), State()
        result = node._coerce_output(_ctx(state), "hello")
        assert isinstance(result, ReturnCall)
        assert result.value == "hello"
        assert result.state is state


class TestCoerceGuards:
    def test_rejects_bool(self) -> None:
        # §6.2: a bool is never a node output — the predictable port of a gate's
        # `return True` must not short-circuit the happy path with True as the answer.
        node = _node()
        with pytest.raises(SeamContractError):
            node._coerce_output(_ctx(State()), True)

    def test_rejects_returning_the_session_state(self) -> None:
        # §6.2: returning the State (the session record) must not serialize the whole
        # conversation out as the node's "answer"; mutate ctx.state in place instead.
        node = _node()
        with pytest.raises(SeamContractError):
            node._coerce_output(_ctx(State()), State())

    def test_rejects_returning_the_seam_context(self) -> None:
        # §6.2: the reflexive `return ctx` idiom is rejected the same way.
        node = _node()
        ctx = _ctx(State())
        with pytest.raises(SeamContractError):
            node._coerce_output(ctx, ctx)

    def test_rejects_bytes(self) -> None:
        # §6.2: bytes has no parts arm (§4.5) — reject rather than guess.
        node = _node()
        with pytest.raises(SeamContractError):
            node._coerce_output(_ctx(State()), b"raw")

    def test_allows_a_plain_user_model_output(self) -> None:
        # A non-State pydantic model is a legitimate structured output (→ DataPart),
        # NOT rejected — the guards target only State/SeamContext, not all BaseModels.
        node = _node()
        result = node._coerce_output(_ctx(State()), ErrorReport(error_type="x"))
        assert isinstance(result, ReturnCall)
