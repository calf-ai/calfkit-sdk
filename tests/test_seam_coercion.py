"""PR-6 step 1 — generic seam-value coercion + the §6.2 teaching guards.

Pure unit tests for ``BaseNodeDef._coerce_output`` (spec §6.9 generic conversion:
seam value → ``ReturnCall``) and the §6.2 guards that reject migration-trap return
types (bool / the session State / the SeamContext / bytes) with ``SeamContractError``.
Additive — these are not wired into the live pipeline until step 3. See
notes/pr6-fault-rail-implementation-plan.md §3 step 1 / §4 layer 2.
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel

from calfkit.exceptions import DeserializationError, SeamContractError
from calfkit.models.actions import Call, Next, ReturnCall, TailCall
from calfkit.models.error_report import ErrorReport
from calfkit.models.payload import DataPart
from calfkit.models.seam_context import SeamContext
from calfkit.models.state import State
from calfkit.nodes.base import BaseNodeDef


def _node() -> BaseNodeDef:
    return BaseNodeDef(node_id="n", subscribe_topics=["in"])


class _Report(BaseModel):
    title: str


class _TypedModelNode(BaseNodeDef):
    """A node whose declared seam output type is a structured model (the agent's pattern: it overrides
    _seam_output_type to its final_output_type). Exercises _coerce_output's output-position validation."""

    @property
    def _seam_output_type(self) -> Any:
        return _Report


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


class TestOutputPositionValidation:
    """scenario 44: an agent's output-position seam substitute (before_node / on_node_error /
    after_node — all flow through _coerce_output) is validated against its declared output type, so a
    type-breaking substitute fails AT THE SEAM (→ a fault), not as a DeserializationError in the
    caller's process. A custom node (untyped) and on_callee_error substitutes (slot position) are exempt."""

    def test_typed_node_accepts_a_matching_substitute(self) -> None:
        node = _TypedModelNode(node_id="t", subscribe_topics=["in"])
        result = node._coerce_output(_ctx(State()), {"title": "ok"})  # validates as _Report
        assert isinstance(result, ReturnCall) and result.value == {"title": "ok"}

    def test_typed_node_rejects_a_type_breaking_substitute(self) -> None:
        # The "after_node returning 'redacted'" case on a structured-output agent.
        node = _TypedModelNode(node_id="t", subscribe_topics=["in"])
        with pytest.raises(SeamContractError):
            node._coerce_output(_ctx(State()), "redacted")

    def test_untyped_node_skips_validation(self) -> None:
        # A custom BaseNodeDef (base _seam_output_type=_UNSET) coerces generically — custom nodes are free.
        result = _node()._coerce_output(_ctx(State()), "anything")
        assert isinstance(result, ReturnCall) and result.value == "anything"

    def test_unschematizable_output_type_skips_validation(self) -> None:
        # An output type TypeAdapter cannot schematize degrades to a lenient SKIP (a valid exotic-output
        # agent config must not crash at the seam) rather than raising.
        class _Unschematizable:
            pass

        class _ExoticNode(BaseNodeDef):
            @property
            def _seam_output_type(self) -> Any:
                return _Unschematizable

        node = _ExoticNode(node_id="e", subscribe_topics=["in"])
        result = node._coerce_output(_ctx(State()), "anything")  # cannot schematize → skip, no raise
        assert isinstance(result, ReturnCall)

    def test_wrapper_instance_output_type_skips_validation(self) -> None:
        # The realistic exotic OutputSpec: a structured-output agent declared via ToolOutput(M) /
        # NativeOutput(M) / PromptedOutput(M) — a wrapper INSTANCE, not a bare type. TypeAdapter(<wrapper
        # instance>) raises AttributeError ('no __mro__'); that must degrade to the lenient SKIP, not
        # escape and spuriously fault EVERY seam substitute for a common agent config (round-1 MAJOR).
        from calfkit._vendor.pydantic_ai.output import ToolOutput

        class _WrapperOutputNode(BaseNodeDef):
            @property
            def _seam_output_type(self) -> Any:
                return ToolOutput(_Report)

        node = _WrapperOutputNode(node_id="w", subscribe_topics=["in"])
        result = node._coerce_output(_ctx(State()), {"title": "ok"})  # lenient skip — no AttributeError fault
        assert isinstance(result, ReturnCall) and result.value == {"title": "ok"}


class _TypedIntNode(BaseNodeDef):
    """A node whose declared seam output type is ``int`` — bool subclasses int, so this
    pins that the §6.2 bool guard runs BEFORE the int-accepting output-position validation."""

    @property
    def _seam_output_type(self) -> Any:
        return int


class TestCoerceGuards:
    def test_rejects_bool(self) -> None:
        # §6.2: a bool is never a node output — the predictable port of a gate's
        # `return True` must not short-circuit the happy path with True as the answer.
        node = _node()
        with pytest.raises(SeamContractError):
            node._coerce_output(_ctx(State()), True)

    def test_bool_guard_precedes_int_output_type_path(self) -> None:
        # §6.2 ordering: bool subclasses int, so on an int-typed node a `True` substitute
        # WOULD validate as int=1 if the output-position validation ran first. The bool guard
        # must fire BEFORE it — `return True` is caught as "never a node output", not silently
        # accepted as the integer 1.
        node = _TypedIntNode(node_id="i", subscribe_topics=["in"])
        node._coerce_output(_ctx(State()), 1)  # a real int still validates and coerces
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

    def test_rejects_next(self) -> None:
        # §6.2: Next is route-dispatch vocabulary (the CoR decline sentinel), not a seam output. It slips
        # past _is_action (Next is not an action), so without a guard a seam returning Next would coerce
        # to a garbage empty DataPart — reject it loudly instead.
        node = _node()
        with pytest.raises(SeamContractError):
            node._coerce_output(_ctx(State()), Next())

    def test_allows_a_plain_user_model_output(self) -> None:
        # A non-State pydantic model is a legitimate structured output (→ DataPart),
        # NOT rejected — the guards target only State/SeamContext, not all BaseModels.
        node = _node()
        result = node._coerce_output(_ctx(State()), ErrorReport(error_type="x"))
        assert isinstance(result, ReturnCall)


class TestOutputView:
    def test_terminal_returncall_projects_lenient_view(self) -> None:
        # _output_view gives after_node the OutputT view of a terminal output. On an
        # untyped node (base _seam_output_type = _UNSET) it is the lenient projection.
        node = _node()
        view = node._output_view(_ctx(State()), ReturnCall[State](state=State(), value="hi"))
        assert view == "hi"

    def test_non_terminal_actions_have_no_view(self) -> None:
        # after_node guards a *produced* output; a Call/TailCall produces nothing yet.
        node, ctx = _node(), _ctx(State())
        assert node._output_view(ctx, Call[State]("topic", State())) is None
        assert node._output_view(ctx, TailCall[State](target_topic="t", state=State())) is None

    def test_empty_output_has_no_view(self) -> None:
        # A terminal output with no content → no view (lenient; never raises).
        node = _node()
        assert node._output_view(_ctx(State()), ReturnCall[State](state=State(), value=None)) is None

    def test_typed_node_projects_strictly_against_its_output_type(self) -> None:
        # A typed node (the agent's pattern) projects the view against its declared
        # output type — strict, so it validates.
        node = _TypedStrNode(node_id="t", subscribe_topics=["in"])
        view = node._output_view(_ctx(State()), ReturnCall[State](state=State(), value="hello"))
        assert view == "hello"

    def test_typed_node_raises_on_an_unprojectable_output(self) -> None:
        # A type-breaking output (no TextPart for a str-typed node) raises at the seam
        # (→ a fault in the pipeline), not silently downstream (spec §6.3).
        node = _TypedStrNode(node_id="t", subscribe_topics=["in"])
        with pytest.raises(DeserializationError):
            node._output_view(_ctx(State()), ReturnCall[State](state=State(), value=[DataPart(data={"x": 1})]))


class _TypedStrNode(BaseNodeDef):
    """A node whose seam output type is ``str`` — exercises _output_view's strict branch
    (the agent overrides _seam_output_type to its final_output_type the same way, step 4)."""

    @property
    def _seam_output_type(self) -> Any:
        return str


class TestInterpret:
    def test_action_passes_through_unchanged(self) -> None:
        # §6.3 tier-2: a boundary seam returning a NodeResult action executes it as-is.
        node, ctx = _node(), _ctx(State())
        call = Call[State]("topic", State())
        assert node._interpret(ctx, call) is call
        tail = TailCall[State](target_topic="t", state=State())
        assert node._interpret(ctx, tail) is tail

    def test_fanout_list_passes_through_but_parts_list_is_a_value(self) -> None:
        # list[Call] is an action (fan-out); list[ContentPart] is a value (parts).
        node, ctx = _node(), _ctx(State())
        calls = [Call[State]("a", State()), Call[State]("b", State())]
        assert node._interpret(ctx, calls) is calls
        parts_value = [DataPart(data={"k": 1})]
        coerced = node._interpret(ctx, parts_value)
        assert isinstance(coerced, ReturnCall) and coerced.value is parts_value

    def test_plain_value_is_coerced_to_output(self) -> None:
        node, ctx = _node(), _ctx(State())
        result = node._interpret(ctx, "answer")
        assert isinstance(result, ReturnCall)
        assert result.value == "answer"


class TestApplyAfter:
    async def test_no_after_node_returns_output_unchanged(self) -> None:
        # Short-circuit: with no after_node registered there is nothing to guard, so the
        # output passes through untouched (and the view is never even computed).
        node = _node()
        output = ReturnCall[State](state=State(), value="orig")
        assert await node._apply_after(_ctx(State()), output) is output

    async def test_after_node_declining_keeps_output(self) -> None:
        node = _node()

        @node.after_node
        def keep(ctx: object, output: object) -> None:
            return None

        output = ReturnCall[State](state=State(), value="orig")
        assert await node._apply_after(_ctx(State()), output) is output

    async def test_after_node_value_replaces_output(self) -> None:
        node = _node()

        @node.after_node
        def replace(ctx: object, output: object) -> str:
            return "replaced"

        result = await node._apply_after(_ctx(State()), ReturnCall[State](state=State(), value="orig"))
        assert isinstance(result, ReturnCall) and result.value == "replaced"

    async def test_after_node_returning_an_action_raises(self) -> None:
        # after_node is values-only (§6.1); returning an action is a contract violation.
        node = _node()

        @node.after_node
        def bad(ctx: object, output: object) -> Call[State]:
            return Call[State]("t", State())

        with pytest.raises(SeamContractError):
            await node._apply_after(_ctx(State()), ReturnCall[State](state=State(), value="orig"))
