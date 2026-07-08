"""PR-6 step 1 — seam registration on BaseNodeDef (fault-rail spec §6.1/§6.8).

Pure unit tests for the ``_chains`` registry, the four instance decorators, the
constructor parameters, and registration-time validation (callable + arity). Seams
are registered here but do NOT fire until the staged pipeline wires them in (step 3).
See notes/pr6-fault-rail-implementation-plan.md §3 step 1e.
"""

from __future__ import annotations

import functools
import inspect

import pytest

from calfkit.exceptions import RegistryConfigError
from calfkit.nodes._seams import AFTER_NODE, BEFORE_NODE, ON_CALLEE_ERROR, ON_NODE_ERROR
from calfkit.nodes.base import BaseNodeDef


def _node(**kwargs: object) -> BaseNodeDef:
    return BaseNodeDef(node_id="n", subscribe_topics=["in"], **kwargs)


class TestDecoratorRegistration:
    def test_before_node_decorator_registers_in_order_and_returns_fn(self) -> None:
        # The decorator appends to the chain (registration order) and returns the
        # function unchanged, so it stays directly unit-testable.
        node = _node()

        @node.before_node
        def first(ctx: object) -> None:
            return None

        @node.before_node
        def second(ctx: object) -> None:
            return None

        assert node._chains[BEFORE_NODE] == [first, second]
        assert callable(first) and callable(second)

    def test_all_four_decorators_register_to_their_chains(self) -> None:
        node = _node()

        @node.before_node
        def b(ctx: object) -> None:
            return None

        @node.after_node
        def a(ctx: object, output: object) -> None:
            return None

        @node.on_node_error
        def ne(ctx: object, fault: object) -> None:
            return None

        @node.on_callee_error
        def ce(ctx: object, fault: object) -> None:
            return None

        assert node._chains[BEFORE_NODE] == [b]
        assert node._chains[AFTER_NODE] == [a]
        assert node._chains[ON_NODE_ERROR] == [ne]
        assert node._chains[ON_CALLEE_ERROR] == [ce]


class TestConstructorRegistration:
    def test_accepts_single_callable_or_list(self) -> None:
        def b(ctx: object) -> None:
            return None

        def a1(ctx: object, output: object) -> None:
            return None

        def a2(ctx: object, output: object) -> None:
            return None

        node = _node(before_node=b, after_node=[a1, a2])

        assert node._chains[BEFORE_NODE] == [b]  # single callable normalized to a 1-list
        assert node._chains[AFTER_NODE] == [a1, a2]  # list preserved in order

    def test_constructor_entries_precede_decorated(self) -> None:
        # spec §6.1 chain-order: constructor entries first, then decorator entries.
        def ctor_handler(ctx: object) -> None:
            return None

        node = _node(before_node=ctor_handler)

        @node.before_node
        def decorated(ctx: object) -> None:
            return None

        assert node._chains[BEFORE_NODE] == [ctor_handler, decorated]


class TestRegistrationValidation:
    def test_rejects_non_callable_handler(self) -> None:
        # Validated at registration (spec §6.8), never mid-message.
        with pytest.raises(RegistryConfigError):
            _node(before_node=42)  # type: ignore[arg-type]

    def test_rejects_wrong_arity_handler(self) -> None:
        # after_node handlers take (ctx, output); a 1-arg handler is rejected at
        # registration with a teaching error, not at first fire.
        node = _node()
        with pytest.raises(RegistryConfigError):

            @node.after_node
            def only_ctx(ctx: object) -> None:
                return None


class TestWrappedHandlerArity:
    """D6a — the base arity check must use the WRAPPER's OWN signature (``follow_wrapped=False``), so a
    ``functools.wraps``'d handler is validated at the arity it will be CALLED with, not the inner's.
    This is what lets the ``on_tool_error`` adapter wrap a 3-param handler into a 2-param
    ``on_callee_error`` chain entry; without it the check follows ``__wrapped__`` and is wrong in both
    directions (spurious reject of a valid adapter, and a wrong-pass that ``TypeError``s mid-flight)."""

    def test_wraps_arity2_wrapper_over_arity3_inner_registers(self) -> None:
        # The adapter's exact shape: an arity-2 (ctx, report) wrapper over an arity-3
        # (tool_call, ctx, report) inner, carrying functools.wraps. The check must accept it at the
        # WRAPPER's real arity (2). Pre-fix it followed __wrapped__ to the arity-3 inner and rejected.
        def inner(tool_call: object, ctx: object, report: object) -> None:
            return None

        @functools.wraps(inner)
        def wrapper(ctx: object, report: object) -> None:
            return inner(None, ctx, report)

        node = _node()
        node.on_callee_error(wrapper)  # must NOT raise
        assert wrapper in node._chains[ON_CALLEE_ERROR]

    def test_wraps_wrong_arity3_wrapper_over_arity2_inner_is_rejected(self) -> None:
        # The wrong-pass direction: an arity-3 wrapper (that will TypeError when the seam calls it with
        # (ctx, report)) carrying functools.wraps over an arity-2 inner. Pre-fix the check followed
        # __wrapped__ to the arity-2 inner and WRONGLY PASSED, deferring the failure to first fire. It
        # must reject at registration.
        def inner(ctx: object, report: object) -> None:
            return None

        @functools.wraps(inner)
        def wrapper(a: object, b: object, c: object) -> None:  # arity 3 — cannot be called as (ctx, report)
            return inner(a, b)

        node = _node()
        with pytest.raises(RegistryConfigError):
            node.on_callee_error(wrapper)

    def test_wraps_updated_empty_does_not_leak_inner_signature(self) -> None:
        # __signature__-copy edge: an inner bearing an explicit __signature__ (arity 3). The adapter
        # wraps with functools.wraps(fn, updated=()) so fn.__dict__ (where __signature__ lives) is NOT
        # copied onto the wrapper — otherwise the leaked inner signature would defeat follow_wrapped=False
        # and wrongly reject the arity-2 wrapper.
        def inner(tool_call: object, ctx: object, report: object) -> None:
            return None

        inner.__signature__ = inspect.signature(inner)  # type: ignore[attr-defined]  # explicit sig in inner.__dict__

        @functools.wraps(inner, updated=())
        def wrapper(ctx: object, report: object) -> None:
            return inner(None, ctx, report)

        node = _node()
        node.on_callee_error(wrapper)  # must NOT raise — the wrapper's real arity (2) is used
        assert wrapper in node._chains[ON_CALLEE_ERROR]


class TestNoSeamsOnObservers:
    """§6.6: observers (``ConsumerNode``) have NO policy seams — they consume via their
    own body and never call, so there is nothing for a seam to guard. Registering any
    seam is rejected at registration time (the caller-capable-only API)."""

    @pytest.mark.parametrize("seam", ["before_node", "after_node", "on_node_error", "on_callee_error"])
    def test_registering_any_seam_on_a_consumer_raises(self, seam: str) -> None:
        from calfkit.nodes.consumer import ConsumerNode

        node = ConsumerNode(name="obs", subscribe_topics="in", consume_fn=lambda ctx: None)
        register = getattr(node, seam)
        # The guard fires before the callable/arity checks, so a perfectly valid handler
        # is still rejected purely because the node is an observer.
        with pytest.raises(RegistryConfigError, match="observer"):
            register(lambda *args: None)
