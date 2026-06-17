"""PR-6 step 1 — seam registration on BaseNodeDef (fault-rail spec §6.1/§6.8).

Pure unit tests for the ``_chains`` registry, the four instance decorators, the
constructor parameters, and registration-time validation (callable + arity). Seams
are registered here but do NOT fire until the staged pipeline wires them in (step 3).
See notes/pr6-fault-rail-implementation-plan.md §3 step 1e.
"""

from __future__ import annotations

import pytest

from calfkit.exceptions import RegistryConfigError
from calfkit.nodes._seams import AFTER_NODE, BEFORE_NODE, ON_CALLEE_ERROR, ON_NODE_ERROR
from calfkit.nodes.base import BaseNodeDef


def _node(**kwargs: object) -> BaseNodeDef:
    return BaseNodeDef(node_id="n", subscribe_topics=["in"], **kwargs)


class TestDecoratorRegistration:
    def test_before_node_decorator_registers_in_order_and_returns_fn(self) -> None:
        # The decorator appends to the chain (registration order) and returns the
        # function unchanged, so it stays directly unit-testable (the .gate() precedent).
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
