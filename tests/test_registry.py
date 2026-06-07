"""Tests for the per-subclass handler registry (`calfkit._registry`)."""

from typing import Any

import pytest

from calfkit._registry import HandlerInfo, RegistryMixin, handler, handler_info
from calfkit.exceptions import RegistryConfigError

# ---------------------------------------------------------------------------
# handler() decorator + handler_info() reader
# ---------------------------------------------------------------------------


def test_handler_returns_method_unchanged_and_marks_it() -> None:
    def fn(self: object) -> None: ...

    decorated = handler("a")(fn)

    assert decorated is fn  # returned unchanged, still an ordinary method
    assert handler_info(fn) == HandlerInfo(name="a", schema=None)


def test_handler_carries_schema_metadata() -> None:
    sentinel = object()

    def fn(self: object) -> None: ...

    handler("a", schema=sentinel)(fn)

    info = handler_info(fn)
    assert info is not None
    assert info.schema is sentinel


def test_handler_rejects_empty_name() -> None:
    with pytest.raises(ValueError):
        handler("")


def test_handler_info_is_none_for_undecorated() -> None:
    def fn(self: object) -> None: ...

    assert handler_info(fn) is None


# ---------------------------------------------------------------------------
# Collection on a single subclass
# ---------------------------------------------------------------------------


def test_subclass_collects_decorated_methods() -> None:
    class Node(RegistryMixin):
        @handler("alpha")
        def a(self) -> str:
            return "a"

        @handler("beta")
        def b(self) -> str:
            return "b"

        def undecorated(self) -> str:
            return "u"

    node = Node()
    assert set(Node._handlers) == {"alpha", "beta"}

    handlers = node.handlers()
    assert set(handlers) == {"alpha", "beta"}
    # Values are bound methods — callable without passing self.
    assert handlers["alpha"]() == "a"
    assert handlers["beta"]() == "b"


def test_get_handler_returns_bound_method_and_raises_on_unknown() -> None:
    class Node(RegistryMixin):
        @handler("alpha")
        def a(self) -> str:
            return "ok"

    node = Node()
    assert node.get_handler("alpha")() == "ok"
    with pytest.raises(KeyError):
        node.get_handler("missing")


def test_handler_info_readable_from_bound_method() -> None:
    schema = object()

    class Node(RegistryMixin):
        @handler("alpha", schema=schema)
        def a(self) -> None: ...

    info = handler_info(Node().handlers()["alpha"])
    assert info is not None
    assert info.name == "alpha"
    assert info.schema is schema


async def test_async_handler_binds_and_awaits() -> None:
    class Node(RegistryMixin):
        @handler("alpha")
        async def a(self) -> str:
            return "async-ok"

    result = await Node().handlers()["alpha"]()
    assert result == "async-ok"


# ---------------------------------------------------------------------------
# Per-subclass isolation (the core requirement)
# ---------------------------------------------------------------------------


def test_registries_are_isolated_per_subclass() -> None:
    class A(RegistryMixin):
        @handler("a")
        def m(self) -> None: ...

    class B(RegistryMixin):
        @handler("b")
        def m(self) -> None: ...

    assert set(A._handlers) == {"a"}
    assert set(B._handlers) == {"b"}
    assert A._handlers is not B._handlers


def test_base_class_registry_is_empty_sentinel() -> None:
    assert RegistryMixin._handlers == {}
    assert RegistryMixin().handlers() == {}


def test_subclass_without_handlers_is_empty() -> None:
    class Empty(RegistryMixin):
        def plain(self) -> None: ...

    assert Empty._handlers == {}
    assert Empty().handlers() == {}


# ---------------------------------------------------------------------------
# Inheritance: merge across the MRO, override semantics
# ---------------------------------------------------------------------------


def test_subclass_inherits_base_handlers_and_adds_own() -> None:
    class Base(RegistryMixin):
        @handler("base")
        def b(self) -> str:
            return "base"

    class Child(Base):
        @handler("child")
        def c(self) -> str:
            return "child"

    child = Child()
    assert set(Child._handlers) == {"base", "child"}
    assert child.handlers()["base"]() == "base"
    assert child.handlers()["child"]() == "child"
    # Base is unaffected by the child's collection.
    assert set(Base._handlers) == {"base"}


def test_override_with_redecoration_resolves_to_subclass_impl() -> None:
    class Base(RegistryMixin):
        @handler("x")
        def m(self) -> str:
            return "base"

    class Child(Base):
        @handler("x")
        def m(self) -> str:  # override + re-decorate
            return "child"

    assert set(Child._handlers) == {"x"}
    assert Child().handlers()["x"]() == "child"


def test_override_without_redecoration_still_resolves_to_override() -> None:
    # Name-based resolution: a plain override is reflected because the bound
    # method is resolved by attribute name at access time.
    class Base(RegistryMixin):
        @handler("x")
        def m(self) -> str:
            return "base"

    class Child(Base):
        def m(self) -> str:  # override, NOT re-decorated
            return "child"

    assert set(Child._handlers) == {"x"}
    assert Child().handlers()["x"]() == "child"


# ---------------------------------------------------------------------------
# Duplicate-name collision check (raise at class-definition time)
# ---------------------------------------------------------------------------


def test_duplicate_name_in_same_class_raises() -> None:
    with pytest.raises(RegistryConfigError):

        class Bad(RegistryMixin):
            @handler("dup")
            def a(self) -> None: ...

            @handler("dup")
            def b(self) -> None: ...


def test_duplicate_name_across_mro_raises() -> None:
    class Base(RegistryMixin):
        @handler("dup")
        def a(self) -> None: ...

    with pytest.raises(RegistryConfigError):

        class Child(Base):
            @handler("dup")
            def b(self) -> None: ...


# ---------------------------------------------------------------------------
# Multiple inheritance / diamond shapes (guards the reversed-MRO walk)
# ---------------------------------------------------------------------------


def test_diamond_inheritance_merges_handlers_from_both_bases() -> None:
    class Left(RegistryMixin):
        @handler("left")
        def m_left(self) -> str:
            return "left"

    class Right(RegistryMixin):
        @handler("right")
        def m_right(self) -> str:
            return "right"

    class Down(Left, Right):
        @handler("down")
        def m_down(self) -> str:
            return "down"

    down = Down()
    assert set(Down._handlers) == {"left", "right", "down"}
    assert {k: v() for k, v in down.handlers().items()} == {"left": "left", "right": "right", "down": "down"}


def test_duplicate_name_across_sibling_bases_raises() -> None:
    # Two distinct, live methods (different attribute names) claim one handler
    # name — a genuine collision, even though it arrives via sibling bases.
    class Left(RegistryMixin):
        @handler("dup")
        def a(self) -> None: ...

    class Right(RegistryMixin):
        @handler("dup")
        def b(self) -> None: ...

    with pytest.raises(RegistryConfigError):

        class Down(Left, Right): ...


def test_same_attr_name_across_sibling_bases_follows_mro() -> None:
    # Two unrelated bases define the SAME attribute name under the same handler
    # name. Combining them is ordinary Python MRO shadowing (only one method is
    # live), so it does NOT raise — the handler resolves to the MRO winner.
    class Left(RegistryMixin):
        @handler("dup")
        def m(self) -> str:
            return "left"

    class Right(RegistryMixin):
        @handler("dup")
        def m(self) -> str:
            return "right"

    class Down(Left, Right): ...

    assert Down._handlers == {"dup": "m"}
    assert Down().handlers()["dup"]() == "left"  # Left precedes Right in the MRO


def test_init_subclass_chains_cooperatively_with_other_base() -> None:
    # Guards the `super().__init_subclass__(**kwargs)` call: a co-base that also
    # defines __init_subclass__ must still run, and collection must still happen.
    seen: list[str] = []

    class Inspector:
        def __init_subclass__(cls, **kwargs: Any) -> None:
            seen.append(cls.__name__)
            super().__init_subclass__(**kwargs)

    class Combo(RegistryMixin, Inspector):
        @handler("h")
        def h(self) -> str:
            return "h"

    assert seen == ["Combo"]  # the co-base's hook still ran
    assert Combo._handlers == {"h": "h"}  # and collection happened
    assert Combo().handlers()["h"]() == "h"


# ---------------------------------------------------------------------------
# Descriptor targets, rename, isolation, ordering, accessor
# ---------------------------------------------------------------------------


def test_staticmethod_and_classmethod_handlers_collected_and_bound() -> None:
    # Guards the `__func__` unwrap in both handler() and handler_info().
    class Node(RegistryMixin):
        @staticmethod
        @handler("st")
        def st() -> str:
            return "static"

        @classmethod
        @handler("cl")
        def cl(cls) -> str:
            return "class"

    node = Node()
    assert set(Node._handlers) == {"st", "cl"}
    assert node.handlers()["st"]() == "static"
    assert node.handlers()["cl"]() == "class"


def test_override_renames_handler_and_old_name_disappears() -> None:
    # The most-derived definition of an attribute wins, so re-decorating an
    # override with a NEW name retracts the old one. (This is why __init_subclass__
    # resolves the winning name per attribute before keying the registry by name.)
    class Base(RegistryMixin):
        @handler("old")
        def m(self) -> str:
            return "base"

    class Child(Base):
        @handler("new")
        def m(self) -> str:
            return "child"

    assert set(Child._handlers) == {"new"}
    assert "old" not in Child._handlers
    assert Child().handlers()["new"]() == "child"


def test_handlers_returns_fresh_dict_isolated_from_registry() -> None:
    class Node(RegistryMixin):
        @handler("a")
        def a(self) -> None: ...

    node = Node()
    first = node.handlers()
    del first["a"]
    first["injected"] = lambda: None
    assert set(node.handlers()) == {"a"}  # registry unaffected
    assert Node._handlers == {"a": "a"}
    assert node.handlers() is not first  # fresh object each call


def test_same_name_in_unrelated_subclasses_is_allowed() -> None:
    # The uniqueness rule is per-hierarchy, not global.
    class A(RegistryMixin):
        @handler("shared")
        def x(self) -> str:
            return "a"

    class B(RegistryMixin):
        @handler("shared")
        def x(self) -> str:
            return "b"

    assert A._handlers == {"shared": "x"}
    assert B._handlers == {"shared": "x"}
    assert A().handlers()["shared"]() == "a"
    assert B().handlers()["shared"]() == "b"


def test_handler_order_follows_definition_then_mro() -> None:
    class Base(RegistryMixin):
        @handler("z")
        def z(self) -> None: ...

        @handler("a")
        def a(self) -> None: ...

    class Child(Base):
        @handler("m")
        def m(self) -> None: ...

    assert list(Child._handlers) == ["z", "a", "m"]  # base definition order, then child
    assert list(Child().handlers()) == ["z", "a", "m"]


def test_handler_names_accessor() -> None:
    class Node(RegistryMixin):
        @handler("first")
        def f(self) -> None: ...

        @handler("second")
        def s(self) -> None: ...

    assert Node.handler_names() == ("first", "second")
    assert RegistryMixin.handler_names() == ()


def test_handlerinfo_is_slotted_value_equal_and_rejects_empty_name() -> None:
    info = HandlerInfo(name="a")
    assert info == HandlerInfo(name="a", schema=None)
    assert not hasattr(info, "__dict__")  # slots
    with pytest.raises(ValueError):
        HandlerInfo(name="")
