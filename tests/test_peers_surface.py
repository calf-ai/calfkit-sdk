"""PR-B Item 3: the ``peers=`` surface — the ``Messaging`` capability handle + ``Agent`` ctor wiring.

``Messaging`` is an identity-only, frozen handle mirroring the shipped ``Tools`` handle (curated names
XOR ``discover``; custom varargs ``__init__``; order-preserving dedupe). It deliberately does NOT
implement the tool protocols, so it can't be absorbed by ``tools=`` (M4). The agent's-own-name reject
and the non-``Messaging`` type-validation live in the ``Agent`` ctor (M2/M4).
"""

import pytest

from calfkit._vendor.pydantic_ai.models.test import TestModel
from calfkit.nodes import Agent
from calfkit.nodes.tool import Tools
from calfkit.peers import Messaging


def _agent(**kw: object) -> Agent[str]:
    return Agent(name="triage", subscribe_topics="triage.in", model_client=TestModel(), **kw)  # type: ignore[arg-type]


def test_messaging_curated_names() -> None:
    assert Messaging("billing", "support").names == ("billing", "support")
    assert Messaging(names=["a", "b"]).names == ("a", "b")
    assert Messaging("billing").discover is False


def test_messaging_discover() -> None:
    m = Messaging(discover=True)
    assert m.discover is True
    assert m.names == ()


def test_messaging_neither_raises() -> None:
    # Neither names nor discover — never an implicit "everything" (fail-loud).
    with pytest.raises(ValueError):
        Messaging()


def test_messaging_both_raises() -> None:
    with pytest.raises(ValueError):
        Messaging("a", discover=True)
    with pytest.raises(ValueError):
        Messaging(names=["a"], discover=True)
    with pytest.raises(ValueError):
        Messaging("a", names=["b"])


def test_messaging_empty_name_raises() -> None:
    with pytest.raises(ValueError):
        Messaging("")


def test_messaging_dedupes_order_preserving() -> None:
    assert Messaging("a", "b", "a").names == ("a", "b")


def test_messaging_frozen_value_equality() -> None:
    assert Messaging("a", "b") == Messaging("a", "b")
    assert hash(Messaging("a")) == hash(Messaging("a"))


def test_messaging_does_not_implement_tool_protocols() -> None:
    # M4: Messaging must NOT be a ToolSelector/ToolProvider, so a stray Messaging in tools= falls
    # through split_tool_declarations to its else -> TypeError rather than being silently absorbed.
    m = Messaging("a")
    assert not hasattr(m, "resolve_tools")
    assert not hasattr(m, "tool_bindings")


# ── Agent ctor wiring: peers= -> self._peers, M2 self-reject, M4 type-validation ──


def test_agent_peers_sets_peers_tuple() -> None:
    a = _agent(peers=[Messaging("billing"), Messaging("support")])
    assert a._peers == (Messaging("billing"), Messaging("support"))


def test_agent_no_peers_is_empty_tuple() -> None:
    assert _agent()._peers == ()
    assert _agent(peers=[])._peers == ()  # peers=[] equivalent to None


def test_agent_peers_rejects_non_messaging() -> None:
    # M4: peers= type-validates each element is a capability handle (Messaging in PR-B); a Tools/
    # ToolBinding/etc. raises TypeError rather than being silently accepted.
    with pytest.raises(TypeError):
        _agent(peers=[Tools("add")])


def test_agent_peers_self_reject() -> None:
    # M2: an agent may not name itself in a Messaging handle — the own-name reject lives in the Agent
    # ctor (the handle can't see the agent's name). (Self is also excluded at render + dispatch.)
    with pytest.raises(ValueError):
        Agent(name="triage", subscribe_topics="triage.in", model_client=TestModel(), peers=[Messaging("triage")])


def test_messaging_in_tools_raises() -> None:
    # M4 (reverse): a Messaging handle placed in tools= falls through split_tool_declarations to its
    # else -> TypeError (Messaging implements neither tool protocol).
    with pytest.raises(TypeError):
        _agent(tools=[Messaging("billing")])


def test_sequential_messaging_agent_registers_fanout_store() -> None:
    # decision 1(b): a `sequential_only_mode` messaging agent still needs the durable-batch store for its
    # lone message_agent (it is `_needs_durable_batch` via `_peers`), even though it routes tools
    # one-at-a-time. A sequential agent WITHOUT peers registers no store.
    from calfkit.nodes._fanout_store import FANOUT_STORE_KEY

    messaging_seq = Agent(name="t", subscribe_topics="t.in", model_client=TestModel(), sequential_only_mode=True, peers=[Messaging("p")])
    assert FANOUT_STORE_KEY in [n for n, _ in messaging_seq._resource_cms()]
    plain_seq = Agent(name="t2", subscribe_topics="t2.in", model_client=TestModel(), sequential_only_mode=True)
    assert FANOUT_STORE_KEY not in [n for n, _ in plain_seq._resource_cms()]


def test_agent_messaging_discover_exclusivity() -> None:
    # §5.1: a discover Messaging handle is the exclusive author of the messaging scope — no named
    # Messaging handle may accompany it (mirrors the shipped Tools discover-exclusivity).
    with pytest.raises(ValueError):
        _agent(peers=[Messaging("a"), Messaging(discover=True)])
    # multiple CURATED handles are independent (allowed; the directory dedupes by name).
    assert len(_agent(peers=[Messaging("a"), Messaging("b")])._peers) == 2


def test_messaging_is_exported_first_class() -> None:
    # `Messaging` is a first-class export (top-level + its package), mirroring how `Tools` is exported, so
    # users write `from calfkit import Messaging` rather than reaching into the deep module path.
    from calfkit import Messaging as TopLevel
    from calfkit.peers import Messaging as FromPkg
    from calfkit.peers.messaging import Messaging as FromModule

    assert TopLevel is FromPkg is FromModule
