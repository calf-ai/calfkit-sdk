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
from calfkit.peers import Handoff, Messaging


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


# ── Handoff handle (PR-C/ADR-0019): byte-for-byte mirror of Messaging — same curated-XOR-discover
# validation, varargs __init__, frozen value semantics, no tool protocols. Gates the handoff tool. ──


def test_handoff_curated_names() -> None:
    assert Handoff("refunds", "billing").names == ("refunds", "billing")
    assert Handoff(names=["a", "b"]).names == ("a", "b")
    assert Handoff("refunds").discover is False


def test_handoff_discover() -> None:
    h = Handoff(discover=True)
    assert h.discover is True
    assert h.names == ()


def test_handoff_neither_raises() -> None:
    # Neither names nor discover — never an implicit "everything" (fail-loud).
    with pytest.raises(ValueError):
        Handoff()


def test_handoff_both_raises() -> None:
    with pytest.raises(ValueError):
        Handoff("a", discover=True)
    with pytest.raises(ValueError):
        Handoff(names=["a"], discover=True)
    with pytest.raises(ValueError):
        Handoff("a", names=["b"])


def test_handoff_empty_name_raises() -> None:
    with pytest.raises(ValueError):
        Handoff("")


def test_handoff_dedupes_order_preserving() -> None:
    assert Handoff("a", "b", "a").names == ("a", "b")


def test_handoff_frozen_value_equality() -> None:
    assert Handoff("a", "b") == Handoff("a", "b")
    assert hash(Handoff("a")) == hash(Handoff("a"))


def test_handoff_does_not_implement_tool_protocols() -> None:
    # M4: Handoff must NOT be a ToolSelector/ToolProvider, so a stray Handoff in tools= falls through
    # split_tool_declarations to its else -> TypeError rather than being silently absorbed.
    h = Handoff("a")
    assert not hasattr(h, "resolve_tools")
    assert not hasattr(h, "tool_bindings")


def test_handoff_is_exported_first_class() -> None:
    # `Handoff` is a first-class export (top-level + its package), mirroring `Messaging`/`Tools`.
    from calfkit import Handoff as TopLevel
    from calfkit.peers import Handoff as FromPkg
    from calfkit.peers.handoff import Handoff as FromModule

    assert TopLevel is FromPkg is FromModule


# ── Agent ctor wiring: peers= accepts Handoff per-capability, independent of Messaging ──


def test_agent_peers_accepts_handoff_and_exposes_handoff_handles() -> None:
    a = _agent(peers=[Handoff("refunds"), Handoff("billing")])
    assert a._peers == (Handoff("refunds"), Handoff("billing"))
    assert a._handoff_handles == [Handoff("refunds"), Handoff("billing")]


def test_agent_messaging_and_handoff_compose_independently() -> None:
    # A mixed peers= list keeps each capability's handles separate (per-capability scope).
    a = _agent(peers=[Messaging("billing"), Handoff("refunds")])
    assert a._messaging_handles == [Messaging("billing")]
    assert a._handoff_handles == [Handoff("refunds")]


def test_agent_handoff_self_reject() -> None:
    # M2: an agent may not name itself in a Handoff handle — the own-name reject lives in the Agent ctor
    # (the handle can't see the agent's name). (Self is also excluded at render + dispatch.)
    with pytest.raises(ValueError):
        Agent(name="triage", subscribe_topics="triage.in", model_client=TestModel(), peers=[Handoff("triage")])


def test_handoff_in_tools_raises() -> None:
    # M4 (reverse): a Handoff handle placed in tools= falls through split_tool_declarations to its
    # else -> TypeError (Handoff implements neither tool protocol).
    with pytest.raises(TypeError):
        _agent(tools=[Handoff("billing")])


def test_agent_handoff_discover_exclusivity() -> None:
    # §5.1: a discover Handoff handle is the exclusive author of the HANDOFF scope — no named Handoff
    # handle may accompany it (mirrors the Messaging discover-exclusivity, per capability).
    with pytest.raises(ValueError):
        _agent(peers=[Handoff("a"), Handoff(discover=True)])
    # multiple CURATED handoff handles are independent (allowed; the directory dedupes by name).
    assert len(_agent(peers=[Handoff("a"), Handoff("b")])._peers) == 2


def test_agent_per_capability_discover_scopes_are_independent() -> None:
    # Scopes are independent PER CAPABILITY: a discover handle of one capability does NOT conflict with a
    # named handle of the OTHER (discover Messaging + named Handoff is fine, and vice versa).
    a = _agent(peers=[Messaging(discover=True), Handoff("refunds")])
    assert a._messaging_handles[0].discover is True
    assert a._handoff_handles == [Handoff("refunds")]
    b = _agent(peers=[Handoff(discover=True), Messaging("billing")])
    assert b._handoff_handles[0].discover is True
    assert b._messaging_handles == [Messaging("billing")]


def test_same_name_in_messaging_and_handoff_is_allowed_and_partitioned() -> None:
    # Per-capability independence: the SAME peer may be both messageable AND a handoff target — the handles
    # partition by type (no dedupe across kinds), so the agent can consult billing OR transfer to it.
    a = _agent(peers=[Messaging("billing"), Handoff("billing")])
    assert a._messaging_handles == [Messaging("billing")]
    assert a._handoff_handles == [Handoff("billing")]


def test_sequential_handoff_only_agent_registers_no_fanout_store() -> None:
    # decision 1(b), NARROWED for PR-C: `_needs_durable_batch` keys on MESSAGING handles, not any peer. A
    # Handoff-only agent never dispatches an isolate_state Call (a winning handoff is a TailCall, not
    # a Call), so a SEQUENTIAL Handoff-only agent (not fanout-capable) provisions NO durable store — unlike
    # a sequential MESSAGING agent (test_sequential_messaging_agent_registers_fanout_store).
    from calfkit.nodes._fanout_store import FANOUT_STORE_KEY

    handoff_seq = Agent(name="t", subscribe_topics="t.in", model_client=TestModel(), sequential_only_mode=True, peers=[Handoff("p")])
    assert FANOUT_STORE_KEY not in [n for n, _ in handoff_seq._resource_cms()]
