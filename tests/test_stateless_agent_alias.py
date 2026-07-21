"""The ``StatelessAgent`` alias (task-keying prep spec §4, migration arm (a)).

In this PR ``StatelessAgent`` and ``Agent`` are the SAME class object — the alias is
behavior-preserving by construction; the durable split (``Agent`` gaining a
``thread_store``) is the durable PR's flip. Existing no-memory usages rename to
``StatelessAgent`` now so the later flip changes no test/example meaning.
"""


def test_stateless_agent_is_agent_from_the_root_export() -> None:
    from calfkit import Agent, StatelessAgent

    assert StatelessAgent is Agent


def test_stateless_agent_is_agent_from_the_nodes_export() -> None:
    from calfkit.nodes import Agent, StatelessAgent
    from calfkit.nodes.agent import BaseAgentNodeDef

    assert StatelessAgent is Agent
    assert StatelessAgent is BaseAgentNodeDef
