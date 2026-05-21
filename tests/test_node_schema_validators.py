"""Negative-path tests for :class:`BaseNodeSchema.__post_init__` validators.

PR #142 closed the co-tenant tool-return leak (issue #141) by routing
returns through a per-instance ``_return_topic`` derived from ``node_id``.
That fix relies on ``node_id`` being a non-empty identifier without a
leading dot — otherwise the derived topic names degenerate to magic
shared strings (``.private.return``, ``.fanout-state``,
``.fanout-returns``) that every empty-id node would collide on,
re-introducing the same leak shape that PR #142 closed for
``_return_topic``.

Mirrors the empty-``subscribe_topics`` parametrize at
``tests/test_co_tenant_tool_isolation.py::test_empty_subscribe_topics_raises_value_error``
so a regression that moves the guard back into ``BaseNodeDef.__init__``
(which ``@dataclass`` subclasses like ``BaseToolNodeDef`` bypass) is
caught for every node kind, not just the ones whose ``__init__`` chain
happens to reach the guard.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from calfkit._vendor.pydantic_ai.messages import (
    ModelMessage,
    ModelResponse,
)
from calfkit._vendor.pydantic_ai.messages import TextPart as ModelTextPart
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit.nodes import Agent, ConsumerNodeDef, ToolNodeDef


def _stub_model() -> FunctionModel:
    """Minimal FunctionModel — never invoked because construction fails first."""

    def _fn(_messages: list[ModelMessage], _info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[ModelTextPart("unused")])

    return FunctionModel(_fn)


@pytest.mark.parametrize(
    "bad_node_id",
    [
        pytest.param("", id="empty-string"),
        pytest.param("   ", id="whitespace-only"),
        pytest.param(".foo", id="dot-prefixed"),
        pytest.param(".", id="single-dot"),
    ],
)
@pytest.mark.parametrize(
    "node_factory",
    [
        pytest.param(
            lambda nid: Agent(
                nid,
                system_prompt="x",
                subscribe_topics=["t.in"],
                model_client=_stub_model(),
            ),
            id="Agent",
        ),
        pytest.param(
            lambda nid: ConsumerNodeDef(
                node_id=nid,
                subscribe_topics=["t.in"],
                consume_fn=lambda r: None,
            ),
            id="ConsumerNodeDef",
        ),
        pytest.param(
            lambda nid: ToolNodeDef(
                node_id=nid,
                tool_schema=MagicMock(),
                subscribe_topics=["t.in"],
                publish_topic="t.out",
                _tool=MagicMock(),
            ),
            id="ToolNodeDef",
        ),
    ],
)
def test_invalid_node_id_raises_value_error(node_factory, bad_node_id):
    """``BaseNodeSchema.__post_init__`` must reject empty / whitespace-only /
    dot-prefixed ``node_id`` for every node kind. Lives in
    ``__post_init__`` (not ``BaseNodeDef.__init__``) because
    ``@dataclass`` subclasses like ``BaseToolNodeDef`` get an
    auto-generated ``__init__`` that bypasses ``BaseNodeDef.__init__``
    entirely — a regression that moved the guard back into
    ``BaseNodeDef.__init__`` would silently re-open the ``ToolNodeDef``
    bypass and let an empty-id tool produce ``.private.return`` topic
    collisions across co-tenants.
    """
    with pytest.raises(ValueError, match="node_id must be a non-empty identifier"):
        node_factory(bad_node_id)
