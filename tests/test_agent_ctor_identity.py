"""The ``StatelessAgent`` constructor takes its identity as ``name`` (not ``node_id``).

This mirrors the MCP node types' precedent (``MCPToolboxNode(name=...)``, PRs #254/#255):
the ctor surface names identity ``name`` and maps it onto the base node's ``node_id``
storage. ``.name`` and ``.node_id`` both read the same value. A surface-only rename —
the legacy ``node_id=`` keyword is a clean pre-1.0 break.
"""

from __future__ import annotations

from typing import Any

import pytest

from calfkit._vendor.pydantic_ai.messages import ModelResponse
from calfkit._vendor.pydantic_ai.messages import TextPart as ModelTextPart
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit.nodes import StatelessAgent


def _model() -> FunctionModel:
    def _fn(messages: list[Any], info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[ModelTextPart("done")])

    return FunctionModel(_fn)


def test_constructs_with_name_keyword() -> None:
    agent = StatelessAgent(name="researcher", subscribe_topics="in", model_client=_model())
    assert agent.name == "researcher"
    assert agent.node_id == "researcher"


def test_constructs_with_positional_identity() -> None:
    agent = StatelessAgent("researcher", subscribe_topics="in", model_client=_model())
    assert agent.name == "researcher"


def test_rejects_legacy_node_id_keyword() -> None:
    with pytest.raises(TypeError):
        StatelessAgent(node_id="researcher", subscribe_topics="in", model_client=_model())
