"""Phase 2 — resource fields on the read-side context/result types.

These cover the read surfaces that lifecycle ``@resource``/callback-owned
resources reach: ``SessionRunContext`` (agent / ``BaseNodeDef``), ``ToolContext``
(``agent_tool``), and ``NodeResult`` (consumer) plus the deserialize seam.

See ``docs/research/node-worker-lifecycle-hooks-plan-v7.md`` §2.5, §3.5.
"""

from __future__ import annotations

from typing import Any


def test_session_context_resources_defaults_to_empty_mapping() -> None:
    from collections.abc import Mapping

    from calfkit.models.session_context import SessionRunContext
    from calfkit.models.state import State

    ctx = SessionRunContext(state=State(), deps={})

    assert isinstance(ctx.resources, Mapping)
    assert dict(ctx.resources) == {}


def test_session_context_resources_never_serialize_and_survive_deep_copy() -> None:
    from calfkit.models.session_context import SessionRunContext
    from calfkit.models.state import State

    ctx = SessionRunContext(state=State(), deps={})

    # PrivateAttr-backed: never appears in either model dump form.
    assert "resources" not in ctx.model_dump()
    assert "resources" not in ctx.model_dump_json()
    assert "_resources" not in ctx.model_dump()
    assert "_resources" not in ctx.model_dump_json()

    # model_copy(deep=True) (used by prepare_context) given an empty bag at copy
    # time: the copy still has an empty resources view.
    stamped = ctx.model_copy(deep=True)
    assert dict(stamped.resources) == {}

    # The server stamps a shallow-copy dict after the copy; reads see it.
    sentinel = object()
    stamped._resources = {"db": sentinel}
    assert set(stamped.resources) == {"db"}
    assert stamped.resources["db"] is sentinel


def test_tool_context_resources_defaults_empty_and_accepts_kw_only() -> None:
    from calfkit.models.tool_context import ToolContext

    # Default: empty mapping when not supplied.
    default_ctx = ToolContext(deps={}, run_id="cid")
    assert dict(default_ctx.resources) == {}

    # Accepts a resources mapping as a keyword argument.
    sentinel = object()
    ctx = ToolContext(deps={}, run_id="cid", resources={"db": sentinel})
    assert ctx.resources["db"] is sentinel


def _make_node_result(**overrides: Any) -> Any:
    from calfkit.models.node_result import NodeResult
    from calfkit.models.state import State

    kwargs: dict[str, Any] = {
        "output": None,
        "state": State(),
        "correlation_id": "cid",
    }
    kwargs.update(overrides)
    return NodeResult(**kwargs)


def test_node_result_resources_defaults_empty_and_accepts_override() -> None:
    # Default: existing construction sites that omit resources still work.
    default_result = _make_node_result()
    assert dict(default_result.resources) == {}

    # Accepts a resources mapping, mirroring how deps is exposed.
    sentinel = object()
    result = _make_node_result(resources={"db": sentinel})
    assert result.resources["db"] is sentinel


def _make_text_envelope(text: str = "hello") -> Any:
    from calfkit.models import State, TextPart
    from calfkit.models.envelope import Envelope
    from calfkit.models.session_context import CallFrameStack, SessionRunContext, WorkflowState

    state = State(final_output_parts=[TextPart(text=text)])
    return Envelope(
        context=SessionRunContext(state=state, deps={}),
        internal_workflow_state=WorkflowState(call_stack=CallFrameStack()),
    )


def test_from_envelope_threads_resources() -> None:
    from calfkit.models.node_result import NodeResult

    envelope = _make_text_envelope()

    # Default: empty mapping when resources not supplied.
    default_result = NodeResult.from_envelope(envelope, str, correlation_id="cid")
    assert dict(default_result.resources) == {}

    # Threaded through into the built NodeResult.
    sentinel = object()
    result = NodeResult.from_envelope(envelope, str, correlation_id="cid", resources={"db": sentinel})
    assert result.resources["db"] is sentinel
