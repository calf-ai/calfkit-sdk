"""PR-B Item 5: the ``message_agent`` external tool — injection, dispatch, target validation, fold.

``message_agent`` is a real ``ExternalToolset`` member (so the model run surfaces it rather than
swallowing it as unknown), rendered with the live Messaging-scoped directory each turn. The agent's
dispatch forks on its well-known name BEFORE the ``tools_registry`` lookup: a valid target dispatches a
``Call`` to ``agent.{name}.private.input`` with a FRESH seeded sub-state and ``isolate_state=True`` (so
the caller resumes on its OWN history via the durable snapshot/restore batch); a bad target (offline /
out-of-scope / self / cycle) becomes an LLM-visible ``RetryPromptPart`` with no dispatch. The peer's
reply folds (all parts serialized) into ``tool_results[tag]``.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from calfkit._vendor.pydantic_ai.messages import ModelResponse, RetryPromptPart, ToolCallPart
from calfkit._vendor.pydantic_ai.messages import TextPart as ModelTextPart
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit._vendor.pydantic_ai.models.test import TestModel
from calfkit._vendor.pydantic_ai.tools import ToolDefinition
from calfkit.models.actions import Call, TailCall
from calfkit.models.agents import AGENTS_VIEW_RESOURCE_KEY
from calfkit.models.payload import FilePart
from calfkit.models.payload import ToolCallPart as PayloadToolCallPart
from calfkit.models.state import State
from calfkit.models.tool_dispatch import ToolBinding
from calfkit.nodes import StatelessAgent
from calfkit.nodes.agent import _serialize_message_reply
from calfkit.peers import Handoff, Messaging
from tests.test_tool_errors import _make_ctx, _model_emits_tool_calls, _unwrap


def _view(cards: dict[str, str | None]) -> object:
    snap = {name: SimpleNamespace(description=desc) for name, desc in cards.items()}
    return SimpleNamespace(snapshot=lambda: snap)


def _ctx_with_view(view: object, *, ancestors: frozenset[tuple[str, str]] = frozenset()) -> object:
    ctx = _make_ctx(State())
    ctx._resources = {AGENTS_VIEW_RESOURCE_KEY: view}
    ctx._ancestor_callers = ancestors
    return ctx


def _msg_call(name: str, message: str = "hi", tool_call_id: str = "tc1") -> ToolCallPart:
    return ToolCallPart(tool_name="message_agent", args={"name": name, "message": message}, tool_call_id=tool_call_id)


async def test_message_agent_injected_as_real_toolset_member_with_directory() -> None:
    captured: dict[str, dict[str, str | None]] = {}

    def _capture(messages: object, info: AgentInfo) -> ModelResponse:
        captured["tools"] = {t.name: t.description for t in info.function_tools}
        return ModelResponse(parts=[ModelTextPart("done")])

    agent = StatelessAgent("triage", subscribe_topics="triage.in", model_client=FunctionModel(_capture), peers=[Messaging("billing")])
    await agent.run(_ctx_with_view(_view({"billing": "Billing questions."})))
    assert "message_agent" in captured["tools"]
    assert "billing" in (captured["tools"]["message_agent"] or "")


async def test_message_agent_dispatches_isolate_state_peer_call() -> None:
    agent = StatelessAgent(
        "triage", subscribe_topics="triage.in", model_client=_model_emits_tool_calls([_msg_call("billing", "balance?")]), peers=[Messaging("billing")]
    )
    ctx = _ctx_with_view(_view({"billing": "Billing."}))
    result = _unwrap(await agent.run(ctx))
    assert isinstance(result, Call)
    assert result.target_topic == "agent.billing.private.input"
    assert result.isolate_state is True
    assert result.tag == "tc1"
    assert "tc1" not in ctx.state.tool_results  # dispatched, not retried
    # fresh seed: the peer sees only the message, staged as a user turn (committed on the peer's first run)
    assert result.state.uncommitted_message is not None


async def test_message_agent_offline_target_retries() -> None:
    agent = StatelessAgent(
        "triage", subscribe_topics="triage.in", model_client=_model_emits_tool_calls([_msg_call("ghost")]), peers=[Messaging(discover=True)]
    )
    ctx = _ctx_with_view(_view({"billing": None}))  # ghost not live
    result = _unwrap(await agent.run(ctx))
    assert isinstance(ctx.state.tool_results.get("tc1"), RetryPromptPart)
    assert isinstance(result, TailCall)  # all calls invalid -> self-retry


async def test_message_agent_self_target_retries() -> None:
    agent = StatelessAgent(
        "triage", subscribe_topics="triage.in", model_client=_model_emits_tool_calls([_msg_call("triage")]), peers=[Messaging(discover=True)]
    )
    ctx = _ctx_with_view(_view({"triage": None, "billing": None}))  # self present but never messageable
    await agent.run(ctx)
    assert isinstance(ctx.state.tool_results.get("tc1"), RetryPromptPart)


async def test_message_agent_cycle_target_retries() -> None:
    agent = StatelessAgent(
        "triage", subscribe_topics="triage.in", model_client=_model_emits_tool_calls([_msg_call("billing")]), peers=[Messaging("billing")]
    )
    ctx = _ctx_with_view(_view({"billing": None}), ancestors=frozenset({("billing", "agent")}))  # billing awaits us (a ring)
    await agent.run(ctx)
    assert isinstance(ctx.state.tool_results.get("tc1"), RetryPromptPart)


def test_message_agent_name_reserved_against_user_tool() -> None:
    bad = ToolBinding(
        dispatch_topic="t.in",
        tool_def=ToolDefinition(name="message_agent", description="x", parameters_json_schema={"type": "object", "properties": {}}),
    )
    with pytest.raises(ValueError):
        StatelessAgent("triage", subscribe_topics="triage.in", model_client=TestModel(), tools=[bad], peers=[Messaging("billing")])


def test_message_agent_name_not_reserved_for_handoff_only_agent() -> None:
    # The reservation is MESSAGING-only (gate keyed on _messaging_handles): a Handoff-only agent injects no
    # message_agent tool, so a user tool named `message_agent` is allowed (no reservation collision).
    user_tool = ToolBinding(
        dispatch_topic="t.in",
        tool_def=ToolDefinition(name="message_agent", description="x", parameters_json_schema={"type": "object", "properties": {}}),
    )
    agent = StatelessAgent("triage", subscribe_topics="triage.in", model_client=TestModel(), tools=[user_tool], peers=[Handoff("refunds")])
    assert agent._handoff_handles == [Handoff("refunds")]
    assert "message_agent" in {b.name for b in agent.tools}  # the user tool survives, unreserved


async def test_message_agent_parallel_mixed_batch_dispatches_per_kind() -> None:
    # A turn emitting a message_agent AND a regular tool opens one durable batch; dispatched PER-KIND
    # (§5.4 / L13): the message_agent sibling gets (derived topic, fresh seed, isolate_state, no
    # ToolCallRef body), the tool sibling gets (its dispatch_topic, deep-copied caller state, ToolCallRef).
    # Both carry their tool_call_id as the tag so each reply folds at its own slot.
    tool = ToolBinding(
        dispatch_topic="adder.in",
        tool_def=ToolDefinition(name="add", description="add", parameters_json_schema={"type": "object", "properties": {}}),
    )
    msg = _msg_call("billing", "balance?", tool_call_id="m1")
    add = ToolCallPart(tool_name="add", args={}, tool_call_id="a1")
    agent = StatelessAgent(
        "triage", subscribe_topics="triage.in", model_client=_model_emits_tool_calls([msg, add]), tools=[tool], peers=[Messaging("billing")]
    )
    result = _unwrap(await agent.run(_ctx_with_view(_view({"billing": "Billing."}))))
    assert isinstance(result, list) and len(result) == 2
    by_tag = {c.tag: c for c in result}
    peer = by_tag["m1"]
    assert peer.target_topic == "agent.billing.private.input"
    assert peer.isolate_state is True
    assert peer.body is None  # no ToolCallRef — the peer runs its own @handler('*') on the fresh seed
    assert peer.state.uncommitted_message is not None  # fresh seed, NOT the caller's state
    tool_call = by_tag["a1"]
    assert tool_call.target_topic == "adder.in"
    assert tool_call.isolate_state is False
    assert tool_call.body is not None  # ToolCallRef


async def test_message_agent_is_never_looked_up_in_tools_registry() -> None:
    # Invariant (§5.4): the dispatch forks on the well-known name BEFORE any tools_registry index, so a
    # message_agent call is NEVER looked up there (its absence would KeyError — as witnessed in the
    # parallel/re-entry RED runs). Even with a POPULATED registry (a real, differently-named tool present),
    # a lone message_agent routes to the peer's derived topic, never the registry tool's dispatch topic.
    tool = ToolBinding(
        dispatch_topic="adder.in",
        tool_def=ToolDefinition(name="add", description="add", parameters_json_schema={"type": "object", "properties": {}}),
    )
    agent = StatelessAgent(
        "triage",
        subscribe_topics="triage.in",
        model_client=_model_emits_tool_calls([_msg_call("billing")]),
        tools=[tool],
        peers=[Messaging("billing")],
    )
    result = _unwrap(await agent.run(_ctx_with_view(_view({"billing": "Billing."}))))
    assert isinstance(result, Call)
    assert result.target_topic == "agent.billing.private.input"  # the peer, NOT the "add" tool (adder.in)
    assert result.isolate_state is True


# --- serialize-all-parts format (§5.2) ---------------------------------------------------------------


def test_serialize_message_reply_empty_returns_sentinel() -> None:
    assert _serialize_message_reply([]) == "(no content)"
    assert _serialize_message_reply(None) == "(no content)"


def test_serialize_message_reply_file_part_placeholder() -> None:
    assert _serialize_message_reply([FilePart(media_type="image/png", uri="http://x/y")]) == "[file: image/png http://x/y]"
    assert _serialize_message_reply([FilePart(media_type="image/png")]) == "[file: image/png <inline>]"  # uri None -> <inline>


def test_serialize_message_reply_other_part_is_json_encoded() -> None:
    # The else arm: a ContentPart that is not Text/Data/File (the union's ToolCallPart member) is
    # JSON-encoded whole, so nothing in a peer's reply is silently dropped.
    out = _serialize_message_reply([PayloadToolCallPart(tool_call_id="x", kwargs={"a": 1}, tool_name="mytool")])
    assert "mytool" in out and '"a":1' in out


# --- message_agent argument validation (§5.2) --------------------------------------------------------


async def test_message_agent_malformed_args_retries() -> None:
    bad = ToolCallPart(tool_name="message_agent", args="{not valid json", tool_call_id="tc1")
    agent = StatelessAgent("triage", subscribe_topics="triage.in", model_client=_model_emits_tool_calls([bad]), peers=[Messaging("billing")])
    ctx = _ctx_with_view(_view({"billing": None}))
    await agent.run(ctx)
    result = ctx.state.tool_results.get("tc1")
    assert isinstance(result, RetryPromptPart)
    assert "Malformed message_agent arguments" in result.content


async def test_message_agent_empty_name_retries() -> None:
    bad = ToolCallPart(tool_name="message_agent", args={"name": "", "message": "hi"}, tool_call_id="tc1")
    agent = StatelessAgent("triage", subscribe_topics="triage.in", model_client=_model_emits_tool_calls([bad]), peers=[Messaging("billing")])
    ctx = _ctx_with_view(_view({"billing": "Billing."}))
    await agent.run(ctx)
    result = ctx.state.tool_results.get("tc1")
    assert isinstance(result, RetryPromptPart)
    assert "non-empty string" in result.content


async def test_message_agent_non_ancestor_target_is_allowed() -> None:
    # The cycle guard rejects only the TARGET being an ancestor — a legitimate diamond (a DIFFERENT agent
    # suspended in the chain) must still dispatch. `other` is an ancestor, `billing` is the target.
    agent = StatelessAgent(
        "triage", subscribe_topics="triage.in", model_client=_model_emits_tool_calls([_msg_call("billing")]), peers=[Messaging("billing")]
    )
    ctx = _ctx_with_view(_view({"billing": None}), ancestors=frozenset({("other", "agent")}))
    result = _unwrap(await agent.run(ctx))
    assert isinstance(result, Call)
    assert result.target_topic == "agent.billing.private.input"  # dispatched, not a false cycle
    assert "tc1" not in ctx.state.tool_results  # no retry


async def test_message_agent_invalid_sibling_excluded_from_batch() -> None:
    # §5.4 / L13: a validation-failed sibling is EXCLUDED so the dispatched-slot set matches the
    # completion check. [bad message_agent (offline) + good message_agent + a tool] -> the bad is retried
    # and dropped; the batch dispatches only the two valid siblings (peer + tool), each at its own slot.
    tool = ToolBinding(
        dispatch_topic="adder.in",
        tool_def=ToolDefinition(name="add", description="add", parameters_json_schema={"type": "object", "properties": {}}),
    )
    bad = _msg_call("ghost", tool_call_id="bad1")
    good = _msg_call("billing", tool_call_id="good1")
    add = ToolCallPart(tool_name="add", args={}, tool_call_id="t1")
    agent = StatelessAgent(
        "triage", subscribe_topics="triage.in", model_client=_model_emits_tool_calls([bad, good, add]), tools=[tool], peers=[Messaging(discover=True)]
    )
    ctx = _ctx_with_view(_view({"billing": None}))  # billing live, ghost offline
    result = _unwrap(await agent.run(ctx))
    assert isinstance(ctx.state.tool_results.get("bad1"), RetryPromptPart)  # ghost excluded
    assert isinstance(result, list) and len(result) == 2  # only the two valid siblings dispatched
    by_tag = {c.tag: c for c in result}
    assert by_tag["good1"].target_topic == "agent.billing.private.input" and by_tag["good1"].isolate_state is True
    assert by_tag["t1"].target_topic == "adder.in" and by_tag["t1"].isolate_state is False


async def test_message_agent_absent_without_a_messaging_handle() -> None:
    # §5.1: no handle -> no surface. An agent with no `peers=` is never given the message_agent tool.
    captured: dict[str, set[str]] = {}

    def _capture(messages: object, info: AgentInfo) -> ModelResponse:
        captured["tools"] = {t.name for t in info.function_tools}
        return ModelResponse(parts=[ModelTextPart("done")])

    agent = StatelessAgent("triage", subscribe_topics="triage.in", model_client=FunctionModel(_capture))  # NO peers=
    await agent.run(_make_ctx(State()))
    assert "message_agent" not in captured["tools"]


def test_handoff_name_reserved_for_handoff_only_agent() -> None:
    # Companion to the messaging-only assertion above (handoff spec §2/§3.0): reservation is
    # per-handle-kind — a Handoff-only agent reserves `handoff_to_agent` (its own built-in) while
    # leaving `message_agent` unreserved.
    user_tool = ToolBinding(
        dispatch_topic="t.in",
        tool_def=ToolDefinition(name="handoff_to_agent", description="x", parameters_json_schema={"type": "object", "properties": {}}),
    )
    with pytest.raises(ValueError, match="reserved"):
        StatelessAgent("triage", subscribe_topics="triage.in", model_client=TestModel(), tools=[user_tool], peers=[Handoff("refunds")])
