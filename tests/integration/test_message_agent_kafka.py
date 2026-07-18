"""Real-broker (``kafka`` lane) end-to-end agent-to-agent messaging (PR-B / §13).

The over-the-wire counterpart to the offline ``tests/test_message_agent.py`` unit suite. The unit
tests drive ``agent.run`` directly with a stubbed agents view + synthetic ancestor set; what they
CANNOT prove — and what these do — is the messaging spine against a live broker, end to end:

* agent B advertises its :class:`AgentCard`; agent A's gated agents view materializes it and renders
  the live directory into the ``message_agent`` tool description;
* A's ``message_agent`` call dispatches a fresh-seeded ``Call(isolate_state=True)`` to B's PR-A
  derived input topic (``agent.{B}.private.input``); B runs its own ``@handler('*')`` on the seed and
  replies through the reply slot;
* the reply folds (all parts serialized) into A's tool result and A — riding the degenerate durable
  batch (snapshot/restore, C1) — resumes on its OWN history, not B's returned state;
* the cycle guard (per-frame ``caller_node_id`` stamped over real hops + ``ctx.ancestor_callers``)
  rejects a public-entry ring; a mixed batch folds per-kind; a peer fault runs A's ``on_callee_error``.

Mirrors ``test_durable_fanout_agent_kafka.py`` (the durable arc) and ``test_discover_mode_kafka.py``
(the discover→dispatch view-warmup pattern). Opt-in (``-m kafka``); skips cleanly without Docker. Run
with ``uv run --group integration pytest tests/integration/test_message_agent_kafka.py -m kafka``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

from calfkit._vendor.pydantic_ai import models
from calfkit._vendor.pydantic_ai.messages import ModelMessage, ModelRequest, ModelResponse, RetryPromptPart, TextPart, ToolCallPart, ToolReturnPart
from calfkit._vendor.pydantic_ai.models.function import AgentInfo, FunctionModel
from calfkit.client import Client
from calfkit.controlplane import ControlPlaneConfig, ControlPlaneView
from calfkit.models.agents import AGENTS_TOPIC, AgentCard
from calfkit.nodes import Agent, ToolNodeDef, agent_tool
from calfkit.peers import Messaging
from calfkit.worker import Worker
from tests.integration._fault_tools import boom
from tests.integration._kafka_helpers import fast_control_plane, profile_for
from tests.integration._roundtrip_helpers import FINAL_OUTPUT, final_model, retry_prompt_texts, returns_by_call_id, scripted_model

# Every test here needs a real broker. FunctionModel is offline, but pydantic-ai still gates "model
# requests" behind this flag (matches the other kafka-lane agent suites).
pytestmark = pytest.mark.kafka
models.ALLOW_MODEL_REQUESTS = True

_EARLIEST = {"auto_offset_reset": "earliest"}


def _worker(bootstrap: str, *, nodes: list, control_plane: ControlPlaneConfig) -> Worker:
    """A Worker on its own broker connection, control plane on, reading earliest.

    Every agent needs the control plane to advertise its AgentCard; a messaging agent additionally
    needs it to materialize the gated agents view its directory enumerates.
    """
    return Worker(Client.connect(bootstrap), nodes=nodes, control_plane=control_plane, extra_subscribe_kwargs=_EARLIEST)


async def _wait(predicate: Callable[[], bool], *, timeout: float, what: str) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.1)
    raise AssertionError(f"timed out after {timeout}s waiting for: {what}")


async def _await_agents_view(bootstrap: str, predicate: Callable[[ControlPlaneView[AgentCard]], bool], *, timeout: float, what: str) -> None:
    """Open a transient agents view, wait for ``predicate`` over it, then close it — so a messaging
    agent's own view catch-up (started after) will include whatever the predicate confirmed is live."""
    view: ControlPlaneView[AgentCard] = ControlPlaneView.open(
        connection=profile_for(bootstrap), topic=AGENTS_TOPIC, record_type=AgentCard, ensure_topic=False
    )
    try:
        await view.start()
        await _wait(lambda: predicate(view), timeout=timeout, what=what)
    finally:
        await view.stop()


def _msg_call(name: str, message: str, *, tool_call_id: str) -> ToolCallPart:
    return ToolCallPart("message_agent", {"name": name, "message": message}, tool_call_id=tool_call_id)


async def test_agent_messages_peer_and_resumes_on_own_history(kafka_bootstrap: str, topic_namespace: str) -> None:
    """A (``peers=[Messaging(B)]``) messages B; B answers on a fresh sub-conversation; B's reply folds
    into A's ``message_agent`` tool result; A — riding the degenerate ``isolate_state`` batch — resumes
    on its OWN history (the C1 property over the wire), not B's returned state."""
    a_name = f"{topic_namespace}-A"
    b_name = f"{topic_namespace}-B"
    a_in = f"{topic_namespace}.A.input"
    control_plane = fast_control_plane(kafka_bootstrap)

    agent_a = Agent(
        a_name,
        system_prompt="you are triage",
        subscribe_topics=a_in,
        model_client=scripted_model([_msg_call(b_name, "what is the balance?", tool_call_id="m1")]),
        peers=[Messaging(b_name)],
    )
    agent_b = Agent(
        b_name,
        system_prompt="you are billing",
        subscribe_topics=f"{topic_namespace}.B.input",  # B's PUBLIC topic; messaging uses the DERIVED private topic
        model_client=final_model("the balance is 42"),
    )

    # A's lone message_agent rides the degenerate durable batch (peers => _needs_durable_batch), so A's
    # worker opens the REAL ktables store; guard that no fake is pre-seeded.
    from calfkit.nodes._fanout_store import FANOUT_STORE_KEY

    assert agent_a._needs_durable_batch and FANOUT_STORE_KEY not in agent_a.resources

    driver = Client.connect(kafka_bootstrap)
    b_worker = _worker(kafka_bootstrap, nodes=[agent_b], control_plane=control_plane)
    a_worker = _worker(kafka_bootstrap, nodes=[agent_a], control_plane=control_plane)

    async with b_worker:
        # A's gated agents view must materialize B's card before A renders the directory + messages it.
        await _await_agents_view(kafka_bootstrap, lambda v: v.get(b_name) is not None, timeout=60, what=f"B's AgentCard {b_name!r}")
        async with a_worker:
            result = await driver.agent(topic=a_in).execute("ask billing for the balance", timeout=120)

    # B's reply folded into A's message_agent tool result (slot m1).
    assert result.output is not None and FINAL_OUTPUT in result.output
    assert "42" in str(returns_by_call_id(result.message_history)["m1"])

    # C1: A resumed on its OWN history — A's message_agent call + A's own user prompt are present, and
    # B's sub-conversation (its seed / its assistant turn) never replaced A's history. If A had adopted
    # B's returned state, these markers would be gone.
    assert any(
        isinstance(p, ToolCallPart) and p.tool_name == "message_agent"
        for m in result.message_history
        if isinstance(m, ModelResponse)
        for p in m.parts
    )
    assert b_name not in retry_prompt_texts(result.message_history)  # B was reachable, not a bad target

    await driver.aclose()
    await b_worker._client.aclose()
    await a_worker._client.aclose()


# NOTE: replica load-balance is NOT re-tested here — it is already proven by
# ``test_agents_plane_kafka.py::test_two_replicas_collapse_to_one_card`` (the view collapses two replicas
# of one name to a single live card) plus the generic name-scoped-consumer-group pooling on the derived
# input topic (a survivor handles the message). Re-deploying it under ``message_agent`` would add no new
# coverage.


def _attempt_target_then_echo_result(target: str) -> FunctionModel:
    """Messages ``target`` with a FRESH tool_call_id each turn until a result comes back, then finalizes
    echoing that result's content. The fresh id per turn keeps the conversation well-formed (a reused id
    confuses the deferred-result pairing); echoing the content makes WHAT the caller got — here the
    model-visible cycle rejection — observable cross-agent through the fold, not merely inferred from
    termination. Without the cycle guard this would instead recurse A<->B unboundedly."""
    seq = {"n": 0}

    def _fn(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        last = messages[-1]
        if isinstance(last, ModelRequest):
            for p in last.parts:
                if isinstance(p, (RetryPromptPart, ToolReturnPart)):
                    return ModelResponse(parts=[TextPart(f"RESULT[{p.content}]")])
        seq["n"] += 1
        return ModelResponse(parts=[_msg_call(target, "ping", tool_call_id=f"b{seq['n']}")])

    return FunctionModel(_fn)


async def test_public_entry_ring_is_rejected(kafka_bootstrap: str, topic_namespace: str) -> None:
    """A public-entry ring client->A->B->A is broken by the cycle guard: when B tries to message A
    (already an ancestor caller via the real-hop ``caller_node_id`` stamp), B gets a model-visible
    cycle ``RetryPromptPart`` — not another dispatch to A — and finalizes. The rejection surfaces
    cross-agent as ``CYCLE_REJECTED`` folded into A's tool result, and the whole flow TERMINATES."""
    a_name = f"{topic_namespace}-A"
    b_name = f"{topic_namespace}-B"
    a_in = f"{topic_namespace}.A.input"
    control_plane = fast_control_plane(kafka_bootstrap)

    agent_a = Agent(
        a_name,
        system_prompt="x",
        subscribe_topics=a_in,
        model_client=scripted_model([_msg_call(b_name, "hi", tool_call_id="m1")]),
        peers=[Messaging(b_name)],
    )
    agent_b = Agent(
        b_name,
        system_prompt="x",
        subscribe_topics=f"{topic_namespace}.B.input",
        model_client=_attempt_target_then_echo_result(a_name),
        peers=[Messaging(a_name)],
    )

    driver = Client.connect(kafka_bootstrap)
    b_worker = _worker(kafka_bootstrap, nodes=[agent_b], control_plane=control_plane)
    a_worker = _worker(kafka_bootstrap, nodes=[agent_a], control_plane=control_plane)

    async with b_worker, a_worker:
        # both agents must see each other's card before either renders its directory (the ring needs B to
        # be able to *target* A — the guard rejects it at dispatch, not by hiding A from B's directory).
        await _await_agents_view(
            kafka_bootstrap, lambda v: v.get(a_name) is not None and v.get(b_name) is not None, timeout=60, what="both A and B cards"
        )
        result = await driver.agent(topic=a_in).execute("kick off the ring", timeout=120)

    # The ring TERMINATED (without the guard B<->A recurses until the envelope blows past 1MB) and B's
    # cycle rejection surfaced cross-agent through A's fold — A messaged B, B's attempt to message A back
    # was rejected as a messaging cycle, B echoed that rejection, and A finalized.
    assert result.output is not None and FINAL_OUTPUT in result.output
    folded = str(returns_by_call_id(result.message_history)["m1"])
    assert "cycle" in folded.lower()

    await driver.aclose()
    await b_worker._client.aclose()
    await a_worker._client.aclose()


def _echo_tool(name: str) -> ToolNodeDef:
    """A per-test, uniquely-named fault-free tool node (the name is the wire address on the shared lane)."""

    def echo() -> str:
        return "TOOL_OK"

    return agent_tool(echo, name=name)


async def test_parallel_mixed_batch_folds_message_agent_and_tool_per_kind(kafka_bootstrap: str, topic_namespace: str) -> None:
    """A turn that emits a ``message_agent`` AND a tool call opens one durable batch over the wire; the
    peer reply and the tool return fold per-kind into their own slots (§5.4 / L13), and A finalizes."""
    a_name = f"{topic_namespace}-A"
    b_name = f"{topic_namespace}-B"
    tool_name = f"{topic_namespace}-tool"
    a_in = f"{topic_namespace}.A.input"
    control_plane = fast_control_plane(kafka_bootstrap)
    tool = _echo_tool(tool_name)

    agent_a = Agent(
        a_name,
        system_prompt="x",
        subscribe_topics=a_in,
        model_client=scripted_model([_msg_call(b_name, "q", tool_call_id="m1"), ToolCallPart(tool_name, {}, tool_call_id="t1")]),
        peers=[Messaging(b_name)],
        tools=[tool],
    )
    agent_b = Agent(b_name, system_prompt="x", subscribe_topics=f"{topic_namespace}.B.input", model_client=final_model("B reply"))

    driver = Client.connect(kafka_bootstrap)
    b_worker = _worker(kafka_bootstrap, nodes=[agent_b], control_plane=control_plane)
    a_worker = _worker(kafka_bootstrap, nodes=[agent_a, tool], control_plane=control_plane)

    async with b_worker:
        await _await_agents_view(kafka_bootstrap, lambda v: v.get(b_name) is not None, timeout=60, what=f"B's AgentCard {b_name!r}")
        async with a_worker:
            result = await driver.agent(topic=a_in).execute("message B and call the tool", timeout=120)

    assert result.output is not None and FINAL_OUTPUT in result.output
    rets = returns_by_call_id(result.message_history)
    assert "B reply" in str(rets["m1"])  # the message_agent (peer) fold
    assert rets["t1"] == "TOOL_OK"  # the tool fold, in its own slot

    await driver.aclose()
    await b_worker._client.aclose()
    await a_worker._client.aclose()


async def test_non_live_curated_peer_is_excluded_and_retried(kafka_bootstrap: str, topic_namespace: str) -> None:
    """A live directory excludes a curated peer that is not online: A is scoped to both B (live) and a
    ghost (never deployed); targeting the ghost is a model-visible "not currently reachable" retry, no
    dispatch — the over-the-wire live-filter that the offline render test stubs."""
    a_name = f"{topic_namespace}-A"
    b_name = f"{topic_namespace}-B"
    ghost = f"{topic_namespace}-ghost"  # in A's curated scope but never deployed -> not live
    a_in = f"{topic_namespace}.A.input"
    control_plane = fast_control_plane(kafka_bootstrap)

    agent_a = Agent(
        a_name,
        system_prompt="x",
        subscribe_topics=a_in,
        model_client=scripted_model([_msg_call(ghost, "hi", tool_call_id="m1")]),
        peers=[Messaging(b_name, ghost)],
    )
    agent_b = Agent(b_name, system_prompt="x", subscribe_topics=f"{topic_namespace}.B.input", model_client=final_model("hi"))

    driver = Client.connect(kafka_bootstrap)
    b_worker = _worker(kafka_bootstrap, nodes=[agent_b], control_plane=control_plane)
    a_worker = _worker(kafka_bootstrap, nodes=[agent_a], control_plane=control_plane)

    async with b_worker:
        await _await_agents_view(kafka_bootstrap, lambda v: v.get(b_name) is not None, timeout=60, what=f"B's AgentCard {b_name!r}")
        async with a_worker:
            result = await driver.agent(topic=a_in).execute("message the ghost", timeout=120)

    # The non-live ghost is excluded from the directory; targeting it is model-visible feedback, not a
    # dispatch. The rejection text reaches the conversation (as a retry or, post-round-trip, a tool-return
    # rendering of it) — assert it landed regardless of the part type.
    assert result.output is not None and FINAL_OUTPUT in result.output
    feedback = retry_prompt_texts(result.message_history) + [str(v) for v in returns_by_call_id(result.message_history).values()]
    assert any("not currently reachable" in t for t in feedback)

    await driver.aclose()
    await b_worker._client.aclose()
    await a_worker._client.aclose()


async def test_peer_fault_runs_callers_on_callee_error(kafka_bootstrap: str, topic_namespace: str) -> None:
    """A peer fault is the GENERIC fault-rail path (the peer is just a callee): A messages B, B's tool
    raises, B's unhandled fault escalates to A's reply slot, and A's ``on_callee_error`` substitutes for
    it (free via ``_resolve_callee``) — the substitute folds into A's tool result and A finalizes."""
    a_name = f"{topic_namespace}-A"
    b_name = f"{topic_namespace}-B"
    a_in = f"{topic_namespace}.A.input"
    control_plane = fast_control_plane(kafka_bootstrap)

    agent_a = Agent(
        a_name,
        system_prompt="x",
        subscribe_topics=a_in,
        model_client=scripted_model([_msg_call(b_name, "do it", tool_call_id="m1")]),
        peers=[Messaging(b_name)],
        on_callee_error=lambda ctx, fault: "PEER_FAULT_HANDLED",
    )
    # B calls a raising tool with no on_callee_error of its own, so its fault escalates to A.
    agent_b = Agent(
        b_name,
        system_prompt="x",
        subscribe_topics=f"{topic_namespace}.B.input",
        model_client=scripted_model([ToolCallPart("boom", {"x": 1}, tool_call_id="bx")]),
        tools=[boom],
    )

    driver = Client.connect(kafka_bootstrap)
    b_worker = _worker(kafka_bootstrap, nodes=[agent_b, boom], control_plane=control_plane)
    a_worker = _worker(kafka_bootstrap, nodes=[agent_a], control_plane=control_plane)

    async with b_worker:
        await _await_agents_view(kafka_bootstrap, lambda v: v.get(b_name) is not None, timeout=60, what=f"B's AgentCard {b_name!r}")
        async with a_worker:
            result = await driver.agent(topic=a_in).execute("ask B to do it", timeout=120)

    # A's on_callee_error substituted for B's escalated fault; the substitute folded into A's m1 slot.
    assert result.output is not None and FINAL_OUTPUT in result.output
    assert "PEER_FAULT_HANDLED" in str(returns_by_call_id(result.message_history)["m1"])

    await driver.aclose()
    await b_worker._client.aclose()
    await a_worker._client.aclose()
