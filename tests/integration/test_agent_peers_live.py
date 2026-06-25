"""Live-LLM (``live`` + ``kafka`` lanes) end-to-end verification of the agent-peers tutorial.

The over-the-wire, real-model counterpart to the scripted ``test_message_agent_kafka.py`` /
``test_handoff_kafka.py`` suites: it stands up the three agents of the "Build a multi-agent support
desk" tutorial (``docs/multi-agent-support-desk.md``) — a ``triage`` agent with
``peers=[Messaging("billing"), Handoff("refunds")]`` — and drives the two flows the tutorial teaches
against a REAL model, proving the tutorial works as written:

* a balance question -> triage MESSAGES billing (keeps control) and billing's reply folds into
  triage's ``message_agent`` slot;
* a refund request -> triage HANDS OFF to refunds, which answers the ORIGINAL caller (the driver).

Needs BOTH a real broker (``kafka`` lane, testcontainers) AND a real model (``live`` lane,
``OPENAI_API_KEY`` + ``TEST_LLM_MODEL_NAME``). The ``kafka_bootstrap`` fixture self-skips without
Docker; ``skip_if_no_live_llm`` skips without credentials — so the test skips cleanly in either single
lane and runs only where both are present (a dev machine, or ``-m "kafka and live"``). Real models are
non-deterministic, so the assertions are BEHAVIORAL (a peer was reached, the answer came back the
right way), never on exact model text.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Callable

import pytest

from calfkit._vendor.pydantic_ai import models
from calfkit._vendor.pydantic_ai.messages import ModelResponse, ToolCallPart
from calfkit.client import Client
from calfkit.controlplane import ControlPlaneConfig, ControlPlaneView
from calfkit.models.agents import AGENTS_TOPIC, AgentCard
from calfkit.nodes import Agent
from calfkit.peers import Handoff, Messaging
from calfkit.providers import OpenAIResponsesModelClient
from calfkit.worker import Worker
from tests.integration._kafka_helpers import fast_control_plane
from tests.utils import skip_if_no_live_llm

# Real broker (kafka lane) + real model (live lane). The kafka_bootstrap fixture self-skips without
# Docker; skip_if_no_live_llm skips without credentials — clean skip in either single lane.
pytestmark = [pytest.mark.kafka, pytest.mark.live, skip_if_no_live_llm]
models.ALLOW_MODEL_REQUESTS = True

_EARLIEST = {"auto_offset_reset": "earliest"}


def _live_model() -> OpenAIResponsesModelClient:
    return OpenAIResponsesModelClient(os.environ["TEST_LLM_MODEL_NAME"], reasoning_effort=os.getenv("TEST_REASONING_EFFORT"))


def _worker(bootstrap: str, *, nodes: list, control_plane: ControlPlaneConfig) -> Worker:
    return Worker(Client.connect(bootstrap), nodes=nodes, control_plane=control_plane, extra_subscribe_kwargs=_EARLIEST)


async def _wait(predicate: Callable[[], bool], *, timeout: float, what: str) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.1)
    raise AssertionError(f"timed out after {timeout}s waiting for: {what}")


async def _await_agents_view(bootstrap: str, predicate: Callable[[ControlPlaneView[AgentCard]], bool], *, timeout: float, what: str) -> None:
    """Open a transient agents view, wait for ``predicate``, then close it — so triage's own view
    catch-up (started after) includes whatever the predicate confirmed is live."""
    view: ControlPlaneView[AgentCard] = ControlPlaneView.open(
        bootstrap_servers=bootstrap, topic=AGENTS_TOPIC, record_type=AgentCard, ensure_topic=False
    )
    try:
        await view.start()
        await _wait(lambda: predicate(view), timeout=timeout, what=what)
    finally:
        await view.stop()


async def test_triage_messages_billing_and_hands_off_to_refunds(kafka_bootstrap: str, topic_namespace: str) -> None:
    """The tutorial scenario, end to end against a real model: triage MESSAGES billing for a balance
    question (keeps control) and HANDS OFF a refund request to refunds (which answers the original
    caller)."""
    triage = f"{topic_namespace}-triage"
    billing = f"{topic_namespace}-billing"
    refunds = f"{topic_namespace}-refunds"
    triage_in = f"{topic_namespace}.triage.input"
    control_plane = fast_control_plane(kafka_bootstrap)

    triage_agent = Agent(
        triage,
        description="Front desk that routes customer requests.",
        system_prompt=(
            "You are the front desk for a support team. Route each customer request: for an account "
            "balance or billing question, use the message_agent tool to ask the billing agent and relay "
            "its answer; for a refund request, hand off the conversation to the refunds agent."
        ),
        subscribe_topics=triage_in,
        model_client=_live_model(),
        peers=[Messaging(billing), Handoff(refunds)],
    )
    billing_agent = Agent(
        billing,
        description="Answers account balance and billing questions.",
        system_prompt="You are the billing department. Answer account balance and billing questions concisely.",
        subscribe_topics=f"{topic_namespace}.billing.input",
        model_client=_live_model(),
    )
    refunds_agent = Agent(
        refunds,
        description="Handles refund requests.",
        system_prompt="You are the refunds department. Approve a reasonable refund request and state the decision concisely.",
        subscribe_topics=f"{topic_namespace}.refunds.input",
        model_client=_live_model(),
    )

    driver = Client.connect(kafka_bootstrap)
    specialists = _worker(kafka_bootstrap, nodes=[billing_agent, refunds_agent], control_plane=control_plane)
    triage_worker = _worker(kafka_bootstrap, nodes=[triage_agent], control_plane=control_plane)

    async with specialists:
        # triage's gated agents view must materialize both specialists before it renders its directory.
        await _await_agents_view(
            kafka_bootstrap,
            lambda v: v.get(billing) is not None and v.get(refunds) is not None,
            timeout=60,
            what="billing + refunds AgentCards",
        )
        async with triage_worker:
            # MESSAGE path: triage consults billing and keeps control.
            balance = await driver.execute("What is the balance on my account?", triage_in, timeout=180)
            # HANDOFF path: triage transfers to refunds, which answers the original caller.
            refund = await driver.execute("I'd like a refund for order 1234.", triage_in, timeout=180)

    # MESSAGE: triage consulted billing via the built-in message_agent tool and stayed in control
    # (it produced the final answer itself).
    assert balance.output is not None
    assert any(
        isinstance(p, ToolCallPart) and p.tool_name == "message_agent"
        for m in balance.message_history
        if isinstance(m, ModelResponse)
        for p in m.parts
    ), "expected triage to consult billing via the message_agent tool"

    # HANDOFF: triage's handoff output rode the carried conversation (attributed to triage), and
    # refunds — not triage — produced the final answer to the original caller.
    assert refund.output is not None
    responses = [m for m in refund.message_history if isinstance(m, ModelResponse)]
    assert any(m.name == triage for m in responses), "expected triage's handoff output in the carried conversation"
    assert responses[-1].name == refunds, "expected refunds (not triage) to produce the final answer"

    await driver.close()
    await specialists._client.close()
    await triage_worker._client.close()
