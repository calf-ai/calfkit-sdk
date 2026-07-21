"""Phase 0 de-risk gates for agent tool-error reception — the real-broker (``kafka``) lane.

The over-the-wire counterpart to ``tests/test_tool_error_reception_gate0.py``. These probe the same
state-first provenance assumptions (spec D3), but against a LIVE broker so the checks the offline
``TestKafkaBroker`` cannot make are made here:

* **Gate A** — a single tool fault folds to ``on_callee_error`` with ``state.tool_calls[tag]`` intact
  after the reply envelope's ``State`` crosses the real wire.
* **Gate B1** — a normal fan-out sibling fault resolves via ``state.tool_calls[tag]`` **with ``.args``**
  after a real **ktables** durable-fold round trip (the offline fake store can mask store-round-trip
  behaviour — handoff landmine).
* **Gate B2** — a ``message_agent`` peer fault (lone + mixed) folds carrying the **peer's own**
  ``state.tool_calls`` (NOT an empty seed — Phase-0 finding), in which the caller's tag is **absent**, so
  state-first misses; and ``failing_call.target_topic`` (``agent.{peer}.private.input``) is populated.
  ``message_agent`` gates dispatch on the live ``calf.agents`` view, which the real control plane populates
  naturally — the reason B2 is authoritative here, not offline. Modeled on
  ``test_message_agent_kafka.py::test_peer_fault_runs_callers_on_callee_error``. See
  ``notes/2026-07-07-agent-tool-error-reception-phase0-findings.md``.

The same folds also confirm the **echo marker rail** end-to-end over the wire: with universal
stamping (caller-side step-emission spec §3.2), ``failing_call.marker`` carries the full call
identity for EVERY tool fold — normal tools and ``message_agent`` peers alike — so ``ctx.tool_call``
reconstructs the full call (including the **real args**) carriage-first from the marker, even when
a peer's foreign state can't provide it.

The probe (:class:`_ToolCallProbe`) captures the seam's view of the failing call, then **declines**
so the fault still escalates (capture-in-callback, assert-in-test-body). Opt-in (``-m kafka``); skips
cleanly without Docker. Run with ``make test-kafka`` or
``uv run --group integration pytest tests/integration/test_tool_error_reception_gate0_kafka.py -m kafka``.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import pytest

from calfkit._vendor.pydantic_ai import models
from calfkit._vendor.pydantic_ai.messages import ToolCallPart
from calfkit.client import Client
from calfkit.controlplane import ControlPlaneConfig, ControlPlaneView
from calfkit.exceptions import NodeFaultError
from calfkit.models.agents import AGENTS_TOPIC, AgentCard
from calfkit.models.error_report import ErrorReport
from calfkit.nodes import StatelessAgent
from calfkit.nodes._tool_error import AgentSeamContext
from calfkit.peers import Messaging
from calfkit.worker import Worker
from tests.integration._fault_kafka import fault_worker
from tests.integration._fault_tools import boom, ok_a
from tests.integration._kafka_helpers import fast_control_plane, profile_for
from tests.integration._roundtrip_helpers import scripted_model

# Every test needs a real broker; FunctionModel is offline but pydantic-ai still gates
# "model requests" behind this flag (matches the other kafka-lane agent suites).
pytestmark = pytest.mark.kafka
models.ALLOW_MODEL_REQUESTS = True

_EARLIEST = {"auto_offset_reset": "earliest"}


@dataclass
class _ToolCallProbe:
    """A raw ``on_callee_error`` recorder capturing the seam's view of the failing call, then
    declining so the fault escalates exactly as if it were absent (a raise inside a seam would
    itself become a node-own fault, so it records rather than asserts)."""

    calls: list[dict[str, Any]] = field(default_factory=list)

    def __call__(self, ctx: AgentSeamContext, fault: ErrorReport) -> None:
        failing = ctx.failing_call
        tag = failing.tag if failing is not None else None
        tool_calls = ctx.state.tool_calls
        resolved = tool_calls.get(tag) if tag is not None else None
        tool_call = ctx.tool_call  # the echo-rail resolution: carriage-first via the marker, else state
        self.calls.append(
            {
                "delivery_kind": ctx.delivery_kind,
                "error_type": fault.error_type,
                "tag": tag,
                "target_topic": failing.target_topic if failing is not None else None,
                # The echoed marker carries the caller's tool identity for EVERY dispatched call (universal
                # stamping); ``ctx.tool_call`` reconstructs the full call carriage-first from it.
                "failing_marker_tool_name": failing.marker.tool_name if (failing is not None and failing.marker is not None) else None,
                "tool_call_args": tool_call.args if tool_call is not None else None,
                "state_tool_call_tags": sorted(tool_calls.keys()),
                "resolved_present": resolved is not None,
                "resolved_has_args": resolved is not None and getattr(resolved, "args", None) is not None,
                "resolved_tool_name": getattr(resolved, "tool_name", None) if resolved is not None else None,
            }
        )
        return None


async def test_gate_a_single_tool_fault_resolves_via_state_over_the_wire(kafka_bootstrap: str, topic_namespace: str) -> None:
    # Gate A (kafka): the reply envelope's State crosses the real wire, so state.tool_calls[tag] still
    # resolves the full ToolCallPart (with .args) at the fold — state-first provenance survives Kafka.
    recorder = _ToolCallProbe()
    agent_in = f"{topic_namespace}.a.input"
    agent = StatelessAgent(
        f"{topic_namespace}-a",
        system_prompt="call the boom tool",
        subscribe_topics=agent_in,
        model_client=scripted_model([ToolCallPart("boom", {"x": 7}, tool_call_id="c1")]),
        tools=[boom],
        on_callee_error=recorder,
    )
    driver = Client.connect(kafka_bootstrap)
    worker = fault_worker(kafka_bootstrap, nodes=[agent, boom])
    try:
        async with worker:
            with contextlib.suppress(NodeFaultError):
                await driver.agent(topic=agent_in).execute("go", timeout=60)
    finally:
        await driver.aclose()
        await worker._client.aclose()

    assert len(recorder.calls) == 1
    (call,) = recorder.calls
    assert call["delivery_kind"] == "fault"
    assert call["tag"] == "c1"
    assert call["resolved_present"] is True  # state carried back across the wire → state-first hits
    assert call["resolved_has_args"] is True  # full ToolCallPart with .args (grounds D3 over the wire)
    assert call["resolved_tool_name"] == "boom"
    assert call["failing_marker_tool_name"] == "boom"  # universal stamping: the marker crossed the wire (step-emission spec §3.2)


async def test_gate_b1_fanout_sibling_resolves_via_real_ktables_store(kafka_bootstrap: str, topic_namespace: str) -> None:
    # Gate B1 (kafka): a fan-out sibling fault folds through the REAL ktables durable store; the
    # deep-copied sibling state is mirrored back so state.tool_calls[tag] resolves WITH .args. The
    # offline fake store cannot prove the real store preserves this (handoff landmine).
    recorder = _ToolCallProbe()
    agent_in = f"{topic_namespace}.b1.input"
    agent = StatelessAgent(
        f"{topic_namespace}-b1",
        system_prompt="call the tools",
        subscribe_topics=agent_in,
        model_client=scripted_model([ToolCallPart("boom", {"x": 7}, tool_call_id="c1"), ToolCallPart("ok_a", {}, tool_call_id="c2")]),
        tools=[boom, ok_a],
        on_callee_error=recorder,
    )
    # A fan-out agent opens the REAL ktables store on worker.start() (the kafka lane is exempt from the
    # offline fake-store fixture) — guard that no fake is pre-seeded, so this genuinely round-trips it.
    from calfkit.nodes._fanout_store import FANOUT_STORE_KEY

    assert agent._is_fanout_capable and FANOUT_STORE_KEY not in agent.resources

    driver = Client.connect(kafka_bootstrap)
    worker = fault_worker(kafka_bootstrap, nodes=[agent, boom, ok_a])
    try:
        async with worker:
            with contextlib.suppress(NodeFaultError):
                await driver.agent(topic=agent_in).execute("go", timeout=60)
    finally:
        await driver.aclose()
        await worker._client.aclose()

    boom_firings = [c for c in recorder.calls if c["tag"] == "c1"]
    assert len(boom_firings) == 1, f"expected one fault firing for the boom sibling, got {recorder.calls}"
    (call,) = boom_firings
    assert call["delivery_kind"] == "fault"
    assert call["resolved_present"] is True  # sibling state mirrored back through the real store
    assert call["resolved_has_args"] is True  # .args intact for the parallel tool (grounds D3 state-first)
    assert call["resolved_tool_name"] == "boom"
    assert call["failing_marker_tool_name"] == "boom"  # universal stamping: each fan-out sibling's marker crossed the wire


def _worker(bootstrap: str, *, nodes: list, control_plane: ControlPlaneConfig) -> Worker:
    """A Worker with the control plane on (needed to advertise AgentCards and materialize the gated
    agents view a messaging agent's directory enumerates), reading earliest."""
    return Worker(Client.connect(bootstrap), nodes=nodes, control_plane=control_plane, extra_subscribe_kwargs=_EARLIEST)


async def _await_agents_view(bootstrap: str, predicate: Callable[[ControlPlaneView[AgentCard]], bool], *, timeout: float, what: str) -> None:
    """Open a transient agents view, wait for ``predicate``, then close it — so the caller's own view
    catch-up (started after) includes whatever the predicate confirmed is live."""
    view: ControlPlaneView[AgentCard] = ControlPlaneView.open(
        connection=profile_for(bootstrap), topic=AGENTS_TOPIC, record_type=AgentCard, ensure_topic=False
    )
    try:
        await view.start()
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            if predicate(view):
                return
            await asyncio.sleep(0.1)
        raise AssertionError(f"timed out after {timeout}s waiting for: {what}")
    finally:
        await view.stop()


def _msg_call(name: str, message: str, *, tool_call_id: str) -> ToolCallPart:
    return ToolCallPart("message_agent", {"name": name, "message": message}, tool_call_id=tool_call_id)


async def test_gate_b2_message_agent_fault_folds_empty_state_but_has_target_topic(kafka_bootstrap: str, topic_namespace: str) -> None:
    # Gate B2 (lone, kafka): A messages B; B's tool raises; B's unhandled fault escalates to A's
    # message_agent slot. B ran over a FRESH seed State (isolate_state), so at A's fold state.tool_calls
    # is EMPTY (the state lookup misses) — but failing_call.target_topic is populated
    # (agent.{B}.private.input), proving the SlotRef→CalleeResult channel the interim carriage rides.
    recorder = _ToolCallProbe()
    a_name = f"{topic_namespace}-A"
    b_name = f"{topic_namespace}-B"
    a_in = f"{topic_namespace}.A.input"
    control_plane = fast_control_plane(kafka_bootstrap)

    agent_a = StatelessAgent(
        a_name,
        system_prompt="message B",
        subscribe_topics=a_in,
        model_client=scripted_model([_msg_call(b_name, "do it", tool_call_id="m1")]),
        peers=[Messaging(b_name)],
        on_callee_error=recorder,
    )
    agent_b = StatelessAgent(
        b_name,
        system_prompt="call the boom tool",
        subscribe_topics=f"{topic_namespace}.B.input",
        model_client=scripted_model([ToolCallPart("boom", {"x": 1}, tool_call_id="bx")]),
        tools=[boom],
    )

    driver = Client.connect(kafka_bootstrap)
    b_worker = _worker(kafka_bootstrap, nodes=[agent_b, boom], control_plane=control_plane)
    a_worker = _worker(kafka_bootstrap, nodes=[agent_a], control_plane=control_plane)
    try:
        async with b_worker:
            await _await_agents_view(kafka_bootstrap, lambda v: v.get(b_name) is not None, timeout=60, what=f"B's AgentCard {b_name!r}")
            async with a_worker:
                with contextlib.suppress(NodeFaultError):
                    await driver.agent(topic=a_in).execute("ask B to do it", timeout=120)
    finally:
        await driver.aclose()
        await b_worker._client.aclose()
        await a_worker._client.aclose()

    peer_firings = [c for c in recorder.calls if c["tag"] == "m1"]
    assert len(peer_firings) == 1, f"expected one fault firing for the message_agent slot, got {recorder.calls}"
    (call,) = peer_firings
    assert call["delivery_kind"] == "fault"
    # FINDING (Phase 0, empirical): the folded state is the PEER's worked state — it carries the peer's
    # OWN tool_calls (e.g. its 'bx' boom call), NOT an empty seed as the spec/plan/round-1 premise stated.
    # The load-bearing invariant still holds: the CALLER's tag is absent from it, so state-first misses.
    assert "m1" not in call["state_tool_call_tags"]  # the caller's tag is absent from the peer's folded state
    assert call["resolved_present"] is False  # state-first lookup MISSES → the SlotRef carriage is needed
    assert call["target_topic"] == f"agent.{b_name}.private.input"  # THE CHANNEL: SlotRef→CalleeResult populated
    assert call["failing_marker_tool_name"] == "message_agent"  # THE MARKER: the caller's tool identity reached the fold
    assert call["tool_call_args"] == {"name": b_name, "message": "do it"}  # gate 3: ctx.tool_call resolves REAL args via the marker


async def test_gate_b2_mixed_batch_amplification(kafka_bootstrap: str, topic_namespace: str) -> None:
    # Gate B2 (mixed, kafka): one batch with a message_agent AND a normal tool, both faulting. The
    # tool sibling folds with populated state (state-first hits with .args); the message_agent sibling
    # folds with EMPTY state but a populated target_topic. This is the amplification the SlotRef fix
    # exists for — an unhandled peer fault escalates the whole batch, discarding the tool conversion.
    recorder = _ToolCallProbe()
    a_name = f"{topic_namespace}-A"
    b_name = f"{topic_namespace}-B"
    a_in = f"{topic_namespace}.A.input"
    control_plane = fast_control_plane(kafka_bootstrap)

    agent_a = StatelessAgent(
        a_name,
        system_prompt="message B and call boom",
        subscribe_topics=a_in,
        model_client=scripted_model([_msg_call(b_name, "do it", tool_call_id="m1"), ToolCallPart("boom", {"x": 7}, tool_call_id="c1")]),
        peers=[Messaging(b_name)],
        tools=[boom],
        on_callee_error=recorder,
    )
    agent_b = StatelessAgent(
        b_name,
        system_prompt="call the boom tool",
        subscribe_topics=f"{topic_namespace}.B.input",
        model_client=scripted_model([ToolCallPart("boom", {"x": 1}, tool_call_id="bx")]),
        tools=[boom],
    )

    driver = Client.connect(kafka_bootstrap)
    b_worker = _worker(kafka_bootstrap, nodes=[agent_b, boom], control_plane=control_plane)
    a_worker = _worker(kafka_bootstrap, nodes=[agent_a, boom], control_plane=control_plane)
    try:
        async with b_worker:
            await _await_agents_view(kafka_bootstrap, lambda v: v.get(b_name) is not None, timeout=60, what=f"B's AgentCard {b_name!r}")
            async with a_worker:
                with contextlib.suppress(NodeFaultError):
                    await driver.agent(topic=a_in).execute("message B and call boom", timeout=120)
    finally:
        await driver.aclose()
        await b_worker._client.aclose()
        await a_worker._client.aclose()

    tool_firings = [c for c in recorder.calls if c["tag"] == "c1"]
    peer_firings = [c for c in recorder.calls if c["tag"] == "m1"]
    assert len(tool_firings) == 1 and len(peer_firings) == 1, f"expected one firing each, got {recorder.calls}"
    # The tool sibling: the state lookup still resolves with args, and its own marker rode the wire.
    assert tool_firings[0]["resolved_present"] is True
    assert tool_firings[0]["resolved_has_args"] is True
    assert tool_firings[0]["failing_marker_tool_name"] == "boom"  # universal stamping: the tool sibling is marked too
    # The message_agent sibling: its tag is absent from the (peer's) folded state → state-first misses,
    # the channel (target_topic) is populated, and the carriage delivered the identity. (The folded state
    # carries the peer's own tool_calls, not an empty seed — the Phase-0 finding for the lone case above
    # applies here too.)
    assert "m1" not in peer_firings[0]["state_tool_call_tags"]
    assert peer_firings[0]["resolved_present"] is False
    assert peer_firings[0]["target_topic"] == f"agent.{b_name}.private.input"
    assert peer_firings[0]["failing_marker_tool_name"] == "message_agent"  # THE MARKER: the identity reached the fold
    assert peer_firings[0]["tool_call_args"] == {"name": b_name, "message": "do it"}  # gate 3: REAL args via the marker
