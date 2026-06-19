"""Suite S — the policy-seam pipeline over the real broker.

``before_node`` / ``after_node`` / ``on_node_error`` fire inside a live Worker (all
offline-only today): a ``before_node`` substitute short-circuits the body, an
``after_node`` replaces the produced output, a ``before_node`` accident is recovered by
``on_node_error``, a ``NodeFaultError`` minted in a seam bypasses ``on_node_error`` and
escalates verbatim, and a ``before_node`` recorder sees the ingress ``SeamContext``.

Channels: most cases read the substituted/recovered output at the client edge (Channel
C — a successful ``return``, no fault); the mint-bypass case taps the fault mirror
(Channel A) and asserts the ``on_node_error`` recorder never fired (Channel B). Seam
registration validation (rejecting a seam on a consumer / wrong arity) is a
registration-time check covered offline (``tests/test_seam_registration.py``), so it is
not duplicated here.

Opt-in (``-m kafka`` / ``make test-kafka``); skips cleanly without Docker.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from calfkit._vendor.pydantic_ai import models
from calfkit.client import Client
from calfkit.exceptions import NodeFaultError
from calfkit.models.error_report import ErrorReport, FaultTypes
from calfkit.models.seam_context import SeamContext
from calfkit.nodes import Agent
from tests.integration._fault_kafka import ensure_topic, fault_worker
from tests.integration._fault_tap import fault_tap
from tests.integration._fault_tools import CalleeErrorRecorder
from tests.integration._roundtrip_helpers import FINAL_OUTPUT, final_model

pytestmark = pytest.mark.kafka
models.ALLOW_MODEL_REQUESTS = True


# ── seam handlers under test (output-position substitutes are plain str, which the
#    agent's default str output type validates at coercion) ─────────────────────────


def before_substitute(ctx: SeamContext[Any]) -> str:
    """``before_node`` short-circuit: the body never runs, this becomes the output."""
    return "substituted"


def after_redact(ctx: SeamContext[Any], output: Any) -> str:
    """``after_node`` replacement of whatever the body produced."""
    return "redacted"


def before_raise_value(ctx: SeamContext[Any]) -> None:
    """A node-own accident in ``before_node`` → routed to ``on_node_error``."""
    raise ValueError("before_node boom")


def on_node_recover(ctx: SeamContext[Any], fault: ErrorReport) -> str:
    """``on_node_error`` recovery: the value becomes the node's (recovered) output."""
    return "recovered"


def before_raise_mint(ctx: SeamContext[Any]) -> None:
    """A deliberate ``NodeFaultError`` minted in a seam → bypasses ``on_node_error``."""
    raise NodeFaultError("seam.mint", message="deliberate seam fault")


@dataclass
class BeforeNodeRecorder:
    """Channel-B ``before_node`` recorder: capture the ingress ``SeamContext`` fields,
    then return ``None`` so the body still runs."""

    calls: list[dict[str, Any]] = field(default_factory=list)

    def __call__(self, ctx: SeamContext[Any]) -> None:
        self.calls.append(
            {
                "delivery_kind": ctx.delivery_kind,
                "route": ctx.route,
                "awaiting_reply": ctx.awaiting_reply,
                "node_id": ctx.node_id,
            }
        )
        return None


def _agent(node_id: str, *, agent_in: str, publish_topic: str | None = None, **seams) -> Agent:
    return Agent(
        node_id,
        system_prompt="respond",
        subscribe_topics=agent_in,
        publish_topic=publish_topic,
        model_client=final_model(),
        sequential_only_mode=True,
        **seams,
    )


async def test_before_node_substitute_short_circuits_body(kafka_bootstrap: str, topic_namespace: str) -> None:
    """S-1: a ``before_node`` that returns a value becomes the output; the body (model
    loop) never runs."""
    agent_in = f"{topic_namespace}.s1.input"
    agent = _agent(f"{topic_namespace}-s1", agent_in=agent_in, before_node=before_substitute)
    driver = Client.connect(kafka_bootstrap)
    worker = fault_worker(kafka_bootstrap, nodes=[agent])

    try:
        async with worker:
            result = await driver.execute("go", agent_in, timeout=60)
            assert result.output == "substituted"
    finally:
        await driver.close()
        await worker._client.close()


async def test_after_node_replaces_output(kafka_bootstrap: str, topic_namespace: str) -> None:
    """S-3: ``after_node`` replaces the body's produced output."""
    agent_in = f"{topic_namespace}.s3.input"
    agent = _agent(f"{topic_namespace}-s3", agent_in=agent_in, after_node=after_redact)
    driver = Client.connect(kafka_bootstrap)
    worker = fault_worker(kafka_bootstrap, nodes=[agent])

    try:
        async with worker:
            result = await driver.execute("go", agent_in, timeout=60)
            assert result.output == "redacted"
    finally:
        await driver.close()
        await worker._client.close()


async def test_before_node_accident_recovered_by_on_node_error(kafka_bootstrap: str, topic_namespace: str) -> None:
    """S-4: a non-``NodeFaultError`` raise in ``before_node`` routes to ``on_node_error``,
    whose returned value becomes the recovered output (no fault reaches the caller)."""
    agent_in = f"{topic_namespace}.s4.input"
    agent = _agent(
        f"{topic_namespace}-s4",
        agent_in=agent_in,
        before_node=before_raise_value,
        on_node_error=on_node_recover,
    )
    driver = Client.connect(kafka_bootstrap)
    worker = fault_worker(kafka_bootstrap, nodes=[agent])

    try:
        async with worker:
            result = await driver.execute("go", agent_in, timeout=60)
            assert result.output == "recovered"
    finally:
        await driver.close()
        await worker._client.close()


async def test_seam_mint_bypasses_on_node_error(kafka_bootstrap: str, topic_namespace: str) -> None:
    """S-6: a ``NodeFaultError`` minted in ``before_node`` bypasses ``on_node_error``
    entirely and escalates verbatim — the recovery seam never fires."""
    recorder = CalleeErrorRecorder()  # reused as the on_node_error handler; must NOT fire
    agent_in = f"{topic_namespace}.s6.input"
    agent_pub = f"{topic_namespace}.s6.mirror"
    agent = _agent(
        f"{topic_namespace}-s6",
        agent_in=agent_in,
        publish_topic=agent_pub,
        before_node=before_raise_mint,
        on_node_error=recorder,
    )
    await ensure_topic(kafka_bootstrap, agent_pub)
    driver = Client.connect(kafka_bootstrap)
    worker = fault_worker(kafka_bootstrap, nodes=[agent])

    try:
        async with worker, fault_tap(kafka_bootstrap, agent_pub) as tap:
            await driver.start("go", agent_in)
            fault, _ = await tap.next_fault(timeout=60)
            assert fault.error.error_type == "seam.mint"
    finally:
        await driver.close()
        await worker._client.close()

    assert recorder.calls == []  # the mint rule bypassed on_node_error entirely


async def test_before_node_recorder_sees_ingress_seam_context(kafka_bootstrap: str, topic_namespace: str) -> None:
    """S-10 (Channel B): a ``before_node`` recorder sees the ingress ``SeamContext`` —
    ``delivery_kind=call``, ``awaiting_reply=True`` (a reply is owed), ``route=None``
    (no inbound route header) — then declines so the body runs."""
    recorder = BeforeNodeRecorder()
    agent_in = f"{topic_namespace}.s10.input"
    agent = _agent(f"{topic_namespace}-s10", agent_in=agent_in, before_node=recorder)
    driver = Client.connect(kafka_bootstrap)
    worker = fault_worker(kafka_bootstrap, nodes=[agent])

    try:
        async with worker:
            result = await driver.execute("go", agent_in, timeout=60)
            assert result.output is not None and FINAL_OUTPUT in result.output
    finally:
        await driver.close()
        await worker._client.close()

    assert len(recorder.calls) >= 1
    ingress = recorder.calls[0]
    assert ingress["delivery_kind"] == "call"
    assert ingress["awaiting_reply"] is True
    assert ingress["route"] is None


@pytest.mark.parametrize("bad_value", [True, b"bytes"], ids=["bool", "bytes"])
async def test_before_node_contract_guard_faults(kafka_bootstrap: str, topic_namespace: str, bad_value: object) -> None:
    """S-8: a ``before_node`` returning a value that can never be a node output (a ``bool``
    or ``bytes`` — the §6.2 coercion guards) raises ``SeamContractError``, which escalates
    as a ``calf.unhandled`` fault rather than silently passing garbage downstream."""

    def before_bad(ctx: SeamContext[Any]) -> object:
        return bad_value

    agent_in = f"{topic_namespace}.s8.input"
    agent_pub = f"{topic_namespace}.s8.mirror"
    agent = _agent(f"{topic_namespace}-s8", agent_in=agent_in, publish_topic=agent_pub, before_node=before_bad)
    await ensure_topic(kafka_bootstrap, agent_pub)
    driver = Client.connect(kafka_bootstrap)
    worker = fault_worker(kafka_bootstrap, nodes=[agent])

    try:
        async with worker, fault_tap(kafka_bootstrap, agent_pub) as tap:
            await driver.start("go", agent_in)
            fault, _ = await tap.next_fault(timeout=60)
            assert fault.error.error_type == FaultTypes.UNHANDLED
            assert fault.error.details.get(FaultTypes.EXCEPTION_TYPE) == "SeamContractError"
    finally:
        await driver.close()
        await worker._client.close()
