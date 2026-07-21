"""Suite O — the oversized-fault degradation ladder against a real broker.

When a fault's serialized envelope exceeds the producer's ``max_request_size``, the publish
raises ``MessageSizeTooLargeError`` **client-side** (aiokafka's ``_serialize``, before any byte
is transmitted). The rail degrades the carriage down the state-elision ladder rather than
dropping the fault: full → **lean** (empty context + a topology-only stack, ``state_elided=True``)
→ lean + minimal report → floor. So an oversized turn — the incident's shape, where the RUN STATE
(not the report) blows the budget — still reaches the caller instead of becoming a silent drop.

- **O-2** (this fix, not xfail): constrains the WORKER's producer ``max_request_size`` — the
  incident's CLIENT-SIDE mode. A node faults while its inbound carriage is inflated past the limit;
  the full fault overflows, the lean fault (state elided, full report kept) fits, and the caller
  receives it on its inbox.
- **O-1** (``xfail``): the distinct SERVER-SIDE mode — a fault under ``max_request_size`` but over a
  topic's ``max.message.bytes`` fatals the idempotent producer. Out of scope for the state-elision
  fix (``docs/designs/oversized-fault-state-elision-spec.md`` §5); the gap stands.

Opt-in (``-m kafka`` / ``make test-kafka``); skips cleanly without Docker.
"""

from __future__ import annotations

import pytest

from calfkit._vendor.pydantic_ai import models
from calfkit._vendor.pydantic_ai.messages import ToolCallPart
from calfkit.client import Client
from calfkit.models import ReturnCall, SessionRunContext, State
from calfkit.nodes import Agent, NodeDef
from tests.integration._fault_kafka import ensure_topic, fault_worker
from tests.integration._fault_tap import fault_tap
from tests.integration._fault_tools import oversized_fault
from tests.integration._roundtrip_helpers import scripted_model

pytestmark = pytest.mark.kafka
models.ALLOW_MODEL_REQUESTS = True


class _RaisingNode(NodeDef[State]):
    """A node whose body raises → a ``calf.exception`` fault synthesized at the chokepoint. The
    fault mirror carries the inbound context (with its deps), so an inflated ``deps`` makes the
    full fault overflow a small producer ``max_request_size``, exercising the ladder client-side."""

    async def run(self, ctx: SessionRunContext) -> ReturnCall[State]:
        raise ValueError("kaboom")


async def test_oversized_fault_elides_state_and_reaches_the_caller(kafka_bootstrap: str, topic_namespace: str) -> None:
    """O-2: a fault whose full carriage exceeds the worker producer's ``max_request_size`` is
    delivered to the caller with the RUN STATE elided (``state_elided=True``) and the FULL report
    intact — the client-side overflow the incident hit, no longer a silent drop.

    The worker's producer is constrained (``max_request_size`` small); the driver runs on a
    default-sized producer, so the ingress call (carrying the inflated ``deps``) publishes fine. The
    node's body raises; its fault mirror carries that inflated inbound context, so rung 1 (full)
    overflows client-side and rung 2 (lean: state/deps dropped, report kept) fits and reaches the
    caller's inbox — where the tap reads it."""
    reply_topic = f"{topic_namespace}.o2.reply"
    node_in = f"{topic_namespace}.o2.input"
    await ensure_topic(kafka_bootstrap, reply_topic)
    await ensure_topic(kafka_bootstrap, node_in)

    node = _RaisingNode(node_id=f"{topic_namespace}-o2", subscribe_topics=[node_in])
    driver = Client.connect(kafka_bootstrap, inbox_topic=reply_topic)
    # 64 KiB size knob (→ producer max_request_size); ~256 KB of deps rides the full fault carriage
    # but is dropped by the lean rung. (Spelled max_message_bytes= since the knob landed: connect()
    # rejects a raw max_request_size kwarg.)
    worker = fault_worker(kafka_bootstrap, nodes=[node], max_message_bytes=65536)

    try:
        async with worker, fault_tap(kafka_bootstrap, reply_topic) as tap:
            cid, state = driver._build_state("go", correlation_id=None, temp_instructions=None, message_history=None, author=None)
            await driver._ensure_started()
            await driver._publish_call(topic=node_in, correlation_id=cid, state=state, deps={"blob": "x" * 262144})

            fault, _ = await tap.next_fault(timeout=60)
            # the caller received the fault (not floored) with the state elided but the full report kept
            assert fault.state_elided is True
            assert fault.error.error_type == "calf.exception"
            assert fault.error.exception is not None and fault.error.exception.type == "ValueError"  # rung 2, not the minimal rung 3
    finally:
        await driver.aclose()
        await worker._client.aclose()


@pytest.mark.xfail(
    reason="known: aiokafka idempotent producer goes FATAL on the server-side strip-retry — see TODO below",
    strict=False,
)
async def test_oversized_fault_strips_to_minimal_and_still_arrives(kafka_bootstrap: str, topic_namespace: str) -> None:
    """O-1: the SERVER-SIDE oversize mode (distinct from O-2's client-side ladder): a fault under
    the producer ``max_request_size`` but over the callback topic's ``max.message.bytes`` is
    rejected server-side and fatals the IDEMPOTENT producer. This gap is out of scope for the
    state-elision fix (``docs/designs/oversized-fault-state-elision-spec.md`` §5).

    The worker runs ``enable_idempotence=True`` deliberately — that is the ONLY configuration under
    which this gap manifests, so the test must set it to genuinely reproduce the documented failure
    (with idempotence off — calfkit's default — the same server rejection just degrades through the
    state-elision ladder to a minimal report that fits, and no fault is lost; that path is O-2's).

    TODO(calfkit): we are aware this fails and must decide how calfkit handles fatal producer
    errors. Fatal error in brief: the full fault overflows the topic's ``max.message.bytes``, so
    the broker rejects it server-side AFTER the idempotent producer assigned it a sequence number
    (a prior committed send — e.g. a step — means it wasn't seq 0). aiokafka doesn't rewind/reset
    the sequence (it lacks KIP-360 recovery), so a retry on the SAME producer is rejected with
    ``OutOfOrderSequenceNumber``, which aiokafka treats as FATAL: it fails all pending batches and
    poisons the producer so every later publish raises. Decision needed: prevent it (size-bound /
    topic ``max.message.bytes`` >= producer ``max_request_size`` contract) vs. recover
    (detect-fatal -> recreate the producer)."""
    reply_topic = f"{topic_namespace}.o1.reply"
    agent_in = f"{topic_namespace}.o1.input"
    # Constrain the callback (client reply) topic so the full ~8 KB fault overflows it but
    # the minimal report fits. (The agent's own return topic + publish mirror stay default.)
    await ensure_topic(kafka_bootstrap, reply_topic, config={"max.message.bytes": "4096"})

    agent = Agent(
        f"{topic_namespace}-o1",
        system_prompt="call oversized_fault",
        subscribe_topics=agent_in,
        model_client=scripted_model([ToolCallPart("oversized_fault", {"x": 1}, tool_call_id="c1")]),
        tools=[oversized_fault],
    )
    driver = Client.connect(kafka_bootstrap, inbox_topic=reply_topic)
    # Idempotence ON is load-bearing here: it is what turns the server-side rejection into the FATAL
    # OutOfOrderSequenceNumber the docstring documents (the standing gap). See the O-2 test for the
    # default (idempotence-off) path, where the ladder delivers instead.
    worker = fault_worker(kafka_bootstrap, nodes=[agent, oversized_fault], enable_idempotence=True)

    try:
        async with worker, fault_tap(kafka_bootstrap, reply_topic) as tap:
            await driver.agent(topic=agent_in).start("go")

            fault, _ = await tap.next_fault(timeout=60)
            # identity survives; the heavy parts were stripped to fit
            assert fault.error.error_type == "billing.oversized"
            assert "blob" not in fault.error.details
            assert fault.error.causes == []
            assert fault.error.frame_chain == []
    finally:
        await driver.aclose()
        await worker._client.aclose()
