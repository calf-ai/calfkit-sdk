"""Suite M — the ``max_message_bytes`` knob over the real broker (design §8.11/§8.12).

The lane's shared Redpanda enforces its ~1 MiB cluster default (``kafka_batch_max_bytes``),
so a >1 MiB flow needs the broker's permission — which is exactly the documented operational
contract ("a production broker must allow at least ``max_message_bytes``"). Both permission
paths are exercised:

* **M-1 (topic override):** the test topics are created with a per-topic
  ``max.message.bytes`` override (the ops-contract knob), and a >1 MiB payload round-trips
  through an agent end-to-end — proving the client legs (producer guard permits, node
  consumer + inbox reader floors deliver) AND that the per-topic size config actually
  gates the flow (a negative control publishes the same payload to a default topic and is
  rejected server-side).
* **M-2 (cluster override):** a dedicated Redpanda booted with a raised
  ``kafka_batch_max_bytes`` runs the same round-trip with NO topic config — the pure
  client-leg proof, unconfounded by per-topic overrides.
* **M-3 (the guard):** a dispatch over the knob raises ``MessageSizeTooLargeError``
  client-side, before the wire — broker config irrelevant by construction.

Opt-in (``-m kafka`` / ``make test-kafka``); skips cleanly without Docker.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from textwrap import dedent

import pytest
from aiokafka import AIOKafkaProducer
from aiokafka.errors import MessageSizeTooLargeError  # type: ignore[import-untyped]

from calfkit._vendor.pydantic_ai import models
from calfkit.client import Client
from calfkit.nodes import Agent
from tests.integration._fault_kafka import ensure_topic, fault_worker
from tests.integration._roundtrip_helpers import final_model

pytestmark = pytest.mark.kafka
models.ALLOW_MODEL_REQUESTS = True

_ONE_MIB = 1024 * 1024
#: Each rail's payload: > Redpanda's ~1 MiB default (the thing being proven), while keeping the
#: REPLY envelope — which carries the output PLUS the message history (the inbound text rides
#: along) — under the worker's own 5 MiB guard. ~1.2 MiB in + ~1.2 MiB out ≈ a <3 MiB reply.
_BIG_IN = "x" * (_ONE_MIB + 200 * 1024)
_BIG_OUT = "y" * (_ONE_MIB + 200 * 1024)
#: Per-topic / cluster permission used by M-1/M-2 — must be >= the largest envelope.
_PERMITTED_BYTES = 8 * _ONE_MIB


def _echo_agent(node_id: str, agent_in: str) -> Agent:
    """An agent whose scripted model finalizes immediately with a >1 MiB output — so the
    round-trip carries a >1 MiB payload on BOTH rails (call in, reply out)."""
    return Agent(node_id, system_prompt="respond", subscribe_topics=agent_in, model_client=final_model(_BIG_OUT))


async def test_topic_override_permits_a_large_round_trip(kafka_bootstrap: str, topic_namespace: str) -> None:
    """M-1: with ``max.message.bytes`` set on the call + reply topics, a >1 MiB message
    round-trips through an agent; the same payload to a default topic is rejected
    server-side (the negative control proving the override is what gated it)."""
    agent_in = f"{topic_namespace}.m1.input"
    reply_topic = f"{topic_namespace}.m1.reply"
    override = {"max.message.bytes": str(_PERMITTED_BYTES)}
    await ensure_topic(kafka_bootstrap, agent_in, config=override)
    await ensure_topic(kafka_bootstrap, reply_topic, config=override)

    driver = Client.connect(kafka_bootstrap, inbox_topic=reply_topic)
    worker = fault_worker(kafka_bootstrap, nodes=[_echo_agent(f"{topic_namespace}-m1", agent_in)])
    try:
        async with worker:
            result = await driver.agent(topic=agent_in).execute(_BIG_IN, timeout=90)
            assert result.output == _BIG_OUT  # >1 MiB survived both rails, byte-for-byte
    finally:
        await driver.aclose()
        await worker._client.aclose()

    # Negative control: the same payload to a NON-overridden (auto-created, cluster-default)
    # topic is refused by the broker — server-side, since the client cap here (5 MiB) permits it.
    control = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap, max_request_size=5 * _ONE_MIB)
    await control.start()
    try:
        with pytest.raises(MessageSizeTooLargeError):
            await control.send_and_wait(f"{topic_namespace}.m1.control", _BIG_IN.encode())
    finally:
        await control.stop()


@pytest.fixture(scope="module")
def big_limit_bootstrap() -> Iterator[str]:
    """A dedicated Redpanda whose CLUSTER limit (``kafka_batch_max_bytes``) is raised — M-2's
    unconfounded environment. Mirrors the shared-broker conftest (image/resources env vars,
    skip-without-Docker semantics); scoped to this module so the lane's shared broker keeps
    its default 1 MiB limit (M-1's negative control depends on it)."""
    try:
        from docker.errors import DockerException
        from testcontainers.kafka import RedpandaContainer
    except ImportError:
        pytest.skip("integration extra (testcontainers) not installed")

    image = os.getenv("CALF_TEST_REDPANDA_IMAGE", "docker.redpanda.com/redpandadata/redpanda:v26.1.10")
    smp = os.getenv("CALF_TEST_REDPANDA_SMP", "2")
    memory = os.getenv("CALF_TEST_REDPANDA_MEMORY", "2G")

    class _BigLimitRedpanda(RedpandaContainer):
        # Mirrors the conftest's _TunedRedpanda start script, adding the raised cluster limit.
        def tc_start(self) -> None:
            host = self.get_container_host_ip()
            port = self.get_exposed_port(self.redpanda_port)
            data = (
                dedent(
                    f"""
                    #!/bin/bash
                    /usr/bin/rpk redpanda start --mode dev-container --smp {smp} --memory {memory} \
                    --set redpanda.kafka_batch_max_bytes={_PERMITTED_BYTES} \
                    --kafka-addr PLAINTEXT://0.0.0.0:29092,OUTSIDE://0.0.0.0:9092  \
                    --advertise-kafka-addr PLAINTEXT://127.0.0.1:29092,OUTSIDE://{host}:{port}
                    """
                )
                .strip()
                .encode("utf-8")
            )
            self.create_file(data, RedpandaContainer.TC_START_SCRIPT)

    container = _BigLimitRedpanda(image)
    try:
        container.start()
    except DockerException:
        pytest.skip("Docker daemon absent / unreachable")
    try:
        yield container.get_bootstrap_server()
    finally:
        container.stop()


async def test_client_legs_round_trip_on_a_permissive_cluster(big_limit_bootstrap: str, topic_namespace: str) -> None:
    """M-2: on a cluster whose limit is raised, a >1 MiB message round-trips through an agent
    with NO topic-level size config — the client legs (guard permits, floors deliver) proven
    without the per-topic override in the picture."""
    agent_in = f"{topic_namespace}.m2.input"
    driver = Client.connect(big_limit_bootstrap)
    worker = fault_worker(big_limit_bootstrap, nodes=[_echo_agent(f"{topic_namespace}-m2", agent_in)])
    try:
        async with worker:
            result = await driver.agent(topic=agent_in).execute(_BIG_IN, timeout=90)
            assert result.output == _BIG_OUT
    finally:
        await driver.aclose()
        await worker._client.aclose()


async def test_oversized_dispatch_raises_client_side(kafka_bootstrap: str, topic_namespace: str) -> None:
    """M-3 (design §8.12): a dispatch whose serialized envelope exceeds the (default 5 MiB)
    knob raises ``MessageSizeTooLargeError`` at the send — client-side, pre-wire, so the
    broker's own limits never enter the picture."""
    driver = Client.connect(kafka_bootstrap)
    try:
        with pytest.raises(MessageSizeTooLargeError):
            await driver.agent(topic=f"{topic_namespace}.m3.input").execute("x" * (6 * _ONE_MIB), timeout=30)
    finally:
        await driver.aclose()
