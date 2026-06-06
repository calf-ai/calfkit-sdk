"""Gated REAL-broker integration tests for opt-in Kafka topic provisioning.

These talk to a LIVE Kafka/Redpanda broker and are therefore gated behind
:data:`tests.utils.skip_if_no_kafka` (driven by the ``CALF_TEST_KAFKA`` env
var). Locally — where no broker is configured — the whole module collects and
skips cleanly. The dedicated CI lane
(``.github/workflows/kafka-integration.yml``) stands up a single-node Redpanda
broker with ``auto_create_topics_enabled=false`` and sets ``CALF_TEST_KAFKA``
so these run for real.

Why a real broker (the fake-admin unit tests in ``tests/test_provisioning*.py``
already cover the classification / wiring logic): only a live broker proves the
two properties that a fake cannot fabricate —

1. with auto-create DISABLED, an un-provisioned topic genuinely does NOT spring
   into existence when something references it (the "no silent create" stance),
   and
2. a provisioned topic is actually visible in cluster metadata
   (``admin.list_topics()``) and usable for a produce -> consume round-trip.

Scenarios (mirroring the plan's "Integration (new gated CI lane)"):

* (a) flag OFF + invoke an un-started node -> the invoke fails; the inbox topic
      is NOT silently created (requires broker auto-create disabled).
* (b) flag ON -> every referenced topic exists via ``admin.list_topics()`` AND a
      client/broker produce -> consume round-trip over a provisioned topic
      succeeds.
* (c) restart -> a second provisioning pass is idempotent (code 36 "already
      exists" is absorbed, no error, the report records them as existing).
* (d) optional ACL-denied -> the warn-and-continue path is actionable. Skipped
      unless ``CALF_TEST_KAFKA_ACL`` names a topic the test principal may not
      create (most CI brokers run fully open).

Implementation notes that the plan calls out explicitly:

* topic-visibility uses a small ASYNC poll loop (``_await_topics_visible``), not
  ``tests.utils.wait_for_condition`` — that helper's predicate is synchronous,
  but ``AIOKafkaAdminClient.list_topics()`` is a coroutine.
* admin ``start()`` is wrapped in a bounded connect-retry
  (``_admin_connected``) so a cold broker raising ``NodeNotReadyError`` /
  ``KafkaConnectionError`` during warm-up doesn't flake the lane.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import uuid
from collections.abc import AsyncIterator, Iterable
from typing import Any

import pytest

from calfkit.client import Client
from calfkit.models.envelope import Envelope
from calfkit.models.payload import TextPart
from calfkit.models.session_context import (
    CallFrameStack,
    SessionRunContext,
    WorkflowState,
)
from calfkit.models.state import State
from calfkit.nodes import consumer
from calfkit.nodes.tool import ToolNodeDef
from calfkit.provisioning import ProvisioningConfig, TopicProvisioner
from calfkit.worker.worker import Worker
from tests.utils import skip_if_no_kafka

# Every test in this module requires a live broker.
pytestmark = skip_if_no_kafka

# Bootstrap server(s) the lane connects to. The CI lane points this at the
# Redpanda services container; default matches a local single-broker setup.
BOOTSTRAP = os.getenv("CALF_TEST_KAFKA_BOOTSTRAP", "localhost:9092")

# Bounded warm-up: a freshly-started broker can refuse the first few admin
# connects with NodeNotReadyError before the controller is elected.
_CONNECT_ATTEMPTS = 20
_CONNECT_BACKOFF_S = 0.5

# Topic-metadata propagation budget for the create -> visible poll loop.
_VISIBLE_TIMEOUT_S = 20.0
_VISIBLE_POLL_S = 0.25


# ---------------------------------------------------------------------------
# Live-broker helpers (real admin client; NOT the fake-admin seam)
# ---------------------------------------------------------------------------


def _unique(prefix: str) -> str:
    """A collision-free topic prefix so reruns against the same broker don't
    trip over leftover topics from a previous run."""
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


async def _admin_connected() -> Any:
    """Open an ``AIOKafkaAdminClient`` with a bounded connect-retry.

    Retries the transient cold-broker errors (``NodeNotReadyError`` /
    ``KafkaConnectionError``) up to ``_CONNECT_ATTEMPTS`` with a fixed backoff,
    so a broker still electing its controller during CI warm-up doesn't flake
    the lane. Any other error propagates immediately. The caller owns
    ``close()`` (use :func:`_admin` for guaranteed teardown).
    """
    from aiokafka.admin import AIOKafkaAdminClient
    from aiokafka.errors import KafkaConnectionError, NodeNotReadyError

    last_exc: BaseException | None = None
    for _ in range(_CONNECT_ATTEMPTS):
        admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP)
        try:
            await admin.start()
            return admin
        except (NodeNotReadyError, KafkaConnectionError) as exc:
            last_exc = exc
            with contextlib.suppress(Exception):
                await admin.close()
            await asyncio.sleep(_CONNECT_BACKOFF_S)
    raise AssertionError(f"admin client could not connect to {BOOTSTRAP!r} after {_CONNECT_ATTEMPTS} attempts") from last_exc


@contextlib.asynccontextmanager
async def _admin() -> AsyncIterator[Any]:
    """Connected admin client as an async context manager (always closed)."""
    admin = await _admin_connected()
    try:
        yield admin
    finally:
        with contextlib.suppress(Exception):
            await admin.close()


async def _list_topics(admin: Any) -> set[str]:
    """Current topic set from cluster metadata. ``list_topics()`` is a
    coroutine, which is exactly why the visibility wait below is an async poll
    loop rather than ``wait_for_condition`` (whose predicate is sync)."""
    return set(await admin.list_topics())


async def _await_topics_visible(
    admin: Any,
    expected: Iterable[str],
    *,
    timeout: float = _VISIBLE_TIMEOUT_S,
    poll: float = _VISIBLE_POLL_S,
) -> None:
    """Async poll until every ``expected`` topic appears in metadata.

    Topic creation acks before the new metadata has fully propagated to the
    node we query, so a bounded poll absorbs that lag. We CANNOT use
    ``tests.utils.wait_for_condition`` here: its predicate must be synchronous,
    but ``list_topics()`` is async — re-deriving the loop is the correct call,
    not a wrapper around a sync predicate.
    """
    want = set(expected)
    deadline = asyncio.get_event_loop().time() + timeout
    missing = want
    while asyncio.get_event_loop().time() < deadline:
        missing = want - await _list_topics(admin)
        if not missing:
            return
        await asyncio.sleep(poll)
    raise AssertionError(f"topics not visible within {timeout}s; still missing: {sorted(missing)}")


@contextlib.asynccontextmanager
async def _running_worker(worker: Worker) -> AsyncIterator[None]:
    """Run a worker's FastStream app in the background for the test duration.

    Starts ``worker.run()`` as a task (which fires ``_on_startup`` -> eager
    provisioning -> ``broker.start()``), waits for the broker connection to come
    up, yields, then cancels and drains cleanly.
    """
    task = asyncio.ensure_future(worker.run())
    try:
        # Wait for the underlying broker to actually connect before the test
        # body produces/consumes. Bounded so a startup crash surfaces as a
        # failed task rather than hanging forever.
        deadline = asyncio.get_event_loop().time() + _VISIBLE_TIMEOUT_S
        broker = worker._client._connection
        while asyncio.get_event_loop().time() < deadline:
            if task.done():
                # Surface a startup exception (e.g. provisioning failure).
                task.result()
                raise AssertionError("worker.run() exited during startup")
            if getattr(broker, "_connection", None):
                break
            await asyncio.sleep(0.1)
        yield
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task


def _tool_node(name: str, sub: str, pub: str) -> ToolNodeDef:
    def _fn(x: int) -> int:
        return x

    _fn.__name__ = name
    return ToolNodeDef.create_tool_node(func=_fn, subscribe_topics=sub, publish_topic=pub)


def _text_envelope(text: str, correlation_id: str) -> Envelope:
    """A terminal-hop envelope carrying ``text`` as its final output part.

    Mirrors the framework's on-wire shape (see ``tests/test_consumer`` helpers)
    so a real ``consumer`` node decodes it and exposes ``result.output == text``.
    """
    state = State()
    state.final_output_parts = [TextPart(text=text)]
    ctx = SessionRunContext(state=state, deps={})
    ctx._correlation_id = correlation_id
    return Envelope(
        context=ctx,
        internal_workflow_state=WorkflowState(call_stack=CallFrameStack()),
    )


# ---------------------------------------------------------------------------
# (a) flag OFF + reference an un-started node -> no silent topic creation
# ---------------------------------------------------------------------------


async def test_flag_off_does_not_silently_create_topic() -> None:
    """With provisioning OFF (and broker auto-create disabled), referencing a
    node whose inbox topic does not exist must NOT bring that topic into being.

    We assert the no-silent-create stance directly against cluster metadata:
    provision nothing, run the worker (provisioning disabled), and confirm the
    inbox topic is still absent afterwards.
    """
    inbox = _unique("calf-it-off.in")
    out = _unique("calf-it-off.out")

    client = Client.connect(BOOTSTRAP)  # provisioning defaults to disabled
    assert client.provisioning.enabled is False
    worker = Worker(client, nodes=[_tool_node("off_echo", inbox, out)])

    async with _admin() as admin:
        before = await _list_topics(admin)
        assert inbox not in before, "precondition: inbox must not pre-exist"

        async with _running_worker(worker):
            # Worker is up with provisioning OFF; give metadata a beat to settle.
            await asyncio.sleep(1.0)
            after = await _list_topics(admin)

        # No silent create: the inbox topic was never provisioned by calfkit and
        # the broker (auto-create disabled) did not conjure it either.
        assert inbox not in after, (
            "provisioning was OFF but the inbox topic appeared — either auto-create "
            "is enabled on the broker (the CI guard step must assert it is false) "
            "or calfkit silently created it"
        )

    await client.close()


# ---------------------------------------------------------------------------
# (b) flag ON -> topics exist (admin.list_topics) + produce/consume round-trip
# ---------------------------------------------------------------------------


async def test_flag_on_provisions_topics_and_round_trips() -> None:
    """With provisioning ON, every referenced topic is created and visible in
    cluster metadata, and a real produce -> consume round-trip over a
    provisioned topic succeeds."""
    inbox = _unique("calf-it-on.in")
    received: list[str] = []

    @consumer(subscribe_topics=inbox)
    def sink(result: Any) -> None:  # NodeResult
        received.append(result.output)

    client = Client.connect(BOOTSTRAP, provisioning=ProvisioningConfig(enabled=True))
    # The round-trip publishes BEFORE the sink consumer has positioned itself on
    # the freshly-created partition. Read from the earliest offset so the message
    # is delivered regardless of consumer-join timing — the aiokafka default
    # ``auto_offset_reset="latest"`` would race the publish and silently drop it
    # (this is a test-determinism concern, not a provisioning behavior).
    worker = Worker(
        client,
        nodes=[sink],
        extra_subscribe_kwargs={"auto_offset_reset": "earliest"},
    )

    # Pre-provision the client's reply topic. ``Client.connect`` registers a
    # reply dispatcher on the shared broker; on an auto-create-OFF broker, that
    # subscriber hammers the cluster with metadata refreshes on its missing
    # inbox while the sink consumer is trying to join its group — on a
    # single-core CI broker that storm starves the join and the round-trip never
    # delivers. This raw-publish round-trip never invokes the client, so the
    # lazy reply-topic provisioning on the invoke path doesn't fire; create it
    # explicitly here (as a framework topic, so user ``topic_configs`` skip it).
    await TopicProvisioner.from_connection(server_urls=BOOTSTRAP, config=client.provisioning).provision(
        [client.reply_topic],
        framework_topics={client.reply_topic},
    )

    async with _running_worker(worker), _admin() as admin:
        # (b.1) topics exist in metadata. The consumer's inbox plus its
        # framework return inbox were eagerly provisioned at _on_startup.
        node = worker._registered_nodes[0]
        await _await_topics_visible(admin, [inbox, node._return_topic])

        # (b.2) produce -> consume round-trip over the provisioned inbox.
        envelope = _text_envelope("ping", correlation_id="calf-it-roundtrip")
        await client.broker.publish(envelope, topic=inbox, correlation_id="calf-it-roundtrip")

        deadline = asyncio.get_event_loop().time() + _VISIBLE_TIMEOUT_S
        while not received and asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.1)

        assert received == ["ping"], f"round-trip over provisioned topic {inbox!r} did not deliver (received={received!r})"

    await client.close()


# ---------------------------------------------------------------------------
# (c) restart -> second provisioning pass is idempotent
# ---------------------------------------------------------------------------


async def test_provisioning_is_idempotent_across_restart() -> None:
    """Running provisioning twice (a worker restart) must be idempotent: the
    second pass sees the topics already exist (code 36) and absorbs it — no
    error — recording them as ``existing`` rather than ``created``."""
    from calfkit.provisioning import TopicProvisioner, topics_for_nodes

    inbox = _unique("calf-it-idem.in")
    out = _unique("calf-it-idem.out")
    node = _tool_node("idem_echo", inbox, out)
    topics = topics_for_nodes([node])
    framework = {node._return_topic}

    config = ProvisioningConfig(enabled=True)

    def _provisioner() -> TopicProvisioner:
        return TopicProvisioner(bootstrap_servers=BOOTSTRAP, config=config)

    # First pass: topics are created.
    first = await _provisioner().provision(topics, framework_topics=framework)
    assert set(first.created) == set(topics), f"first pass should create all topics; created={first.created!r}"
    assert first.existing == []

    async with _admin() as admin:
        await _await_topics_visible(admin, topics)

    # Second pass (restart): every topic already exists -> idempotent no-op,
    # reported as existing, NOT re-created, and NO error raised.
    second = await _provisioner().provision(topics, framework_topics=framework)
    assert set(second.existing) == set(topics), f"second pass should see all topics as existing; existing={second.existing!r}"
    assert second.created == []


# ---------------------------------------------------------------------------
# (d) optional ACL-denied -> actionable warn-and-continue
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not os.getenv("CALF_TEST_KAFKA_ACL"),
    reason="Skipping ACL test: CALF_TEST_KAFKA_ACL not set (name a topic the test principal may NOT create to enable)",
)
async def test_acl_denied_is_reported_and_does_not_raise() -> None:
    """When the broker denies topic creation (ACL, code 29), provisioning warns
    and continues — the topic is reported in ``unauthorized`` and NOT created,
    and no exception is raised. This is the no-silent-failure contract: the
    operator gets an actionable signal that consumers/producers will stall.
    """
    from calfkit.provisioning import TopicProvisioner

    denied_topic = os.environ["CALF_TEST_KAFKA_ACL"]
    config = ProvisioningConfig(enabled=True)
    provisioner = TopicProvisioner(bootstrap_servers=BOOTSTRAP, config=config)

    report = await provisioner.provision([denied_topic], framework_topics=set())

    assert denied_topic in report.unauthorized, f"ACL-denied topic {denied_topic!r} must be reported as unauthorized; report={report!r}"
    assert denied_topic not in report.created
    assert denied_topic not in report.existing


# ---------------------------------------------------------------------------
# (#180) reply-topic provisioning at broker start — no hang on a no-auto-create
# broker. Regression tests for the silent infinite hang; the reply consumer's
# inbox is created before it subscribes, on every start path.
# ---------------------------------------------------------------------------


async def test_issue_180_bare_broker_start_provisions_reply_topic() -> None:
    """ENABLED client + a DIRECT ``client.broker.start()`` returns (it HUNG
    before the fix) and the reply topic exists — the exact #180 repro."""
    reply = f"calf-test-180-{uuid.uuid4().hex[:8]}.reply"
    client = Client.connect(BOOTSTRAP, reply_topic=reply, provisioning=ProvisioningConfig(enabled=True))
    try:
        # An infinite hang before the fix; the bounded wait turns a regression
        # into a clear failure rather than a stuck suite.
        await asyncio.wait_for(client.broker.start(), timeout=_VISIBLE_TIMEOUT_S)
        async with _admin() as admin:
            await _await_topics_visible(admin, [reply])
    finally:
        await client.close()


async def test_issue_180_worker_run_does_not_hang_on_reply_topic() -> None:
    """A Worker on a no-auto-create broker reaches serving without hanging on
    its client's reply topic (the worker path also hung before the fix). The
    reply topic appearing in metadata proves the reply inbox was provisioned at
    broker start, so the reply consumer never looped on missing metadata."""
    inbox = f"calf-test-180-{uuid.uuid4().hex[:8]}.in"
    client = Client.connect(BOOTSTRAP, provisioning=ProvisioningConfig(enabled=True))
    reply = client.reply_topic

    @consumer(subscribe_topics=inbox)
    async def _sink(result: Any) -> None:
        return None

    worker = Worker(client, nodes=[_sink])
    async with _running_worker(worker):
        async with _admin() as admin:
            await _await_topics_visible(admin, [inbox, reply])
