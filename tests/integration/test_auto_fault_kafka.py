"""Suite R — the reply-owing auto-fault over the real broker.

When a reply-owing delivery's route chain finds no matching handler (and no ``run()``
fallback), the body produces nothing — but a caller is owed an answer, so the framework
auto-faults ``calf.delivery.rejected`` instead of stranding the caller (#201 closed by
construction; catalogue §10). This drives that over the wire with a custom routed
``NodeDef``: a single ``@handler("known.route")`` and no catch-all, hit with an unmatched
route. The ``schema_rejected`` reason and the fire-and-forget no-op are covered offline
(``tests/test_fault_pipeline.py``).

Opt-in (``-m kafka`` / ``make test-kafka``); skips cleanly without Docker.
"""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from calfkit._protocol import HDR_ERROR_TYPE
from calfkit._registry import handler
from calfkit.client import Client
from calfkit.models import ReturnCall, SessionRunContext, State
from calfkit.models.error_report import FaultTypes
from calfkit.nodes import NodeDef
from tests.integration._fault_kafka import ensure_topic, fault_worker
from tests.integration._fault_tap import fault_tap

pytestmark = pytest.mark.kafka


class RoutedNode(NodeDef[State]):
    """A node that handles exactly one route and nothing else — so any other route
    all-declines (no matching handler, no ``run()`` fallback)."""

    @handler("known.route")
    async def on_known(self, ctx: SessionRunContext) -> ReturnCall[State]:
        return ReturnCall(state=ctx.state, value="handled")


class Order(BaseModel):
    amount: int


class SchemaNode(NodeDef[State]):
    """A node whose single handler matches the route but pins a payload schema — a
    matching route with a body that fails the schema declines as ``schema_rejected``
    (distinct from ``all_declined``: a route DID match, the body did not)."""

    @handler("create", schema=Order)
    async def on_create(self, ctx: SessionRunContext, payload: Order) -> ReturnCall[State]:
        return ReturnCall(state=ctx.state, value="created")


async def _publish_routed(driver: Client, topic: str, prompt: str, *, route: str, body: object = None) -> None:
    """Send a reply-owing routed call via the internal lower-level path: route/body are NOT on the
    public gateway verbs (spec §9.2), so a routed client dispatch goes through ``_publish_call``
    directly. ``callback_topic`` is the client's inbox, so the delivery is reply-owing."""
    cid, state = driver._build_state(prompt, correlation_id=None, temp_instructions=None, message_history=None, author=None)
    await driver._ensure_started()
    await driver._publish_call(topic=topic, correlation_id=cid, state=state, deps=None, route=route, body=body)


async def test_reply_owing_all_declined_auto_faults(kafka_bootstrap: str, topic_namespace: str) -> None:
    """R-1: a reply-owing delivery on a route no handler matches auto-faults
    ``calf.delivery.rejected`` with ``details.reason='all_declined'`` — the caller is
    never stranded."""
    node_in = f"{topic_namespace}.r1.input"
    node_pub = f"{topic_namespace}.r1.mirror"
    node = RoutedNode(node_id=f"{topic_namespace}-r1", subscribe_topics=[node_in], publish_topic=node_pub)
    await ensure_topic(kafka_bootstrap, node_pub)
    driver = Client.connect(kafka_bootstrap)
    worker = fault_worker(kafka_bootstrap, nodes=[node])

    try:
        async with worker, fault_tap(kafka_bootstrap, node_pub) as tap:
            # A reply is owed (the call's callback is the client inbox), but the route matches no handler.
            await _publish_routed(driver, node_in, "go", route="unknown.route")

            fault, headers = await tap.next_fault(timeout=60)
            assert fault.error.error_type == FaultTypes.DELIVERY_REJECTED
            assert headers[HDR_ERROR_TYPE] == FaultTypes.DELIVERY_REJECTED
            assert fault.error.details[FaultTypes.REASON] == FaultTypes.REASON_ALL_DECLINED
    finally:
        await driver.aclose()
        await worker._client.aclose()


async def test_reply_owing_schema_rejection_auto_faults(kafka_bootstrap: str, topic_namespace: str) -> None:
    """R-2: a reply-owing delivery on a matching route whose body fails the handler's
    schema auto-faults ``calf.delivery.rejected`` with ``details.reason='schema_rejected'``."""
    node_in = f"{topic_namespace}.r2.input"
    node_pub = f"{topic_namespace}.r2.mirror"
    node = SchemaNode(node_id=f"{topic_namespace}-r2", subscribe_topics=[node_in], publish_topic=node_pub)
    await ensure_topic(kafka_bootstrap, node_pub)
    driver = Client.connect(kafka_bootstrap)
    worker = fault_worker(kafka_bootstrap, nodes=[node])

    try:
        async with worker, fault_tap(kafka_bootstrap, node_pub) as tap:
            # the route matches "create", but the body does not satisfy Order(amount: int)
            await _publish_routed(driver, node_in, "go", route="create", body={"wrong": "shape"})

            fault, headers = await tap.next_fault(timeout=60)
            assert fault.error.error_type == FaultTypes.DELIVERY_REJECTED
            assert headers[HDR_ERROR_TYPE] == FaultTypes.DELIVERY_REJECTED
            assert fault.error.details[FaultTypes.REASON] == FaultTypes.REASON_SCHEMA_REJECTED
    finally:
        await driver.aclose()
        await worker._client.aclose()
