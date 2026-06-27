"""Commit 3+ — the new caller-surface Client (calfkit/client/caller.py), built standalone in a
new module ALONGSIDE the old Client (untouched) and made canonical at the Commit-6 cutover.

This commit lands lazy+sync `connect()`, the firehose `events()`, the guarded broker start, and
graceful `aclose()`. The verb triad + `agent()` arrive in Commit 5.
"""

from __future__ import annotations

import asyncio
from typing import Annotated, Any
from unittest.mock import AsyncMock

import pytest
from faststream import Context
from faststream.kafka import TestKafkaBroker

from calfkit._protocol import HDR_EMITTER, HDR_EMITTER_KIND, HDR_KIND
from calfkit.client.caller import Client
from calfkit.client.events import RunCompleted
from calfkit.client.gateway import Dispatch
from calfkit.client.hub import InvocationHandle, _RunChannel
from calfkit.exceptions import ClientClosedError
from calfkit.models import CallFrameStack, Envelope, SessionRunContext, WorkflowState
from calfkit.models.payload import TextPart
from calfkit.models.reply import ReturnMessage
from calfkit.models.state import State


def _return_env(text: str) -> Envelope:
    return Envelope(
        context=SessionRunContext(state=State(), deps={}),
        internal_workflow_state=WorkflowState(call_stack=CallFrameStack()),
        reply=ReturnMessage(in_reply_to=None, tag=None, parts=[TextPart(text=text)]),
    )


def _headers(kind: str) -> dict[str, str]:
    return {HDR_KIND: kind, HDR_EMITTER: "agent.x", HDR_EMITTER_KIND: "agent"}


def _register_echo_agent(client: Client, topic: str, reply_text: str = "reply") -> list[dict[str, Any]]:
    """Register a subscriber on *topic* that captures inbound deps and echoes a return to the inbox —
    a stand-in for a real worker, so a client round-trip (start/execute/send) completes offline."""
    captured: list[dict[str, Any]] = []

    @client._broker.subscriber(topic)
    async def agent_node(env: Envelope, correlation_id: Annotated[str, Context()]) -> None:
        captured.append(dict(env.context.deps))
        await client._broker.publish(_return_env(reply_text), client.inbox_topic, correlation_id=correlation_id, headers=_headers("return"))

    return captured


def test_connect_is_sync_and_lazy_with_a_default_inbox_topic() -> None:
    client = Client.connect("localhost:9092")
    assert isinstance(client.inbox_topic, str) and client.inbox_topic  # a generated ephemeral inbox
    assert not client._broker._connection  # lazy — no I/O, the broker is unstarted (spec §2.7)


def test_connect_uses_a_provided_inbox_topic() -> None:
    client = Client.connect("localhost:9092", inbox_topic="my.inbox")
    assert client.inbox_topic == "my.inbox"


def test_connect_rejects_raw_security_kwargs() -> None:
    with pytest.raises(ValueError, match="security="):
        Client.connect("localhost:9092", sasl_mechanism="PLAIN")


async def test_ensure_started_starts_the_broker_at_most_once() -> None:
    # broker.start() is not self-idempotent; _ensure_started must start it once then no-op. The
    # `_started` flag is load-bearing: TestKafkaBroker (and real brokers) gate via broker.running,
    # but TestKafkaBroker never flips running, so without the flag we would re-start every dispatch.
    client = Client.connect("localhost:9092", inbox_topic="i")
    client._broker.start = AsyncMock()
    await client._ensure_started()
    await client._ensure_started()
    await client._ensure_started()
    client._broker.start.assert_awaited_once()


async def test_ensure_started_skips_start_when_the_broker_is_already_running() -> None:
    # A co-located Worker's app.start() set broker.running=True before the client dispatched. The
    # client must detect that via `running` (the subscribers-are-up signal) and NOT re-start —
    # gating on the producer-only `_connection` would publish into a not-yet-consuming inbox.
    client = Client.connect("localhost:9092", inbox_topic="i")
    client._broker.start = AsyncMock()
    client._broker._connection = object()  # producer connected, but the readiness signal is `running`
    client._broker.running = True  # the Worker fully started the shared broker (subscribers up)
    await client._ensure_started()
    client._broker.start.assert_not_awaited()


async def test_events_firehose_yields_a_reply_published_to_the_inbox() -> None:
    client = Client.connect("localhost:9092", inbox_topic="inbox.t")
    async with TestKafkaBroker(client._broker):  # starts the broker in simulation
        async with client.events() as stream:  # registers the firehose outlet + ensures the broker is up
            await client._broker.publish(_return_env("hello"), "inbox.t", correlation_id="cid-1", headers=_headers("return"))
            ev = await asyncio.wait_for(anext(stream), timeout=1.0)
    assert isinstance(ev, RunCompleted)
    assert ev.correlation_id == "cid-1"
    assert ev.output == "hello"


async def test_events_iterated_without_async_with_raises_not_hangs() -> None:
    # A bare `async for ev in client.events():` (forgetting `async with`) registers no firehose
    # outlet and never starts the broker, so iterating un-entered would park forever. It must raise
    # an actionable error instead of silently hanging (CRITICAL-1).
    client = Client.connect("localhost:9092", inbox_topic="inbox.t")
    stream = client.events()
    with pytest.raises(RuntimeError, match="async with"):
        await anext(stream)


async def test_aclose_resolves_pending_runs_with_client_closed_error() -> None:
    client = Client.connect("localhost:9092", inbox_topic="i.t")
    handle = InvocationHandle(correlation_id="cid-1", _channel=_RunChannel())
    client._hub.track(handle)
    await client.aclose()  # spec §5.8: a pending result() resolves with ClientClosedError
    with pytest.raises(ClientClosedError):
        await asyncio.wait_for(handle._channel.await_terminal(), timeout=1.0)


async def test_async_context_manager_closes_the_client_on_exit() -> None:
    handle = InvocationHandle(correlation_id="cid-1", _channel=_RunChannel())
    async with Client.connect("localhost:9092", inbox_topic="i.t") as client:
        client._hub.track(handle)
    with pytest.raises(ClientClosedError):  # __aexit__ → aclose() resolved the pending run
        await asyncio.wait_for(handle._channel.await_terminal(), timeout=1.0)


# ── Commit 5: the gateway — agent() minting (4 overloads, name|topic, both/neither guard) ──


def test_agent_by_name_derives_the_private_input_topic() -> None:
    gw = Client.connect("localhost:9092", inbox_topic="i").agent("summarizer")
    assert gw._topic == "agent.summarizer.private.input"  # derive_input_topic (ADR-0017)
    assert gw._output_type is str  # output_type defaults to str (spec §2.2)


def test_agent_by_topic_uses_the_escape_hatch_topic() -> None:
    gw = Client.connect("localhost:9092", inbox_topic="i").agent(topic="shared.work")
    assert gw._topic == "shared.work"


def test_agent_binds_the_output_type_at_mint() -> None:
    gw = Client.connect("localhost:9092", inbox_topic="i").agent("x", output_type=int)
    assert gw._output_type is int


def test_agent_rejects_both_name_and_topic() -> None:
    client = Client.connect("localhost:9092", inbox_topic="i")
    with pytest.raises(ValueError):
        client.agent("x", topic="y")  # type: ignore[call-overload]


def test_agent_rejects_neither_name_nor_topic() -> None:
    client = Client.connect("localhost:9092", inbox_topic="i")
    with pytest.raises(ValueError):
        client.agent()  # type: ignore[call-overload]


# ── Commit 5: the verb triad — send / start / execute ──


async def test_start_publishes_a_call_to_the_agent_topic_and_the_reply_resolves() -> None:
    client = Client.connect("localhost:9092", inbox_topic="inbox")
    captured: list[tuple[str, dict[str, Any]]] = []

    @client._broker.subscriber("agent.summarizer.private.input")
    async def agent_node(
        env: Envelope,
        correlation_id: Annotated[str, Context()],
        headers: Annotated[dict[str, Any], Context("message.headers")],
    ) -> None:
        captured.append((correlation_id, dict(headers)))

    async with TestKafkaBroker(client._broker):
        handle = await client.agent("summarizer", output_type=str).start("hello", correlation_id="cid-1")
        assert len(captured) == 1  # the call reached the agent's Private input topic
        cid, headers = captured[0]
        assert cid == "cid-1"
        assert headers[HDR_KIND] == "call"  # x-calf-kind=call on the outbound dispatch (§2.6)
        # simulate the agent's terminal reply landing on the client inbox
        await client._broker.publish(_return_env("summary"), "inbox", correlation_id="cid-1", headers=_headers("return"))
        res = await asyncio.wait_for(handle.result(), timeout=1.0)
    assert res.output == "summary"
    assert res.correlation_id == "cid-1"


async def test_execute_is_start_plus_result_round_trip() -> None:
    client = Client.connect("localhost:9092", inbox_topic="inbox")
    _register_echo_agent(client, "agent.x.private.input", "answer")
    async with TestKafkaBroker(client._broker):
        res = await asyncio.wait_for(client.agent("x").execute("q", correlation_id="cid-e"), timeout=1.0)
    assert res.output == "answer"
    assert res.correlation_id == "cid-e"


async def test_send_returns_a_dispatch_and_registers_no_per_run_handle() -> None:
    client = Client.connect("localhost:9092", inbox_topic="inbox")
    _register_echo_agent(client, "agent.x.private.input")
    async with TestKafkaBroker(client._broker):
        d = await client.agent("x").send("go", correlation_id="cid-s")
        assert isinstance(d, Dispatch)
        assert d.correlation_id == "cid-s"
        assert "cid-s" not in client._hub._runs  # send() registers NO per-run handle (spec §2.2)


async def test_send_reply_is_observable_on_the_firehose() -> None:
    client = Client.connect("localhost:9092", inbox_topic="inbox")
    _register_echo_agent(client, "agent.x.private.input", "sent-reply")
    async with TestKafkaBroker(client._broker):
        async with client.events(terminal_only=True) as stream:  # subscribe BEFORE send (spec §8)
            await client.agent("x").send("go", correlation_id="cid-s")
            ev = await asyncio.wait_for(anext(stream), timeout=1.0)
    assert ev.correlation_id == "cid-s"  # send()'s reply lands on the inbox → firehose (ADR-0022)


async def test_deps_factory_seed_merges_under_per_call_deps() -> None:
    client = Client.connect("localhost:9092", inbox_topic="inbox", deps_factory=lambda: {"a": 1, "b": 2})
    captured = _register_echo_agent(client, "agent.x.private.input")
    async with TestKafkaBroker(client._broker):
        await client.agent("x").send("go", deps={"b": 99, "c": 3})
    assert captured[0] == {"a": 1, "b": 99, "c": 3}  # per-call overrides the ambient seed (spec §2.3)


async def test_multi_turn_continuation_feeds_message_history_forward() -> None:
    client = Client.connect("localhost:9092", inbox_topic="inbox")
    _register_echo_agent(client, "agent.x.private.input", "reply")
    async with TestKafkaBroker(client._broker):
        gw = client.agent("x")
        r1 = await asyncio.wait_for(gw.execute("turn1", correlation_id="c1"), timeout=1.0)
        r2 = await asyncio.wait_for(gw.execute("turn2", message_history=r1.message_history, correlation_id="c2"), timeout=1.0)
    assert r1.output == r2.output == "reply"
    assert isinstance(r1.message_history, list)  # result.message_history → next turn (first-class)


async def test_unserializable_model_settings_bubbles_at_publish_not_a_preflight() -> None:
    client = Client.connect("localhost:9092", inbox_topic="inbox")
    _register_echo_agent(client, "agent.x.private.input")
    async with TestKafkaBroker(client._broker):
        with pytest.raises(Exception):  # serialization error bubbles from publish — no call-site pre-flight (§2.5)
            await client.agent("x").send("go", model_settings={"bad": object()})  # type: ignore[typeddict-item]


async def test_start_with_a_duplicate_live_cid_raises_value_error() -> None:
    client = Client.connect("localhost:9092", inbox_topic="inbox")
    _register_echo_agent(client, "agent.x.private.input")
    async with TestKafkaBroker(client._broker):
        h1 = await client.agent("x").start("a", correlation_id="dup")  # held → live in the weak map
        with pytest.raises(ValueError):
            await client.agent("x").start("b", correlation_id="dup")  # duplicate live cid (spec §5.2)
        assert h1.correlation_id == "dup"
