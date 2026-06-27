"""Commit 3+ — the new caller-surface Client (calfkit/client/caller.py), built standalone in a
new module ALONGSIDE the old Client (untouched) and made canonical at the Commit-6 cutover.

This commit lands lazy+sync `connect()`, the firehose `events()`, the guarded broker start, and
graceful `aclose()`. The verb triad + `agent()` arrive in Commit 5.
"""

from __future__ import annotations

import asyncio

import pytest
from faststream.kafka import TestKafkaBroker

from calfkit._protocol import HDR_EMITTER, HDR_EMITTER_KIND, HDR_KIND
from calfkit.client.caller import Client
from calfkit.client.events import RunCompleted
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


async def test_events_firehose_yields_a_reply_published_to_the_inbox() -> None:
    client = Client.connect("localhost:9092", inbox_topic="inbox.t")
    async with TestKafkaBroker(client._broker):  # starts the broker in simulation
        async with client.events() as stream:  # registers the firehose outlet + ensures the broker is up
            await client._broker.publish(_return_env("hello"), "inbox.t", correlation_id="cid-1", headers=_headers("return"))
            ev = await asyncio.wait_for(anext(stream), timeout=1.0)
    assert isinstance(ev, RunCompleted)
    assert ev.correlation_id == "cid-1"
    assert ev.output == "hello"


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
