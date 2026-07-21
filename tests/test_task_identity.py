"""Task-identity substrate (PR-0.5 prep spec §2): the origin mint + ``x-calf-task`` carriage.

The client is the SOLE origin (spec §2-A — the only client publish site is
``_publish_call``; no node originates a run): ``_build_state`` mints ``task_id`` (uuid7,
once per run) beside the ``correlation_id`` default, and ``_publish_call`` stamps it as
the forwarded ``x-calf-task`` header. The minted task is DISTINCT from the correlation id
(a fresh uuid7) — the property that makes the Phase-2 key flip observable (spec §5-G).

Ingress (spec §2-C): the middleware-scoped value reaches handlers via FastStream-native
param injection — these direct-drive tests pass the param exactly as they pass
``correlation_id`` (header injection is inert without a broker). The context accessor
follows the ``correlation_id`` RAISING template; the client reply dispatcher stamps the
same scoped value (the hub arm — takeover ruling 2026-07-20).
"""

from unittest.mock import AsyncMock

import pytest

from calfkit._protocol import HDR_KIND, HDR_TASK
from calfkit.client import Client
from calfkit.client.hub import InvocationHandle, _Hub, _RunChannel
from calfkit.models import CallFrame, CallFrameStack, Envelope, ReturnCall, SessionRunContext, WorkflowState
from calfkit.models.payload import TextPart
from calfkit.models.reply import ReturnMessage
from calfkit.models.state import State
from calfkit.nodes.node import NodeDef
from tests._broker_fakes import CaptureBroker

# ---------------------------------------------------------------------------
# The origin mint (spec §2-A)
# ---------------------------------------------------------------------------


def test_hdr_task_wire_constant() -> None:
    # The wire contract: the header name is part of the protocol surface (spec §6.2).
    assert HDR_TASK == "x-calf-task"


def test_build_state_mints_a_distinct_task_id() -> None:
    client = Client.connect()
    cid, task_id, _state = client._build_state(
        "hello",
        correlation_id=None,
        temp_instructions=None,
        message_history=None,
        author=None,
    )
    assert isinstance(task_id, str) and len(task_id) == 32
    int(task_id, 16)  # valid uuid hex
    assert task_id != cid, "task_id must be a DISTINCT uuid7, not the correlation_id"


def test_caller_supplied_correlation_still_gets_a_fresh_task_id() -> None:
    # correlation_id is caller-suppliable (run identity); task_id has NO caller-supply
    # surface in prep (spec §2 MIN-2) — the framework mints it regardless.
    client = Client.connect()
    cid, task_id, _state = client._build_state(
        "hello",
        correlation_id="cid-supplied",
        temp_instructions=None,
        message_history=None,
        author=None,
    )
    assert cid == "cid-supplied"
    assert task_id and task_id != cid


async def test_entry_publish_stamps_x_calf_task(monkeypatch) -> None:  # noqa: ANN001
    """The origin stamp (spec §2-A): the entry publish carries ``x-calf-task`` = the
    minted task_id, alongside the existing emitter/kind/wire headers."""
    client = Client.connect()
    published: list[dict] = []

    async def spy_publish(message, **kwargs):  # noqa: ANN001, ANN003
        published.append(kwargs)

    monkeypatch.setattr(client._broker, "publish", spy_publish)

    cid, task_id, state = client._build_state(
        "hello",
        correlation_id=None,
        temp_instructions=None,
        message_history=None,
        author=None,
    )
    await client._publish_call(topic="some.topic", correlation_id=cid, task_id=task_id, state=state, deps=None)

    assert len(published) == 1
    assert published[0]["headers"][HDR_TASK] == task_id


async def test_send_threads_the_minted_task_to_the_publish() -> None:
    """The gateway verbs thread the ``_build_state`` mint into ``_publish_call`` — one
    mint per run, forwarded, never re-minted at the publish site."""
    client = Client.connect()
    client._ensure_started = AsyncMock()
    client._publish_call = AsyncMock()

    await client.agent(topic="agent.input").send("hi")

    kwargs = client._publish_call.await_args.kwargs
    assert kwargs["task_id"], "the gateway verb must thread the minted task_id"
    assert kwargs["task_id"] != kwargs["correlation_id"]


# ---------------------------------------------------------------------------
# Context carriage (spec §2-C): the raising accessor + the ingress threading
# ---------------------------------------------------------------------------


def _task_envelope() -> Envelope:
    stack = CallFrameStack()
    stack.push(CallFrame(target_topic="t", callback_topic="reply.topic"))
    return Envelope(
        internal_workflow_state=WorkflowState(call_stack=stack),
        context=SessionRunContext(state=State(), deps={}),
    )


def test_task_id_read_before_stamping_raises() -> None:
    """The ``correlation_id`` RAISING template (spec §2-C): a read outside a stamping
    handler is a loud RuntimeError — never a None leaking into key/log sites."""
    ctx = SessionRunContext(state=State(), deps={})
    with pytest.raises(RuntimeError):
        _ = ctx.task_id


async def test_prepare_context_stamps_task_id_onto_the_run_context() -> None:
    node = NodeDef(node_id="n-task-ctx", subscribe_topics=["t"])
    ctx = await node.prepare_context(_task_envelope(), correlation_id="c-1", task_id="t-1")
    assert ctx.task_id == "t-1"


async def test_handler_threads_task_id_to_the_body() -> None:
    """handler → prepare_context → ctx: the injected param IS the read (batch-15) — the
    same threaded value the publish paths key and stamp with."""
    seen: list[str] = []

    class N(NodeDef):
        async def run(self, ctx: SessionRunContext) -> ReturnCall:
            seen.append(ctx.task_id)
            return ReturnCall(state=ctx.state, value="ok")

    node = N(node_id="n-task-run", subscribe_topics=["t"])
    await node.handler(_task_envelope(), correlation_id="c-2", task_id="t-2", headers={}, broker=CaptureBroker())
    assert seen == ["t-2"]


async def test_hub_reply_dispatch_stamps_the_scoped_task() -> None:
    """The stamp's third site (takeover ruling, 2026-07-20): the client reply dispatcher
    stamps the middleware-scoped task onto the reply context — parity with
    ``correlation_id``, and always a real value on this path."""
    hub = _Hub()
    handle = InvocationHandle(correlation_id="cid-t", _channel=_RunChannel())
    hub.track(handle)
    env = Envelope(
        context=SessionRunContext(state=State(), deps={}),
        internal_workflow_state=WorkflowState(call_stack=CallFrameStack()),
        reply=ReturnMessage(in_reply_to=None, tag=None, parts=[TextPart(text="done")]),
    )
    hub._on_reply(env, "cid-t", {HDR_KIND: "return"}, task_id="t-hub")
    assert env.context.task_id == "t-hub"
