"""Task-identity substrate (PR-0.5 prep spec §2): the origin mint + ``x-calf-task`` carriage.

The client is the SOLE origin (spec §2-A — the only client publish site is
``_publish_call``; no node originates a run): ``_build_state`` mints ``task_id`` (uuid7,
once per run) beside the ``correlation_id`` default, and ``_publish_call`` stamps it as
the forwarded ``x-calf-task`` header. The minted task is DISTINCT from the correlation id
(a fresh uuid7) — the property that makes the Phase-2 key flip observable (spec §5-G).
Node-side forwarding is covered per publish path in ``test_task_header_forwarding``; the
middleware ingress arm has its own tests.
"""

from unittest.mock import AsyncMock

from calfkit._protocol import HDR_TASK
from calfkit.client import Client


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
