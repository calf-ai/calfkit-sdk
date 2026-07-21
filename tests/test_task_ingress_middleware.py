"""The identity middleware's task arm (task-keying prep spec §2, rulings batch 15).

``ContextInjectionMiddleware.consume_scope`` reads the forwarded ``x-calf-task`` and
scopes it for handler injection; an ENVELOPE-wire delivery that arrives WITHOUT it
(raw-producer entry, or header loss upstream) gets a task MINTED at ingress — the
boundary invariant that keeps raw-producer runs coherently keyed after the cutover.
Carve-outs (round-2 adversarial MAJOR-1): step-wire deliveries carry no task header in
this PR by design — absent-header step deliveries neither mint nor log; a PRESENT header
is scoped regardless of wire kind. The mint log is plain DEBUG, unthrottled (takeover
ruling 2026-07-20 — prod runs with debug logging off).

These tests exercise the REAL consume path (``TestKafkaBroker``), so they also pin the
FastStream-native ``Annotated[str, Context()]`` injection the node handler relies on.
"""

from __future__ import annotations

import logging
from typing import Annotated, Any

import pytest
from faststream import Context
from faststream.kafka import KafkaBroker, TestKafkaBroker

from calfkit._protocol import HDR_TASK, HDR_WIRE
from calfkit.client.middleware import ContextInjectionMiddleware

_MW_LOGGER = "calfkit.client.middleware"
_MINT_MARK = "minted task at ingress"


def _broker_and_sink() -> tuple[KafkaBroker, list[str | None]]:
    """A broker carrying ONLY the identity middleware + a subscriber that records the
    scoped ``task_id`` (default ``None`` so the no-scope arm is observable, not an
    injection error)."""
    broker = KafkaBroker(middlewares=[ContextInjectionMiddleware])
    seen: list[str | None] = []

    @broker.subscriber("n.in")
    async def node(body: Any, task_id: Annotated[str | None, Context(default=None)]) -> None:
        seen.append(task_id)

    return broker, seen


async def test_headerless_envelope_delivery_mints_a_task_and_logs_debug(caplog: pytest.LogCaptureFixture) -> None:
    broker, seen = _broker_and_sink()
    async with TestKafkaBroker(broker):
        with caplog.at_level(logging.DEBUG, logger=_MW_LOGGER):
            await broker.publish({"x": 1}, "n.in", headers={HDR_WIRE: "envelope"})
    assert len(seen) == 1
    minted = seen[0]
    assert isinstance(minted, str) and len(minted) == 32  # a fresh uuid7 hex, scoped for the handler
    int(minted, 16)
    assert _MINT_MARK in caplog.text  # the ingress-mint DEBUG line (the only emission — no throttle)


async def test_header_carrying_envelope_delivery_passes_through_no_mint_no_log(caplog: pytest.LogCaptureFixture) -> None:
    broker, seen = _broker_and_sink()
    async with TestKafkaBroker(broker):
        with caplog.at_level(logging.DEBUG, logger=_MW_LOGGER):
            await broker.publish({"x": 1}, "n.in", headers={HDR_WIRE: "envelope", HDR_TASK: "t-carried"})
    assert seen == ["t-carried"]  # forwarded value scoped verbatim
    assert _MINT_MARK not in caplog.text


async def test_step_wire_delivery_without_header_neither_mints_nor_logs(caplog: pytest.LogCaptureFixture) -> None:
    # The round-2 MAJOR-1 carve-out: steps carry no HDR_TASK in this PR by design and are
    # terminal telemetry — minting there is waste that would drown the header-loss signal.
    broker, seen = _broker_and_sink()
    async with TestKafkaBroker(broker):
        with caplog.at_level(logging.DEBUG, logger=_MW_LOGGER):
            await broker.publish({"x": 1}, "n.in", headers={HDR_WIRE: "step"})
    assert seen == [None]  # no scope set — the absent-header non-envelope arm
    assert _MINT_MARK not in caplog.text


async def test_present_header_is_scoped_regardless_of_wire_kind(caplog: pytest.LogCaptureFixture) -> None:
    broker, seen = _broker_and_sink()
    async with TestKafkaBroker(broker):
        with caplog.at_level(logging.DEBUG, logger=_MW_LOGGER):
            await broker.publish({"x": 1}, "n.in", headers={HDR_WIRE: "step", HDR_TASK: "t-on-step"})
    assert seen == ["t-on-step"]
    assert _MINT_MARK not in caplog.text


async def test_undecodable_junk_without_envelope_wire_never_mints(caplog: pytest.LogCaptureFixture) -> None:
    # Round-2 MINOR-2: junk without the envelope wire header never mints — no mint noise
    # for arbitrary broker traffic.
    broker, seen = _broker_and_sink()
    async with TestKafkaBroker(broker):
        with caplog.at_level(logging.DEBUG, logger=_MW_LOGGER):
            await broker.publish(b"junk-bytes", "n.in", headers={})
    assert seen == [None]
    assert _MINT_MARK not in caplog.text
