"""The identity middleware's task arm (task-keying prep spec §2).

``ContextInjectionMiddleware.consume_scope`` reads the forwarded ``x-calf-task`` and
scopes it for handler injection; an ENVELOPE-wire delivery that arrives WITHOUT it
(raw-producer entry, or header loss upstream) gets a task MINTED at ingress — the
boundary invariant that keeps raw-producer runs coherently keyed after the cutover.
Carve-outs (spec §2): step-wire deliveries carry no task header in this PR by design —
absent-header step deliveries neither mint nor log; a present non-empty header is
scoped regardless of wire kind; a BLANK header reads as absent. The mint log is plain
DEBUG, unthrottled (prod runs with debug logging off).

These tests exercise the REAL consume path (``TestKafkaBroker``). The carve-out probes
use a NULLABLE ``Context(default=None)`` param so the no-scope arm is observable; the
production REQUIRED ``Annotated[str, Context()]`` form — and its fail-loud behavior
when unscoped — is pinned separately by the interlock tests at the bottom.
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


async def test_blank_task_header_reads_as_absent_and_mints(caplog: pytest.LogCaptureFixture) -> None:
    """A present-but-EMPTY ``x-calf-task`` is unusable identity — treating it as carried
    would key ``b""`` (a non-null key that also slips the keyless backstop) and silently
    pin the run to one partition. Blank reads as absent: an envelope-wire delivery mints."""
    broker, seen = _broker_and_sink()
    async with TestKafkaBroker(broker):
        with caplog.at_level(logging.DEBUG, logger=_MW_LOGGER):
            await broker.publish({"x": 1}, "n.in", headers={HDR_WIRE: "envelope", HDR_TASK: ""})
    assert len(seen) == 1
    minted = seen[0]
    assert minted, "a blank task header must not be scoped verbatim"
    assert isinstance(minted, str) and len(minted) == 32
    assert _MINT_MARK in caplog.text


async def test_blank_task_header_on_step_wire_is_not_scoped() -> None:
    # Blank-as-absent holds for scoping too: a step-wire delivery with an empty header
    # gets NO scope (like the absent-header step arm), never a scoped "".
    broker, seen = _broker_and_sink()
    async with TestKafkaBroker(broker):
        await broker.publish({"x": 1}, "n.in", headers={HDR_WIRE: "step", HDR_TASK: ""})
    assert seen == [None]


# ---------------------------------------------------------------------------
# The production injection contract: required param + the mint/filter interlock
# ---------------------------------------------------------------------------


def _required_param_broker() -> tuple[KafkaBroker, list[str]]:
    """A subscriber declaring the PRODUCTION form — a REQUIRED ``task_id`` Context param,
    exactly like ``BaseNodeDef.handler`` and the hub's ``_handle_reply``."""
    broker = KafkaBroker(middlewares=[ContextInjectionMiddleware])
    seen: list[str] = []

    @broker.subscriber("req.in")
    async def node(body: Any, task_id: Annotated[str, Context()]) -> None:
        seen.append(task_id)

    return broker, seen


async def test_interlock_required_param_always_scoped_for_envelope_wire() -> None:
    """The invariant's INTERLOCK pin: every handler that REQUIRES ``task_id`` subscribes
    with ``wire_filter(Envelope)`` (worker.py / hub.py), and the mint predicate covers
    exactly that wire kind — so a delivery reaching a required-param handler is always
    scoped, headerless or not. A drift between the mint predicate and the wire filter
    breaks HERE before it can surface in production as a misattributed decode fault."""
    broker, seen = _required_param_broker()
    async with TestKafkaBroker(broker):
        await broker.publish({"x": 1}, "req.in", headers={HDR_WIRE: "envelope"})  # headerless → minted
        await broker.publish({"x": 1}, "req.in", headers={HDR_WIRE: "envelope", HDR_TASK: "t-carried"})
    assert len(seen) == 2
    assert len(seen[0]) == 32 and seen[1] == "t-carried"


async def test_required_param_without_the_scope_fails_loud_never_none() -> None:
    """The fail-loud backstop (spec §2): if the scope is ever missing for a required
    param — reachable only if the mint predicate and the subscriber wire filter drift —
    injection RAISES (a pydantic ValidationError from the signature model); the handler
    never runs with a None task and nothing publishes keyless. (In production the
    outermost decode floor would catch this ValidationError and floor it as
    ``calf.delivery.undecodable`` — loud but misattributed; this pin is what keeps that
    path unreachable.)"""
    from pydantic import ValidationError

    broker, seen = _required_param_broker()
    async with TestKafkaBroker(broker):
        with pytest.raises(ValidationError):
            await broker.publish({"x": 1}, "req.in", headers={HDR_WIRE: "step"})  # no scope for a required param
    assert seen == []
