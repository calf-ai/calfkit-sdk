"""F — the broker-level decode floor (fault-rail spec §6.7 / §13).

An inbound message whose body fails to decode/validate never reaches a handler — FastStream
would log generically and ack-first drop it (the silent-drop hole on the decode path). The
``DecodeFloorMiddleware`` catches the decode ``ValidationError`` in ``consume_scope``, floors a
typed ``calf.delivery.undecodable`` event, and re-raises (suppressing would push an empty body
through any attached ``@publisher`` — verified by the FastStream 0.7.1 spike).
"""

from __future__ import annotations

import json
import logging

import pytest
from faststream.kafka import KafkaBroker, TestKafkaBroker
from pydantic import BaseModel, ValidationError

from calfkit.client.middleware import DecodeFloorMiddleware
from calfkit.models.error_report import ErrorReport, FaultTypes

_MW_LOGGER = "calfkit.client.middleware"


class _Body(BaseModel):
    x: int


async def test_undecodable_body_is_floored_and_reraised(caplog: pytest.LogCaptureFixture) -> None:
    ran: list[_Body] = []
    broker = KafkaBroker(middlewares=[DecodeFloorMiddleware])

    @broker.subscriber("n.in")
    async def node(body: _Body) -> None:
        ran.append(body)

    async with TestKafkaBroker(broker):
        with caplog.at_level(logging.ERROR, logger=_MW_LOGGER):
            with pytest.raises(ValidationError):  # re-raised after flooring (no empty-body publish)
                await broker.publish(b'{"not": "valid"}', "n.in")  # missing x → decode fails

    assert ran == []  # the handler never ran — the decode failed first
    assert "calf.delivery.undecodable" in caplog.text  # floored, typed


async def test_malformed_json_with_json_content_type_is_floored_and_reraised(caplog: pytest.LogCaptureFixture) -> None:
    # A JSON-content-type body that is not valid JSON raises ``json.JSONDecodeError`` at decode
    # (FastStream's ``json_loads`` is un-suppressed on the JSON content-type path), which a narrow
    # ``except ValidationError`` misses. The floor must catch, floor, and re-raise it too.
    ran: list[_Body] = []
    broker = KafkaBroker(middlewares=[DecodeFloorMiddleware])

    @broker.subscriber("n.in")
    async def node(body: _Body) -> None:
        ran.append(body)

    async with TestKafkaBroker(broker):
        with caplog.at_level(logging.ERROR, logger=_MW_LOGGER):
            with pytest.raises(json.JSONDecodeError):  # re-raised after flooring (no empty-body publish)
                await broker.publish(b"{not valid json", "n.in", headers={"content-type": "application/json"})

    assert ran == []  # the handler never ran — the decode failed first
    assert "calf.delivery.undecodable" in caplog.text  # floored, typed


async def test_bad_utf8_with_text_content_type_is_floored_and_reraised(caplog: pytest.LogCaptureFixture) -> None:
    # A ``text/plain`` body with invalid UTF-8 raises ``UnicodeDecodeError`` at decode
    # (``body.decode()``), which a narrow ``except ValidationError`` misses too.
    ran: list[_Body] = []
    broker = KafkaBroker(middlewares=[DecodeFloorMiddleware])

    @broker.subscriber("n.in")
    async def node(body: _Body) -> None:
        ran.append(body)

    async with TestKafkaBroker(broker):
        with caplog.at_level(logging.ERROR, logger=_MW_LOGGER):
            with pytest.raises(UnicodeDecodeError):  # re-raised after flooring (no empty-body publish)
                await broker.publish(b"\xff\xfe\xfa", "n.in", headers={"content-type": "text/plain"})

    assert ran == []  # the handler never ran — the decode failed first
    assert "calf.delivery.undecodable" in caplog.text  # floored, typed


async def test_valid_body_passes_through_without_flooring(caplog: pytest.LogCaptureFixture) -> None:
    ran: list[_Body] = []
    broker = KafkaBroker(middlewares=[DecodeFloorMiddleware])

    @broker.subscriber("n.in")
    async def node(body: _Body) -> None:
        ran.append(body)

    async with TestKafkaBroker(broker):
        with caplog.at_level(logging.ERROR, logger=_MW_LOGGER):
            await broker.publish(_Body(x=1), "n.in")

    assert ran == [_Body(x=1)]  # the handler ran normally
    assert "undecodable" not in caplog.text  # no floor on a valid body


def test_decode_floor_is_registered_outermost_on_the_connect_broker(monkeypatch: pytest.MonkeyPatch) -> None:
    # The shim is installed broker-wide by Client.connect, and OUTERMOST so its consume_scope wraps
    # the whole chain (ahead of ContextInjectionMiddleware) — every node + reply subscriber covered.
    from calfkit.client import Client
    from calfkit.client._broker import _PreStartHookBroker
    from calfkit.client.middleware import ContextInjectionMiddleware

    captured: dict[str, object] = {}
    orig_init = _PreStartHookBroker.__init__

    def spy_init(self: object, *args: object, **kwargs: object) -> None:
        captured.update(kwargs)
        orig_init(self, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(_PreStartHookBroker, "__init__", spy_init)
    Client.connect(server_urls="localhost:9092")

    mws = captured.get("middlewares")
    assert isinstance(mws, list)
    assert DecodeFloorMiddleware in mws
    assert mws.index(DecodeFloorMiddleware) < mws.index(ContextInjectionMiddleware)  # outermost


# ── Option-B undecodable-sink seam: a configured builder holding a {topic: sink} registry ──


async def test_floor_seam_calls_the_registered_sink_with_cid_and_report(caplog: pytest.LogCaptureFixture) -> None:
    calls: list[tuple[str, ErrorReport]] = []
    registry = {"inbox.topic": lambda cid, report: calls.append((cid, report))}
    broker = KafkaBroker(middlewares=[DecodeFloorMiddleware.builder(registry)])

    @broker.subscriber("inbox.topic")
    async def node(body: _Body) -> None: ...

    async with TestKafkaBroker(broker):
        with caplog.at_level(logging.ERROR, logger=_MW_LOGGER):
            with pytest.raises(ValidationError):
                await broker.publish(b'{"not": "valid"}', "inbox.topic", correlation_id="cid-1")

    assert "calf.delivery.undecodable" in caplog.text  # still synthesizes + ERROR-logs (unchanged)
    assert len(calls) == 1  # the seam handed the synthesized report to the topic's sink
    cid, report = calls[0]
    assert cid == "cid-1"  # the surviving transport correlation id
    assert report.error_type == FaultTypes.DELIVERY_UNDECODABLE


async def test_floor_seam_skips_a_topic_not_in_the_registry(caplog: pytest.LogCaptureFixture) -> None:
    # Topic-key scoping: an undecodable on a topic with NO registered sink (e.g. a node hop in a
    # co-located Worker, carrying the run's correlation_id) must never reach the client's sink (§5.8).
    calls: list[tuple[str, ErrorReport]] = []
    registry = {"inbox.topic": lambda cid, report: calls.append((cid, report))}
    broker = KafkaBroker(middlewares=[DecodeFloorMiddleware.builder(registry)])

    @broker.subscriber("node.topic")  # NOT in the registry
    async def node(body: _Body) -> None: ...

    async with TestKafkaBroker(broker):
        with caplog.at_level(logging.ERROR, logger=_MW_LOGGER):
            with pytest.raises(ValidationError):
                await broker.publish(b'{"not": "valid"}', "node.topic", correlation_id="cid-1")

    assert "calf.delivery.undecodable" in caplog.text  # the floor still synthesizes + ERROR-logs
    assert calls == []  # but the seam did not call the inbox sink — topic-scoped
