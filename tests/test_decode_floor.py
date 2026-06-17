"""F — the broker-level decode floor (fault-rail spec §6.7 / §13).

An inbound message whose body fails to decode/validate never reaches a handler — FastStream
would log generically and ack-first drop it (the silent-drop hole on the decode path). The
``DecodeFloorMiddleware`` catches the decode ``ValidationError`` in ``consume_scope``, floors a
typed ``calf.delivery.undecodable`` event, and re-raises (suppressing would push an empty body
through any attached ``@publisher`` — verified by the FastStream 0.7.1 spike).
"""

from __future__ import annotations

import logging

import pytest
from faststream.kafka import KafkaBroker, TestKafkaBroker
from pydantic import BaseModel, ValidationError

from calfkit.client.middleware import DecodeFloorMiddleware

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
