"""Factory + registration surface of the extension (spec §6.5, D6/D9/D15/D16).

Everything here is calfkit-free: plain FastStream handlers/filters, FastStream's own
``SetupError``. ``TestKafkaBroker`` executes handlers via ``process_message`` directly
(bypassing dispatch — verified), so the composition test proves registration/processing/
publishing WIRING; dispatch semantics live in ``test_dispatch.py`` and the real-broker
integration lane.
"""

from __future__ import annotations

import pytest
from faststream import AckPolicy
from faststream.exceptions import SetupError
from faststream.kafka import KafkaBroker, TestKafkaBroker

from calfkit._faststream_ext import KeyOrderedRegistratorMixin
from calfkit._faststream_ext._subscriber import KeyOrderedSubscriber


class _KeyOrderedBroker(KeyOrderedRegistratorMixin, KafkaBroker):
    """What a consumer of the extension does (calfkit: _PreStartHookBroker)."""


def test_registers_a_key_ordered_subscriber_on_the_broker() -> None:
    broker = _KeyOrderedBroker()
    sub = broker.key_ordered_subscriber("topic-a", "topic-b", group_id="g", max_workers=4)
    assert isinstance(sub, KeyOrderedSubscriber)
    assert sub.max_workers == 4
    assert sub in broker.subscribers
    assert sub.group_id == "g"
    # Multi-topic accepted — the whole point vs ConcurrentBetweenPartitionsSubscriber.
    assert set(sub.topics) == {"topic-a", "topic-b"}


def test_stock_subscriber_builder_is_untouched() -> None:
    broker = _KeyOrderedBroker()
    stock = broker.subscriber("topic-a")
    assert not isinstance(stock, KeyOrderedSubscriber)


def test_rejects_broker_inherited_non_ack_first_policy() -> None:
    broker = _KeyOrderedBroker(ack_policy=AckPolicy.ACK)
    with pytest.raises(SetupError, match="ACK_FIRST"):
        broker.key_ordered_subscriber("topic-a", group_id="g", max_workers=2)


def test_rejects_max_workers_below_one() -> None:
    broker = _KeyOrderedBroker()
    with pytest.raises(SetupError, match="max_workers"):
        broker.key_ordered_subscriber("topic-a", group_id="g", max_workers=0)


def test_upstream_validator_cases_still_fire() -> None:
    broker = _KeyOrderedBroker()
    with pytest.raises(SetupError):
        broker.key_ordered_subscriber(group_id="g", max_workers=2)  # no topics at all


@pytest.mark.parametrize(
    "kwarg",
    [
        # structural (stock-builder params the narrow surface deliberately omits)
        {"batch": True},
        {"ack_policy": AckPolicy.ACK},
        {"pattern": "t-.*"},
        {"partitions": [("t", 0)]},
        {"listener": object()},
        {"no_reply": True},
        # spec-level
        {"title": "x"},
        {"description": "x"},
        {"include_in_schema": False},
        {"batch_timeout_ms": 100},
        {"max_records": 5},
        # aiokafka-accepted but NOT part of the stock per-subscriber connection surface
        {"sasl_plain_password": "hunter2"},
        {"ssl_context": object()},
        {"enable_auto_commit": False},
        # plain unknown
        {"bogus_option": 1},
    ],
)
def test_unsupported_kwargs_raise_named_setup_error(kwarg: dict) -> None:
    """D15 allow-list: never a bare TypeError, never a silent drop, never a fall-through
    into connection_args that would explode at connect time."""
    broker = _KeyOrderedBroker()
    (name,) = kwarg
    with pytest.raises(SetupError, match=name):
        broker.key_ordered_subscriber("topic-a", group_id="g", max_workers=2, **kwarg)


def test_connection_kwargs_flow_into_connection_args() -> None:
    broker = _KeyOrderedBroker()
    sub = broker.key_ordered_subscriber(
        "topic-a",
        group_id="g",
        max_workers=2,
        fetch_max_bytes=1234567,
        session_timeout_ms=12000,
        client_rack="rack-1",  # registrator-supported despite absence from AIOKafkaConsumer.__init__
    )
    assert sub._connection_args["fetch_max_bytes"] == 1234567
    assert sub._connection_args["session_timeout_ms"] == 12000
    assert sub._connection_args["client_rack"] == "rack-1"
    # ACK_FIRST wiring: KafkaSubscriberConfig.__post_init__ derived auto-commit.
    assert sub._connection_args["enable_auto_commit"] is True


def test_connection_allowlist_constant_matches_canary_expectation() -> None:
    from calfkit._faststream_ext._broker import CONNECTION_ARG_KEYS
    from tests.unit.faststream_ext.test_upstream_seams import EXPECTED_CONNECTION_KEYS

    assert CONNECTION_ARG_KEYS == EXPECTED_CONNECTION_KEYS


def test_asyncapi_schema_parity_with_stock_subscriber() -> None:
    """D9: schema output derives from specification + calls only — identical to a stock
    subscriber on the same topics."""

    async def handler(body: str) -> None: ...

    stock_broker = KafkaBroker()
    stock = stock_broker.subscriber("topic-a", "topic-b", group_id="g")
    stock(handler)

    ko_broker = _KeyOrderedBroker()
    ko = ko_broker.key_ordered_subscriber("topic-a", "topic-b", group_id="g", max_workers=4)
    ko(handler)

    assert ko.schema() == stock.schema()


async def test_generic_composition_handler_filter_publisher() -> None:
    """The subscriber object composes exactly like a stock one: decorator attach with a
    plain FastStream filter, publisher decoration, in-memory round trip."""
    broker = _KeyOrderedBroker()
    subscriber = broker.key_ordered_subscriber("in-topic", group_id="g", max_workers=2)

    seen: list[str] = []

    async def handler(body: str) -> str:
        seen.append(body)
        return f"echo:{body}"

    handler_wrapped = subscriber(handler, filter=lambda msg: True)
    publisher = broker.publisher("out-topic")
    publisher(handler_wrapped)

    async with TestKafkaBroker(broker) as test_broker:
        await test_broker.publish("hello", topic="in-topic")
        assert seen == ["hello"]
        publisher.mock.assert_called_once_with("echo:hello")


def test_add_call_passthroughs_actually_flow() -> None:
    """D15's allow-listed passthroughs must reach the subscriber's call options — not
    just be accepted by the signature."""
    broker = _KeyOrderedBroker()

    def sentinel_parser(msg):  # pragma: no cover - identity sentinel
        return msg

    def sentinel_decoder(msg):  # pragma: no cover - identity sentinel
        return msg

    sub = broker.key_ordered_subscriber("topic-a", group_id="g", max_workers=2, parser=sentinel_parser, decoder=sentinel_decoder)
    assert sub._call_options.parser is sentinel_parser
    assert sub._call_options.decoder is sentinel_decoder
