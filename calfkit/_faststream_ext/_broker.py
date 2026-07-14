"""``KeyOrderedRegistratorMixin`` — the registration seam.

Mix into a ``KafkaBroker`` subclass (first base) to gain ``key_ordered_subscriber()``.
The method replays the stock registrator's three steps — build via the factory, register
via the *generic* ``Registrator.subscriber``, attach the call — with one critical detail:
the generic registration MUST be the unbound ``Registrator.subscriber(self, ...)`` call.
From a mixin sitting above ``KafkaBroker`` in the MRO, ``super().subscriber(...)``
resolves to ``KafkaRegistrator``'s ``*topics`` topic-builder overload, which would bind
the subscriber instance as a topic name (the stock code reaches the generic method only
because its call site lives *inside* ``KafkaRegistrator``).

Kwargs contract (fail loud): the surface allow-lists the connection-level kwargs the
stock builder forwards per-subscriber into ``AIOKafkaConsumer`` — ``CONNECTION_ARG_KEYS``,
canaried against the stock builder's signature. That set, not aiokafka's constructor, is
the source of truth: aiokafka's signature would over-admit broker-level security kwargs
(``sasl_*``, ``ssl_context``, ``security_protocol``) and ``enable_auto_commit`` (which
collides with the value the config derives from the ack policy) — per-subscriber knobs
the stock builder deliberately does not expose. Anything
else — the stock builder's structural/spec params or plain unknowns — raises ``SetupError``
naming the key: never a bare ``TypeError`` at registration, never a silent drop, never an
unvalidated fall-through that explodes at connect time. Omitted connection kwargs fall
back to aiokafka's own defaults, the same values the stock builder's parameter defaults
copy.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any, cast

from faststream._internal.broker.registrator import Registrator
from faststream.exceptions import SetupError

from ._factory import create_key_ordered_subscriber
from ._subscriber import KeyOrderedSubscriber

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant
    from faststream.kafka.configs import KafkaBrokerConfig

# The stock KafkaRegistrator.subscriber's per-subscriber connection_args surface
# (kafka/broker/registrator.py, the `connection_args={...}` literal). Canaried in
# tests/unit/faststream_ext/test_upstream_seams.py against the builder's signature.
CONNECTION_ARG_KEYS = frozenset(
    {
        "group_instance_id",
        "key_deserializer",
        "value_deserializer",
        "fetch_max_wait_ms",
        "fetch_max_bytes",
        "fetch_min_bytes",
        "max_partition_fetch_bytes",
        "auto_offset_reset",
        "auto_commit_interval_ms",
        "check_crcs",
        "partition_assignment_strategy",
        "max_poll_interval_ms",
        "rebalance_timeout_ms",
        "session_timeout_ms",
        "heartbeat_interval_ms",
        "consumer_timeout_ms",
        "max_poll_records",
        "exclude_internal_topics",
        "isolation_level",
        "client_rack",
    }
)


class KeyOrderedRegistratorMixin:
    """Adds ``key_ordered_subscriber()`` to a ``KafkaBroker`` subclass.

    Additive only: the stock ``subscriber()`` builder is untouched.
    """

    if TYPE_CHECKING:
        # Provided by the KafkaBroker the mixin is composed with. Deliberately Any, not
        # KafkaBrokerConfig: Registrator declares this attribute as its ConfigComposition
        # proxy, so a narrower annotation here is base-incompatible at composition time —
        # the cast at the factory call below mirrors the stock registrator's own idiom
        # (kafka/broker/registrator.py casts the same attribute).
        config: Any

    def key_ordered_subscriber(
        self,
        *topics: str,
        group_id: str | None = None,
        max_workers: int,
        parser: Callable[..., Any] | None = None,
        decoder: Callable[..., Any] | None = None,
        dependencies: Iterable[Dependant] = (),
        codec: Any = None,
        **connection_kwargs: Any,
    ) -> KeyOrderedSubscriber:
        """Register a key-ordered subscriber: ``max_workers``-way parallel, strictly
        serial and in-order per partition key, multi-topic, ACK_FIRST only."""
        if unsupported := set(connection_kwargs) - CONNECTION_ARG_KEYS:
            msg = (
                f"key_ordered_subscriber() does not support {sorted(unsupported)}. "
                "Beyond its named parameters it accepts only the per-subscriber "
                f"connection options {sorted(CONNECTION_ARG_KEYS)}. For batch/pattern/"
                "partition modes, other ack policies, or AsyncAPI overrides, use the "
                "stock subscriber() — key-ordered dispatch does not define them."
            )
            raise SetupError(msg)

        subscriber = create_key_ordered_subscriber(
            *topics,
            group_id=group_id,
            max_workers=max_workers,
            connection_args=dict(connection_kwargs),
            config=cast("KafkaBrokerConfig", self.config),
        )
        # Generic registration, called unbound on purpose — see the module docstring.
        # The cast states the mixin's composition contract: its host IS a Registrator.
        Registrator.subscriber(cast("Registrator[Any, Any]", self), subscriber, persistent=True)
        subscriber.add_call(
            parser_=parser,
            decoder_=decoder,
            dependencies_=dependencies,
            codec_=codec,
        )
        return subscriber
