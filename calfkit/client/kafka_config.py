"""Kafka connection configuration captured at Client.connect time.

FastStream exposes no public API to retrieve a KafkaBroker's bootstrap
servers or security/SASL/SSL settings after construction. The aggregator
subsystem needs them for the transient AIOKafkaConsumer used during
state-topic rehydration. Rather than reaching into FastStream private
internals, Client.connect captures the kwargs it passed and threads
them forward through Worker._prepare_aggregators -> FanOutAggregator.
setup -> _KafkaStateStore.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from calfkit.exceptions import DurabilityConfigError

logger = logging.getLogger(__name__)


# Minimum recommended ``rebalance_timeout_ms`` for groups that own
# aggregator state-topic partitions. Rehydration reads the entire
# compacted partition contents on every reassignment; if the worker
# can't finish before the broker considers it dead, the broker fires
# another rebalance and the system enters a rebalance storm.
#
# aiokafka's default is 30s, which is fine for stateless consumers but
# too tight for state-store rehydration on production-sized topics.
# Five minutes gives realistic headroom for partition counts in the
# low hundreds and segments in the low GB range, without becoming so
# permissive that genuine worker hangs go undetected.
#
# Public (no leading underscore) because the value is operator-relevant
# — it appears in the :class:`DurabilityConfigError` raised by
# :meth:`KafkaConfig.assert_rehydration_timeout_ok` and operators may
# wish to reference the same constant when wiring custom monitoring.
REHYDRATE_REBALANCE_TIMEOUT_FLOOR_MS: int = 300_000


# aiokafka kwargs accepted by ``AIOKafkaProducer`` but rejected by
# ``AIOKafkaConsumer.__init__`` with ``TypeError: __init__() got an
# unexpected keyword argument ...``.
#
# WHY this matters: ``KafkaConfig`` is the *consumer-side* snapshot used
# to construct the transient ``AIOKafkaConsumer`` the aggregator state
# store spins up during rehydration. Producer-only kwargs reach the
# broker's producer via ``KafkaBroker(**broker_kwargs)`` through
# FastStream — they have no business in ``KafkaConfig.client_kwargs``,
# but if they leak through (e.g. a future caller constructs
# ``KafkaConfig`` directly with producer kwargs in ``client_kwargs``,
# or a regression in ``client.base._build_kafka_config``) the
# rehydration consumer construction would ``TypeError`` on first deploy.
#
# Source: cross-referenced against the ``AIOKafkaProducer.__init__``
# signature in aiokafka (see https://aiokafka.readthedocs.io/en/stable/
# api.html#producer-class). Keep this list conservative — adding a
# consumer-accepted kwarg here would silently strip it from the
# rehydration consumer config.
_PRODUCER_ONLY_KWARGS: frozenset[str] = frozenset(
    {
        "acks",
        "enable_idempotence",
        "linger_ms",
        "compression_type",
        "transactional_id",
        "max_request_size",
        "delivery_timeout_ms",
        "max_batch_size",
        "buffer_memory",
        "send_backoff_ms",
        "transaction_timeout_ms",
    }
)


# Tuple of typed-field names on :class:`KafkaConfig`. Used by
# ``__post_init__`` to detect ``client_kwargs`` entries that collide
# with typed fields (which is an error — see Issue 12).
_KAFKA_CONFIG_TYPED_FIELDS: tuple[str, ...] = (
    "security_protocol",
    "sasl_mechanism",
    "sasl_plain_username",
    "sasl_plain_password",
    "ssl_context",
    "client_id",
)


@dataclass(frozen=True)
class KafkaConfig:
    """Snapshot of the Kafka client kwargs Client.connect used to
    construct the FastStream KafkaBroker.

    The common Kafka client options are surfaced as typed fields for
    discoverability and static-type checking. ``client_kwargs`` is the
    escape hatch for kwargs not covered by typed fields (e.g.
    ``rebalance_timeout_ms``, ``session_timeout_ms``). Keys that overlap
    typed-field names raise :class:`DurabilityConfigError` at
    construction — there is no precedence rule; collisions are
    misconfigurations.

    ``client_kwargs`` is defensively copied and wrapped in
    :class:`types.MappingProxyType` at construction time so the captured
    snapshot cannot be mutated after the fact (the dataclass is frozen,
    but a plain ``dict`` would still be in-place-mutable).

    Used by :class:`~calfkit.worker.worker.Worker` to construct the
    transient :class:`AIOKafkaConsumer` the aggregator's state store
    spins up during rehydration. Without this snapshot, rehydration
    cannot inherit the broker's security / SASL / SSL settings.
    """

    bootstrap_servers: str | list[str]
    security_protocol: str | None = None
    sasl_mechanism: str | None = None
    sasl_plain_username: str | None = None
    sasl_plain_password: str | None = None
    # Typed as Any to avoid importing ssl in this hot module; callers
    # pass an ``ssl.SSLContext`` here.
    ssl_context: Any | None = None
    client_id: str | None = None
    # Escape hatch for kwargs not covered by typed fields. Keys that
    # overlap typed-field names raise ``DurabilityConfigError`` at
    # construction. Defensively copied + wrapped in MappingProxyType
    # in ``__post_init__`` so the snapshot is immutable after build.
    client_kwargs: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # Issue 12: typed field / client_kwargs collisions are
        # misconfigurations, not silently-resolved precedence rules.
        # Detect at construction so the failure surfaces at
        # ``Client.connect`` (the user's call site) rather than at
        # rehydration time (operationally far away).
        for name in _KAFKA_CONFIG_TYPED_FIELDS:
            if name in self.client_kwargs:
                raise DurabilityConfigError(
                    f"KafkaConfig: {name!r} is set both as a typed field "
                    f"({getattr(self, name)!r}) and in client_kwargs "
                    f"({self.client_kwargs[name]!r}); remove it from "
                    "client_kwargs and use the typed field.",
                    kwarg_name=name,
                    offending_value=self.client_kwargs[name],
                    expected_value=getattr(self, name),
                )

        # Issue 17 (KafkaConfig): defensive copy + immutable view so the
        # snapshot cannot be tampered with after construction. The
        # dataclass is frozen, but a plain dict would still expose
        # ``client_kwargs["k"] = v`` mutation. ``object.__setattr__``
        # is needed because ``frozen=True`` blocks normal assignment.
        object.__setattr__(self, "client_kwargs", MappingProxyType(dict(self.client_kwargs)))

    def to_consumer_kwargs(self) -> dict[str, Any]:
        """Return the merged kwargs dict for ``AIOKafkaConsumer(**kwargs)``.

        Typed fields and ``client_kwargs`` cannot collide (collisions
        are rejected at construction — see :meth:`__post_init__`), so
        the merge is unambiguous. ``None`` values are skipped so they
        don't override aiokafka defaults.

        Producer-only kwargs listed in :data:`_PRODUCER_ONLY_KWARGS`
        are stripped from the returned dict as a defense-in-depth safety
        net: callers who construct ``KafkaConfig`` directly with
        producer kwargs in ``client_kwargs`` would otherwise hit a
        ``TypeError`` at consumer construction. ``Client.connect``
        already partitions producer kwargs out of ``client_kwargs``
        upstream, but this filter guarantees correctness even when that
        path is bypassed.
        """
        # ``client_kwargs`` first; typed fields are layered on top. No
        # collision is possible thanks to the __post_init__ check.
        merged: dict[str, Any] = {k: v for k, v in self.client_kwargs.items() if v is not None}
        merged["bootstrap_servers"] = self.bootstrap_servers
        typed_fields: dict[str, Any] = {
            "security_protocol": self.security_protocol,
            "sasl_mechanism": self.sasl_mechanism,
            "sasl_plain_username": self.sasl_plain_username,
            "sasl_plain_password": self.sasl_plain_password,
            "ssl_context": self.ssl_context,
            "client_id": self.client_id,
        }
        for key, value in typed_fields.items():
            if value is not None:
                merged[key] = value

        # Defense in depth: strip any producer-only kwargs that snuck
        # through. ``AIOKafkaConsumer.__init__`` would raise TypeError
        # on these. See ``_PRODUCER_ONLY_KWARGS`` docstring for the
        # rationale and source.
        for producer_kwarg in _PRODUCER_ONLY_KWARGS:
            merged.pop(producer_kwarg, None)

        return merged

    def assert_rehydration_timeout_ok(self) -> None:
        """Raise :class:`DurabilityConfigError` if ``rebalance_timeout_ms``
        is below the recommended floor for aggregator-wired workers.

        Rehydration reads the entire compacted state-topic partition on
        every reassignment. The worker must finish before
        ``rebalance_timeout_ms`` expires, otherwise the broker considers
        it dead and triggers another rebalance — leading to a
        rebalance storm on production-sized topics. The storm is
        operationally severe (group stuck rebalancing, no message
        progress, alert pages) and recovery requires raising the
        timeout and restarting workers, so we fail fast at startup
        rather than warn.

        aiokafka's default ``rebalance_timeout_ms`` is 30s; the
        recommended floor for aggregator workers is documented on
        :data:`REHYDRATE_REBALANCE_TIMEOUT_FLOOR_MS` (5 minutes).
        """
        merged = self.to_consumer_kwargs()
        # aiokafka's default when unset; mirrors aiokafka.AIOKafkaConsumer
        # session/rebalance timeouts at the time of writing (30s).
        aiokafka_default_ms = 30_000
        configured = merged.get("rebalance_timeout_ms", aiokafka_default_ms)
        if configured < REHYDRATE_REBALANCE_TIMEOUT_FLOOR_MS:
            raise DurabilityConfigError(
                f"rebalance_timeout_ms={configured} is below the recommended floor "
                f"of {REHYDRATE_REBALANCE_TIMEOUT_FLOOR_MS} for workers that own "
                "fan-out aggregator partitions. Rehydration may not complete before "
                "the broker considers the worker dead and triggers another rebalance "
                "(rebalance-storm risk). Raise rebalance_timeout_ms via "
                f"Client.connect(..., rebalance_timeout_ms={REHYDRATE_REBALANCE_TIMEOUT_FLOOR_MS}).",
                kwarg_name="rebalance_timeout_ms",
                offending_value=configured,
                expected_value=f">= {REHYDRATE_REBALANCE_TIMEOUT_FLOOR_MS}",
            )
