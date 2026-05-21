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
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from calfkit.exceptions import DurabilityConfigError, _safe_repr

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


# aiokafka kwargs accepted by ``AIOKafkaProducer.__init__`` but rejected
# by ``AIOKafkaConsumer.__init__`` with ``TypeError: __init__() got an
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
# Derived empirically from
# ``set(AIOKafkaProducer.__init__ kwargs) - set(AIOKafkaConsumer.__init__ kwargs)``
# against aiokafka 0.13.0. ``partitioner`` is also producer-only but is
# rejected upstream in ``Client.connect`` so it is omitted here.
# Update this list (and the pinned version) when bumping aiokafka.
# A meta-test in ``tests/test_kafka_config.py`` introspects the live
# signatures and will fail if this list ever drifts from reality.
_PRODUCER_ONLY_KWARGS: frozenset[str] = frozenset(
    {
        "acks",
        "compression_type",
        "enable_idempotence",
        "key_serializer",
        "linger_ms",
        "max_batch_size",
        "max_request_size",
        "transaction_timeout_ms",
        "transactional_id",
        "value_serializer",
    }
)


# Tuple of typed-field names on :class:`KafkaConfig`. Used by
# ``__post_init__`` to detect ``client_kwargs`` entries that collide
# with typed fields — a collision with different values is a
# misconfiguration (the same kwarg set twice with conflicting values).
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
    """Snapshot of broker connection settings captured at
    ``Client.connect`` time.

    Carries the typed Kafka kwargs forward so any consumer needing to
    construct a transient :class:`AIOKafkaConsumer` with the same
    auth/transport credentials gets a single source of truth.

    The common Kafka client options are surfaced as typed fields for
    discoverability and static-type checking. ``client_kwargs`` is the
    escape hatch for kwargs not covered by typed fields (e.g.
    ``rebalance_timeout_ms``, ``session_timeout_ms``). A key that
    overlaps a typed-field name AND is also set as the typed field with
    a DIFFERENT value raises :class:`DurabilityConfigError` at
    construction — there is no precedence rule for conflicting values.
    If the typed field is unset (``None``), the same key in
    ``client_kwargs`` is treated as escape-hatch usage; if both are set
    and equal, the duplicate is silently stripped (see
    "``dataclasses.replace`` support" below).

    ``client_kwargs`` is defensively copied and wrapped in
    :class:`types.MappingProxyType` at construction time so the captured
    snapshot cannot be mutated after the fact (the dataclass is frozen,
    but a plain ``dict`` would still be in-place-mutable).

    ``dataclasses.replace`` support
    -------------------------------
    Because ``client_kwargs`` from the original instance is carried
    forward by ``dataclasses.replace`` (the user can't simultaneously
    rewrite it through the replace kwargs interface), the typed-field /
    ``client_kwargs`` collision check is relaxed to allow a no-op
    collision: when the new typed-field value equals the value already
    present in ``client_kwargs``, the duplicate is silently stripped
    from ``client_kwargs`` instead of raising. Different values still
    raise — those are genuine misconfigurations.
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
    #
    # Annotated as ``Mapping`` (read-only, covariant) rather than
    # ``dict`` because the runtime value after ``__post_init__`` is a
    # ``MappingProxyType``; the dict annotation lied to mypy and forced
    # ``# type: ignore[index]`` at every test-side mutation attempt,
    # masking the very real-world bugs the runtime guard exists to
    # catch. ``default_factory=dict`` still works — ``Mapping`` is the
    # supertype of ``dict``.
    client_kwargs: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # Check typed-field / client_kwargs collisions, then wrap the
        # sanitized dict in an immutable view. Three cases:
        #   1. typed unset, kwargs set: legacy escape-hatch usage.
        #   2. Both set, values equal: silently strip the duplicate.
        #   3. Both set, values DIFFER: raise — genuine misconfig.
        # See per-case ``# Case N:`` inline comments below for rationale.
        sanitized: dict[str, Any] = dict(self.client_kwargs)
        for name in _KAFKA_CONFIG_TYPED_FIELDS:
            if name not in sanitized:
                continue
            typed_value = getattr(self, name)
            kwargs_value = sanitized[name]
            if typed_value is None:
                # Case 1: escape-hatch usage for a kwarg that also has a
                # typed slot. Legacy / pre-typed-field shape; must keep
                # working or existing users break. Leave it alone.
                continue
            if typed_value == kwargs_value:
                # Case 2: no-op collision. Carve-out for
                # ``dataclasses.replace(cfg, X=value)`` where the
                # original cfg's client_kwargs carries the same X value
                # forward. Merged consumer kwargs are identical either
                # way, so silently strip the duplicate.
                del sanitized[name]
                continue
            # Case 3: conflicting values — operator set the same kwarg
            # twice with different values. Raise so the failure surfaces
            # at the user's call site rather than during rehydration.
            raise DurabilityConfigError(
                f"KafkaConfig: {name!r} is set both as a typed field "
                f"({_safe_repr(typed_value)}) and in client_kwargs "
                f"({_safe_repr(kwargs_value)}); remove it from "
                "client_kwargs and use the typed field.",
                kwarg_name=name,
                offending_value=kwargs_value,
                expected_value=typed_value,
            )

        # Defensive copy + immutable view so the snapshot cannot be
        # tampered with after construction. The dataclass is frozen,
        # but a plain dict would still expose ``client_kwargs["k"] = v``
        # mutation. ``object.__setattr__`` is needed because
        # ``frozen=True`` blocks normal assignment.
        object.__setattr__(self, "client_kwargs", MappingProxyType(sanitized))

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
        # aiokafka 0.13.0 default for ``rebalance_timeout_ms``
        # (30000 ms). The meta-test
        # ``tests/test_kafka_config.py::test_producer_only_kwargs_list_matches_aiokafka_introspection``
        # does NOT pin this constant — bump alongside aiokafka upgrades.
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
