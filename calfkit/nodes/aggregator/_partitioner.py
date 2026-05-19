"""Custom partitioner for co-partitioning across fan-out topics.

The state topic uses composite keys ``f"{correlation_id}|{fan_out_id}"`` so
each fan-out batch gets its own compacted record. To keep state co-partitioned
with the agent's main topic and the fan-out-returns topic — both keyed by
``correlation_id`` alone — the partitioner extracts ``correlation_id`` from
composite keys before hashing.

Configured broker-wide via ``KafkaBroker(partitioner=FanOutAggregatorPartitioner())``
because aiokafka's producer partitioner is a broker-level setting, not
per-publisher.
"""

from __future__ import annotations

import random

from aiokafka.partitioner import murmur2

_COMPOSITE_KEY_DELIMITER = b"|"
_COMPOSITE_KEY_DELIMITER_STR = "|"


class FanOutAggregatorPartitioner:
    """aiokafka-compatible partitioner that extracts ``correlation_id`` from
    composite keys before hashing.

    Composite-key format: ``f"{correlation_id}|{fan_out_id}".encode()``. The
    partitioner splits on the first ``b"|"``, hashes the prefix via Kafka's
    standard murmur2, and routes the message to ``partitions[hash % len]``.
    For non-composite keys (e.g., the main topic's plain ``correlation_id``)
    it hashes the full key — exactly the standard Kafka default behaviour.

    Net effect: a message keyed ``"abc"`` and a message keyed ``"abc|fan_42"``
    land on the same partition number across all topics — the invariant the
    aggregator's state store and returns subscriber depend on.

    For ``None`` keys, falls back to the standard "random available
    partition" behaviour matching :class:`aiokafka.partitioner.DefaultPartitioner`.
    """

    def __call__(
        self,
        key: bytes | None,
        all_partitions: list[int],
        available: list[int],
    ) -> int:
        if key is None:
            if available:
                return random.choice(available)
            return random.choice(all_partitions)

        partition_key = self._extract_partition_key(key)
        idx = murmur2(partition_key)
        idx &= 0x7FFFFFFF
        idx %= len(all_partitions)
        return all_partitions[idx]

    @staticmethod
    def _extract_partition_key(key: bytes) -> bytes:
        """Return the correlation_id prefix from a composite key, or the
        original key if no delimiter is present."""
        delim_idx = key.find(_COMPOSITE_KEY_DELIMITER)
        if delim_idx < 0:
            return key
        return key[:delim_idx]


def build_composite_key(correlation_id: str, fan_out_id: str) -> bytes:
    """Build the state-topic key for a ``(correlation_id, fan_out_id)`` pair.

    The ``correlation_id`` must not contain ``"|"`` (the delimiter), or the
    partitioner cannot extract it correctly. The framework rejects messages
    with such ``correlation_id``\\ s at the client boundary; this function
    raises :class:`ValueError` if the constraint is violated to catch
    framework-internal mistakes.
    """
    if _COMPOSITE_KEY_DELIMITER_STR in correlation_id:
        raise ValueError(
            f"correlation_id contains delimiter '|' which conflicts with the "
            f"fan-out state-topic key format: {correlation_id!r}"
        )
    return f"{correlation_id}{_COMPOSITE_KEY_DELIMITER_STR}{fan_out_id}".encode()


def parse_composite_key(key: bytes) -> tuple[str, str]:
    """Inverse of :func:`build_composite_key`. Returns ``(correlation_id, fan_out_id)``.

    Splits on the FIRST ``b"|"`` (since ``correlation_id`` is guaranteed
    delimiter-free; any subsequent ``"|"`` is part of ``fan_out_id``).

    Raises:
        ValueError: if ``key`` does not contain ``b"|"``.
    """
    delim_idx = key.find(_COMPOSITE_KEY_DELIMITER)
    if delim_idx < 0:
        raise ValueError(f"key does not contain composite delimiter: {key!r}")
    return key[:delim_idx].decode(), key[delim_idx + 1 :].decode()


def has_composite_delimiter(value: str | bytes) -> bool:
    """True if ``value`` contains the composite-key delimiter ``"|"``.

    Used at the client boundary to validate ``correlation_id`` before any
    aggregator-using node sees it.
    """
    if isinstance(value, bytes):
        return _COMPOSITE_KEY_DELIMITER in value
    return _COMPOSITE_KEY_DELIMITER_STR in value


# Re-export Iterable so this module can be imported without pulling typing in
# downstream tests; otherwise unused.
__all__ = [
    "FanOutAggregatorPartitioner",
    "build_composite_key",
    "has_composite_delimiter",
    "parse_composite_key",
]
