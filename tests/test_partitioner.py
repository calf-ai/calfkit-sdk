"""Pure-Python tests for FanOutAggregatorPartitioner + composite-key helpers.

The partitioner's job is to keep the state topic co-partitioned with the
agent's main topic (and the fanout-returns topic). A message keyed
``b"corr"`` and a message keyed ``b"corr|fanout"`` must land on the same
partition number, otherwise the aggregator's "owned partition" invariant
breaks.
"""

import pytest
from aiokafka.partitioner import murmur2

from calfkit.nodes.aggregator._partitioner import (
    FanOutAggregatorPartitioner,
    build_composite_key,
    has_composite_delimiter,
    parse_composite_key,
)


def test_partitioner_routes_plain_key_via_murmur2() -> None:
    partitioner = FanOutAggregatorPartitioner()
    key = b"correlation-abc"
    all_partitions = list(range(6))

    chosen = partitioner(key, all_partitions, all_partitions)
    expected = all_partitions[(murmur2(key) & 0x7FFFFFFF) % 6]

    assert chosen == expected


def test_partitioner_extracts_correlation_id_from_composite_key() -> None:
    """A composite key and its correlation_id prefix MUST hash identically.

    This is the central invariant: state-topic writes (keyed
    ``corr|fanout``) co-partition with main + returns writes (keyed
    ``corr``).
    """
    partitioner = FanOutAggregatorPartitioner()
    all_partitions = list(range(6))

    plain = partitioner(b"abc", all_partitions, all_partitions)
    composite = partitioner(b"abc|fan_42", all_partitions, all_partitions)

    assert plain == composite


def test_partitioner_co_partitioning_across_partition_counts() -> None:
    """The invariant holds for any partition count, not just 6."""
    partitioner = FanOutAggregatorPartitioner()
    for partition_count in (1, 2, 3, 6, 12, 24):
        all_partitions = list(range(partition_count))
        plain = partitioner(b"xyz", all_partitions, all_partitions)
        composite = partitioner(b"xyz|some_fan", all_partitions, all_partitions)
        assert plain == composite, f"divergence at partition_count={partition_count}"


def test_partitioner_handles_none_key() -> None:
    """Null keys fall back to a random available partition."""
    partitioner = FanOutAggregatorPartitioner()
    all_partitions = [0, 1, 2]

    chosen = partitioner(None, all_partitions, all_partitions)

    assert chosen in all_partitions


def test_partitioner_handles_none_key_with_no_available() -> None:
    """Null key + empty available set falls back to all_partitions."""
    partitioner = FanOutAggregatorPartitioner()
    all_partitions = [0, 1, 2]

    chosen = partitioner(None, all_partitions, [])

    assert chosen in all_partitions


def test_build_composite_key_roundtrip() -> None:
    key = build_composite_key("corr-xyz", "fan-out-1")
    assert key == b"corr-xyz|fan-out-1"

    corr, fanout = parse_composite_key(key)
    assert corr == "corr-xyz"
    assert fanout == "fan-out-1"


def test_build_composite_key_rejects_pipe_in_correlation_id() -> None:
    with pytest.raises(ValueError, match="contains delimiter"):
        build_composite_key("corr|with-pipe", "fan-out")


def test_parse_composite_key_rejects_non_composite() -> None:
    with pytest.raises(ValueError, match="does not contain composite delimiter"):
        parse_composite_key(b"no-pipe-here")


def test_parse_composite_key_preserves_pipes_in_fan_out_id() -> None:
    """Splits on the FIRST pipe — subsequent pipes are part of fan_out_id."""
    key = b"corr|fanout|with|extras"
    corr, fanout = parse_composite_key(key)
    assert corr == "corr"
    assert fanout == "fanout|with|extras"


def test_has_composite_delimiter() -> None:
    assert has_composite_delimiter("abc|def")
    assert has_composite_delimiter(b"abc|def")
    assert not has_composite_delimiter("abc-def")
    assert not has_composite_delimiter(b"abc-def")
