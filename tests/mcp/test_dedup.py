"""Unit tests for ``calfkit.mcp._dedup``.

Coverage focus:
- ``_hash_args`` is deterministic across dict key ordering
- ``make_cache_key`` composes ``(tool_call_id, args_hash)``
- IdempotencyCache hit/miss semantics
- TTL expiry (with a controllable clock)
- LRU eviction at capacity
- ``__contains__`` is side-effect-free
- Constructor validation rejects invalid parameters
"""

from __future__ import annotations

import pytest

from calfkit.mcp._dedup import (
    DEFAULT_MAX_ENTRIES,
    DEFAULT_TTL_SECONDS,
    IdempotencyCache,
    _hash_args,
    make_cache_key,
)

# ---------------------------------------------------------------------------
# _hash_args
# ---------------------------------------------------------------------------


def test_hash_args_deterministic() -> None:
    """Same dict (across orderings) hashes the same."""
    h1 = _hash_args({"a": 1, "b": 2})
    h2 = _hash_args({"b": 2, "a": 1})
    assert h1 == h2


def test_hash_args_different_for_different_values() -> None:
    assert _hash_args({"x": 1}) != _hash_args({"x": 2})


def test_hash_args_different_for_different_keys() -> None:
    assert _hash_args({"a": 1}) != _hash_args({"b": 1})


def test_hash_args_none_equals_empty_dict() -> None:
    """None args and empty dict are semantically the same call."""
    assert _hash_args(None) == _hash_args({})


def test_hash_args_handles_nested_structures() -> None:
    h = _hash_args({"list": [1, {"k": "v"}], "n": 42})
    assert isinstance(h, str)
    assert len(h) == 64  # SHA-256 hex digest


def test_hash_args_consistent_for_nested_dict_ordering() -> None:
    """Sort applies recursively via json.dumps(sort_keys=True)."""
    h1 = _hash_args({"outer": {"a": 1, "b": 2}})
    h2 = _hash_args({"outer": {"b": 2, "a": 1}})
    assert h1 == h2


# ---------------------------------------------------------------------------
# make_cache_key
# ---------------------------------------------------------------------------


def test_make_cache_key_composes_id_and_hash() -> None:
    key = make_cache_key("tc-123", {"x": 1})
    assert key[0] == "tc-123"
    assert key[1] == _hash_args({"x": 1})


def test_make_cache_key_different_for_different_ids() -> None:
    k1 = make_cache_key("tc-1", {})
    k2 = make_cache_key("tc-2", {})
    assert k1 != k2


# ---------------------------------------------------------------------------
# IdempotencyCache — basic get/put
# ---------------------------------------------------------------------------


def test_cache_miss_returns_none() -> None:
    c = IdempotencyCache()
    assert c.get(("tc", "h")) is None


def test_cache_put_then_get_returns_value() -> None:
    c = IdempotencyCache()
    c.put(("tc", "h"), "stored-value")
    assert c.get(("tc", "h")) == "stored-value"


def test_cache_put_replaces_existing() -> None:
    c = IdempotencyCache()
    c.put(("tc", "h"), "first")
    c.put(("tc", "h"), "second")
    assert c.get(("tc", "h")) == "second"


def test_cache_len_tracks_entries() -> None:
    c = IdempotencyCache()
    assert len(c) == 0
    c.put(("a", "h"), "v")
    assert len(c) == 1
    c.put(("b", "h"), "v")
    assert len(c) == 2


def test_cache_clear() -> None:
    c = IdempotencyCache()
    c.put(("a", "h"), "v")
    c.put(("b", "h"), "v")
    c.clear()
    assert len(c) == 0
    assert c.get(("a", "h")) is None


def test_cache_defaults() -> None:
    c = IdempotencyCache()
    assert c.ttl_seconds == DEFAULT_TTL_SECONDS
    assert c.max_entries == DEFAULT_MAX_ENTRIES


# ---------------------------------------------------------------------------
# IdempotencyCache — TTL expiry (with controllable clock)
# ---------------------------------------------------------------------------


class _FakeClock:
    """Mutable monotonic clock for deterministic TTL tests."""

    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_cache_get_within_ttl_returns_value() -> None:
    clock = _FakeClock(start=100.0)
    c = IdempotencyCache(ttl_seconds=10.0, clock=clock)
    c.put(("tc", "h"), "v")
    clock.advance(5.0)
    assert c.get(("tc", "h")) == "v"


def test_cache_get_after_ttl_returns_none() -> None:
    clock = _FakeClock(start=100.0)
    c = IdempotencyCache(ttl_seconds=10.0, clock=clock)
    c.put(("tc", "h"), "v")
    clock.advance(11.0)
    assert c.get(("tc", "h")) is None


def test_cache_get_lazy_evicts_expired() -> None:
    """An expired entry is removed from the cache on get()."""
    clock = _FakeClock(start=100.0)
    c = IdempotencyCache(ttl_seconds=10.0, clock=clock)
    c.put(("tc", "h"), "v")
    assert len(c) == 1
    clock.advance(11.0)
    c.get(("tc", "h"))  # triggers lazy eviction
    assert len(c) == 0


def test_cache_put_refreshes_ttl() -> None:
    """Putting the same key resets its TTL window."""
    clock = _FakeClock(start=100.0)
    c = IdempotencyCache(ttl_seconds=10.0, clock=clock)
    c.put(("tc", "h"), "v1")
    clock.advance(5.0)
    c.put(("tc", "h"), "v2")  # refresh
    clock.advance(8.0)  # 8s after refresh — within TTL
    assert c.get(("tc", "h")) == "v2"


# ---------------------------------------------------------------------------
# IdempotencyCache — LRU eviction
# ---------------------------------------------------------------------------


def test_cache_lru_evicts_oldest_when_over_capacity() -> None:
    c = IdempotencyCache(max_entries=3)
    c.put(("a", "h"), "1")
    c.put(("b", "h"), "2")
    c.put(("c", "h"), "3")
    c.put(("d", "h"), "4")  # should evict "a"
    assert c.get(("a", "h")) is None
    assert c.get(("b", "h")) == "2"
    assert c.get(("c", "h")) == "3"
    assert c.get(("d", "h")) == "4"
    assert len(c) == 3


def test_cache_get_touches_lru_position() -> None:
    """Accessing a key moves it to most-recently-used; it survives the next eviction."""
    c = IdempotencyCache(max_entries=3)
    c.put(("a", "h"), "1")
    c.put(("b", "h"), "2")
    c.put(("c", "h"), "3")
    # Touch "a" — now LRU order is b, c, a
    c.get(("a", "h"))
    c.put(("d", "h"), "4")  # should evict "b" (the new oldest)
    assert c.get(("a", "h")) == "1"  # survives
    assert c.get(("b", "h")) is None  # evicted
    assert c.get(("c", "h")) == "3"


def test_cache_put_existing_key_touches_lru_position() -> None:
    """Re-putting an existing key promotes it to MRU."""
    c = IdempotencyCache(max_entries=3)
    c.put(("a", "h"), "1")
    c.put(("b", "h"), "2")
    c.put(("c", "h"), "3")
    c.put(("a", "h"), "1-new")  # re-put — should be MRU now
    c.put(("d", "h"), "4")  # evicts "b" (oldest), not "a"
    assert c.get(("a", "h")) == "1-new"
    assert c.get(("b", "h")) is None


# ---------------------------------------------------------------------------
# IdempotencyCache — __contains__ is side-effect-free
# ---------------------------------------------------------------------------


def test_contains_returns_true_for_live_entry() -> None:
    c = IdempotencyCache()
    c.put(("tc", "h"), "v")
    assert ("tc", "h") in c


def test_contains_returns_false_for_missing_entry() -> None:
    c = IdempotencyCache()
    assert ("tc", "h") not in c


def test_contains_returns_false_for_expired_entry() -> None:
    clock = _FakeClock(start=100.0)
    c = IdempotencyCache(ttl_seconds=10.0, clock=clock)
    c.put(("tc", "h"), "v")
    clock.advance(11.0)
    assert ("tc", "h") not in c


def test_contains_does_not_evict_or_touch_lru() -> None:
    """``__contains__`` must NOT mutate the cache (eviction or LRU touch).

    Used by tests/metrics that need to inspect cache state.
    """
    c = IdempotencyCache(max_entries=3)
    c.put(("a", "h"), "1")
    c.put(("b", "h"), "2")
    c.put(("c", "h"), "3")
    # Repeatedly check "a" — if it touched LRU, "a" would survive next eviction
    for _ in range(10):
        _ = ("a", "h") in c
    c.put(("d", "h"), "4")  # evicts the OLDEST, which is still "a"
    assert ("a", "h") not in c
    assert ("b", "h") in c


# ---------------------------------------------------------------------------
# Constructor validation
# ---------------------------------------------------------------------------


def test_constructor_rejects_zero_ttl() -> None:
    with pytest.raises(ValueError, match="ttl_seconds"):
        IdempotencyCache(ttl_seconds=0)


def test_constructor_rejects_negative_ttl() -> None:
    with pytest.raises(ValueError, match="ttl_seconds"):
        IdempotencyCache(ttl_seconds=-1.0)


def test_constructor_rejects_zero_max_entries() -> None:
    with pytest.raises(ValueError, match="max_entries"):
        IdempotencyCache(max_entries=0)


def test_constructor_rejects_negative_max_entries() -> None:
    with pytest.raises(ValueError, match="max_entries"):
        IdempotencyCache(max_entries=-5)
