"""Idempotency-key dedup cache — the v1 hot fix.

Tracked for generalization to a BaseNodeDef-level abstraction in
[#161](https://github.com/calf-ai/calfkit-sdk/issues/161). When that lands,
:class:`McpBridge` will migrate from importing this module to consuming
the generic mechanism with no functional change.

**Why this exists.** Kafka delivers at-least-once. A bridge worker that
crashes between successfully calling MCP and committing the offset will,
on restart (or on another consumer in the group), re-receive the same
envelope. Without dedup, a non-idempotent tool (``gmail.send``,
``delete_repo``, ``charge_card``) executes twice.

**What this fixes.** A worker-local in-process LRU cache keyed on
``(tool_call_id, args_hash)``. On a cache hit, we return the cached
``ToolReturn`` instead of re-dispatching to MCP.

**What this does NOT fix** (intentional v1 limitations):

- **Cross-process dedup.** If the worker crashes hard, the cache is lost.
  A redelivery to a different process replica has no cache to consult.
  v2 + #161 may add a Redis/Kafka-backed cache.
- **Tool-error / transport-error dedup.** We cache successful
  ``ToolReturn`` only — not ``RetryPromptPart`` (LLM-visible error,
  should be re-surfaced if the LLM retries) and not ``FailedToolCall``
  (transport failure, should retry naturally).
- **Idempotent tools.** Tools annotated ``idempotentHint=True`` skip the
  cache entirely. Re-running them is safe by definition and we save
  memory by not tracking them.

**Cache parameters** (v1 plan §11 Q2):

- TTL: 1 hour. Long enough to absorb most worker restart windows;
  short enough that stale entries don't accumulate.
- Max entries: 10,000. ~10KB per entry typical → ~100MB upper bound.
- Per-process (not shared across replicas).
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


# Default cache parameters per v1 plan §11 Q2.
DEFAULT_TTL_SECONDS: float = 3600.0  # 1 hour
DEFAULT_MAX_ENTRIES: int = 10_000


# ---------------------------------------------------------------------------
# Cache key construction
# ---------------------------------------------------------------------------


def _hash_args(args: dict[str, Any] | None) -> str:
    """Deterministic SHA-256 of args for cache key derivation.

    Uses ``sort_keys=True`` so dict ordering doesn't affect the hash —
    the LLM might emit ``{"a": 1, "b": 2}`` and ``{"b": 2, "a": 1}`` and
    we should treat those as the same call.

    Empty dict / ``None`` both hash to the same canonical empty-dict hash.
    """
    if args is None:
        args = {}
    canonical = json.dumps(args, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def make_cache_key(tool_call_id: str, args: dict[str, Any] | None) -> tuple[str, str]:
    """Construct the ``(tool_call_id, args_hash)`` cache key.

    Both fields are included for defense-in-depth: the ``tool_call_id``
    is the LLM-emitted correlation key (sufficient for Kafka redelivery
    of the same envelope); the ``args_hash`` defends against the unusual
    case where two semantically different calls accidentally share an
    ID (bug or hash collision).
    """
    return (tool_call_id, _hash_args(args))


# ---------------------------------------------------------------------------
# IdempotencyCache — LRU + TTL
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _CacheEntry:
    """One cache slot: the cached value + the wall-clock expiry timestamp."""

    value: Any
    expires_at: float  # time.monotonic() seconds


class IdempotencyCache:
    """In-process LRU cache with TTL eviction.

    Not thread-safe (asyncio is single-threaded; calfkit doesn't currently
    run handlers across threads). If that changes, wrap mutations with a
    threading.Lock.

    Eviction strategy:

    - On every ``get``: expired entries discovered are evicted lazily.
    - On every ``put``: if over capacity, evict the least-recently-used
      entry (OrderedDict.popitem(last=False)).
    """

    def __init__(
        self,
        *,
        ttl_seconds: float = DEFAULT_TTL_SECONDS,
        max_entries: int = DEFAULT_MAX_ENTRIES,
        clock: Any = time.monotonic,
    ) -> None:
        if ttl_seconds <= 0:
            raise ValueError(f"ttl_seconds must be > 0, got {ttl_seconds}")
        if max_entries <= 0:
            raise ValueError(f"max_entries must be > 0, got {max_entries}")
        self._ttl = ttl_seconds
        self._max_entries = max_entries
        self._clock = clock
        # OrderedDict for LRU: move-to-end on touch; popitem(last=False) evicts oldest.
        self._entries: OrderedDict[tuple[str, str], _CacheEntry] = OrderedDict()

    def get(self, key: tuple[str, str]) -> Any | None:
        """Return the cached value, or ``None`` if not present / expired."""
        entry = self._entries.get(key)
        if entry is None:
            return None
        if entry.expires_at <= self._clock():
            # Expired; evict lazily and treat as miss.
            del self._entries[key]
            return None
        # LRU touch: move to end (most-recently-used).
        self._entries.move_to_end(key)
        return entry.value

    def put(self, key: tuple[str, str], value: Any) -> None:
        """Insert / replace a cache entry. Evicts LRU when over capacity."""
        entry = _CacheEntry(value=value, expires_at=self._clock() + self._ttl)
        # If the key exists, move it to the end (refresh LRU position).
        if key in self._entries:
            self._entries.move_to_end(key)
        self._entries[key] = entry
        # Evict oldest if over capacity.
        while len(self._entries) > self._max_entries:
            evicted_key, _ = self._entries.popitem(last=False)
            logger.debug("IdempotencyCache LRU-evicted key %s; size now %d", evicted_key, len(self._entries))

    def __contains__(self, key: tuple[str, str]) -> bool:
        """Cache hit/miss check that does NOT touch LRU order or evict.

        Useful for tests and metrics that want to inspect cache state
        without side effects.
        """
        entry = self._entries.get(key)
        return entry is not None and entry.expires_at > self._clock()

    def __len__(self) -> int:
        """Number of entries (including possibly-expired ones)."""
        return len(self._entries)

    def clear(self) -> None:
        """Wipe the cache. Used by tests and (potentially) operator tooling."""
        self._entries.clear()

    @property
    def ttl_seconds(self) -> float:
        return self._ttl

    @property
    def max_entries(self) -> int:
        return self._max_entries
