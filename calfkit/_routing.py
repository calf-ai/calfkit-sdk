"""Pure route-pattern grammar and matching helpers (no calfkit.* deps).

A *route pattern* (the string in ``@handler(...)``) is ``.``-delimited with
non-empty segments. ``*`` is legal only as the entire final segment (a
trailing-prefix wildcard, e.g. ``order.*``) or as the lone universal ``*``.
See the header-route-dispatch spec, §6.
"""

from __future__ import annotations

from collections.abc import Iterable


def validate_route_pattern(pattern: str) -> None:
    """Raise ``ValueError`` if ``pattern`` is not a valid route pattern (§6.1).

    Valid: an exact key (``order.created``), a single trailing-``*`` prefix
    (``order.*``), or the lone universal ``*``. Rejected: empty pattern, empty
    segment (``a..b``), leading/trailing ``.``, ``*`` anywhere but the entire
    final segment (``a.*.b``, ``ord*er``, ``**``).
    """
    if not pattern:
        raise ValueError("route pattern must be non-empty")
    segments = pattern.split(".")
    last = len(segments) - 1
    for i, seg in enumerate(segments):
        if not seg:
            raise ValueError(f"route pattern {pattern!r} has an empty segment")
        if "*" in seg and (seg != "*" or i != last):
            raise ValueError(f"route pattern {pattern!r}: '*' is only valid as the entire final segment")


def is_concrete_route_key(key: str | None) -> bool:
    """Whether ``key`` is a valid *concrete* route key — a producer-set value or an
    inbound header (not a pattern): non-empty, no ``*``, ``.``-delimited with
    non-empty segments. Malformed keys (``order.``, ``a..b``) and an absent route
    (``None``, the header-less case) are rejected so they can never partial-match a
    prefix pattern (§6 / §10)."""
    return key is not None and "*" not in key and all(key.split("."))


def route_matches(pattern: str, key: str | None) -> bool:
    """Whether a (validated) route ``pattern`` matches a concrete route ``key`` (§6.2).

    ``*`` matches any key (including ``None`` — the header-less case); an exact
    pattern matches an equal key; a trailing ``prefix.*`` matches keys strictly
    *below* ``prefix`` (segment-aware), so ``order.*`` matches ``order.created`` but
    not bare ``order`` nor ``orders.created``.
    """
    if key is None or not is_concrete_route_key(key):
        # A None (header-less) or malformed inbound key never partial-matches a
        # specific pattern; only the universal "*" can catch it. (Handling this
        # first keeps the function order-independent and narrows ``key`` to ``str``.)
        return pattern == "*"
    if pattern == "*":
        return True
    if pattern.endswith(".*"):
        prefix = pattern[:-2].split(".")
        segs = key.split(".")
        return len(segs) > len(prefix) and segs[: len(prefix)] == prefix
    return pattern == key


def _specificity(pattern: str) -> tuple[int, int]:
    """Sort key (descending) for specific→general ordering: more fixed (non-``*``)
    segments first, then exact before prefix at an equal fixed count."""
    segments = pattern.split(".")
    fixed = sum(1 for s in segments if s != "*")
    return (fixed, int("*" not in pattern))


def match_chain(key: str | None, patterns: Iterable[str]) -> list[str]:
    """The patterns matching ``key``, ordered most-specific → most-general (§6.3).

    For trailing-prefix patterns the matches are nested prefixes of ``key`` and so
    order without ties.
    """
    matched = [p for p in patterns if route_matches(p, key)]
    matched.sort(key=_specificity, reverse=True)
    return matched
