"""Pure route grammar + matching helpers (`calfkit._routing`)."""

import pytest

from calfkit._routing import match_chain, route_matches, validate_route_pattern


@pytest.mark.parametrize(
    "pattern",
    ["order", "order.created", "order.created.line", "order.*", "a.b.c.*", "*"],
)
def test_valid_route_patterns_accepted(pattern: str) -> None:
    validate_route_pattern(pattern)  # must not raise


@pytest.mark.parametrize(
    "pattern",
    [
        "",  # empty
        "order.",  # trailing dot
        ".order",  # leading dot
        "order..created",  # empty segment
        "order.*.line",  # wildcard mid-pattern
        "ord*er",  # partial-segment wildcard
        "**",  # double wildcard
        "a.**",  # double wildcard segment
        "order*",  # wildcard fused to a segment
    ],
)
def test_invalid_route_patterns_rejected(pattern: str) -> None:
    with pytest.raises(ValueError):
        validate_route_pattern(pattern)


@pytest.mark.parametrize(
    "pattern,key,expected",
    [
        ("order.created", "order.created", True),  # exact hit
        ("order.created", "order.shipped", False),  # exact miss
        ("*", "order", True),  # universal matches bare
        ("*", "a.b.c", True),  # universal matches deep
        ("order.*", "order.created", True),  # prefix one below
        ("order.*", "order.created.line", True),  # prefix any depth below
        ("order.*", "order", False),  # NOT the bare prefix
        ("order.*", "orders.created", False),  # segment-safe
        ("order.created.*", "order.created", False),  # needs strictly more segments
    ],
)
def test_route_matches(pattern: str, key: str, expected: bool) -> None:
    assert route_matches(pattern, key) is expected


def test_match_chain_orders_specific_to_general() -> None:
    patterns = ["*", "order.*", "order.created.*", "order.created.line", "payment.*"]
    assert match_chain("order.created.line", patterns) == [
        "order.created.line",
        "order.created.*",
        "order.*",
        "*",
    ]


def test_match_chain_excludes_non_matching_and_bare_prefix() -> None:
    assert match_chain("order", ["order", "order.*", "*"]) == ["order", "*"]
