"""Tests for the public ``calfkit`` import surface.

The aggregator's safer default (``MergeErrorPolicy.FALLBACK_TO_DEFAULT``)
makes the ``HDR_DEGRADED_MERGE`` header the only signal of silent
degradation — so the constant must be importable from a stable public
path. Similarly, ``Client.connect`` raises ``DurabilityConfigError`` on
misconfiguration, so users wiring durability defenses need to import
the exception type without reaching into the private ``calfkit.exceptions``
module. These tests pin the public re-exports so accidental removal in
a future refactor fails CI rather than the user's import.
"""

from __future__ import annotations


def test_calfkit_root_exports_hdr_degraded_merge() -> None:
    """``HDR_DEGRADED_MERGE`` must be importable from the package root.

    Used by reply-side consumers to count degraded merges without
    importing the private ``calfkit._protocol`` module.
    """
    from calfkit import HDR_DEGRADED_MERGE

    assert HDR_DEGRADED_MERGE == "x-calf-degraded-merge", (
        f"HDR_DEGRADED_MERGE must match the wire-protocol value 'x-calf-degraded-merge'; got {HDR_DEGRADED_MERGE!r}"
    )


def test_calfkit_root_exports_hdr_fanout_id() -> None:
    """``HDR_FANOUT_ID`` must be importable from the package root."""
    from calfkit import HDR_FANOUT_ID

    assert HDR_FANOUT_ID == "x-calf-fanout-id", f"HDR_FANOUT_ID must match the wire-protocol value 'x-calf-fanout-id'; got {HDR_FANOUT_ID!r}"


def test_calfkit_root_exports_hdr_frame_id() -> None:
    """``HDR_FRAME_ID`` must be importable from the package root."""
    from calfkit import HDR_FRAME_ID

    assert HDR_FRAME_ID == "x-calf-frame-id", f"HDR_FRAME_ID must match the wire-protocol value 'x-calf-frame-id'; got {HDR_FRAME_ID!r}"


def test_calfkit_root_exports_durability_config_error() -> None:
    """``DurabilityConfigError`` must be importable from the package root
    AND inherit ``CalfkitError`` so a single ``except CalfkitError`` clause
    catches it alongside other SDK exceptions.
    """
    from calfkit import CalfkitError, DurabilityConfigError

    assert issubclass(DurabilityConfigError, CalfkitError), (
        f"DurabilityConfigError must inherit CalfkitError so the family-wide except clause catches it; got MRO {DurabilityConfigError.__mro__!r}"
    )


def test_calfkit_root_exports_match_exceptions_module() -> None:
    """The root re-export must be the same class object as the
    ``calfkit.exceptions`` definition — not a stale shadow.
    """
    import calfkit
    import calfkit.exceptions

    assert calfkit.DurabilityConfigError is calfkit.exceptions.DurabilityConfigError, (
        "calfkit.DurabilityConfigError must be the same class object as calfkit.exceptions.DurabilityConfigError"
    )
    assert calfkit.CalfkitError is calfkit.exceptions.CalfkitError, (
        "calfkit.CalfkitError must be the same class object as calfkit.exceptions.CalfkitError"
    )


def test_calfkit_client_exports_durability_config_error() -> None:
    """``DurabilityConfigError`` must be importable from ``calfkit.client``.

    The natural place users look when wiring ``Client.connect`` — the
    exception originates there and the import shouldn't require
    cross-module knowledge.
    """
    from calfkit.client import DurabilityConfigError as ClientDurabilityConfigError
    from calfkit.exceptions import DurabilityConfigError as RootDurabilityConfigError

    assert ClientDurabilityConfigError is RootDurabilityConfigError, (
        "calfkit.client.DurabilityConfigError must be the same class object as calfkit.exceptions.DurabilityConfigError"
    )


def test_calfkit_root_all_includes_new_exports() -> None:
    """``__all__`` must list the new exports so ``from calfkit import *``
    surfaces them and static-analysis tools (mypy/pyright with
    ``implicit_reexport=False``) accept the re-exports.
    """
    import calfkit

    required = {
        "HDR_DEGRADED_MERGE",
        "HDR_FANOUT_ID",
        "HDR_FRAME_ID",
        "DurabilityConfigError",
        "CalfkitError",
    }
    missing = required - set(calfkit.__all__)
    assert not missing, f"calfkit.__all__ is missing required public symbols: {missing}"
