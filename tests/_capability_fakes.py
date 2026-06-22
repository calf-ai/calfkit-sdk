"""Shared test fakes for the capability view.

``_FakeView`` is a dict-backed :class:`~calfkit.models.capability.EnumerableCapabilityView`:
``get()`` is inherited from ``dict`` (the :class:`CapabilityLookup` surface) and
``snapshot()`` adds the bulk enumeration the discover-mode kernel needs. Named-path
selector tests keep using bare ``dict``s (their resolution calls only ``get()``); only
the discover path needs ``snapshot()``.
"""

from __future__ import annotations

from calfkit.models.capability import CapabilityRecord


class _FakeView(dict[str, CapabilityRecord]):
    def snapshot(self) -> dict[str, CapabilityRecord]:
        return dict(self)
