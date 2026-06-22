"""Module whose top-level (import side-effect) code raises.

Used by the ``ck topics provision`` CLI tests to assert that an import
*side-effect* failure is reported distinctly from a missing-module
``ImportError`` (both still exit 2).
"""

from __future__ import annotations

raise RuntimeError("side-effect boom at import time")
