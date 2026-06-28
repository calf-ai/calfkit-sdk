"""Pure-stdlib totality helpers shared by the lowest-level modules.

This module imports nothing from calfkit, so the two leaf modules that must stay import-cycle-free
can both use it: ``calfkit.exceptions`` imports ``calfkit.models.error_report`` (for ``ErrorReport``),
so ``error_report`` cannot import ``exceptions`` back — yet both need the same robust exception-to-
string helper on the fault path. Keep this module dependency-free.
"""

from __future__ import annotations


def safe_exc_message(exc: BaseException) -> str:
    """Best-effort string of an exception — FULLY total, robust against a broken ``__str__``,
    ``__repr__``, *and* a hostile metaclass ``__name__``.

    A bare ``str(exc)`` can itself raise (a broken ``__str__``, or args that don't coerce). On the
    fault path that would propagate out and re-open the silent-drop hole the rail closes (e.g. while
    synthesizing an ``ErrorReport`` from a tool's exception). Mirrors stdlib ``traceback._some_str``
    with a ``repr`` fallback — but, unlike stdlib, the final ``<unprintable {type(exc).__name__}>``
    is itself guarded: a hostile metaclass whose ``__name__`` raises must not break totality, since
    ``ErrorReport.from_exception`` calls this UNGUARDED at the rail chokepoint.
    """
    try:
        return str(exc)
    except Exception:
        pass
    try:
        return repr(exc)
    except Exception:
        pass
    try:
        return f"<unprintable {type(exc).__name__}>"
    except Exception:
        return "<unprintable>"
