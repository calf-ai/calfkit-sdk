"""Policy-seam chain runners (fault-rail spec §6.8 / §6.5).

Pure, module-level functions with **no transport imports** — the chain semantics
the staged pipeline drives its four seams through. ``run_chain`` is the generic
first-non-None runner used by every seam; ``run_chain_guarded`` is the
``on_node_error``-only variant whose deliberate raise-handling is specced in §6.5.
"""

from __future__ import annotations

import inspect
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from calfkit.exceptions import NodeFaultError, safe_exc_message
from calfkit.models.error_report import ErrorReport, FaultTypes

logger = logging.getLogger(__name__)

_MAX_SEAM_ERROR_CHARS = 2000

# The four policy seams (spec §6.1) — names double as the ``_chains`` registry keys and
# the instance decorator/constructor parameter names on ``BaseNodeDef``.
BEFORE_NODE = "before_node"
AFTER_NODE = "after_node"
ON_NODE_ERROR = "on_node_error"
ON_CALLEE_ERROR = "on_callee_error"
SEAM_NAMES: tuple[str, ...] = (BEFORE_NODE, AFTER_NODE, ON_NODE_ERROR, ON_CALLEE_ERROR)


@dataclass(frozen=True)
class _Minted:
    """A deliberate fault minted *inside* the ``on_node_error`` chain (spec §6.5/§6.8).

    Returned by :func:`run_chain_guarded` when a handler raises ``NodeFaultError``:
    the chain stops and the minted report (the original fault chained via ``causes``)
    is published verbatim — the mint rule must hold inside the error seam too. Distinct
    from a plain recovery value so the skeleton can ``isinstance``-discriminate it.
    """

    report: ErrorReport


async def run_chain(handlers: Sequence[Callable[..., Any]], *args: Any) -> Any:
    """Run a seam chain in registration order; the first non-``None`` return wins.

    Each handler is invoked with the seam-specific ``*args`` (``ctx``; or
    ``ctx, fault``; or ``ctx, output``) and may be sync or async. A handler
    returning ``None`` *declines* and the chain advances to the next; the first
    non-``None`` return is the result and short-circuits the remaining handlers
    (pluggy ``firstresult`` semantics, spec §6.8). An empty / all-declining
    chain returns ``None``.
    """
    for handler in handlers:
        result = handler(*args)
        if inspect.isawaitable(result):
            result = await result
        if result is not None:
            # §13: seam handling is logged at INFO (the handler that fired / why a delivery was shaped).
            logger.info("seam handler %s resolved (returned a value)", getattr(handler, "__name__", repr(handler)))
            return result
    return None


async def run_chain_guarded(handlers: Sequence[Callable[..., Any]], ctx: Any, report: ErrorReport) -> _Minted | Any | None:
    """Run the ``on_node_error`` chain with the §6.5 raise semantics.

    Like :func:`run_chain` (first non-None recovery value wins), but with the
    error-seam's deliberate raise handling: a recovery value resolves the node's
    failure; an *accidental* (non-``NodeFaultError``) raise is that handler
    *declining* — logged, the chain continues; ``None`` from every handler
    escalates the original ``report``.
    """
    for handler in handlers:
        try:
            result = handler(ctx, report)
            if inspect.isawaitable(result):
                result = await result
        except NodeFaultError as nfe:
            # §6.5 mint rule: a deliberate fault inside the error seam STOPS the chain
            # and converts verbatim, with the original fault chained via causes (the
            # `raise ... from` analog, §4.4). Returned as _Minted so the skeleton
            # publishes it instead of escalating the original. (Publish-time carriage
            # bounding is applied later, at _publish_fault.)
            minted = nfe.report.model_copy(update={"causes": [*nfe.report.causes, report]})
            return _Minted(minted)
        except Exception as exc:
            # §6.5: an accidental raise is that handler declining — never propagate
            # (that would skip the remaining handlers and re-open a swallow path);
            # log it and advance. ``CancelledError`` (BaseException) still propagates.
            handler_name = getattr(handler, "__name__", repr(handler))
            logger.exception(
                "on_node_error handler %s raised; treating as decline and continuing the chain",
                handler_name,
            )
            # Note the failure on the report (scenario 9). In-place mutation of the
            # frozen-shallow report's details dict is deliberate and safe HERE: the
            # report is mid-assembly, owned solely by this synchronous chain run, and
            # is the very object the skeleton escalates on all-declined — so the note
            # must land on it (model_copy would discard the note the skeleton publishes).
            report.details.setdefault(FaultTypes.SEAM_ERRORS, []).append(
                {
                    "handler": handler_name,
                    "exc_type": type(exc).__name__,
                    "exc_message": safe_exc_message(exc)[:_MAX_SEAM_ERROR_CHARS],
                }
            )
            continue
        if result is not None:
            # §13: a successful recovery is logged at INFO (the on_node_error handler that fired).
            logger.info("on_node_error handler %s recovered the node", getattr(handler, "__name__", repr(handler)))
            return result
    return None
