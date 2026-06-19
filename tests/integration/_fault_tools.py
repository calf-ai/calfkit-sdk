"""Example fault-emitting tool nodes + a recorder seam for the real-broker fault tests.

A tool node's topics are derived from its function name (``tool.<name>.input`` /
``tool.<name>.output``, via ``@agent_tool``), so the names here are module-global
addresses — keep them unique and descriptive. Each tool exercises one entry point of
the rail:

- :func:`boom` — a generic uncaught exception → the chokepoint synthesizes
  ``calf.unhandled`` (catalogue FR-2), carrying the exception class in
  ``details["calf.exception_type"]``.
- :func:`quota` — a deliberate ``NodeFaultError`` minted in the tool body → carried
  **verbatim** (the mint rule bypasses ``on_node_error``; FR-3/FR-4).
- :func:`ok_a` / :func:`ok_b` — fault-free siblings, for mixed fan-out batches.

:class:`CalleeErrorRecorder` is the Channel-B observation seam: an ``on_callee_error``
handler that captures the live ``SeamContext`` / ``ErrorReport`` and then **declines**
(returns ``None``) so the fault still escalates. It records rather than asserts so a
failed assertion in the test body — not a raise inside the seam (which would itself
become a node-own fault) — reports the failure.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from calfkit._vendor.pydantic_ai.exceptions import ModelRetry
from calfkit.exceptions import NodeFaultError
from calfkit.models.error_report import ErrorReport
from calfkit.models.seam_context import SeamContext
from calfkit.nodes import agent_tool


@agent_tool
def boom(x: int) -> int:
    """Raise a generic exception → synthesized as ``calf.unhandled`` at the chokepoint."""
    raise ValueError(f"kaboom({x})")


@agent_tool
def needs_retry(x: int) -> int:
    """Raise ``ModelRetry`` → a *recoverable* (model-visible) failure, NOT a fault: the
    tool renders it at origin to a ``calf.retry``-marked ``TextPart`` that the agent
    materializes back into a ``RetryPromptPart`` (catalogue XC-4)."""
    raise ModelRetry("provide a positive x")


@agent_tool
def quota(x: int) -> int:
    """Mint a deliberate typed fault → carried verbatim (bypasses ``on_node_error``)."""
    raise NodeFaultError("billing.quota_exceeded", message="over quota", retryable=False, details={"x": x})


#: A details payload large enough that the wrapping fault envelope exceeds a small
#: per-topic ``max.message.bytes`` (but within ``build_safe``'s 16 KB details bound, so it
#: is carried whole — not elided — until the strip-and-retry drops it at the size limit).
_OVERSIZED_DETAIL = "x" * 8000


@agent_tool
def oversized_fault(x: int) -> int:
    """Mint a fault whose ``details`` make the envelope exceed a constrained callback
    topic's size limit — exercising the strip-to-minimal-and-retry floor (FR-21)."""
    raise NodeFaultError("billing.oversized", message="oversized fault", details={"blob": _OVERSIZED_DETAIL})


@agent_tool
def ok_a() -> str:
    """A fault-free sibling for mixed fan-out batches."""
    return "a_result"


@agent_tool
def ok_b() -> str:
    """A fault-free sibling for mixed fan-out batches."""
    return "b_result"


@dataclass
class CalleeErrorRecorder:
    """A Channel-B ``on_callee_error`` recorder: capture-in-callback, assert-in-test-body.

    Records one entry per firing (the live ``delivery_kind``, the fault's ``error_type``,
    the failing call's ``tag``, and the fault's ``frame_chain`` depth), then returns
    ``None`` to decline — so the fault escalates exactly as if the seam were absent. A
    raised assertion here would be a node-own accident routed to ``on_node_error``, so
    the test asserts on :attr:`calls` *after* the roundtrip instead.
    """

    calls: list[dict[str, Any]] = field(default_factory=list)

    def __call__(self, ctx: SeamContext[Any], fault: ErrorReport) -> None:
        self.calls.append(
            {
                "delivery_kind": ctx.delivery_kind,
                "error_type": fault.error_type,
                "tag": ctx.failing_call.tag if ctx.failing_call is not None else None,
                "frame_chain_len": len(fault.frame_chain),
            }
        )
        return None
