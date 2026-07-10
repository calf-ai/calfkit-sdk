"""Agent tool-error reception surface (spec ``docs/designs/agent-tool-error-reception-spec.md``).

The ergonomic reception surface over the shipped ``on_callee_error`` fault rail: it converts an
out-of-band tool-node fault into an in-band, model-visible tool result. This module is the cohesive
home for the feature's in-process pieces — the level-A fault renderer (D8), the budget-free
``surface_to_model()`` prebuilt (D9), the ``AgentSeamContext`` + ``resolve_failing_tool_call``
identity substrate (D3), and the thin ``on_tool_error`` adapter (D5/D6). ``base``/``agent``/``actions``/
``fanout``/``seam_context`` stay free of it.
"""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import TypeVar

from calfkit._vendor.pydantic_ai.messages import ToolCallPart as ToolCall
from calfkit.models.error_report import ErrorReport
from calfkit.models.marker import CallMarker
from calfkit.models.payload import retry_text_part
from calfkit.models.seam_context import SeamContext, SeamReturn
from calfkit.models.state import State

# ``ToolCall`` is calfkit's public name for the vendored ``pydantic_ai.messages.ToolCallPart`` — the
# model-request record (``.tool_name`` / ``.args``) that ``State.tool_calls`` holds and ``on_tool_error``
# hands the developer. Aliased here (and re-exported as ``calfkit.ToolCall``) so the public surface reads
# a stable calfkit name — distinct from the wire ``calfkit.models.ToolCallPart`` (``.kwargs``) — and so
# the LiteLLM migration swaps only this one alias, not every call site.

# Explicit public surface (also marks ``ToolCall`` as a re-export for strict mypy).
__all__ = [
    "AgentSeamContext",
    "ToolCall",
    "ToolErrorHandler",
    "render_fault_for_model",
    "resolve_failing_tool_call",
    "resolve_tool_call",
    "surface_to_model",
]


def render_fault_for_model(report: ErrorReport) -> str:
    """Render a faulting callee's :class:`ErrorReport` as the model-visible failure text — **level A**
    (spec D8): the top exception line only.

    * a harvested exception is present (a tool-code fault) → ``"{exception.type}: {message}"`` — or the
      type alone when the message is empty (no dangling ``": "``);
    * no harvested exception (a framework/transport fault — timeout, ``calf.fault_group``,
      materialization failure) → ``message`` alone.

    Deliberately performs **no** ``__cause__`` chain walk: ``report.causes`` is overloaded (recovery
    priors, mint chains, and ``calf.fault_group`` children reachable via a ``message_agent`` peer), so a
    faithful walk would need arm-selection the top line makes unnecessary. And **no** calfkit-internal
    field (``error_type``, ``origin_*``, ``frame_chain``, ``retryable``, ``details``, ``causes``,
    ``exception.module``/``attrs``) ever reaches the model — only ``exception.type`` and ``message``.
    """
    if report.exception is not None:
        exc_type = report.exception.type
        return f"{exc_type}: {report.message}" if report.message else exc_type
    return report.message


class AgentSeamContext(SeamContext[State]):
    """The agent-enriched seam context (spec D3): a :class:`SeamContext` subclass carrying a lazy
    :attr:`tool_call` for the failing tool. The canonical "richer context for a specific handler type"
    pattern (the ADK ``CallbackContext``/``ToolContext`` split ``seam_context.py`` already cites). Built
    by the agent's ``_build_seam_context`` (a covariant, LSP-safe override); ``on_tool_error`` is
    agent-only, so a handler never receives a bare base ``SeamContext``. Adds no dataclass field — only
    the computed property — so it inherits ``SeamContext``'s constructor unchanged."""

    @property
    def tool_call(self) -> ToolCall | None:
        """The failing tool's :class:`ToolCall`, or ``None`` when the fault is not tool-attributable
        (off the ``on_callee_error`` stage, or a genuinely un-attributable fault). Lazy — resolved per
        access from ``failing_call`` (D3): carriage-first from the echoed ``CallMarker`` on the
        ``message_agent`` arm, else from ``state.tool_calls``."""
        return resolve_failing_tool_call(self)


def resolve_tool_call(state: State, tag: str | None, *, carried_marker: CallMarker | None) -> ToolCall | None:
    """The single ``tag → ToolCall`` resolution (spec D3/D7), shared by :attr:`AgentSeamContext.tool_call`
    and the agent's ``_resolve_slot`` so the lookup lives in one place.

    **Carriage-first** whenever a ``carried_marker`` (the echoed
    :class:`~calfkit.models.marker.CallMarker`) is present — with universal stamping (caller-side
    step-emission spec §3.2 / L18k) that is EVERY agent-dispatched call, normal tools included: the
    marker ALONE reconstructs the **full** call — name, id, AND **parsed-dict** args — WITHOUT reading
    the reply state (for a ``message_agent`` peer that state is *foreign*, so consulting it would only
    "work" by tag non-collision — the Phase-0 finding; the marker is the identity carriage of record).
    The ``tool_call_id`` comes from ``marker.tool_call_id``, never ``tag`` (Q1: the follow-up deprecates
    ``tag``'s echo role, so the reconstruction must not lean on it). **State** as the marker-absent
    fallback (``state.tool_calls[tag]``): a reply answering an unstamped call.
    """
    if carried_marker is not None:
        return ToolCall(tool_name=carried_marker.tool_name, tool_call_id=carried_marker.tool_call_id, args=carried_marker.args)
    if not tag:
        return None
    return state.tool_calls.get(tag)


def resolve_failing_tool_call(ctx: AgentSeamContext) -> ToolCall | None:
    """Resolve the FAILING slot's :class:`ToolCall` for :attr:`AgentSeamContext.tool_call` — the
    ``ctx.failing_call``-based entry to :func:`resolve_tool_call`. ``None`` off the ``on_callee_error``
    stage (``failing_call`` is set during that stage only — D6b)."""
    failing = ctx.failing_call
    if failing is None:
        return None
    return resolve_tool_call(ctx.state, failing.tag, carried_marker=failing.marker)


# The agent-facing ``on_tool_error`` handler shape (spec D5): the flat, ADK-faithful three-param
# signature the adapter hoists ``ctx.tool_call`` into.
ToolErrorHandler = Callable[[ToolCall, AgentSeamContext, ErrorReport], SeamReturn]


def _adapt_tool_error(fn: ToolErrorHandler) -> Callable[[AgentSeamContext, ErrorReport], SeamReturn]:
    """Wrap a flat ``on_tool_error(tool_call, ctx, report)`` handler into an arity-2
    ``on_callee_error(ctx, report)`` chain entry (spec D5/D6) — a pure hoist that carries no policy.

    Resolves ``ctx.tool_call`` (D3) and passes it as the flat param; when the fault is not
    tool-attributable (``ctx.tool_call is None``, D6b) it DECLINES (returns ``None``) so the fault
    continues down the chain, escalating only if unhandled. The handler's return flows through
    **untouched** (D6f) — the base coerces it uniformly (``_coerce_to_parts``). ``functools.wraps(fn,
    updated=())`` names the developer's handler in the §13 INFO and leaves the wrapper's own arity-2
    signature intact — ``updated=()`` so a handler bearing an explicit ``__signature__`` cannot leak it
    via ``fn.__dict__``; the base arity check (``follow_wrapped=False``, D6a) then registers the wrapper
    at arity 2. The sync wrapper serves an async handler too: it returns the coroutine, which
    ``run_chain`` awaits."""

    @functools.wraps(fn, updated=())
    def _wrapper(ctx: AgentSeamContext, report: ErrorReport) -> SeamReturn:
        tool_call = ctx.tool_call
        if tool_call is None:
            return None  # D6b: not tool-attributable → decline → the fault continues escalating
        return fn(tool_call, ctx, report)  # D6f: hoist to the flat param; the return flows through untouched

    return _wrapper


_T = TypeVar("_T")


def _as_list(value: _T | list[_T] | None) -> list[_T]:
    """Normalize a seam constructor arg (one handler / a list / unset) to a list, preserving order."""
    if value is None:
        return []
    return list(value) if isinstance(value, list) else [value]


def surface_to_model() -> ToolErrorHandler:
    """The budget-free ``on_tool_error`` prebuilt (spec D9): convert **every** faulting tool result
    into a model-visible error.

    Zero-arg. Returns a handler that renders the fault as the level-A top exception line
    (:func:`render_fault_for_model`, D8) and returns it ``is_error=True`` via the ``calf.retry`` marker
    (``retry_text_part`` → ``RetryPromptPart`` → Anthropic ``is_error``). It renders from the ``report``
    and ignores ``tool_call``/``ctx``. **Budget-free** — no per-tool failure counting or type filtering
    (D13); a registered prebuilt converts every matching fault, bounded only by the agent's turn limit
    until the durable per-tool budget (``State.seam_budgets``) lands as #251. Register via
    ``Agent(on_tool_error=[surface_to_model()])``.
    """

    def _surface(tool_call: ToolCall, ctx: AgentSeamContext, report: ErrorReport) -> SeamReturn:
        return retry_text_part(render_fault_for_model(report))

    return _surface
