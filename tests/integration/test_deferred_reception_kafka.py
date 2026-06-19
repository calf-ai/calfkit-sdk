"""Deferred-behavior stubs — the forward targets, skipped until their PR lands.

These document behaviors the design specs describe but the merged code does NOT yet
implement (see the behavior catalogue's deferred register). Each is a ``skip`` rather
than a live test or an ``xfail``: the feature is *absent*, so a real test would error at
the missing API, not merely fail. When the named issue lands, drop the ``skip`` and fill
in the body (the docstring states the intended assertion).

Tracked deferrals:
* **#250** — fault reception at the client (typed ``NodeFaultError``) and on consumers
  (``ConsumerContext.fault`` / ``.delivery_kind``).
* **#251** — agent self-healing budgets (``surface_to_model`` + ``State.seam_budgets`` +
  a bounded ``self_retry_budget`` with ``calf.agent.self_retry_exhausted``).
* **#193** — provider classification of ``calf.model.context_window_exceeded``.
* MCP ``isError=True`` → ``calf.retry`` marking (the B2 follow-up).

Opt-in (``-m kafka``); all skipped, so they never need a broker.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.kafka


@pytest.mark.skip(reason="#250 fault reception: the reply dispatcher has no x-calf-kind branch yet")
async def test_client_raises_node_fault_error_on_fault_reply() -> None:
    """When #250 lands: a routed fault makes ``await handle.result()`` raise
    ``NodeFaultError``, and ``e.report.find(error_type)`` branches on the slotted report —
    replacing today's ``DeserializationError`` (see Suite F's F-1 Channel C)."""


@pytest.mark.skip(reason="#250 fault reception: ConsumerContext has no .fault / .delivery_kind yet")
async def test_consumer_tap_sees_typed_fault() -> None:
    """When #250 lands: a ``@consumer`` subscribed to a fault mirror reads
    ``ctx.fault.error_type`` and ``ctx.delivery_kind == 'fault'`` — replacing today's raw
    ``AIOKafkaConsumer`` tap (``_fault_tap``), which exists precisely because the sink
    surface cannot see the fault yet."""


@pytest.mark.skip(reason="#251 self-healing budgets: surface_to_model prebuilt does not exist")
async def test_surface_to_model_bounds_then_escalates() -> None:
    """When #251 lands: the ``surface_to_model(max_failures=...)`` prebuilt resolves
    failing slots as model-visible results until the per-tool budget exhausts, then
    declines → escalates. Today the only failure-surfacing seam is a user-authored
    ``on_callee_error`` (see Suite X's X-3), with no budget."""


@pytest.mark.skip(reason="#251 self-healing budgets: self_retry_budget / seam_budgets do not exist")
async def test_self_retry_budget_exhausts() -> None:
    """When #251 lands: an all-invalid turn self-retries up to ``self_retry_budget`` (one
    WARNING-logged turn by default), then faults ``calf.agent.self_retry_exhausted``;
    ``self_retry_budget=0`` faults immediately with ``details.reason='self_retry_disabled'``.
    Today the all-invalid ``TailCall`` self-retry loop is unbounded."""


@pytest.mark.skip(reason="#193 provider rider: calf.model.context_window_exceeded has no producer")
async def test_context_window_error_classifies() -> None:
    """When #193 lands: a provider context-window error classifies to
    ``calf.model.context_window_exceeded`` at the provider layer. Today such an error
    surfaces as a generic ``calf.unhandled`` fault via the chokepoint."""


@pytest.mark.skip(reason="B2 follow-up: MCP isError is passed through transparently/unmarked")
async def test_mcp_iserror_marks_calf_retry() -> None:
    """When the MCP-isError marking follow-up lands: an MCP ``isError=True`` result is
    marked ``calf.retry`` and materializes a ``RetryPromptPart`` (Anthropic ``is_error``
    fidelity). Today it rides the reply slot as an ordinary, unmarked return."""
