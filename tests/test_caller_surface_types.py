"""Commit 1 — foundational caller-surface value types & typed errors (spec §2.2/§2.5/§3.3).

Pure types, no transport: ``Dispatch`` (the send() fire token), the ``RunEvent`` terminal
union, the ``DEFAULT_FIREHOSE_BUFFER_SIZE`` knob default, and the two typed run-survives
signals ``ClientTimeoutError`` / ``ClientClosedError``.
"""

from __future__ import annotations

import asyncio
import copy
from dataclasses import FrozenInstanceError
from typing import get_args

import pytest

from calfkit.client.events import DEFAULT_FIREHOSE_BUFFER_SIZE, RunCompleted, RunEvent, RunFailed
from calfkit.client.gateway import Dispatch
from calfkit.exceptions import ClientClosedError, ClientTimeoutError
from calfkit.models import CallFrameStack, Envelope, SessionRunContext, WorkflowState
from calfkit.models.error_report import ErrorReport
from calfkit.models.state import State


def _env() -> Envelope:
    return Envelope(
        context=SessionRunContext(state=State(), deps={}),
        internal_workflow_state=WorkflowState(call_stack=CallFrameStack()),
    )


def test_dispatch_is_a_frozen_fire_token_carrying_only_correlation_id() -> None:
    d = Dispatch(correlation_id="cid-1")
    assert d.correlation_id == "cid-1"
    with pytest.raises(FrozenInstanceError):
        d.correlation_id = "cid-2"  # type: ignore[misc]


def test_dispatch_is_deliberately_not_a_handle() -> None:
    # The type itself says the result is not retrievable by id (spec §2.2).
    d = Dispatch(correlation_id="cid-1")
    assert not hasattr(d, "result")
    assert not hasattr(d, "stream")


def test_run_completed_carries_output_correlation_id_agent_and_reply_envelope() -> None:
    env = _env()
    ev = RunCompleted(output={"k": "v"}, correlation_id="cid-1", agent="summarizer", _envelope=env)
    assert ev.output == {"k": "v"}
    assert ev.correlation_id == "cid-1"
    assert ev.agent == "summarizer"
    assert ev._envelope is env  # the decoded reply, for result()'s typed projection (spec §3.3)
    with pytest.raises(FrozenInstanceError):
        ev.output = "x"  # type: ignore[misc]


def test_run_failed_carries_the_error_report_verbatim_and_correlation_id() -> None:
    report = ErrorReport(error_type="billing.quota")
    ev = RunFailed(report=report, correlation_id="cid-1")
    assert ev.report is report
    assert ev.correlation_id == "cid-1"


def test_run_event_is_the_closed_two_terminal_union() -> None:
    assert set(get_args(RunEvent)) == {RunCompleted, RunFailed}


def test_default_firehose_buffer_size_is_a_positive_int() -> None:
    assert isinstance(DEFAULT_FIREHOSE_BUFFER_SIZE, int)
    assert DEFAULT_FIREHOSE_BUFFER_SIZE > 0


def test_client_timeout_error_is_a_distinct_typed_signal() -> None:
    err = ClientTimeoutError(correlation_id="cid-1", timeout=30.0)
    assert isinstance(err, Exception)
    # never a bare asyncio.TimeoutError (spec §2.5)
    assert not isinstance(err, asyncio.TimeoutError)
    assert err.correlation_id == "cid-1"
    assert err.timeout == 30.0


def test_client_closed_error_is_a_distinct_typed_signal() -> None:
    err = ClientClosedError(correlation_id="cid-1")
    assert isinstance(err, Exception)
    # never a bare CancelledError (spec §2.5)
    assert not isinstance(err, asyncio.CancelledError)
    assert err.correlation_id == "cid-1"


def test_typed_errors_are_flat_no_artificial_base_class() -> None:
    # spec §2.5: "No artificial base class — exceptions are flat."
    assert ClientTimeoutError.__bases__ == (Exception,)
    assert ClientClosedError.__bases__ == (Exception,)


def test_client_timeout_error_reconstructs_from_its_fields() -> None:
    # Required positional args break the default reduction (which replays the message string);
    # a custom __reduce__ rebuilds from the real fields (cf. ReplyExpiredError it replaces).
    restored = copy.deepcopy(ClientTimeoutError(correlation_id="cid-1", timeout=30.0))
    assert restored.correlation_id == "cid-1"
    assert restored.timeout == 30.0


def test_client_closed_error_reconstructs_from_its_fields() -> None:
    restored = copy.deepcopy(ClientClosedError(correlation_id="cid-1"))
    assert restored.correlation_id == "cid-1"
