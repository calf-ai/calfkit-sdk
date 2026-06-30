"""Tests for the ``ck chat`` orchestration (``calfkit.cli._chat``).

Three layers, each as real as the broker allows:
- discovery / picker / offline-name: ``client.mesh.get_agents`` patched, the rest real;
- the fault & timeout turn paths: a **real** ``InvocationHandle`` + ``_RunChannel`` (a
  synchronous test broker cannot produce a client-side timeout), injected via a thin gateway;
- the happy multi-turn transcript: a full offline ``TestKafkaBroker`` run of a ``FunctionModel``
  agent, driving ``_chat_loop`` with a scripted reader.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime, timezone

import pytest
import typer
from faststream.kafka import KafkaBroker, TestKafkaBroker

from calfkit._vendor.pydantic_ai.messages import ModelMessage, ModelRequest, ModelResponse, TextPart, ToolCallPart, ToolReturnPart
from calfkit._vendor.pydantic_ai.models.function import AgentInfo as _FnAgentInfo
from calfkit._vendor.pydantic_ai.models.function import FunctionModel
from calfkit.cli._chat import _chat_loop, _render_and_collect, _resolve_target, _run_turn, run_chat_session
from calfkit.client import Client
from calfkit.client.events import RunCompleted, RunFailed
from calfkit.client.hub import InvocationHandle, _RunChannel
from calfkit.client.mesh import AgentInfo, Mesh
from calfkit.exceptions import MeshUnavailableError
from calfkit.models import CallFrameStack, Envelope, SessionRunContext, WorkflowState
from calfkit.models.error_report import ErrorReport
from calfkit.models.payload import TextPart as _WireTextPart
from calfkit.models.state import State
from calfkit.models.step import AgentMessageEvent, HandoffEvent
from calfkit.models.tool_context import ToolContext
from calfkit.nodes import Agent, agent_tool
from calfkit.worker import Worker
from tests.providers import prepare_worker


def _agent(name: str, desc: str | None) -> AgentInfo:
    return AgentInfo(name=name, description=desc, last_seen=datetime(2026, 1, 1, tzinfo=timezone.utc))


def _scripted(lines: list[str]) -> Callable[[str], Awaitable[str]]:
    """A reader that returns the scripted lines in order, then raises ``EOFError``."""
    it = iter(lines)

    async def read_line(_prompt: str) -> str:
        try:
            return next(it)
        except StopIteration:
            raise EOFError from None

    return read_line


def _patch_get_agents(monkeypatch: pytest.MonkeyPatch, *, result: object = None, raises: Exception | None = None) -> None:
    async def _fake(_self: Mesh) -> object:
        if raises is not None:
            raise raises
        return result

    monkeypatch.setattr(Mesh, "get_agents", _fake)


class _FakeGateway:
    """A thin ``client.agent(...)`` stand-in: ``start`` returns a pre-built handle so the
    fault / timeout branches of ``_run_turn`` can be driven with a controlled terminal."""

    def __init__(self, handle: InvocationHandle) -> None:
        self._handle = handle

    async def start(self, _prompt: str, *, message_history: list[ModelMessage] | None = None) -> InvocationHandle:
        return self._handle


# --- _resolve_target: name online / offline, picker selection -------------------


async def test_resolve_target_name_online_returns_it() -> None:
    agents = {"helpbot": _agent("helpbot", "x")}
    assert await _resolve_target("helpbot", agents, _scripted([])) == "helpbot"


async def test_resolve_target_name_offline_exits_2_listing_online(capsys: pytest.CaptureFixture[str]) -> None:
    agents = {"helpbot": _agent("helpbot", "x"), "researcher": _agent("researcher", "y")}
    with pytest.raises(typer.Exit) as exc:
        await _resolve_target("missing", agents, _scripted([]))
    assert exc.value.exit_code == 2
    err = capsys.readouterr().err
    assert "is not online" in err
    assert "helpbot, researcher" in err  # alphabetical online list


async def test_resolve_target_picker_selects_by_index() -> None:
    agents = {"beta": _agent("beta", "x"), "alpha": _agent("alpha", "y")}  # sorted: alpha, beta
    assert await _resolve_target(None, agents, _scripted(["2"])) == "beta"


async def test_resolve_target_picker_quit_returns_none() -> None:
    agents = {"alpha": _agent("alpha", "x")}
    assert await _resolve_target(None, agents, _scripted(["q"])) is None


async def test_resolve_target_picker_eof_returns_none() -> None:
    agents = {"alpha": _agent("alpha", "x")}
    assert await _resolve_target(None, agents, _scripted([])) is None


async def test_resolve_target_picker_invalid_then_valid(capsys: pytest.CaptureFixture[str]) -> None:
    agents = {"alpha": _agent("alpha", "x")}
    picked = await _resolve_target(None, agents, _scripted(["9", "notanumber", "1"]))
    assert picked == "alpha"
    assert "Enter a number from 1 to 1" in capsys.readouterr().out


# --- run_chat_session discovery paths (mesh.get_agents patched) ------------------


async def test_run_chat_session_no_agents_prints_and_returns(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    _patch_get_agents(monkeypatch, result={})
    await run_chat_session(None, "localhost:9092", None)
    out = capsys.readouterr().out
    assert "Discovering agents" in out
    assert "No agents are online" in out


async def test_run_chat_session_offline_name_exits_2(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    _patch_get_agents(monkeypatch, result={"helpbot": _agent("helpbot", "x")})
    with pytest.raises(typer.Exit) as exc:
        await run_chat_session("missing", "localhost:9092", None)
    assert exc.value.exit_code == 2
    assert "is not online" in capsys.readouterr().err


async def test_run_chat_session_propagates_mesh_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_get_agents(monkeypatch, raises=MeshUnavailableError("down", reason="open_failed"))
    with pytest.raises(MeshUnavailableError):
        await run_chat_session(None, "localhost:9092", None)


# --- _run_turn: fault + timeout (real handle + channel, thin injected gateway) ---


async def test_run_turn_fault_renders_error_and_keeps_history(capsys: pytest.CaptureFixture[str]) -> None:
    channel = _RunChannel()
    channel.push(RunFailed(report=ErrorReport(error_type="billing.denied", message="over limit"), correlation_id="c"))
    handle = InvocationHandle(correlation_id="c", _channel=channel)
    result = await _run_turn(_FakeGateway(handle), "bot", "hi", ["OLD"], None)
    assert result is None  # turn failed -> caller keeps the old history
    out = capsys.readouterr().out
    assert "bot > [error] over limit" in out
    assert out.count("bot > ") == 1  # ONLY the error line — the normal answer is suppressed


async def test_run_turn_timeout_prints_notice_and_keeps_history(capsys: pytest.CaptureFixture[str]) -> None:
    channel = _RunChannel()  # never terminated -> stream() parks
    handle = InvocationHandle(correlation_id="c", _channel=channel)
    result = await _run_turn(_FakeGateway(handle), "bot", "hi", ["OLD"], 0.05)
    assert result is None
    assert "no response within the timeout" in capsys.readouterr().out


class _FailingStartGateway:
    """A gateway whose dispatch (start) fails — e.g. a transient broker/publish error."""

    async def start(self, _prompt: str, *, message_history: list[ModelMessage] | None = None) -> InvocationHandle:
        raise ConnectionError("broker unreachable")


async def test_run_turn_dispatch_failure_keeps_repl_alive(capsys: pytest.CaptureFixture[str]) -> None:
    # A dispatch failure (gw.start) must be surfaced per-turn and return None (keep history),
    # not propagate out and tear down the whole REPL.
    result = await _run_turn(_FailingStartGateway(), "bot", "hi", ["OLD"], None)
    assert result is None
    out = capsys.readouterr().out
    assert "bot >" in out and "turn could not be started" in out and "ConnectionError" in out


# --- happy multi-turn transcript over a real (offline) TestKafkaBroker -----------


@agent_tool
def _lookup_year(_ctx: ToolContext) -> str:
    return "1967"


_seen: list[list[ModelMessage]] = []


def _preamble_call_then_final(messages: list[ModelMessage], _info: _FnAgentInfo) -> ModelResponse:
    _seen.append(list(messages))
    last = messages[-1]
    if isinstance(last, ModelRequest) and any(isinstance(p, ToolReturnPart) for p in last.parts):
        returned = next(p for p in last.parts if isinstance(p, ToolReturnPart))
        return ModelResponse(parts=[TextPart(f"answer: {returned.content}")])
    return ModelResponse(parts=[TextPart("checking the records"), ToolCallPart("_lookup_year")])


async def test_chat_loop_renders_transcript_and_threads_history(container: object, capsys: pytest.CaptureFixture[str]) -> None:
    _seen.clear()
    worker = container.get(Worker)  # type: ignore[attr-defined]
    agent = Agent(
        "helpbot",
        system_prompt="x",
        subscribe_topics="agent.helpbot.private.input",  # == derive_input_topic("helpbot")
        model_client=FunctionModel(_preamble_call_then_final),
        tools=[_lookup_year],
        sequential_only_mode=True,
    )
    worker.add_nodes(agent, _lookup_year)
    prepare_worker(container)
    broker = container.get(KafkaBroker)  # type: ignore[attr-defined]
    client = container.get(Client)  # type: ignore[attr-defined]

    async with TestKafkaBroker(broker):
        await _chat_loop(client, "helpbot", 10.0, _scripted(["what year", "what else", "/exit"]))

    out = capsys.readouterr().out
    assert "helpbot" in out  # the emitter header attributes the work to the agent
    assert "[message]" in out and "checking the records" in out  # preamble step
    assert "[tool call]" in out and "_lookup_year()" in out  # tool call step
    assert "[tool result]" in out and "1967" in out  # tool result step
    assert "helpbot > answer: 1967" in out  # the final answer line
    # the work log streams BEFORE the answer, in step order (first occurrences = turn 1)
    assert out.index("[message]") < out.index("[tool call]") < out.index("[tool result]") < out.index("helpbot > answer: 1967")
    assert out.count("answer: 1967") == 2  # both turns completed
    # threading: turn 2's first model call saw the accumulated turn-1 history (more messages).
    assert len(_seen[2]) > len(_seen[0])


# --- loop & dispatch edge branches (no turn dispatched, so no broker needed) -----


class _FakeClient:
    """A client whose gateway is never used: the loop branches under test break before any turn."""

    def agent(self, _name: str) -> object:
        return None


async def test_chat_loop_eof_breaks_immediately(capsys: pytest.CaptureFixture[str]) -> None:
    await _chat_loop(_FakeClient(), "bot", None, _scripted([]))  # Ctrl-D at the first prompt
    assert "Chatting with bot" in capsys.readouterr().out


async def test_chat_loop_empty_line_is_skipped() -> None:
    # an empty line is skipped (D6); /quit ends — neither dispatches a turn, so the gateway is unused.
    await _chat_loop(_FakeClient(), "bot", None, _scripted(["", "/quit"]))


async def test_run_chat_session_picker_quit_returns(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    _patch_get_agents(monkeypatch, result={"alpha": _agent("alpha", "x"), "beta": _agent("beta", "y")})
    monkeypatch.setattr("calfkit.cli._chat.make_reader", lambda _loop: _scripted(["q"]))
    await run_chat_session(None, "localhost:9092", None)
    assert "Online agents" in capsys.readouterr().out  # picker shown, then quit -> clean return


async def test_run_chat_session_dispatches_to_chat_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_get_agents(monkeypatch, result={"helpbot": _agent("helpbot", "x")})
    called: list[str] = []

    async def _record(_client: object, agent_name: str, _timeout: object, _read_line: object) -> None:
        called.append(agent_name)

    monkeypatch.setattr("calfkit.cli._chat._chat_loop", _record)
    await run_chat_session("helpbot", "localhost:9092", None)
    assert called == ["helpbot"]


async def test_chat_loop_survives_a_faulting_turn(capsys: pytest.CaptureFixture[str]) -> None:
    """A turn that faults renders an [error] line and the REPL keeps going (reaches /exit)."""
    channel = _RunChannel()
    channel.push(RunFailed(report=ErrorReport(error_type="billing.denied", message="boom"), correlation_id="c"))
    handle = InvocationHandle(correlation_id="c", _channel=channel)

    class _FaultingClient:
        def agent(self, _name: str) -> _FakeGateway:
            return _FakeGateway(handle)

    await _chat_loop(_FaultingClient(), "bot", None, _scripted(["trigger a fault", "/exit"]))
    # the fault rendered, and reaching /exit (no raise) proves the loop survived it
    out = capsys.readouterr().out
    assert "bot > [error] boom" in out
    assert out.count("bot > ") == 1  # only the error line — no answer line printed


# --- handoff / multi-emitter attribution (real channel, fed before close) --------
# Output PROJECTION is covered by the integration + caller-surface tests; here we assert only the
# ATTRIBUTION — which emitter heads each block, and who the answer is attributed to — so the
# terminal carries no reply (the answer projects to the empty-reply "(no text reply)" placeholder).


def _completed(agent: str | None) -> RunCompleted:
    envelope = Envelope(
        context=SessionRunContext(state=State(), deps={}),
        internal_workflow_state=WorkflowState(call_stack=CallFrameStack()),
    )
    return RunCompleted(output="", correlation_id="c", agent=agent, _envelope=envelope)


def _amsg(emitter: str, text: str) -> AgentMessageEvent:
    return AgentMessageEvent(correlation_id="c", depth=1, frame_id="f", emitter=emitter, parts=[_WireTextPart(text=text)])


def _handoff(emitter: str, target: str, reason: str) -> HandoffEvent:
    return HandoffEvent(correlation_id="c", depth=1, frame_id="f", emitter=emitter, target=target, reason=reason)


async def test_render_and_collect_attributes_handoff_to_actual_responder(capsys: pytest.CaptureFixture[str]) -> None:
    channel = _RunChannel()
    channel.push_intermediate(_amsg("support-bot", "handing this to billing"))
    channel.push_intermediate(_handoff("support-bot", "billing", "refund authority"))
    channel.push_intermediate(_amsg("billing", "processing the refund"))
    channel.push(_completed(agent="billing"))
    handle = InvocationHandle(correlation_id="c", _channel=channel)

    history = await _render_and_collect(handle, "support-bot")
    out = capsys.readouterr().out
    assert out.index("support-bot") < out.index("billing")  # the new emitter's header follows the handoff
    assert "billing > " in out  # the answer is attributed to the ACTUAL responder (billing)...
    assert "support-bot > " not in out  # ...not the addressed agent
    assert isinstance(history, list)


async def test_render_and_collect_falls_back_to_addressed_agent_when_responder_unknown(capsys: pytest.CaptureFixture[str]) -> None:
    channel = _RunChannel()
    channel.push(_completed(agent=None))  # terminal carries no emitter
    handle = InvocationHandle(correlation_id="c", _channel=channel)
    await _render_and_collect(handle, "support-bot")
    assert "support-bot > " in capsys.readouterr().out  # the `or agent_name` fallback
