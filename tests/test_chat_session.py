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
from calfkit.cli import _dev_agents
from calfkit.cli._chat import _chat_loop, _render_and_collect, _resolve_target, _run_turn, run_chat_session
from calfkit.cli._dev_agents import DevAgentError, TargetNodes, TargetOutcome
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


async def test_resolve_target_offline_name_appends_the_dev_daemon_hint(capsys: pytest.CaptureFixture[str]) -> None:
    """Spec §7 (review round 1, Ryan-approved): when the dev layer knows a managed daemon owns
    the offline name, the shipped not-online error gains the logs hint."""
    agents = {"other": _agent("other", "x")}
    hint_calls: list[str] = []

    def hint(name: str) -> str | None:
        hint_calls.append(name)
        return f"a managed daemon for '{name}' exists but its agents are offline — logs: /tmp/agents-x.log (ck dev status)"

    with pytest.raises(typer.Exit) as exc:
        await _resolve_target("general", agents, _scripted([]), offline_hint=hint)
    assert exc.value.exit_code == 2
    err = capsys.readouterr().err
    assert "is not online" in err
    assert "a managed daemon for 'general' exists but its agents are offline — logs: /tmp/agents-x.log (ck dev status)" in err
    assert hint_calls == ["general"]


async def test_resolve_target_offline_name_hint_returning_none_adds_nothing(capsys: pytest.CaptureFixture[str]) -> None:
    """No daemon owns the name (or the scan is unavailable on a core install): the shipped error
    stands alone."""
    with pytest.raises(typer.Exit):
        await _resolve_target("general", {"other": _agent("other", "x")}, _scripted([]), offline_hint=lambda name: None)
    err = capsys.readouterr().err
    assert "is not online" in err
    assert "managed daemon" not in err


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


def _spy_client_connect(monkeypatch: pytest.MonkeyPatch) -> dict:
    """Spy ``Client.connect``, capturing its ``provisioning`` kwarg while keeping the real (lazy) connect."""
    captured: dict = {}
    real_connect = Client.connect

    def spy(server_urls: object = None, **kwargs: object) -> object:
        captured["provisioning"] = kwargs.get("provisioning")
        return real_connect(server_urls, **kwargs)

    monkeypatch.setattr(Client, "connect", spy)
    return captured


async def test_run_chat_session_provision_enables_provisioning(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_get_agents(monkeypatch, result={})  # early return after connect — no broker I/O
    captured = _spy_client_connect(monkeypatch)
    await run_chat_session(None, "localhost:9092", None, provision=True)
    assert captured["provisioning"] is not None
    assert captured["provisioning"].enabled is True


async def test_run_chat_session_default_passes_no_provisioning(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_get_agents(monkeypatch, result={})
    captured = _spy_client_connect(monkeypatch)
    await run_chat_session(None, "localhost:9092", None, provision=False)
    assert captured["provisioning"] is None


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

    outcome = await _render_and_collect(handle, "support-bot")
    out = capsys.readouterr().out
    assert out.index("support-bot") < out.index("billing")  # the new emitter's header follows the handoff
    assert "billing > " in out  # the answer is attributed to the ACTUAL responder (billing)...
    assert "support-bot > " not in out  # ...not the addressed agent
    assert isinstance(outcome.history, list)
    assert outcome.responder == "billing"  # the responder the REPL re-binds to (sticky handoff)


async def test_render_and_collect_falls_back_to_addressed_agent_when_responder_unknown(capsys: pytest.CaptureFixture[str]) -> None:
    channel = _RunChannel()
    channel.push(_completed(agent=None))  # terminal carries no emitter
    handle = InvocationHandle(correlation_id="c", _channel=channel)
    outcome = await _render_and_collect(handle, "support-bot")
    assert "support-bot > " in capsys.readouterr().out  # the `or agent_name` fallback
    assert outcome.responder == "support-bot"  # no emitter -> stays with the addressed agent (no spurious re-bind)


# --- sticky handoff: the REPL follows a transfer across turns --------------------
# A handoff moves control to a peer that answers the original caller; the next turn should go
# to THAT peer (the transfer sticks), not back to the originally-picked agent. A consult
# (message_agent) keeps control, so the responder stays the current agent and nothing re-binds.


class _ScriptedStickyGateway:
    """A gateway bound to one agent name; each ``start()`` yields a handle whose terminal is
    attributed to the responder the script maps this agent to (default: itself)."""

    def __init__(self, name: str, responders: dict[str, str]) -> None:
        self._name = name
        self._responders = responders

    async def start(self, _prompt: str, *, message_history: list[ModelMessage] | None = None) -> InvocationHandle:
        channel = _RunChannel()
        channel.push(_completed(agent=self._responders.get(self._name, self._name)))
        return InvocationHandle(correlation_id="c", _channel=channel)


class _RecordingStickyClient:
    """Records every ``agent(name)`` bind and hands back a gateway scripted per name — so a
    test can assert which agent each turn was dispatched to."""

    def __init__(self, responders: dict[str, str]) -> None:
        self._responders = responders
        self.binds: list[str] = []

    def agent(self, name: str) -> _ScriptedStickyGateway:
        self.binds.append(name)
        return _ScriptedStickyGateway(name, self._responders)


async def test_chat_loop_sticks_to_handoff_target_on_next_turn(capsys: pytest.CaptureFixture[str]) -> None:
    # Turn 1: support-bot hands off -> the answer comes from 'billing'. The REPL must re-bind to
    # 'billing' so turn 2 dispatches THERE (billing then answers as itself -> no further re-bind).
    client = _RecordingStickyClient({"support-bot": "billing", "billing": "billing"})
    await _chat_loop(client, "support-bot", None, _scripted(["q1", "q2", "/exit"]))
    assert client.binds == ["support-bot", "billing"]  # bound at start, re-bound to the handoff target
    assert "(now chatting with billing)" in capsys.readouterr().out


async def test_chat_loop_does_not_rebind_when_answer_stays_with_current_agent(capsys: pytest.CaptureFixture[str]) -> None:
    # support-bot answers every turn itself (e.g. it consulted a peer but kept control): the
    # responder equals the current agent, so the REPL never re-binds and prints no move notice.
    client = _RecordingStickyClient({"support-bot": "support-bot"})
    await _chat_loop(client, "support-bot", None, _scripted(["q1", "q2", "/exit"]))
    assert client.binds == ["support-bot"]  # bound once, never re-bound
    assert "now chatting with" not in capsys.readouterr().out


async def test_chat_loop_keeps_current_agent_when_a_turn_fails(capsys: pytest.CaptureFixture[str]) -> None:
    # A faulting turn returns None: the REPL keeps the current agent (no re-bind off a failed turn).
    channel = _RunChannel()
    channel.push(RunFailed(report=ErrorReport(error_type="x", message="boom"), correlation_id="c"))
    handle = InvocationHandle(correlation_id="c", _channel=channel)

    class _FaultingBindClient:
        def __init__(self) -> None:
            self.binds: list[str] = []

        def agent(self, name: str) -> _FakeGateway:
            self.binds.append(name)
            return _FakeGateway(handle)

    client = _FaultingBindClient()
    await _chat_loop(client, "bot", None, _scripted(["boom", "/exit"]))
    assert client.binds == ["bot"]  # never re-bound despite the fault
    assert "bot > [error] boom" in capsys.readouterr().out


# --- ck dev chat TARGET: the in-process session worker (agent-lifecycle spec §3.2) ---------------------


@pytest.fixture
def isolated_home(monkeypatch: pytest.MonkeyPatch, tmp_path: object) -> object:
    """The session takes the agent-layer flock under ~/.calfkit — keep it out of the real home."""
    monkeypatch.setenv("HOME", str(tmp_path))
    return tmp_path


def _target_nodes(spec: str = "app:agent", agent_names: tuple[str, ...] = ("general",), tool_names: tuple[str, ...] = ()) -> TargetNodes:
    nodes = tuple(object() for _ in (*agent_names, *tool_names))
    return TargetNodes(spec=spec, nodes=nodes, agent_names=agent_names, tool_names=tool_names)


class _SessionWorker:
    """Captures the in-process worker construction + lifecycle the session drives."""

    instances: list[_SessionWorker] = []

    def __init__(self, client: object, nodes: object = None, **kwargs: object) -> None:
        self.client = client
        self.nodes = nodes
        self.kwargs = kwargs
        self.started = False
        self.stopped = False
        _SessionWorker.instances.append(self)

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True


@pytest.fixture
def session_seams(monkeypatch: pytest.MonkeyPatch, isolated_home: object) -> dict:
    """Default drivable seams: one launched target, readiness OK, picker quit immediately."""
    seams: dict = {"evaluate": ([], [_target_nodes()]), "waits": []}
    _SessionWorker.instances = []

    async def fake_evaluate(plan: object, host_key: object, mesh: object) -> tuple:
        seams["evaluate_args"] = (plan, host_key, mesh)
        result: tuple = seams["evaluate"]
        return result

    async def fake_wait(proc: object, agent_names: object, tool_names: object, mesh: object, **kwargs: object) -> None:
        seams["waits"].append({"proc": proc, "agent_names": agent_names, "tool_names": tool_names, **kwargs})

    monkeypatch.setattr(_dev_agents, "evaluate_targets", fake_evaluate)
    monkeypatch.setattr(_dev_agents, "wait_agents_ready", fake_wait)
    monkeypatch.setattr("calfkit.worker.Worker", _SessionWorker)
    _patch_get_agents(monkeypatch, result={"general": _agent("general", None)})
    monkeypatch.setattr("calfkit.cli._chat.make_reader", lambda _loop: _scripted(["q"]))
    return seams


async def test_session_launches_the_worker_on_the_shared_client_and_stops_it(session_seams: dict, capsys: pytest.CaptureFixture[str]) -> None:
    """The co-located contract (spec §9 'Session worker's Client'): the worker shares the chat
    session's Client, carries the 5s dev heartbeat preset explicitly, hosts the launched nodes,
    and is stopped before the client closes — on the session's normal end."""
    from calfkit.controlplane import ControlPlaneConfig

    plan = [_target_nodes()]
    session_seams["evaluate"] = ([], plan)  # the worker hosts what the evaluation says to launch
    await run_chat_session(None, "localhost:9092", None, session_plan=plan, session_host_key="127.0.0.1:9092")
    (worker,) = _SessionWorker.instances
    assert isinstance(worker.client, Client)
    assert worker.started and worker.stopped
    assert list(worker.nodes) == list(plan[0].nodes)  # type: ignore[call-overload]
    assert worker.kwargs["control_plane"] == ControlPlaneConfig(heartbeat_interval=5.0)
    out = capsys.readouterr().out
    assert "✦ stopped 'general' (ran in this session)" in out
    assert "still running" not in out
    # The chat variant of the readiness gate: no process arm.
    (wait,) = session_seams["waits"]
    assert wait["proc"] is None
    assert wait["agent_names"] == ["general"]


async def test_session_all_reused_starts_no_worker(session_seams: dict, capsys: pytest.CaptureFixture[str]) -> None:
    """All targets reused: attach-plus-narration only (spec §3.2) — nothing is started, and the
    exit line's still-running names are the reused ones."""
    target = _target_nodes()
    session_seams["evaluate"] = ([TargetOutcome(target=target, reused=True, ages={"general": 2.0})], [])
    await run_chat_session(None, "localhost:9092", None, session_plan=[target], session_host_key="127.0.0.1:9092")
    assert _SessionWorker.instances == []
    assert session_seams["waits"] == []
    out = capsys.readouterr().out
    assert "✦ still running: general — 'ck dev chat' to rejoin, 'ck dev down' to stop everything" in out
    assert "ran in this session" not in out


async def test_session_mixed_launch_and_reuse_narrates_both(session_seams: dict, capsys: pytest.CaptureFixture[str]) -> None:
    launched = _target_nodes(spec="app:general", agent_names=("general",))
    reused = _target_nodes(spec="app:finance", agent_names=("finance",))
    session_seams["evaluate"] = ([TargetOutcome(target=reused, reused=True, ages={"finance": 1.0})], [launched])
    await run_chat_session(None, "localhost:9092", None, session_plan=[launched, reused], session_host_key="127.0.0.1:9092")
    out = capsys.readouterr().out
    assert "✦ stopped 'general' (ran in this session)" in out
    assert "✦ still running: finance" in out


async def test_session_requires_an_agent_among_launched_and_reused(session_seams: dict) -> None:
    """§7: tools-only launched+reused = nobody to chat with — a preflight-style error, NOT
    softened into a picker over unrelated online agents; nothing is started."""
    tools_only = _target_nodes(spec="tools:get_weather", agent_names=(), tool_names=("get_weather",))
    session_seams["evaluate"] = ([], [tools_only])
    with pytest.raises(DevAgentError, match=r"nobody to chat with"):
        await run_chat_session(None, "localhost:9092", None, session_plan=[tools_only], session_host_key="127.0.0.1:9092")
    assert _SessionWorker.instances == []


async def test_session_worker_stopped_when_readiness_fails(session_seams: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    """Teardown on every exit path (§10): a readiness deadline after a successful start must
    still stop the in-process worker."""

    async def deadline(*args: object, **kwargs: object) -> None:
        raise DevAgentError("the launched agents did not come online within 15.0s")

    monkeypatch.setattr(_dev_agents, "wait_agents_ready", deadline)
    with pytest.raises(DevAgentError, match="did not come online"):
        await run_chat_session(None, "localhost:9092", None, session_plan=[_target_nodes()], session_host_key="127.0.0.1:9092")
    (worker,) = _SessionWorker.instances
    assert worker.started and worker.stopped


async def test_session_worker_start_failure_surfaces_directly(
    session_seams: dict, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """§7: an in-process Worker.start() failure surfaces in the terminal (same process) and is
    exit-2 material for the command boundary."""

    async def broken_start(self: object) -> None:
        raise RuntimeError("kafka down")

    monkeypatch.setattr(_SessionWorker, "start", broken_start)
    with pytest.raises(DevAgentError, match="failed to start"):
        await run_chat_session(None, "localhost:9092", None, session_plan=[_target_nodes()], session_host_key="127.0.0.1:9092")
    assert "kafka down" in capsys.readouterr().err  # the traceback, surfaced directly


async def test_session_holds_the_flock_through_readiness_and_releases_before_the_repl(
    session_seams: dict, monkeypatch: pytest.MonkeyPatch, isolated_home: object
) -> None:
    """Chat flock scope (spec §9): evaluate→start→readiness under the agent-layer lock, released
    before the REPL opens."""
    import fcntl
    from pathlib import Path

    lock_path = Path(str(isolated_home)) / ".calfkit" / "dev-agents.lock"
    states: dict[str, bool] = {}

    def _held() -> bool:
        with lock_path.open("rb") as fd:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
            fcntl.flock(fd, fcntl.LOCK_UN)
            return False

    async def probing_wait(*args: object, **kwargs: object) -> None:
        states["during_readiness"] = _held()

    monkeypatch.setattr(_dev_agents, "wait_agents_ready", probing_wait)

    def probing_reader(_loop: object) -> object:
        async def read_line(_prompt: str) -> str:
            states.setdefault("at_repl", _held())
            return "q"

        return read_line

    monkeypatch.setattr("calfkit.cli._chat.make_reader", probing_reader)
    await run_chat_session(None, "localhost:9092", None, session_plan=[_target_nodes()], session_host_key="127.0.0.1:9092")
    assert states == {"during_readiness": True, "at_repl": False}


async def test_attach_only_sessions_exit_silently(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """No session_plan = today's attach behavior exactly: no ✦ narration on exit (spec §3.2)."""
    _patch_get_agents(monkeypatch, result={"alpha": _agent("alpha", "x")})
    monkeypatch.setattr("calfkit.cli._chat.make_reader", lambda _loop: _scripted(["q"]))
    await run_chat_session(None, "localhost:9092", None)
    assert "✦" not in capsys.readouterr().out


async def test_session_worker_stopped_when_the_repl_is_cancelled(session_seams: dict) -> None:
    """E9 (review round 1, Ryan-approved): a real Ctrl-C reaches the session as task
    cancellation while the reader is parked in the REPL — the finally must still stop the
    in-process worker (and the cancellation must propagate)."""
    import asyncio

    parked = asyncio.Event()

    def parked_reader(_loop: object) -> object:
        async def read_line(_prompt: str) -> str:
            parked.set()
            await asyncio.Event().wait()  # park forever until cancelled
            raise AssertionError("unreachable")

        return read_line

    import pytest as _pytest

    with _pytest.MonkeyPatch.context() as mp:
        mp.setattr("calfkit.cli._chat.make_reader", parked_reader)
        task = asyncio.create_task(run_chat_session(None, "localhost:9092", None, session_plan=[_target_nodes()], session_host_key="127.0.0.1:9092"))
        await asyncio.wait_for(parked.wait(), timeout=5.0)
        task.cancel()
        with _pytest.raises(asyncio.CancelledError):
            await task
    (worker,) = _SessionWorker.instances
    assert worker.started and worker.stopped


async def test_session_worker_stopped_when_the_repl_raises(session_seams: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    """E9: a mid-REPL failure (e.g. the mesh dying under the picker) still stops the worker and
    propagates to the command boundary."""
    _patch_get_agents(monkeypatch, raises=MeshUnavailableError("died mid-session", reason="reader_dead"))
    with pytest.raises(MeshUnavailableError):
        await run_chat_session(None, "localhost:9092", None, session_plan=[_target_nodes()], session_host_key="127.0.0.1:9092")
    (worker,) = _SessionWorker.instances
    assert worker.started and worker.stopped


async def test_session_failed_launch_prints_no_narration(
    session_seams: dict, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """E11: a launch that never reached the REPL reports its error, not ✦ narration (the
    repl_entered gate)."""

    async def deadline(*args: object, **kwargs: object) -> None:
        raise DevAgentError("the launched agents did not come online within 15.0s")

    monkeypatch.setattr(_dev_agents, "wait_agents_ready", deadline)
    with pytest.raises(DevAgentError):
        await run_chat_session(None, "localhost:9092", None, session_plan=[_target_nodes()], session_host_key="127.0.0.1:9092")
    assert "✦" not in capsys.readouterr().out


async def test_session_plan_without_host_key_is_a_programming_error(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ValueError, match="session_host_key"):
        await run_chat_session(None, "localhost:9092", None, session_plan=[])


async def test_session_shows_the_readiness_roster(session_seams: dict, capsys: pytest.CaptureFixture[str]) -> None:
    """PR2 adoption: the chat-TARGET launch shows a readiness roster titled by launched count + host."""
    plan = [_target_nodes()]
    session_seams["evaluate"] = ([], plan)
    await run_chat_session(None, "localhost:9092", None, session_plan=plan, session_host_key="127.0.0.1:9092")
    assert "Starting 1 node(s) on 127.0.0.1:9092" in capsys.readouterr().out


async def test_attach_uses_the_live_picker_on_an_interactive_terminal(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """On a TTY with no named agent, the live picker (which polls the mesh itself) replaces the
    static discover+pick; a quit returns without entering the REPL."""
    called: dict[str, object] = {}

    async def fake_live_pick(client: object, **_: object) -> None:
        called["client"] = client
        return None

    monkeypatch.setattr("calfkit.cli._chat.is_interactive", lambda: True)
    monkeypatch.setattr("calfkit.cli._chat.live_pick", fake_live_pick)
    _patch_get_agents(monkeypatch, result={"a": _agent("a", None)})
    await run_chat_session(None, "localhost:9092", None)
    assert "client" in called  # the live picker was used
    assert "Discovering agents" not in capsys.readouterr().out  # the static path was skipped


async def test_attach_falls_back_to_the_static_picker_when_not_interactive(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Off a TTY (piped/redirected/CI), the live picker is bypassed for the static discover+numbered
    menu — so the cbreak-mode live picker never runs where it cannot render."""

    async def boom_live_pick(client: object, **_: object) -> None:
        raise AssertionError("live_pick must not run when the terminal is not interactive")

    monkeypatch.setattr("calfkit.cli._chat.is_interactive", lambda: False)
    monkeypatch.setattr("calfkit.cli._chat.live_pick", boom_live_pick)
    monkeypatch.setattr("calfkit.cli._chat.make_reader", lambda _loop: _scripted(["q"]))
    _patch_get_agents(monkeypatch, result={"alpha": _agent("alpha", "x")})
    await run_chat_session(None, "localhost:9092", None)
    assert "Discovering agents" in capsys.readouterr().out  # the static path was taken
