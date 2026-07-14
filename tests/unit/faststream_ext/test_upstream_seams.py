"""Canary tests for every FastStream/anyio seam ``calfkit._faststream_ext`` touches.

``_faststream_ext`` subclasses ``faststream._internal`` subscriber machinery (FastStream has
no injection seam for custom subscriber classes). These canaries are executable
documentation of exactly which upstream facts the extension relies on, pinned against
``faststream>=0.7.1,<0.8``: a lock-file upgrade that moves any seam fails HERE in CI, not
in production at runtime.

Two quarantine canaries guard the package's contracts (see its docstring):

- *independence*: the extension imports nothing from calfkit (it is a pure FastStream
  extension; calfkit is a consumer), and
- *internals-coupling*: ``faststream._internal`` imports exist nowhere in calfkit outside
  the extension package.
"""

from __future__ import annotations

import asyncio
import inspect
import re
from pathlib import Path
from types import SimpleNamespace

import anyio
import pytest
from faststream._internal.broker.registrator import Registrator
from faststream._internal.endpoint.subscriber import SubscriberSpecification
from faststream._internal.endpoint.subscriber.call_item import CallsCollection
from faststream._internal.endpoint.subscriber.mixins import TasksMixin
from faststream._internal.endpoint.subscriber.supervisor import TaskCallbackSupervisor
from faststream._internal.endpoint.subscriber.usecase import SubscriberUsecase
from faststream._internal.endpoint.subscriber.utils import MultiLock
from faststream.kafka import KafkaBroker
from faststream.kafka.subscriber.config import KafkaSubscriberConfig
from faststream.kafka.subscriber.factory import (
    _validate_input_for_misconfigure,
    create_subscriber,
)
from faststream.kafka.subscriber.specification import KafkaSubscriberSpecification
from faststream.kafka.subscriber.usecase import DefaultSubscriber, LogicSubscriber

import calfkit
import calfkit._faststream_ext as _ext_pkg

EXT_DIR = Path(_ext_pkg.__file__).parent
CALFKIT_DIR = Path(calfkit.__file__).parent

# The per-subscriber connection-arg surface the stock KafkaRegistrator.subscriber forwards
# into AIOKafkaConsumer (kafka/broker/registrator.py, the `connection_args={...}` literal).
# This — not aiokafka's constructor signature — is the extension's connection allow-list
# source (spec D15): aiokafka's signature would over-admit broker-level security kwargs
# (sasl_*, ssl_context, security_protocol) and enable_auto_commit (which collides with the
# value KafkaSubscriberConfig derives from ack_first), while under-admitting client_rack
# (registrator-supported but absent from AIOKafkaConsumer.__init__).
EXPECTED_CONNECTION_KEYS = frozenset(
    {
        "group_instance_id",
        "key_deserializer",
        "value_deserializer",
        "fetch_max_wait_ms",
        "fetch_max_bytes",
        "fetch_min_bytes",
        "max_partition_fetch_bytes",
        "auto_offset_reset",
        "auto_commit_interval_ms",
        "check_crcs",
        "partition_assignment_strategy",
        "max_poll_interval_ms",
        "rebalance_timeout_ms",
        "session_timeout_ms",
        "heartbeat_interval_ms",
        "consumer_timeout_ms",
        "max_poll_records",
        "exclude_internal_topics",
        "isolation_level",
        "client_rack",
    }
)

# Exactly what the extension's factory mirror passes to its constructor wiring — asserted
# as the exact parameter set of upstream create_subscriber so an upstream add/remove/rename
# trips CI at lock-upgrade time.
EXPECTED_CREATE_SUBSCRIBER_PARAMS = frozenset(
    {
        "topics",
        "batch",
        "batch_timeout_ms",
        "max_records",
        "group_id",
        "listener",
        "pattern",
        "connection_args",
        "partitions",
        "ack_policy",
        "max_workers",
        "no_reply",
        "config",
        "title_",
        "description_",
        "include_in_schema",
    }
)

_IMPORT_RE = re.compile(r"^\s*(?:from|import)\s+([a-zA-Z0-9_.]+)", re.MULTILINE)


def _imports_in(path: Path) -> set[str]:
    return set(_IMPORT_RE.findall(path.read_text()))


# ---------------------------------------------------------------------------
# Quarantine canaries
# ---------------------------------------------------------------------------


def test_extension_imports_nothing_from_calfkit() -> None:
    """Independence contract (spec D16): the extension is a pure FastStream extension."""
    offenders = {f"{py.name}: {mod}" for py in EXT_DIR.rglob("*.py") for mod in _imports_in(py) if mod == "calfkit" or mod.startswith("calfkit.")}
    assert not offenders, f"extension must not import calfkit: {sorted(offenders)}"


def test_faststream_internals_quarantined_to_extension() -> None:
    """Internals-coupling contract: `faststream._internal` appears ONLY in the extension."""
    offenders = {
        f"{py.relative_to(CALFKIT_DIR)}: {mod}"
        for py in CALFKIT_DIR.rglob("*.py")
        if not py.is_relative_to(EXT_DIR)
        for mod in _imports_in(py)
        if mod.startswith("faststream._internal")
    }
    assert not offenders, f"faststream._internal leaked outside _faststream_ext: {sorted(offenders)}"


def test_every_extension_internals_import_has_a_canary_here() -> None:
    """Mechanical completeness: each `faststream._internal` module the extension imports
    is also imported by this canary module, so a moved seam fails here first."""
    canary_imports = _imports_in(Path(__file__))
    ext_internal_imports = {mod for py in EXT_DIR.rglob("*.py") for mod in _imports_in(py) if mod.startswith("faststream._internal")}
    missing = ext_internal_imports - canary_imports
    assert not missing, f"extension imports internals with no canary coverage: {sorted(missing)}"


# ---------------------------------------------------------------------------
# Class-hierarchy seams
# ---------------------------------------------------------------------------


def test_tasksmixin_is_already_in_defaultsubscriber_mro() -> None:
    """Why KeyOrderedSubscriber subclasses DefaultSubscriber ALONE: listing TasksMixin as an
    additional (more-derived) base is a C3 violation that fails at class-definition time."""
    assert TasksMixin in DefaultSubscriber.__mro__
    with pytest.raises(TypeError, match="MRO|mro"):
        type("Doomed", (TasksMixin, DefaultSubscriber), {})


def test_generic_registrator_subscriber_registers_prebuilt_instances() -> None:
    """The mixin must call Registrator.subscriber(self, sub) UNBOUND: from a mixin above
    KafkaBroker, `super().subscriber(...)` resolves to the Kafka *topic-builder* overload."""
    params = list(inspect.signature(Registrator.subscriber).parameters)
    assert params == ["self", "subscriber", "persistent"]
    # And the Kafka override is a different, *topics-shaped builder — the trap.
    kafka_params = list(inspect.signature(KafkaBroker.subscriber).parameters.values())
    assert kafka_params[0].name == "self"
    assert kafka_params[1].kind is inspect.Parameter.VAR_POSITIONAL, "KafkaBroker.subscriber is expected to be the *topics builder overload"


# ---------------------------------------------------------------------------
# Factory-mirror seams
# ---------------------------------------------------------------------------


def test_create_subscriber_parameter_set_is_exactly_what_the_mirror_replicates() -> None:
    assert frozenset(inspect.signature(create_subscriber).parameters) == EXPECTED_CREATE_SUBSCRIBER_PARAMS


def test_misconfigure_validator_signature() -> None:
    assert frozenset(inspect.signature(_validate_input_for_misconfigure).parameters) == {
        "topics",
        "ack_policy",
        "max_workers",
        "pattern",
        "partitions",
    }


def test_connection_allowlist_matches_stock_registrator_surface() -> None:
    """Spec D15: the connection allow-list is the registrator's connection_args key set.
    Every key must be a named parameter of the stock builder, and the builder must have
    gained no new connection-shaped parameters we'd be silently missing."""
    kafka_params = frozenset(inspect.signature(KafkaBroker.subscriber).parameters)
    missing = EXPECTED_CONNECTION_KEYS - kafka_params
    assert not missing, f"registrator dropped connection params: {sorted(missing)}"
    # Reconstruct the connection_args literal from source: the keys assigned inside the
    # `connection_args={...}` dict in the builder body must equal our constant.
    src = inspect.getsource(KafkaBroker.subscriber)
    body = src[src.index("connection_args={") :]
    body = body[: body.index("},")]
    keys_in_source = set(re.findall(r'"([a-z_]+)":', body)) | set(re.findall(r'\{"([a-z_]+)":', body))
    assert keys_in_source == set(EXPECTED_CONNECTION_KEYS)


def test_subscriber_config_derives_auto_commit_from_ack_first() -> None:
    """The mirror MUST build a real KafkaSubscriberConfig: its __post_init__ sets
    enable_auto_commit from ack_first (ACK_FIRST resolution incl. the EMPTY default)."""
    config = KafkaSubscriberConfig(topics=("t",), _outer_config=SimpleNamespace(ack_policy=None))
    # Default (EMPTY) resolves to ACK_FIRST when the outer config carries no policy.
    config_default = KafkaSubscriberConfig(topics=("t",))
    assert config_default.ack_first is True
    assert config_default.connection_args["enable_auto_commit"] is True
    del config


def test_specification_constructor_shape() -> None:
    params = list(inspect.signature(KafkaSubscriberSpecification.__init__).parameters)
    assert params[1:] == ["_outer_config", "specification_config", "calls"]
    assert issubclass(KafkaSubscriberSpecification, SubscriberSpecification)  # the ctor's annotated type
    CallsCollection()  # the factory mirror constructs it with no arguments


def test_add_call_accepts_parser_decoder_dependencies_codec() -> None:
    params = frozenset(inspect.signature(SubscriberUsecase.add_call).parameters)
    assert {"parser_", "decoder_", "dependencies_", "codec_"} <= params


# ---------------------------------------------------------------------------
# Dispatch-seam behavior
# ---------------------------------------------------------------------------


def test_read_loop_hands_off_via_consume_one() -> None:
    src = inspect.getsource(LogicSubscriber._run_consume_loop)
    assert "consume_one" in src and "get_msg" in src
    assert "while self.running" in src


def test_graceful_timeout_resolves_via_outer_config() -> None:
    broker = KafkaBroker()  # never connected
    sub = broker.subscriber("canary-topic")
    assert sub._outer_config.graceful_timeout == pytest.approx(15.0)


def test_subscriber_stop_waits_multilock_then_tasksmixin_cancels_without_awaiting() -> None:
    stop_src = inspect.getsource(SubscriberUsecase.stop)
    assert "wait_release" in stop_src
    tasks_stop_src = inspect.getsource(TasksMixin.stop)
    assert "cancel()" in tasks_stop_src
    assert "gather" not in tasks_stop_src  # cancel-without-await: why _do_stop adds a barrier


async def test_multilock_wait_release_skips_on_falsy_timeout() -> None:
    """_do_stop mirrors this falsy-means-no-wait semantics for its drain (spec §6.4)."""
    lock = MultiLock()
    with lock:  # an in-flight entry that would block a real wait
        await asyncio.wait_for(lock.wait_release(0), timeout=1.0)
        await asyncio.wait_for(lock.wait_release(None), timeout=1.0)


async def test_supervisor_rearms_failed_task_with_same_args_and_ignores_cancel() -> None:
    """D12: a crashed lane worker restarts with the SAME (stream, limiter) args; cancelled
    and normally-finished tasks are not restarted."""
    rearmed: list[tuple] = []
    fake_logger = SimpleNamespace(log=lambda *a, **k: None)
    subscriber = SimpleNamespace(
        add_task=lambda func, args=None, kwargs=None: rearmed.append((func, args, kwargs)),
        _outer_config=SimpleNamespace(logger=fake_logger),
    )

    async def boom(marker: object) -> None:
        raise RuntimeError("lane died")

    marker = object()
    task = asyncio.get_running_loop().create_task(boom(marker))
    await asyncio.gather(task, return_exceptions=True)
    TaskCallbackSupervisor(boom, (marker,), None, subscriber)(task)
    assert rearmed == [(boom, (marker,), {})]

    async def cancelled_coro() -> None:
        await asyncio.sleep(30)

    rearmed.clear()
    task2 = asyncio.get_running_loop().create_task(cancelled_coro())
    await asyncio.sleep(0)
    task2.cancel()
    await asyncio.gather(task2, return_exceptions=True)
    TaskCallbackSupervisor(cancelled_coro, None, None, subscriber)(task2)
    assert rearmed == []

    async def fine() -> None:
        return None

    task3 = asyncio.get_running_loop().create_task(fine())
    await task3
    TaskCallbackSupervisor(fine, None, None, subscriber)(task3)
    assert rearmed == []


async def test_manually_raised_cancellederror_counts_as_cancelled_for_supervisor() -> None:
    """consume_one's _stop_initiated guard raises CancelledError to end a racing read loop;
    the supervisor must treat that as a cancellation (no restart storm)."""

    async def self_cancel() -> None:
        raise asyncio.CancelledError

    task = asyncio.get_running_loop().create_task(self_cancel())
    await asyncio.gather(task, return_exceptions=True)
    assert task.cancelled()


# ---------------------------------------------------------------------------
# anyio primitive semantics (why D3/D7/D13 hold)
# ---------------------------------------------------------------------------


async def test_anyio_memory_stream_close_then_drain() -> None:
    """D7: closing the send stream lets the receiver drain every buffered item, then end."""
    send, receive = anyio.create_memory_object_stream(max_buffer_size=4)
    for i in range(3):
        send.send_nowait(i)
    send.close()
    assert [item async for item in receive] == [0, 1, 2]


async def test_anyio_semaphore_cross_task_release_and_max_value_tripwire() -> None:
    """D13: lane workers release a semaphore acquired by the read loop (cross-task legal);
    D3: max_value makes any double-release raise at the bug site."""
    sem = anyio.Semaphore(1, max_value=1)
    await sem.acquire()

    async def release_from_other_task() -> None:
        sem.release()

    await asyncio.get_running_loop().create_task(release_from_other_task())
    assert sem.value == 1
    with pytest.raises(ValueError):
        sem.release()


async def test_capacitylimiter_rejects_foreign_release() -> None:
    """Why D13 picked Semaphore: CapacityLimiter enforces same-task release."""
    limiter = anyio.CapacityLimiter(1)
    await limiter.acquire()

    async def foreign_release() -> None:
        limiter.release()

    task = asyncio.get_running_loop().create_task(foreign_release())
    done = await asyncio.gather(task, return_exceptions=True)
    limiter.release()
    assert isinstance(done[0], RuntimeError)


async def test_anyio_send_nowait_raises_wouldblock_when_full() -> None:
    """The failure mode D3's buffer-==-bound invariant provably avoids."""
    send, receive = anyio.create_memory_object_stream(max_buffer_size=1)
    send.send_nowait("a")
    with pytest.raises(anyio.WouldBlock):
        send.send_nowait("b")
    del receive
