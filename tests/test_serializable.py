"""Roundtrip serialization tests for all network-serialized models.

These tests verify that every model used in the Kafka wire format survives
``model_dump_json()`` → ``model_validate_json()`` (or the TypeAdapter equivalent
for standalone dataclasses).  The existing test suite uses ``TestKafkaBroker``
which passes objects in-memory, bypassing actual JSON serde — so these tests
catch missing fields, broken discriminated unions, dataclass quirks, etc.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Roundtrip helpers
# ---------------------------------------------------------------------------


def assert_json_roundtrip(instance):
    """Assert a Pydantic BaseModel survives JSON roundtrip."""
    json_bytes = instance.model_dump_json()
    cls = type(instance)
    reconstructed = cls.model_validate_json(json_bytes)
    assert reconstructed == instance, f"Roundtrip mismatch for {cls.__name__}"


# ---------------------------------------------------------------------------
# Factory functions
# ---------------------------------------------------------------------------


def test_envelope_serialization_roundtrip(make_envelope):
    assert_json_roundtrip(make_envelope)


def test_envelope_with_null_callback_topic_roundtrips():
    """A fire-and-forget bottom call frame (``callback_topic=None``) must survive
    a real JSON hop unchanged. ``None`` means "no requester to return to"; the
    nullable wire field is what lets the worker's terminal guard skip the
    point-to-point callback publish (``BaseNodeDef._publish_action``)."""
    from calfkit.models.envelope import Envelope
    from calfkit.models.session_context import CallFrame, CallFrameStack, SessionRunContext, WorkflowState
    from calfkit.models.state import State

    stack = CallFrameStack()
    stack.push(CallFrame(target_topic="agent.input", callback_topic=None))
    envelope = Envelope(
        context=SessionRunContext(state=State(), deps={}),
        internal_workflow_state=WorkflowState(call_stack=stack),
    )

    assert_json_roundtrip(envelope)

    restored = Envelope.model_validate_json(envelope.model_dump_json())
    assert restored.internal_workflow_state.current_frame.callback_topic is None


def test_nested_deps_survive_json_roundtrip_and_correlation_id_stays_off_the_wire():
    """The deps dict (incl. nested/list values) must survive a real JSON hop
    verbatim, while ``correlation_id`` (a transport-sourced PrivateAttr) must NOT
    appear in the serialized envelope body and is unset after a body-only
    round-trip."""
    from calfkit.models.envelope import Envelope
    from calfkit.models.session_context import CallFrameStack, SessionRunContext, WorkflowState
    from calfkit.models.state import State

    deps = {"discord": {"channel_id": 7, "author": "ana"}, "tags": ["a", "b"]}
    ctx = SessionRunContext(state=State(), deps=deps)
    ctx._correlation_id = "cid-wire"  # stamped as a handler would
    envelope = Envelope(context=ctx, internal_workflow_state=WorkflowState(call_stack=CallFrameStack()))

    body = envelope.model_dump_json()
    assert "correlation_id" not in body, "correlation_id must not ride on the envelope body"
    assert "provided_deps" not in body, "the old Deps wrapper must not appear on the wire"

    restored = Envelope.model_validate_json(body)
    assert restored.context.deps == deps  # nested + list values preserved
    # PrivateAttr is not serialized, so it does not survive a body-only round-trip.
    assert restored.context._correlation_id is None
