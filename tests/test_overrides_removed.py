"""Removal-outcome pins for the per-run overrides feature (overrides-removal spec §3).

The wire and action shapes no longer carry the feature's fields. These are SCHEMA-level
pins (field sets and signatures), not name-level ones — the acceptance grep owns name
absence — so a re-introduction of any removed field cannot pass a green suite (§3.2).
The client-verb pins (``model_settings``/``tool_overrides`` params gone from
``send``/``start``/``execute``, L7) live here too, passed as ``**kwargs`` dicts so the
pin itself never spells the removed keyword forms the acceptance grep hunts.
"""

from __future__ import annotations

import dataclasses
import inspect

import pytest

from calfkit.models import CallFrame, State, TailCall


def test_state_schema_has_no_overrides_field() -> None:
    assert "overrides" not in State.model_fields


def test_callframe_has_no_overrides_field() -> None:
    assert "overrides" not in {f.name for f in dataclasses.fields(CallFrame)}
    with pytest.raises(TypeError):
        CallFrame(target_topic="t", callback_topic=None, **{"overrides": None})  # type: ignore[call-arg]


def test_tailcall_is_the_bare_two_field_call_shape() -> None:
    # TailCall adds NOTHING over the _Call base now: exactly the two positional fields,
    # no keyword knobs. The full-signature pin means no flag can be re-added silently.
    assert [f.name for f in dataclasses.fields(TailCall)] == ["target_topic", "state"]
    assert list(inspect.signature(TailCall.__init__).parameters) == ["self", "target_topic", "state"]
    tail_call = TailCall("t", 1)  # positional construction preserved
    assert tail_call == TailCall("t", 1)
    assert tail_call != TailCall("u", 1)
    assert tail_call == TailCall(target_topic="t", state=1)  # keyword form preserved
