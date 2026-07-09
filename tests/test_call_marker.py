"""``ToolCallMarker`` / ``CallMarker`` — the typed, kind-discriminated call-origin marker (echo-rail
spec D1). The leaf model the rail stamps on ``CallFrame.marker`` and echoes verbatim on the reply.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from calfkit.models.marker import CallMarker, ToolCallMarker


class TestToolCallMarker:
    def test_carries_the_complete_call_identity(self) -> None:
        m = ToolCallMarker(tool_name="web_search", tool_call_id="c1", args={"q": "kafka"})
        assert m.kind == "tool_call"
        assert m.tool_name == "web_search"
        assert m.tool_call_id == "c1"
        assert m.args == {"q": "kafka"}

    def test_args_defaults_none_and_tolerates_none(self) -> None:
        # ``| None`` is decode-tolerance (never producer-set): a marker off the wire may carry no args.
        assert ToolCallMarker(tool_name="t", tool_call_id="i").args is None
        assert ToolCallMarker(tool_name="t", tool_call_id="i", args=None).args is None

    def test_is_frozen(self) -> None:
        m = ToolCallMarker(tool_name="t", tool_call_id="i")
        with pytest.raises(ValidationError):
            m.tool_name = "other"  # type: ignore[misc]

    def test_call_marker_is_the_single_member(self) -> None:
        assert CallMarker is ToolCallMarker

    def test_wire_round_trip_preserves_the_typed_marker(self) -> None:
        m = ToolCallMarker(tool_name="message_agent", tool_call_id="m1", args={"name": "billing", "message": "hi"})
        assert ToolCallMarker.model_validate_json(m.model_dump_json()) == m


class TestClosedUnionDecodePosture:
    """D10 (deploy-together): an unknown ``kind`` rejects; an absent ``kind`` decodes to the default."""

    def test_unknown_kind_is_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ToolCallMarker.model_validate({"kind": "nope", "tool_name": "t", "tool_call_id": "i"})

    def test_absent_kind_defaults_to_tool_call(self) -> None:
        # old→new decode safety: a producer that never wrote ``kind`` still validates.
        m = ToolCallMarker.model_validate({"tool_name": "t", "tool_call_id": "i"})
        assert m.kind == "tool_call"


class TestNotHashable:
    def test_dict_args_make_the_marker_unhashable(self) -> None:
        # ``frozen`` implies immutability, NOT hashability: the ``args`` dict makes ``hash()`` raise, so
        # the marker can never be a set/dict key. Inert (``CallFrame`` is never hashed/keyed).
        with pytest.raises(TypeError):
            hash(ToolCallMarker(tool_name="t", tool_call_id="i", args={"a": 1}))
