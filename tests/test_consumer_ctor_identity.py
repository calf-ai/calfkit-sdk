"""The consumer node surfaces take their identity as ``name`` (not ``node_id``).

Both the ``@consumer`` decorator factory and the ``ConsumerNode`` class map ``name`` onto
the base node's ``node_id`` storage, mirroring ``Agent`` (ADR-0020, PR #272) and the MCP
node types. ``.name`` and ``.node_id`` both read the same value. A surface-only rename — the
legacy ``node_id=`` keyword is a clean pre-1.0 break on both surfaces. The factory's
default-id derivation (``consumer_<fn>`` when no name is given) is unchanged.
"""

from __future__ import annotations

import pytest

from calfkit.nodes import ConsumerNode, consumer


def test_factory_constructs_with_name_keyword() -> None:
    @consumer(subscribe_topics="t", name="custom_sink")
    def sink(ctx: object) -> None:  # pragma: no cover - body never runs
        return None

    assert sink.name == "custom_sink"
    assert sink.node_id == "custom_sink"


def test_factory_default_id_still_derives_from_fn_name() -> None:
    @consumer(subscribe_topics="t")
    def save_weather(ctx: object) -> None:  # pragma: no cover - body never runs
        return None

    assert save_weather.name == "consumer_save_weather"


def test_class_constructs_with_name_keyword() -> None:
    node = ConsumerNode(name="obs", subscribe_topics="t", consume_fn=lambda ctx: None)
    assert node.name == "obs"
    assert node.node_id == "obs"


def test_factory_rejects_legacy_node_id_keyword() -> None:
    with pytest.raises(TypeError):

        @consumer(subscribe_topics="t", node_id="custom_sink")
        def sink(ctx: object) -> None:  # pragma: no cover - body never runs
            return None


def test_class_rejects_legacy_node_id_keyword() -> None:
    with pytest.raises(TypeError):
        ConsumerNode(node_id="obs", subscribe_topics="t", consume_fn=lambda ctx: None)
