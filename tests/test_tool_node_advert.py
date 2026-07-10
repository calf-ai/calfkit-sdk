"""A function tool node advertises its single tool on the capability control plane.

Like ``MCPToolboxNode``, a tool node is a content contributor: it declares one
``@advertises`` factory that the worker-owned ``ControlPlanePublisher`` pulls each
heartbeat tick. Unlike MCP, its schema is static (no session, no cache), so the factory
reads ``tool_schema`` directly and ``content_updated_at`` is the process boot time.
The heartbeat loop + tombstone live in the substrate's publisher (tested in
``test_controlplane_publisher.py``), so they are not re-tested here.
"""

from __future__ import annotations

from datetime import datetime, timezone

from calfkit.client import Client
from calfkit.controlplane import ControlPlaneStamp
from calfkit.controlplane.publisher import control_plane_writer_key
from calfkit.models.capability import CAPABILITY_TOPIC, CapabilityRecord, record_to_bindings
from calfkit.nodes.tool import ToolNodeDef, agent_tool
from calfkit.worker.worker import Worker


def make_node(name: str = "add") -> ToolNodeDef:
    def add(a: int, b: int) -> int:
        """Add two integers."""
        return a + b

    return agent_tool(add, name=name)


def make_stamp(*, node_kind: str = "tool") -> ControlPlaneStamp:
    now = datetime.now(tz=timezone.utc)
    return ControlPlaneStamp(started_at=now, last_heartbeat_at=now, heartbeat_interval=30.0, node_kind=node_kind)


class TestAdvertDeclaration:
    def test_declares_one_capability_advert(self) -> None:
        adverts = type(make_node())._adverts
        assert CAPABILITY_TOPIC in adverts
        assert adverts[CAPABILITY_TOPIC].record is CapabilityRecord

    def test_advert_factory_is_a_bound_method(self) -> None:
        factories = make_node().control_plane_adverts()
        assert CAPABILITY_TOPIC in factories
        assert callable(factories[CAPABILITY_TOPIC])


class TestCapabilityFactory:
    def test_factory_builds_one_tool_record_from_schema_and_stamp(self) -> None:
        node = make_node("add")
        stamp = make_stamp()
        record = node._capability_advert(stamp)
        assert isinstance(record, CapabilityRecord)
        # the worker-stamped fields ride through verbatim
        assert record.started_at == stamp.started_at
        assert record.last_heartbeat_at == stamp.last_heartbeat_at
        assert record.heartbeat_interval == 30.0
        assert record.node_kind == "tool"
        # content: exactly one tool; dispatch = the node's public inbox
        assert record.dispatch_topic == "tool.add.input"
        assert [t.name for t in record.tools] == ["add"]
        assert record.tools[0].description == "Add two integers."
        assert record.tools[0].parameters_json_schema == node.tool_schema.parameters_json_schema

    def test_content_updated_at_is_the_boot_time(self) -> None:
        # Static schema: content currency == process boot time, stable across ticks.
        node = make_node()
        stamp = make_stamp()
        assert node._capability_advert(stamp).content_updated_at == stamp.started_at

    def test_node_kind_rides_the_stamp(self) -> None:
        # The factory never sets node_kind itself; it rides on the worker stamp, so the
        # advertised record carries whatever kind the worker stamped (a tool node -> "tool").
        record = make_node()._capability_advert(make_stamp(node_kind="tool"))
        assert record.node_kind == "tool"


class TestWorkerAutoRegistration:
    def test_hosted_tool_node_registers_the_control_plane(self) -> None:
        # Deploying a tool node trips the worker's @advertises auto-wiring: the publisher
        # and the calf.capabilities writer are registered with zero user config.
        worker = Worker(Client.connect("kafka:9092"), nodes=[make_node()])
        worker._maybe_register_control_plane()
        names = [name for name, _ in worker._resource_cms()]
        assert control_plane_writer_key(CAPABILITY_TOPIC) in names
        assert worker._control_plane_publisher is not None


class TestEagerDiscoveredParity:
    """§7 fidelity boundary: the ToolDefinition an agent sees is identical whether the tool
    node is consumed eagerly (a live node passed in ``tools=``) or discovered (a ``Tools``
    handle). The only intended difference is the validation locus. A future ``Tool`` knob
    that breaks this parity must fail loudly here.
    """

    def test_eager_and_discovered_tool_def_are_identical(self) -> None:
        node = make_node("add")
        eager = node.tool_bindings()[0]  # live path: schema baked in, with a local validator
        discovered = record_to_bindings(node._capability_advert(make_stamp()), name="add")[0]  # discovered; node_kind="tool" stays BARE (C2)
        # Same ToolDefinition — name, description, JSON schema, and every defaulted field.
        assert discovered.tool_def == eager.tool_def
        # The only difference is the validation LOCUS, not whether validation happens: the eager
        # binding carries a signature validator; the discovered binding has none, so the agent
        # falls back to a validator built from the advertised schema. Same dispatch rail either way.
        assert eager.validator is not None
        assert discovered.validator is None
