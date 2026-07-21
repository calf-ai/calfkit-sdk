"""Tests for ToolBinding + ToolProvider — the agent-facing tool contract.

A ToolBinding carries exactly what ``BaseAgentNodeDef.run`` consumes per tool:
the ``ToolDefinition`` advertised to the LLM, the dispatch topic for the
``Call``, and an optional args validator. ``ToolProvider`` is the structural
contract for anything that contributes bindings (tool node, toolbox, MCP
toolbox) — no shared base class required.
"""

import pytest
from pydantic import ValidationError

from calfkit._vendor.pydantic_ai.tools import ToolDefinition
from calfkit.models.tool_dispatch import ToolBinding, ToolProvider


def make_tool_def(name: str = "get_weather") -> ToolDefinition:
    return ToolDefinition(
        name=name,
        description="Get the weather for a city",
        parameters_json_schema={"type": "object", "properties": {"city": {"type": "string"}}},
    )


class TestToolBinding:
    def test_constructs_with_tool_def_and_topic_and_validator_defaults_to_none(self) -> None:
        binding = ToolBinding(tool_def=make_tool_def(), dispatch_topic="tool.get_weather.input")
        assert binding.tool_def.name == "get_weather"
        assert binding.dispatch_topic == "tool.get_weather.input"
        assert binding.validator is None

    def test_name_property_delegates_to_tool_def(self) -> None:
        binding = ToolBinding(tool_def=make_tool_def("geocode"), dispatch_topic="tool.geocode.input")
        assert binding.name == "geocode"

    def test_holds_a_callable_validator(self) -> None:
        def validate(args: dict) -> dict:
            return args

        binding = ToolBinding(tool_def=make_tool_def(), dispatch_topic="t", validator=validate)
        assert binding.validator is validate
        assert binding.validator({"city": "Oslo"}) == {"city": "Oslo"}

    def test_is_frozen(self) -> None:
        binding = ToolBinding(tool_def=make_tool_def(), dispatch_topic="t")
        with pytest.raises(ValidationError):
            binding.dispatch_topic = "other"  # type: ignore[misc]

    def test_rejects_empty_dispatch_topic(self) -> None:
        # A binding without a topic is undispatchable — the agent-side
        # "unreachable tool" guard exists only because the legacy schema type
        # could not enforce this at construction.
        with pytest.raises(ValueError, match="dispatch_topic"):
            ToolBinding(tool_def=make_tool_def(), dispatch_topic="")


class TestToolBindingWireForm:
    """ToolBinding doubles as the wire model for per-run tool overrides: the
    ``validator`` callable is process-local and must never serialize, so a
    deserialized binding always carries ``validator=None`` (the agent then
    validates its args against the advertised schema at dispatch, not here)."""

    def make_binding_with_validator(self) -> ToolBinding:
        return ToolBinding(
            tool_def=make_tool_def(),
            dispatch_topic="tool.get_weather.input",
            validator=lambda args: args,
        )

    def test_model_dump_excludes_validator(self) -> None:
        dumped = self.make_binding_with_validator().model_dump()
        assert "validator" not in dumped
        assert dumped["dispatch_topic"] == "tool.get_weather.input"
        assert dumped["tool_def"]["name"] == "get_weather"

    def test_json_round_trip_strips_validator_and_preserves_tool_def(self) -> None:
        binding = self.make_binding_with_validator()
        restored = ToolBinding.model_validate_json(binding.model_dump_json())
        assert restored.validator is None
        assert restored.name == "get_weather"
        assert restored.dispatch_topic == binding.dispatch_topic
        assert restored.tool_def.parameters_json_schema == binding.tool_def.parameters_json_schema


class TestToolProvider:
    def test_object_with_tool_bindings_method_satisfies_protocol(self) -> None:
        class FakeToolbox:
            def tool_bindings(self) -> list[ToolBinding]:
                return [ToolBinding(tool_def=make_tool_def(), dispatch_topic="toolbox.input")]

        assert isinstance(FakeToolbox(), ToolProvider)

    def test_object_without_tool_bindings_does_not_satisfy_protocol(self) -> None:
        class NotAProvider:
            pass

        assert not isinstance(NotAProvider(), ToolProvider)

    def test_a_binding_itself_is_not_a_provider(self) -> None:
        binding = ToolBinding(tool_def=make_tool_def(), dispatch_topic="t")
        assert not isinstance(binding, ToolProvider)


class TestToolNodeDefProvidesBindings:
    def test_tool_node_yields_one_binding_with_topic_and_validator(self) -> None:
        from pydantic import ValidationError

        from calfkit.nodes.tool import ToolNodeDef

        def get_weather(city: str) -> str:
            return f"sunny in {city}"

        node = ToolNodeDef.create_tool_node(
            func=get_weather,
            subscribe_topics="tool.get_weather.input",
            publish_topic="tool.get_weather.output",
        )
        bindings = node.tool_bindings()
        assert len(bindings) == 1
        binding = bindings[0]
        assert binding.name == "get_weather"
        assert binding.tool_def is node.tool_schema
        assert binding.dispatch_topic == "tool.get_weather.input"
        assert binding.validator is not None
        binding.validator({"city": "Oslo"})  # valid args pass
        with pytest.raises(ValidationError):
            binding.validator({"city": 0xBAD_BAD, "bogus": True})

    def test_tool_node_satisfies_tool_provider_protocol(self) -> None:
        from calfkit.nodes.tool import ToolNodeDef

        def fn(x: int) -> int:
            return x

        node = ToolNodeDef.create_tool_node(func=fn, subscribe_topics="t.in", publish_topic="t.out")
        assert isinstance(node, ToolProvider)


class TestToolNodeIdentity:
    """The tool name drives node_id (the capability key), the LLM tool name, and topics.

    No `tool_` prefix: `node_id == tool name`, so an agent can reference the node by the
    same name it calls. `name=` overrides all three without renaming the function.
    """

    def test_agent_tool_node_id_is_the_function_name(self) -> None:
        from calfkit.nodes.tool import agent_tool

        def get_weather(city: str) -> str:
            return city

        node = agent_tool(get_weather)
        assert node.node_id == "get_weather"  # no tool_ prefix
        assert node.tool_schema.name == "get_weather"  # capability key == LLM tool name
        assert node.subscribe_topics == ["tool.get_weather.input"]
        assert node.publish_topic == "tool.get_weather.output"

    def test_agent_tool_name_override_drives_all_three_identities(self) -> None:
        from calfkit.nodes.tool import agent_tool

        def get_weather(city: str) -> str:
            return city

        node = agent_tool(get_weather, name="weather")
        assert node.node_id == "weather"
        assert node.tool_schema.name == "weather"
        assert node.subscribe_topics == ["tool.weather.input"]
        assert node.publish_topic == "tool.weather.output"

    def test_agent_tool_decorator_with_args_drives_all_three_identities(self) -> None:
        from calfkit.nodes.tool import ToolNodeDef, agent_tool

        @agent_tool(name="weather")
        def get_weather(city: str) -> str:
            return city

        assert isinstance(get_weather, ToolNodeDef)  # the decorator replaces the function with the node
        assert get_weather.node_id == "weather"
        assert get_weather.tool_schema.name == "weather"
        assert get_weather.subscribe_topics == ["tool.weather.input"]
        assert get_weather.publish_topic == "tool.weather.output"

    def test_agent_tool_decorator_empty_parens_uses_function_name(self) -> None:
        from calfkit.nodes.tool import agent_tool

        @agent_tool()
        def get_weather(city: str) -> str:
            return city

        assert get_weather.node_id == "get_weather"
        assert get_weather.tool_schema.name == "get_weather"
        assert get_weather.subscribe_topics == ["tool.get_weather.input"]

    def test_create_tool_node_name_override(self) -> None:
        from calfkit.nodes.tool import ToolNodeDef

        def fn(x: int) -> int:
            return x

        node = ToolNodeDef.create_tool_node(func=fn, subscribe_topics="t.in", publish_topic="t.out", name="renamed")
        assert node.node_id == "renamed"
        assert node.tool_schema.name == "renamed"

    def test_empty_name_is_rejected(self) -> None:
        from calfkit.nodes.tool import ToolNodeDef, agent_tool

        def fn(x: int) -> int:
            return x

        with pytest.raises(ValueError, match="name"):
            agent_tool(fn, name="")
        with pytest.raises(ValueError, match="name"):
            agent_tool(name="")  # decorator-with-args form rejects eagerly, before decorating
        with pytest.raises(ValueError, match="name"):
            ToolNodeDef.create_tool_node(func=fn, subscribe_topics="t.in", publish_topic="t.out", name="")


from calfkit.providers.pydantic_ai.model_client import PydanticModelClient  # noqa: E402


class _FakeModel(PydanticModelClient):
    """Minimal pydantic-ai Model so a real Agent can be built without any
    network / API key; never invoked by these ctor tests."""

    @property
    def model_name(self) -> str:
        return "fake"

    @property
    def system(self) -> str:
        return "fake"

    async def request(self, *args: object, **kwargs: object) -> object:
        raise NotImplementedError


def make_agent(tools=None):
    from calfkit.nodes.agent import Agent

    return Agent(
        "test_agent",
        subscribe_topics="test_agent.input",
        model_client=_FakeModel(),
        tools=tools,
    )


def make_tool_node(name: str):
    from calfkit.nodes.tool import ToolNodeDef

    def _fn(x: int) -> int:
        return x

    _fn.__name__ = name
    return ToolNodeDef.create_tool_node(func=_fn, subscribe_topics=f"tool.{name}.input", publish_topic=f"tool.{name}.output")


class TestAgentToolNormalization:
    def test_no_tools_yields_empty_list(self) -> None:
        assert make_agent().tools == []

    def test_providers_are_flattened_into_bindings(self) -> None:
        agent = make_agent(tools=[make_tool_node("alpha"), make_tool_node("beta")])
        assert all(isinstance(b, ToolBinding) for b in agent.tools)
        assert [b.name for b in agent.tools] == ["alpha", "beta"]
        assert agent.tools[0].dispatch_topic == "tool.alpha.input"

    def test_raw_bindings_are_accepted_verbatim(self) -> None:
        binding = ToolBinding(tool_def=make_tool_def(), dispatch_topic="custom.topic")
        agent = make_agent(tools=[binding])
        assert agent.tools == [binding]

    def test_mixed_bindings_and_providers(self) -> None:
        binding = ToolBinding(tool_def=make_tool_def("manual"), dispatch_topic="manual.topic")
        agent = make_agent(tools=[binding, make_tool_node("gamma")])
        assert [b.name for b in agent.tools] == ["manual", "gamma"]

    def test_multi_binding_provider_is_fully_flattened(self) -> None:
        class Toolbox:
            def tool_bindings(self) -> list[ToolBinding]:
                return [
                    ToolBinding(tool_def=make_tool_def("search"), dispatch_topic="toolbox.input"),
                    ToolBinding(tool_def=make_tool_def("fetch"), dispatch_topic="toolbox.input"),
                ]

        agent = make_agent(tools=[Toolbox()])
        assert [b.name for b in agent.tools] == ["search", "fetch"]

    def test_non_provider_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="ToolBinding|ToolProvider"):
            make_agent(tools=["not a tool"])

    def test_add_tools_normalizes_providers_and_bindings(self) -> None:
        agent = make_agent()
        binding = ToolBinding(tool_def=make_tool_def("manual"), dispatch_topic="manual.topic")
        agent.add_tools(make_tool_node("delta"), binding)
        assert [b.name for b in agent.tools] == ["delta", "manual"]
        assert all(isinstance(b, ToolBinding) for b in agent.tools)


class TestExports:
    def test_importable_from_models_package(self) -> None:
        from calfkit.models import ToolBinding as FromModels
        from calfkit.models import ToolProvider as FromModelsProvider

        assert FromModels is ToolBinding
        assert FromModelsProvider is ToolProvider
