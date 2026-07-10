"""Roundtrip contract: a tool's ADVERTISED schema (as the vendored generator emits it) must be
consumable by the caller-side validator factory.

The factory unit tests (``test_args_schema.py``) use hand-written schemas; this suite closes the
loop against the *real* ``Tool(func).tool_def.parameters_json_schema`` the framework advertises for
a discovered tool — nested models (``$defs``/``$ref``), defaults, ``Optional`` (``anyOf`` + null),
``**kwargs`` (``additionalProperties: true``), and no-arg tools — plus a loose MCP-style
``inputSchema``. It fails loudly if the generator ever emits a shape the factory cannot validate.
"""

from __future__ import annotations

import pytest
from pydantic import BaseModel, ValidationError

from calfkit.models import ToolContext
from calfkit.models.args_schema import schema_args_validator
from calfkit.nodes import ToolNodeDef


class Point(BaseModel):
    lat: float
    lon: float


def _schema_for(func) -> dict:
    """The advertised ``parameters_json_schema`` for ``func`` — the exact dict a discovered
    binding carries and the agent hands the factory."""
    node = ToolNodeDef.create_tool_node(func=func, subscribe_topics=f"tool.{func.__name__}.input", publish_topic=f"tool.{func.__name__}.output")
    return node.tool_schema.parameters_json_schema


def _locs(exc: pytest.ExceptionInfo[ValidationError]) -> list[tuple]:
    return [e["loc"] for e in exc.value.errors(include_url=False)]


def test_simple_with_defaults_roundtrips():
    def tool(ctx: ToolContext, a: int, b: str = "hi") -> str:
        return "x"

    validate = schema_args_validator(_schema_for(tool))
    validate({"a": 1})  # b defaulted — valid
    validate({"a": 1, "b": "x"})
    with pytest.raises(ValidationError):
        validate({})  # missing required `a`
    with pytest.raises(ValidationError) as exc:
        validate({"a": "not-an-int"})
    assert ("a",) in _locs(exc)
    with pytest.raises(ValidationError):
        validate({"a": 1, "surprise": True})  # additionalProperties: false


def test_nested_model_roundtrips_through_ref():
    def tool(ctx: ToolContext, p: Point, tag: str) -> str:
        return "x"

    validate = schema_args_validator(_schema_for(tool))
    validate({"p": {"lat": 1.0, "lon": 2.0}, "tag": "t"})
    with pytest.raises(ValidationError) as exc:
        validate({"p": {"lat": "nope", "lon": 2.0}, "tag": "t"})
    # The violation is located THROUGH the $ref, at the nested field.
    assert ("p", "lat") in _locs(exc)
    with pytest.raises(ValidationError):
        validate({"tag": "t"})  # missing required `p`


def test_optional_model_roundtrips_through_anyof_null():
    def tool(ctx: ToolContext, loc: Point | None = None) -> str:
        return "x"

    validate = schema_args_validator(_schema_for(tool))
    validate({})  # loc optional, defaults null
    validate({"loc": None})  # anyOf null branch
    validate({"loc": {"lat": 1.0, "lon": 2.0}})  # anyOf Point branch
    with pytest.raises(ValidationError):
        validate({"loc": {"lat": "nope", "lon": 2.0}})  # neither branch matches


def test_var_kwargs_allows_additional_properties():
    def tool(ctx: ToolContext, a: int, **rest: object) -> str:
        return "x"

    validate = schema_args_validator(_schema_for(tool))
    validate({"a": 1, "anything": "goes", "more": [1, 2, 3]})  # additionalProperties: true
    with pytest.raises(ValidationError):
        validate({"a": "not-an-int"})  # the declared field is still enforced


def test_no_arg_tool_roundtrips():
    def tool(ctx: ToolContext) -> str:
        return "x"

    validate = schema_args_validator(_schema_for(tool))
    validate({})
    with pytest.raises(ValidationError):
        validate({"unexpected": 1})  # additionalProperties: false on the empty object


def test_loose_mcp_style_input_schema_is_permissive_but_typed():
    # An MCP server's inputSchema rides verbatim: no `required`, no `additionalProperties`. The
    # subset check enforces declared types where present and admits everything else — this
    # permissiveness is correct, not a gap (the server stays authoritative on receipt).
    loose = {"type": "object", "properties": {"q": {"type": "string"}}}
    validate = schema_args_validator(loose)
    validate({})  # nothing required
    validate({"q": "hi", "extra": 1})  # additionalProperties defaults open
    with pytest.raises(ValidationError):
        validate({"q": 123})  # the declared type IS enforced
