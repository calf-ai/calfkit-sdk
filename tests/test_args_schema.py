"""Unit tests for the caller-side JSON-schema arg-validator factory
(``calfkit/models/args_schema.py``).

The factory compiles a discovered tool's advertised ``parameters_json_schema`` into an
``ArgsValidator`` that raises ``pydantic.ValidationError`` on a real instance violation and is
otherwise TOTAL — every compile/validate-time failure degrades OPEN (dispatch unvalidated) with a
warning, never propagating past the contract error (spec D4).
"""

from __future__ import annotations

import logging

import pytest
from pydantic import ValidationError

from calfkit.models import args_schema
from calfkit.models.args_schema import schema_args_validator


@pytest.fixture(autouse=True)
def _clear_validator_cache():
    """The factory's compile cache is module-global and outlives each test, so warn-once and
    cache-identity assertions are order-dependent without a reset. Clear before AND after."""
    args_schema.cache_clear()
    yield
    args_schema.cache_clear()


# --------------------------------------------------------------------------- #
# Cycle 1 — translation shape, valid args, determinism                        #
# --------------------------------------------------------------------------- #


def test_multiple_violations_raise_one_validation_error_with_all_entries():
    schema = {
        "type": "object",
        "properties": {
            "city": {"type": "string"},
            "days": {"type": "integer"},
            "units": {"enum": ["metric", "imperial"]},
        },
        "required": ["city"],
        "additionalProperties": False,
    }
    validator = schema_args_validator(schema)

    with pytest.raises(ValidationError) as exc_info:
        validator({"days": "twenty", "units": "kelvin", "bogus": 1})

    errors = exc_info.value.errors(include_url=False, include_context=False)
    # Every violation surfaces at once (not first-only), each in the pydantic error shape.
    assert len(errors) == 4
    assert all(e["type"] == "json_schema_violation" for e in errors)
    assert all({"type", "loc", "msg", "input"} <= set(e) for e in errors)
    # The jsonschema message text rides through verbatim (brace-safe: the reason is a ctx value,
    # never a format template).
    assert any("'city' is a required property" in e["msg"] for e in errors)
    # For a whole-object keyword (additionalProperties), `input` is the FULL args subtree, not the
    # offending scalar — pin it, since that payload is what the retry prompt / step text echoes (D2).
    extra_key = next(e for e in errors if "Additional properties" in e["msg"])
    assert extra_key["loc"] == ()
    assert extra_key["input"] == {"days": "twenty", "units": "kelvin", "bogus": 1}


def test_bool_is_rejected_for_an_integer_field():
    # Python's ``bool`` is an ``int`` subclass, but jsonschema (correctly) rejects ``True``/``False``
    # for ``integer``. The strictness delta (spec D6.2) names both ``"3"`` and ``True``; this pins
    # the subtler bool case so a future engine/dialect change can't silently start accepting it.
    validator = schema_args_validator({"type": "object", "properties": {"n": {"type": "integer"}}})
    with pytest.raises(ValidationError):
        validator({"n": True})
    validator({"n": 3})  # a real integer still passes


def test_valid_args_do_not_raise():
    schema = {
        "type": "object",
        "properties": {"city": {"type": "string"}, "days": {"type": "integer", "default": 3}},
        "required": ["city"],
    }
    validator = schema_args_validator(schema)
    # Conforming args, including an omitted optional with a default, return without raising.
    validator({"city": "SF"})


def test_violation_order_is_deterministic_and_carries_int_array_indices():
    schema = {
        "type": "object",
        "properties": {"tags": {"type": "array", "items": {"type": "string"}}},
    }
    validator = schema_args_validator(schema)

    with pytest.raises(ValidationError) as first:
        validator({"tags": [1, 2, 3]})
    with pytest.raises(ValidationError) as second:
        validator({"tags": [1, 2, 3]})

    locs_first = [e["loc"] for e in first.value.errors(include_url=False)]
    locs_second = [e["loc"] for e in second.value.errors(include_url=False)]
    assert locs_first == locs_second  # stable across runs
    # Array-item paths put an int index in loc — the sort key must not choke on mixed str/int.
    assert ("tags", 0) in locs_first and ("tags", 1) in locs_first and ("tags", 2) in locs_first


# --------------------------------------------------------------------------- #
# Cycle 2 — content-keyed cache identity                                      #
# --------------------------------------------------------------------------- #


def test_equal_content_schemas_share_one_compiled_validator():
    # Distinct dict objects, shuffled key order — same content once canonicalized.
    schema_a = {"type": "object", "properties": {"x": {"type": "integer"}, "y": {"type": "string"}}}
    schema_b = {"properties": {"y": {"type": "string"}, "x": {"type": "integer"}}, "type": "object"}
    assert schema_args_validator(schema_a) is schema_args_validator(schema_b)


def test_different_content_schemas_get_distinct_validators():
    v1 = schema_args_validator({"type": "object", "properties": {"x": {"type": "integer"}}})
    v2 = schema_args_validator({"type": "object", "properties": {"x": {"type": "string"}}})
    assert v1 is not v2


# --------------------------------------------------------------------------- #
# Cycle 3 — totality (spec D4): the factory never raises but the contract error #
# --------------------------------------------------------------------------- #


def _warnings(caplog) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.levelno == logging.WARNING]


@pytest.mark.parametrize(
    "bad_schema",
    [
        {"type": "not-a-type"},  # check_schema rejects
        {"$schema": 3},  # crashes validator_for BEFORE check_schema
        None,  # json-serializable ("null") but validator_for(None) raises
        [1, 2, 3],  # non-dict schema
    ],
    ids=["bad-type", "bad-$schema", "none", "non-dict"],
)
def test_compile_time_failure_degrades_open_and_warns_once(bad_schema, caplog):
    with caplog.at_level(logging.WARNING, logger="calfkit.models.args_schema"):
        validator = schema_args_validator(bad_schema)
        # Degrades OPEN: the returned validator accepts anything (dispatch unvalidated).
        validator({"anything": [1, {"nested": True}]})
        validator({})
        # A second construction of the same schema hits the cache — no second warning.
        schema_args_validator(bad_schema)
    assert len(_warnings(caplog)) == 1


def test_validate_time_unresolvable_external_ref_swallows_per_call_and_warns_once(caplog):
    schema = {"type": "object", "properties": {"b": {"$ref": "https://example.invalid/s.json"}}}
    validator = schema_args_validator(schema)
    with caplog.at_level(logging.WARNING, logger="calfkit.models.args_schema"):
        validator({"b": "touches the unresolvable branch"})  # no raise — swallowed
        validator({"b": "again"})  # still no raise; no second warning
    assert len(_warnings(caplog)) == 1


def test_validate_time_internal_dangling_ref_is_lazy_and_keeps_enforcing_untouched_shapes(caplog):
    # `b`'s $ref dangles; `a` is a real constraint. A call that never touches `b` must still be
    # validated (and rejected) — the per-call-swallow (b) property, not permanent degrade.
    schema = {
        "type": "object",
        "properties": {"a": {"type": "string"}, "b": {"$ref": "#/$defs/nope"}},
    }
    validator = schema_args_validator(schema)
    with caplog.at_level(logging.WARNING, logger="calfkit.models.args_schema"):
        # Touches `b` → resolves the dangling ref → swallowed, no raise.
        validator({"b": "x"})
        # Does NOT touch `b` → the real `a` violation is still enforced.
        with pytest.raises(ValidationError):
            validator({"a": 123})
    assert len(_warnings(caplog)) == 1


def test_validate_time_cyclic_ref_recursionerror_is_swallowed(caplog):
    schema = {
        "type": "object",
        "properties": {"x": {"$ref": "#/$defs/a"}},
        "$defs": {"a": {"$ref": "#/$defs/b"}, "b": {"$ref": "#/$defs/a"}},
    }
    validator = schema_args_validator(schema)
    with caplog.at_level(logging.WARNING, logger="calfkit.models.args_schema"):
        validator({"x": 1})  # RecursionError from iter_errors → swallowed, no raise
    assert len(_warnings(caplog)) == 1


def test_validate_time_deep_args_on_valid_recursive_schema_swallow_then_shallow_still_validated(caplog):
    # A LEGITIMATELY recursive schema (a tree). Deep args blow Python's recursion limit at
    # validate time (instance-driven, not schema-driven), but a following shallow call must still
    # be enforced — this is exactly why per-call swallow beats permanent degrade [R2-M1].
    schema = {
        "type": "object",
        "properties": {"name": {"type": "string"}, "children": {"type": "array", "items": {"$ref": "#"}}},
        "required": ["name"],
    }
    validator = schema_args_validator(schema)
    node: dict = {"name": "leaf", "children": []}
    for _ in range(2000):  # 2000 levels × several frames each ≫ the default 1000 recursion limit
        node = {"name": "n", "children": [node]}
    with caplog.at_level(logging.WARNING, logger="calfkit.models.args_schema"):
        validator(node)  # RecursionError → swallowed, no raise
    assert len(_warnings(caplog)) == 1
    # The working tool is NOT permanently disabled: a shallow call is still validated + rejected.
    with pytest.raises(ValidationError):
        validator({"children": []})  # missing required "name"


def test_within_call_all_or_none_no_partial_report(caplog):
    # One call that both violates `required` AND touches an unresolvable branch → NO partial
    # report; the whole call is swallowed (the eager sorted() drain discards yielded partials).
    schema = {"type": "object", "properties": {"b": {"$ref": "#/$defs/nope"}}, "required": ["a"]}
    validator = schema_args_validator(schema)
    with caplog.at_level(logging.WARNING, logger="calfkit.models.args_schema"):
        validator({"b": "x"})  # touches ref AND misses required "a" → swallowed, no raise
        # Contrast: not touching the ref → the required violation IS reported.
        with pytest.raises(ValidationError):
            validator({})
    assert len(_warnings(caplog)) == 1


def test_unserializable_schema_degrades_open_and_warns_every_call(caplog):
    schema = {"type": "object", "x": {1, 2}}  # a set is not JSON-serializable → cannot be cached
    with caplog.at_level(logging.WARNING, logger="calfkit.models.args_schema"):
        v1 = schema_args_validator(schema)
        v1({"anything": True})  # degrades open
        schema_args_validator(schema)  # uncached → warns AGAIN
    assert len(_warnings(caplog)) == 2


def test_pathologically_deep_schema_degrades_open_not_raises(caplog):
    # A deeply-nested (acyclic, valid) advertised schema makes the cache-key `json.dumps` blow the
    # recursion limit — a RecursionError, which is NOT a TypeError/ValueError. Totality (D4) means
    # this degrades OPEN (dispatch unvalidated + warn), never escaping the factory to crash the run.
    schema: dict = {"type": "integer"}
    for _ in range(6000):
        schema = {"type": "object", "properties": {"x": schema}}
    with caplog.at_level(logging.WARNING, logger="calfkit.models.args_schema"):
        validator = schema_args_validator(schema)  # must NOT raise
        validator({"x": {"x": 1}})  # pass-through accepts anything
    assert len(_warnings(caplog)) >= 1


def test_dialect_defaults_to_2020_12_when_no_schema_declared():
    # `prefixItems` (tuple validation) is a 2020-12 keyword. With no `$schema`, the factory must
    # default to Draft 2020-12 and ENFORCE it.
    schema = {"type": "object", "properties": {"pair": {"type": "array", "prefixItems": [{"type": "integer"}]}}}
    validator = schema_args_validator(schema)
    with pytest.raises(ValidationError):
        validator({"pair": ["should-be-int"]})


def test_dialect_honors_a_declared_draft07_schema():
    # The SAME body under a declared draft-07 `$schema`: draft-07 has no `prefixItems`, so it is an
    # unknown (ignored) keyword — the args that failed under 2020-12 now pass. Proves `$schema` is honored.
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "properties": {"pair": {"type": "array", "prefixItems": [{"type": "integer"}]}},
    }
    validator = schema_args_validator(schema)
    validator({"pair": ["should-be-int"]})  # no raise — draft-07 ignores prefixItems


def test_permissive_floors_accept_anything():
    for schema in ({"type": "object", "properties": {}}, {}):
        validator = schema_args_validator(schema)
        validator({"whatever": [1, 2, 3], "nested": {"a": True}})


def test_format_is_annotation_only_not_enforced():
    schema = {"type": "object", "properties": {"when": {"type": "string", "format": "date-time"}}}
    validator = schema_args_validator(schema)
    validator({"when": "definitely not a date"})  # format is advisory — no raise
