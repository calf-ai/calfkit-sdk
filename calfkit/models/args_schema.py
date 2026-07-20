"""Caller-side JSON-schema arg-validator factory (issue: discovered tools get no caller-side arg
validation).

A discovered tool (capability-plane ``Tools``/``Toolboxes`` discovery, wire-deserialized per-run
overrides, hand-rolled bindings) carries no process-local validator — only its advertised
``parameters_json_schema``. :func:`schema_args_validator` compiles that schema into an
:data:`~calfkit.models.tool_dispatch.ArgsValidator` the agent consults at the dispatch chokepoint,
so the LLM's args are checked against the advertised contract before crossing the wire.

Three laws (spec D2/D3/D4):

- **Contract by translation (D2).** The returned validator raises the same
  ``pydantic.ValidationError`` every ``ArgsValidator`` promises — synthesized from jsonschema's
  violations via ``pydantic_core.ValidationError.from_exception_data`` — so the agent's existing
  ``except ValidationError`` arm needs no new species.
- **Non-coercing subset check (D3).** ``jsonschema`` checks conformance to the advertised schema
  *as written* (no coercion, no ``format`` enforcement); the callee's own validation stays
  authoritative. The dialect follows a declared ``$schema`` (default Draft 2020-12).
- **Totality (D4).** The only exception this module ever raises is that contract error, for a real
  instance violation. Every other failure degrades OPEN — dispatch unvalidated, today's behavior —
  with a warning:
    * a schema that cannot compile (malformed ``$schema`` non-string, bad keyword, non-dict) or
      cannot be serialized into a cache key (unserializable value, or a pathologically deep schema
      that blows ``json.dumps``'s recursion limit) yields a pass-through validator (warn-once per
      schema; the uncached key-construction failures warn per call);
    * a schema that compiles but fails *while validating an instance* (unresolvable ``$ref``,
      ``RecursionError`` from a cyclic schema or deep args) swallows THAT call (warn-once per
      schema) and keeps enforcing every other args shape — per-call, never a permanent disable,
      because such failures can be instance-driven (spec §8.3 [R2-M1]).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from functools import lru_cache
from typing import Any

from jsonschema.exceptions import ValidationError as JsonSchemaValidationError
from jsonschema.validators import validator_for
from pydantic_core import InitErrorDetails, PydanticCustomError
from pydantic_core import ValidationError as CoreValidationError

from calfkit.models.tool_dispatch import ArgsValidator

logger = logging.getLogger(__name__)

# Bounded: per-run override schemas vary over process lifetime, so an unbounded cache would leak;
# the bound is a runaway backstop, not a working set (cluster tool count is small and repeats).
_CACHE_SIZE = 1024
# Truncate the schema echoed in a degrade warning — an advertised schema is unbounded wire data.
_MAX_LOGGED_SCHEMA = 500


def _passthrough(args: dict[str, Any]) -> None:  # noqa: ARG001
    """The degrade-open validator: accepts any args (dispatch unvalidated)."""


def _truncate(text: str) -> str:
    return text if len(text) <= _MAX_LOGGED_SCHEMA else text[:_MAX_LOGGED_SCHEMA] + "…"


def _sort_key(error: JsonSchemaValidationError) -> tuple[list[str], str, str]:
    """Deterministic ordering for a violation set. Path elements are stringified so a mixed
    ``str`` (object key) / ``int`` (array index) path never raises on comparison; the failing
    keyword and message break ties for full stability."""
    return ([str(part) for part in error.absolute_path], str(error.validator), str(error.message))


@lru_cache(maxsize=_CACHE_SIZE)
def _compiled(schema_key: str) -> ArgsValidator:
    """Compile a canonical schema key into the validator closure — cached so the per-turn rebuilt
    bindings for one tool share a single compiled validator. Keyed by canonical JSON content
    (``lru_cache`` can only key the string), loaded back to the schema dict here.

    Warn-once-per-schema falls out of the cache: the compile-time warning fires exactly once (the
    cached entry is reused thereafter), and the validate-time warning is deduped by the closure's
    ``warned`` cell.
    """
    try:
        schema = json.loads(schema_key)
        validator_cls = validator_for(schema)
        validator_cls.check_schema(schema)
        validator = validator_cls(schema)
    except Exception:
        # Compile-time failure (bad $schema, bad keyword, non-dict, …): degrade open. exc_info is
        # kept — a compile exception is shallow and identifies the malformed keyword.
        logger.warning("tool arg schema does not compile; dispatching unvalidated: %s", _truncate(schema_key), exc_info=True)
        return _passthrough

    warned = False  # validate-time per-call swallow, deduped to warn-once per schema

    def _validate(args: dict[str, Any]) -> None:
        nonlocal warned
        try:
            # Eager drain (via sorted): a generator that yields real violations and THEN raises
            # (e.g. required-then-unresolvable-$ref) must discard the partials — enforcement is
            # all-or-none per call, not a nondeterministic partial report.
            errors = sorted(validator.iter_errors(args), key=_sort_key)
        except Exception:
            # An instance-driven failure (unresolvable $ref reached by this instance, RecursionError
            # from a cyclic schema or deep args): swallow THIS call, keep enforcing others. exc_info
            # is suppressed — a RecursionError traceback is ~2000 frames.
            if not warned:
                logger.warning("tool arg schema failed to validate an instance; dispatching this call unvalidated: %s", _truncate(schema_key))
                warned = True
            return
        if not errors:
            return
        # The contract raise sits OUTSIDE the swallow catch: it is built from already-materialized
        # violation data and must never be mistaken for an "unenforceable" degrade.
        line_errors: list[InitErrorDetails] = [
            {
                "type": PydanticCustomError("json_schema_violation", "{reason}", {"reason": error.message}),
                "loc": tuple(error.absolute_path),
                "input": error.instance,
            }
            for error in errors
        ]
        raise CoreValidationError.from_exception_data("tool-args", line_errors)

    return _validate


def schema_args_validator(schema: Mapping[str, Any]) -> ArgsValidator:
    """Compile ``schema`` into an :data:`ArgsValidator` that raises ``pydantic.ValidationError`` on
    a real instance violation and is otherwise total (spec D4). Content-keyed and cached:
    equal-content schemas share one validator."""
    try:
        schema_key = json.dumps(schema, sort_keys=True, separators=(",", ":"))
    except Exception:
        # The cache key could not be built. Two known causes, both degrade OPEN (dispatch
        # unvalidated), uncached — so this warns on every dispatch of that tool: a schema value that
        # is not JSON-serializable (hand-rolled edge; wire schemas are JSON-native by provenance),
        # or a pathologically deep schema whose serialization blows the recursion limit. The message
        # carries only the top-level keys — ``repr`` of a deep schema would itself recurse and raise.
        hint = list(schema)[:10] if isinstance(schema, Mapping) else type(schema).__name__
        logger.warning("tool arg schema cannot be serialized as a cache key; dispatching unvalidated (top-level keys=%r)", hint)
        return _passthrough
    return _compiled(schema_key)


def cache_clear() -> None:
    """Reset the compile cache (test isolation)."""
    _compiled.cache_clear()
