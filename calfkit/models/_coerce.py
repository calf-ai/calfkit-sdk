from typing import Any, cast

import pydantic_core

from calfkit.models.payload import ContentPart, DataPart, FilePart, TextPart, ToolCallPart

_CONTENT_PART_TYPES = (TextPart, FilePart, DataPart, ToolCallPart)


def _coerce_to_parts(value: Any) -> list[ContentPart]:
    """Coerce a node's output ``value`` into the content-part vocabulary (spec §4.5).

    The single ``value → parts`` conversion, called at the publish chokepoint when a
    ``ReturnCall(value=…)`` is sent (PR-C adds a second call site, slot materialization):

    * ``None`` → ``[]`` (no output);
    * ``str`` → one ``TextPart``;
    * a bare ``ContentPart`` → ``[part]`` — the vocabulary's own values are fixed
      points of the coercion (``coerce(part) == coerce([part])``), so per-part
      ``metadata`` (e.g. the ``calf.retry`` marker) survives instead of being buried
      inside a ``DataPart`` (docs/issues/coerce-to-parts-drops-bare-content-part-metadata.md);
    * a ``list[ContentPart]`` passes through unchanged (the agent's preamble case —
      an empty list is vacuously such a list and yields ``[]``);
    * anything else JSON-serializable → one ``DataPart``, eagerly wire-checked via
      ``pydantic_core.to_json`` (the ``tool.py`` precedent): a non-serializable value
      raises here, at the chokepoint, rather than killing serialization mid-publish.
    """
    if value is None:
        return []
    if isinstance(value, str):
        return [TextPart(text=value)]
    if isinstance(value, _CONTENT_PART_TYPES):
        return [value]
    if isinstance(value, list) and all(isinstance(p, _CONTENT_PART_TYPES) for p in value):
        return cast("list[ContentPart]", value)
    part = DataPart(data=value)
    pydantic_core.to_json(part)
    return [part]
