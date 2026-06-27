from collections.abc import Callable
from typing import Annotated, Any, Literal

import pydantic_core
from pydantic import BaseModel, Discriminator, Field


class TextPart(BaseModel):
    kind: Literal["text"] = "text"
    text: str
    metadata: dict[str, Any] | None = None


class FilePart(BaseModel):
    kind: Literal["file"] = "file"
    media_type: str
    uri: str | None = None
    data: str | None = None
    metadata: dict[str, Any] | None = None


class DataPart(BaseModel):
    kind: Literal["data"] = "data"
    data: dict[str, Any] | list[Any] | Any
    schema_: dict[str, Any] | None = Field(default=None, alias="schema")
    metadata: dict[str, Any] | None = None


class ToolCallPart(BaseModel):
    kind: Literal["tool"] = "tool"
    tool_call_id: str
    kwargs: dict[str, Any]
    tool_name: str
    metadata: dict[str, Any] | None = None


ContentPart = Annotated[TextPart | FilePart | DataPart | ToolCallPart, Discriminator("kind")]


def render_parts_as_text(
    parts: list[ContentPart] | None,
    *,
    render_other: Callable[[ContentPart], str | None],
    empty: str,
) -> str:
    """Render reply parts into one newline-joined string — the canonical "parts → text" routine.

    A ``TextPart`` contributes its ``text`` verbatim; a ``DataPart`` its ``data`` as a JSON string
    (``pydantic_core.to_json`` — the framework's own serializer, robust across datetime/UUID/Decimal/
    nested models and consistent with the wire); any other part is passed to ``render_other``, which
    returns its string form or ``None`` to drop it. An empty/absent ``parts`` returns ``empty``.

    Shared by the client/consumer ``output_type=str`` projection (spec §2.2 — File/ToolCall skipped,
    ``empty=""``) and the agent's ``message_agent`` peer-reply fold (agent spec §5.2 — File rendered as
    a placeholder, ``empty="(no content)"``), so the join rule lives in exactly one place."""
    if not parts:
        return empty
    rendered: list[str] = []
    for part in parts:
        if isinstance(part, TextPart):
            rendered.append(part.text)
        elif isinstance(part, DataPart):
            rendered.append(pydantic_core.to_json(part.data).decode())
        else:
            piece = render_other(part)
            if piece is not None:
                rendered.append(piece)
    return "\n".join(rendered)


# ── the calf.retry marker (fault-rail spec §4.5) ─────────────────────────────────────
# A model-visible retry (a tool's ModelRetry) rides the wire as an ordinary TextPart carrying its
# RAW message + this marker in ``metadata`` — deliberately NOT a new part ``kind`` (which would
# force every ContentPart consumer to widen its union or fail to decode), so non-agent consumers
# read it as plain text while the agent honors the marker (materializing a RetryPromptPart for
# Anthropic is_error fidelity). The agent hydrates the richer retry on its side; the wire stays raw.
RETRY_MARKER = "calf.retry"


def retry_text_part(message: str) -> TextPart:
    """A ``TextPart`` carrying ``message`` verbatim plus the ``calf.retry`` marker (§4.5, option 1).

    The single producer of the marker, so the key lives in one place. The text is the raw retry
    message (no fix-and-retry suffix rendered at origin — the agent's ``RetryPromptPart`` adds that
    once via the provider, so rendering it here would double it)."""
    return TextPart(text=message, metadata={RETRY_MARKER: True})


def is_retry(parts: list[ContentPart] | None) -> bool:
    """True iff any part carries the ``calf.retry`` marker (§4.5) — the agent's discriminator
    between a normal return (→ ``ToolReturn``) and a retry (→ ``RetryPromptPart``). Reads only the
    open ``metadata`` slot, so it is total over the part vocabulary and tolerant of ``None``/[]."""
    return any((part.metadata or {}).get(RETRY_MARKER) for part in (parts or []))
