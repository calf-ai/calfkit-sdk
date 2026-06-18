"""The ``calf.retry`` metadata marker (fault-rail spec §4.5, option 1).

A reply that is a model-visible *retry* (a tool's ``ModelRetry``) travels the wire as an ordinary
``TextPart`` carrying its RAW message text plus a ``calf.retry`` marker in ``metadata`` — NOT a new
part ``kind``, so every non-agent consumer decodes it as plain text, while the agent honors the
marker by materializing a ``RetryPromptPart`` (Anthropic ``is_error=True`` fidelity). The agent
hydrates the richer ``RetryPromptPart`` (tool_name, suffix) on its side; the wire stays raw.
"""

from calfkit.models.payload import RETRY_MARKER, DataPart, TextPart, is_retry, retry_text_part


def test_retry_text_part_carries_raw_text_and_marker() -> None:
    part = retry_text_part("rate limit exceeded")
    assert isinstance(part, TextPart)
    assert part.text == "rate limit exceeded"  # raw content, no rendering at origin (option 1)
    assert part.metadata == {RETRY_MARKER: True}


def test_is_retry_true_for_a_marked_part() -> None:
    assert is_retry([retry_text_part("retry me")]) is True


def test_is_retry_false_for_a_normal_return() -> None:
    assert is_retry([TextPart(text="normal output")]) is False
    assert is_retry([DataPart(data={"k": "v"})]) is False


def test_is_retry_false_for_empty_or_none() -> None:
    assert is_retry([]) is False
    assert is_retry(None) is False


def test_is_retry_detects_the_marker_among_multiple_parts() -> None:
    assert is_retry([TextPart(text="preamble"), retry_text_part("retry")]) is True
