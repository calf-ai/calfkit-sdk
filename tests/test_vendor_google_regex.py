"""Regression tests for the vendored `_FILE_SEARCH_QUERY_PATTERN` regex.

The vendored ``pydantic_ai/models/google.py`` module requires ``google-genai``
at import time, which is not a dependency of this project. To still test the
exact regex that ships in the vendored source, the pattern literal is
extracted from the file with ``ast`` instead of importing the module.

Covers the ReDoS fix from upstream pydantic-ai (pydantic/pydantic-ai#5106):
the original pattern ``(?:\\.|(?!\1).)*?`` backtracks exponentially on inputs
with many escape sequences and no closing quote.
"""

import ast
import re
import time
from pathlib import Path

import pytest

import calfkit

_GOOGLE_PY = Path(calfkit.__file__).parent / "_vendor" / "pydantic_ai" / "models" / "google.py"


def _load_file_search_query_pattern() -> re.Pattern[str]:
    """Extract and compile `_FILE_SEARCH_QUERY_PATTERN` from the vendored source."""
    tree = ast.parse(_GOOGLE_PY.read_text())
    for node in tree.body:
        if (
            isinstance(node, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == "_FILE_SEARCH_QUERY_PATTERN" for t in node.targets)
            and isinstance(node.value, ast.Call)
            and isinstance(node.value.args[0], ast.Constant)
            and isinstance(node.value.args[0].value, str)
        ):
            return re.compile(node.value.args[0].value)
    raise AssertionError(f"_FILE_SEARCH_QUERY_PATTERN not found in {_GOOGLE_PY}")


@pytest.fixture(scope="module")
def pattern() -> re.Pattern[str]:
    return _load_file_search_query_pattern()


def test_adversarial_input_completes_quickly(pattern: re.Pattern[str]) -> None:
    """An unterminated query full of escapes must not trigger exponential backtracking."""
    adversarial = 'file_search.query(query="' + "\\a" * 100
    start = time.perf_counter()
    result = pattern.search(adversarial)
    elapsed = time.perf_counter() - start
    assert result is None
    assert elapsed < 1.0, f"regex took {elapsed:.2f}s on adversarial input; likely ReDoS"


@pytest.mark.parametrize(
    ("code", "expected_quote", "expected_raw_query"),
    [
        (
            'print(file_search.query(query="what is the capital of France?"))',
            '"',
            "what is the capital of France?",
        ),
        (
            "print(file_search.query(query='single quoted'))",
            "'",
            "single quoted",
        ),
        (
            'file_search.query(query="say \\"hello\\" twice")',
            '"',
            'say \\"hello\\" twice',
        ),
        (
            'file_search.query(query="path C:\\\\temp")',
            '"',
            "path C:\\\\temp",
        ),
        (
            "file_search.query(query='it\\'s fine')",
            "'",
            "it\\'s fine",
        ),
    ],
)
def test_query_extraction_semantics(pattern: re.Pattern[str], code: str, expected_quote: str, expected_raw_query: str) -> None:
    """The fixed pattern must capture the same quote and raw query as before."""
    match = pattern.search(code)
    assert match is not None
    assert match.group(1) == expected_quote
    assert match.group(2) == expected_raw_query


def test_no_match_without_closing_quote(pattern: re.Pattern[str]) -> None:
    match = pattern.search('file_search.query(query="unterminated')
    assert match is None
