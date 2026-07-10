"""The single-construction-plane seal (caller-side step-emission spec §3.1, I7) — CI infrastructure.

An AST sweep over the tree asserts no wire ``*Step``/``StepMessage`` type is CONSTRUCTED outside
``calfkit/models/step.py`` (the definitions), ``calfkit/nodes/_steps.py`` (the ledger — the sole
mint), and ``tests/``. Chosen over an import-linter contract, which is module-granular and cannot
express this (the client plane legitimately imports the types for decode — ``model_validate`` reads
are fine; a positional/keyword CONSTRUCTOR call is what the seal forbids).
"""

from __future__ import annotations

import ast
from pathlib import Path

_WIRE_STEP_TYPES = frozenset({"AgentMessageStep", "ToolCallStep", "ToolResultStep", "HandoffStep", "AgentThinkingStep", "StepMessage"})
_ALLOWED = {Path("calfkit/models/step.py"), Path("calfkit/nodes/_steps.py")}
_ROOT = Path(__file__).resolve().parent.parent


def _constructions(source: str) -> list[str]:
    """Names of wire step types CONSTRUCTED (called) in ``source`` — ``Name(...)`` and
    ``module.Name(...)`` call shapes; imports/annotations/isinstance reads don't count."""
    found: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Call):
            func = node.func
            name = func.id if isinstance(func, ast.Name) else func.attr if isinstance(func, ast.Attribute) else None
            if name in _WIRE_STEP_TYPES:
                found.append(name)
    return found


def test_no_wire_step_construction_outside_the_two_module_plane() -> None:
    violations: list[str] = []
    for path in sorted((_ROOT / "calfkit").rglob("*.py")):
        rel = path.relative_to(_ROOT)
        if "_vendor" in rel.parts or rel in _ALLOWED:
            continue
        for name in _constructions(path.read_text()):
            violations.append(f"{rel}: constructs {name}")
    assert not violations, "wire step construction outside models/step.py + nodes/_steps.py (I7):\n" + "\n".join(violations)


def test_the_checker_detects_a_violation() -> None:
    # The seal's own tripwire: prove the AST sweep actually catches both construction shapes
    # (a silent checker would make the test above meaningless).
    direct = "from calfkit.models.step import ToolResultStep\nx = ToolResultStep(tool_call_id='c', name='n', parts=[], outcome='success')\n"
    attribute = "from calfkit.models import step\nx = step.StepMessage(correlation_id='c', emitter='e', depth=1, frame_id='f', events=[])\n"
    read_only = "from calfkit.models.step import StepMessage\nx = StepMessage.model_validate_json(raw)\ny = isinstance(z, StepMessage)\n"
    assert _constructions(direct) == ["ToolResultStep"]
    assert _constructions(attribute) == ["StepMessage"]
    assert _constructions(read_only) == []  # decode reads and isinstance checks are NOT constructions
