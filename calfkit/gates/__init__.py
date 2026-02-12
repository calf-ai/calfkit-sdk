"""Calf Decision Gate System.

Gates control agent participation in groupchat conversations. Each gate
evaluates a model's response and decides whether the agent should contribute
or skip its turn.

Example:
    from calfkit.gates import DecisionGate, GateResult

    class MyCustomGate(DecisionGate):
        kind: ClassVar[str] = "my_gate"

        def gate(self, model_response: str | None) -> GateResult:
            return GateResult(skip=model_response is None)

        def prompt(self) -> str:
            return "Should you respond?"
"""

from calfkit.gates.base import DecisionGate, GateResult
from calfkit.gates.registry import load_gate, register_gate

__all__ = [
    "DecisionGate",
    "GateResult",
    "load_gate",
    "register_gate",
]
