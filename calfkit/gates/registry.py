from calfkit.gates.base import DecisionGate

_GATES: dict[str, DecisionGate] = {}

# TODO: eventually the registry should move off local,
# this requires every runtime to have the latest copy of the code


def register_gate(gate: DecisionGate) -> None:
    """Register a gate instance in the global registry.

    Raises:
        ValueError: If a gate with the same ``kind`` is already registered.
    """
    if gate.kind in _GATES:
        raise ValueError(
            f"A gate with kind {gate.kind!r} is already registered. Use a unique `kind` value."
        )
    _GATES[gate.kind] = gate


def load_gate(gate_kind: str) -> DecisionGate:
    """Look up a registered gate by its ``kind`` discriminator.

    Raises:
        KeyError: If no gate with the given ``kind`` is registered.
    """
    return _GATES[gate_kind]
