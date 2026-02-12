from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import ClassVar


@dataclass
class GateResult:
    skip: bool


class DecisionGate(ABC):
    kind: ClassVar[str]  # discriminator

    @abstractmethod
    def gate(self, model_response: str | None) -> GateResult: ...

    @abstractmethod
    def prompt(self) -> str: ...
