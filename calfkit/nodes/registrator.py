from abc import ABC, abstractmethod
from typing import Any

from calfkit.broker.broker import BrokerClient


class Registrator(ABC):
    @abstractmethod
    def register_on(self, broker: BrokerClient, *args: Any, **kwargs: Any) -> None:
        """Function to fluently register a node's handlers onto a broker to serve traffic"""
        pass
