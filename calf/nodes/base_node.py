from abc import ABC, abstractmethod
from typing import Any


class BaseNode(ABC):
    @abstractmethod
    async def on_enter(self) -> Any: ...

    @abstractmethod
    def get_on_enter_topic(self) -> str: ...

    @abstractmethod
    def get_post_to_topic(self) -> str: ...
