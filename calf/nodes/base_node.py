from abc import ABC, abstractmethod
from typing import Any, Sequence

from pydantic_ai import ToolDefinition

from calf.models.event_envelope import EventEnvelope


class BaseNode(ABC):
    @abstractmethod
    async def on_enter(self, event_envelope: EventEnvelope, *args, **kwargs) -> EventEnvelope: ...

    @classmethod
    @abstractmethod
    def get_on_enter_topic(cls) -> str: ...

    @classmethod
    @abstractmethod
    def get_post_to_topic(cls) -> str: ...
