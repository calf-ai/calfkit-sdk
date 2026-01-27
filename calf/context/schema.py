from pydantic_ai import ModelMessage

from calf.types import CompactBaseModel


class EventContext(CompactBaseModel):
    text: str | None = None
    latest_message: ModelMessage
    trace_id: str | None = None
